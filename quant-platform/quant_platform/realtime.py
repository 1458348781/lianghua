from __future__ import annotations

import re
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dt_time
from typing import Any

from .data_sources import normalize_symbol
from .ml_signal import score_divergence_signal
from .storage import MarketDatabase, board_label, filter_by_board
from .strategy import create_strategy


SINA_QUOTE_RE = re.compile(r'var hq_str_(?P<code>[a-z0-9]+)="(?P<body>[^"]*)";')


def symbol_to_sina(symbol: str) -> str:
    code, exchange = normalize_symbol(symbol).split(".")
    return f"{exchange.lower()}{code}"


def sina_to_symbol(code: str) -> str:
    code = code.lower()
    if code.startswith("sh"):
        return f"{code[2:]}.SH"
    if code.startswith("sz"):
        return f"{code[2:]}.SZ"
    if code.startswith("bj"):
        return f"{code[2:]}.BJ"
    return normalize_symbol(code[-6:])


def fetch_sina_realtime(
    symbols: list[str],
    chunk_size: int = 220,
    timeout: int = 12,
    workers: int = 16,
) -> dict[str, dict[str, Any]]:
    normalized = [normalize_symbol(symbol) for symbol in symbols]
    chunks = [normalized[index : index + chunk_size] for index in range(0, len(normalized), chunk_size)]
    if not chunks:
        return {}

    quotes: dict[str, dict[str, Any]] = {}
    max_workers = max(1, min(workers, len(chunks)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for chunk_quotes in executor.map(lambda chunk: _fetch_sina_chunk(chunk, timeout), chunks):
            quotes.update(chunk_quotes)
    return quotes


def _fetch_sina_chunk(chunk: list[str], timeout: int) -> dict[str, dict[str, Any]]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    code_list = ",".join(symbol_to_sina(symbol) for symbol in chunk)
    url = f"https://hq.sinajs.cn/list={urllib.parse.quote(code_list, safe=',')}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 quant-platform/0.1",
            "Referer": "https://finance.sina.com.cn/",
        },
    )
    with opener.open(req, timeout=timeout) as resp:
        text = resp.read().decode("gbk", errors="ignore")
    return parse_sina_quotes(text)


def parse_sina_quotes(text: str) -> dict[str, dict[str, Any]]:
    quotes: dict[str, dict[str, Any]] = {}
    for match in SINA_QUOTE_RE.finditer(text):
        body = match.group("body")
        if not body:
            continue
        parts = body.split(",")
        if len(parts) < 32:
            continue
        symbol = sina_to_symbol(match.group("code"))
        quote = {
            "symbol": symbol,
            "name": parts[0],
            "open": _to_float(parts[1]),
            "pre_close": _to_float(parts[2]),
            "price": _to_float(parts[3]),
            "high": _to_float(parts[4]),
            "low": _to_float(parts[5]),
            "volume": _to_float(parts[8]),
            "amount": _to_float(parts[9]),
            "quote_date": parts[30],
            "quote_time": parts[31],
            "source": "sina",
        }
        if quote["price"] <= 0 or quote["open"] <= 0 or quote["pre_close"] <= 0:
            continue
        quote["pct_chg"] = (quote["price"] / quote["pre_close"] - 1) * 100
        quotes[symbol] = quote
    return quotes


def fetch_sina_minutes(symbol: str, period: str = "1") -> list[dict[str, Any]]:
    import akshare as ak  # type: ignore

    normalized = normalize_symbol(symbol)
    frame = ak.stock_zh_a_minute(symbol=symbol_to_sina(normalized), period=str(period), adjust="")
    if frame is None or frame.empty:
        return []
    rows: list[dict[str, Any]] = []
    for row in frame.to_dict("records"):
        minute_time = str(row.get("day") or "")
        if not minute_time:
            continue
        rows.append(
            {
                "symbol": normalized,
                "trade_time": minute_time,
                "trade_date": minute_time[:10],
                "period": str(period),
                "open": float(row.get("open") or 0),
                "high": float(row.get("high") or 0),
                "low": float(row.get("low") or 0),
                "close": float(row.get("close") or 0),
                "volume": float(row.get("volume") or 0),
                "amount": float(row.get("amount") or 0),
                "source": "akshare_sina_minute",
            }
        )
    return rows


def scan_realtime_divergence(
    db: MarketDatabase,
    params: dict[str, Any] | None = None,
    limit: int = 300,
    board: str | None = "all",
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    board = board or "all"
    if symbols is None:
        universe_symbols = [item["symbol"] for item in db.list_realtime_symbols(board, 34)]
        universe_label = board_label(board)
    else:
        seen: set[str] = set()
        universe_symbols = []
        for symbol in symbols:
            normalized = normalize_symbol(symbol)
            if normalized not in seen:
                seen.add(normalized)
                universe_symbols.append(normalized)
        universe_label = f"{board_label(board)} candidate pool"
    if not universe_symbols:
        return _empty_result(board, "本地历史数据不足，至少需要 34 个交易日数据。", started)

    quotes = fetch_sina_realtime(universe_symbols)
    if not quotes:
        return _empty_result(board, "实时行情源没有返回有效快照。", started)

    quote_date = max((quote.get("quote_date") or "") for quote in quotes.values())
    active_quotes = {symbol: quote for symbol, quote in quotes.items() if quote.get("quote_date") == quote_date}
    quote_time = max((quote.get("quote_time") or "") for quote in active_quotes.values()) if active_quotes else ""
    quote_datetime = f"{quote_date} {quote_time}".strip()

    profiles = db.symbol_profiles(list(active_quotes))
    tradable_symbols = [
        symbol
        for symbol, quote in active_quotes.items()
        if profiles.get(symbol, {}).get("is_st", 0) in (0, "0", None)
        and "ST" not in (quote.get("name") or "").upper()
        and "退" not in (quote.get("name") or "")
    ]
    history = db.query_recent_before(tradable_symbols, quote_date, 40)
    strategy = create_strategy("divergence_tactic", params or {})
    signals: list[dict[str, Any]] = []

    for symbol in tradable_symbols:
        rows = history.get(symbol, [])
        if len(rows) < 34:
            continue
        quote = active_quotes[symbol]
        t = rows[-2]
        t1 = rows[-1]
        t2 = {
            "symbol": symbol,
            "trade_date": quote_date,
            "open": quote["open"],
            "high": quote["high"],
            "low": quote["low"],
            "close": quote["price"],
            "pre_close": quote["pre_close"],
            "volume": quote["volume"],
            "amount": quote["amount"],
            "turnover": 0,
            "pct_chg": quote["pct_chg"],
            "is_st": 0,
            "source": "sina_realtime",
        }
        full_history = rows + [t2]
        if strategy._matches(symbol, t, t1, t2, full_history[-35:]):  # type: ignore[attr-defined]
            pre_close = float(t1.get("pre_close") or t["close"])
            day2_low_pct_chg = float(t1["low"]) / pre_close * 100 - 100 if pre_close else 0
            day2_high_pct_chg = float(t1["high"]) / pre_close * 100 - 100 if pre_close else 0
            today_open_gap_pct_chg = float(quote["open"]) / float(quote["pre_close"]) * 100 - 100
            today_high_from_open_pct_chg = float(quote["high"]) / float(quote["open"]) * 100 - 100
            signal = {
                "symbol": symbol,
                "name": profiles.get(symbol, {}).get("name") or quote.get("name", ""),
                "quote_date": quote_date,
                "quote_time": quote.get("quote_time", ""),
                "price": round(float(quote["price"]), 3),
                "pct_chg": round(float(quote["pct_chg"]), 4),
                "open": round(float(quote["open"]), 3),
                "high": round(float(quote["high"]), 3),
                "today_open_gap_pct_chg": round(today_open_gap_pct_chg, 4),
                "today_high_from_open_pct_chg": round(today_high_from_open_pct_chg, 4),
                "day1_date": t["trade_date"],
                "day1_pct_chg": round(strategy._pct_chg(t), 4),  # type: ignore[attr-defined]
                "day2_date": t1["trade_date"],
                "day2_pct_chg": round(strategy._pct_chg(t1), 4),  # type: ignore[attr-defined]
                "day2_low_pct_chg": round(day2_low_pct_chg, 4),
                "day2_high_pct_chg": round(day2_high_pct_chg, 4),
                "reason": "最近交易日实时快照符合分歧战法介入条件",
            }
            signal.update(score_divergence_signal(symbol, full_history, len(full_history) - 1))
            signals.append(signal)
            if len(signals) >= limit:
                break

    return {
        "source": "sina",
        "universe": board,
        "universe_label": universe_label,
        "quote_datetime": quote_datetime,
        "candidate_symbols": len(universe_symbols),
        "quoted_symbols": len(active_quotes),
        "scanned_symbols": len(tradable_symbols),
        "signals": signals,
        "message": "",
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def scan_realtime_signal_engine(
    db: MarketDatabase,
    params: dict[str, Any] | None = None,
    limit: int = 500,
    board: str | None = "all",
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    board = board or "all"
    if symbols is None:
        universe_symbols = [item["symbol"] for item in db.list_realtime_symbols(board, 34)]
        universe_label = board_label(board)
    else:
        seen: set[str] = set()
        universe_symbols = []
        for symbol in symbols:
            normalized = normalize_symbol(symbol)
            if normalized not in seen:
                seen.add(normalized)
                universe_symbols.append(normalized)
        universe_label = f"{board_label(board)} candidate pool"
    if not universe_symbols:
        return _empty_engine_result(board, "本地历史数据不足，至少需要 34 个交易日数据。", started)

    quotes = fetch_sina_realtime(universe_symbols)
    if not quotes:
        return _empty_engine_result(board, "实时行情源没有返回有效快照。", started)

    quote_date = max((quote.get("quote_date") or "") for quote in quotes.values())
    active_quotes = {symbol: quote for symbol, quote in quotes.items() if quote.get("quote_date") == quote_date}
    quote_time = max((quote.get("quote_time") or "") for quote in active_quotes.values()) if active_quotes else ""
    quote_datetime = f"{quote_date} {quote_time}".strip()

    profiles = db.symbol_profiles(list(active_quotes))
    tradable_symbols = [
        symbol
        for symbol, quote in active_quotes.items()
        if profiles.get(symbol, {}).get("is_st", 0) in (0, "0", None)
        and "ST" not in (quote.get("name") or "").upper()
        and "退" not in (quote.get("name") or "")
    ]
    history = db.query_recent_before(tradable_symbols, quote_date, 40)
    strategy = create_strategy("divergence_tactic", params or {})
    items: list[dict[str, Any]] = []
    counts = {"candidate": 0, "triggered": 0, "tail_ready": 0, "invalid": 0}
    after_tail_window = _is_after_tail_window(quote_time)

    for symbol in tradable_symbols:
        rows = history.get(symbol, [])
        if len(rows) < 34:
            continue
        quote = active_quotes[symbol]
        t = rows[-2]
        t1 = rows[-1]
        t2 = {
            "symbol": symbol,
            "trade_date": quote_date,
            "open": quote["open"],
            "high": quote["high"],
            "low": quote["low"],
            "close": quote["price"],
            "pre_close": quote["pre_close"],
            "volume": quote["volume"],
            "amount": quote["amount"],
            "turnover": 0,
            "pct_chg": quote["pct_chg"],
            "is_st": 0,
            "source": "sina_realtime",
        }
        full_history = rows + [t2]
        if not strategy._setup_ok(symbol, t, t1, full_history[-35:]):  # type: ignore[attr-defined]
            continue

        previous_close = float(t2.get("pre_close") or t1["close"])
        open_ = float(quote["open"])
        high = float(quote["high"])
        price = float(quote["price"])
        t1_high = float(t1["high"])
        buy_price = strategy._entry_price(t1, t2)  # type: ignore[attr-defined]
        open_gap = open_ / previous_close * 100 - 100 if previous_close else 0
        high_from_open = high / open_ * 100 - 100 if open_ else 0
        current_from_open = price / open_ * 100 - 100 if open_ else 0
        current_vs_buy = price / buy_price * 100 - 100 if buy_price else 0
        day2_pre_close = float(t1.get("pre_close") or t["close"])
        entry_ok = strategy._entry_day_ok(t1, t2)  # type: ignore[attr-defined]
        status, action, reasons = _classify_realtime_status(
            strategy,
            t1,
            t2,
            open_gap,
            high_from_open,
            current_from_open,
            current_vs_buy,
            entry_ok,
            after_tail_window,
        )
        counts[status] += 1
        item = {
            "symbol": symbol,
            "name": profiles.get(symbol, {}).get("name") or quote.get("name", ""),
            "quote_date": quote_date,
            "quote_time": quote.get("quote_time", ""),
            "status": status,
            "status_label": _status_label(status),
            "action": action,
            "reasons": reasons,
            "price": round(price, 3),
            "pct_chg": round(float(quote["pct_chg"]), 4),
            "open": round(open_, 3),
            "high": round(high, 3),
            "buy_price": round(float(buy_price), 3),
            "today_open_gap_pct_chg": round(open_gap, 4),
            "today_high_from_open_pct_chg": round(high_from_open, 4),
            "current_from_open_pct_chg": round(current_from_open, 4),
            "current_vs_buy_pct_chg": round(current_vs_buy, 4),
            "day1_date": t["trade_date"],
            "day1_pct_chg": round(strategy._pct_chg(t), 4),  # type: ignore[attr-defined]
            "day2_date": t1["trade_date"],
            "day2_pct_chg": round(strategy._pct_chg(t1), 4),  # type: ignore[attr-defined]
            "day2_low_pct_chg": round((float(t1["low"]) / day2_pre_close - 1) * 100, 4) if day2_pre_close else 0,
            "day2_high_pct_chg": round((float(t1["high"]) / day2_pre_close - 1) * 100, 4) if day2_pre_close else 0,
        }
        item.update(score_divergence_signal(symbol, full_history, len(full_history) - 1))
        items.append(item)

    status_rank = {"tail_ready": 0, "triggered": 1, "candidate": 2, "invalid": 3}
    items.sort(
        key=lambda item: (
            status_rank.get(str(item.get("status")), 9),
            -float(item.get("trade_worth_probability") or 0),
            str(item.get("symbol")),
        )
    )
    limited_items = items[:limit]
    return {
        "source": "sina",
        "mode": "signal_engine_v1",
        "universe": board,
        "universe_label": universe_label,
        "quote_datetime": quote_datetime,
        "candidate_symbols": len(universe_symbols),
        "quoted_symbols": len(active_quotes),
        "scanned_symbols": len(tradable_symbols),
        "setup_symbols": len(items),
        "counts": counts,
        "items": limited_items,
        "signals": [item for item in limited_items if item["status"] in ("triggered", "tail_ready")],
        "message": "",
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _empty_result(board: str, message: str, started: float) -> dict[str, Any]:
    return {
        "source": "sina",
        "universe": board,
        "universe_label": board_label(board),
        "quote_datetime": "",
        "candidate_symbols": 0,
        "quoted_symbols": 0,
        "scanned_symbols": 0,
        "signals": [],
        "message": message,
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _empty_engine_result(board: str, message: str, started: float) -> dict[str, Any]:
    result = _empty_result(board, message, started)
    result.update(
        {
            "mode": "signal_engine_v1",
            "setup_symbols": 0,
            "counts": {"candidate": 0, "triggered": 0, "tail_ready": 0, "invalid": 0},
            "items": [],
        }
    )
    return result


def _classify_realtime_status(
    strategy: Any,
    previous_row: dict[str, Any],
    entry_row: dict[str, Any],
    open_gap: float,
    high_from_open: float,
    current_from_open: float,
    current_vs_buy: float,
    entry_ok: bool,
    after_tail_window: bool,
) -> tuple[str, str, list[str]]:
    reasons: list[str] = []
    min_gap = float(strategy.params["entry_min_open_gap_pct_chg"])
    max_gap = float(strategy.params["entry_max_open_gap_pct_chg"])
    min_high = float(strategy.params["entry_min_high_from_open_pct_chg"])
    high = float(entry_row["high"])
    previous_high = float(previous_row["high"])

    if open_gap < min_gap:
        return "invalid", "今日高开不足，先剔除", [f"高开 {open_gap:.2f}% < {min_gap:.2f}%"]
    if open_gap > max_gap:
        return "invalid", "今日高开过高，先剔除", [f"高开 {open_gap:.2f}% > {max_gap:.2f}%"]
    if high <= previous_high:
        reasons.append("尚未突破 T+1 高点")
    if high_from_open < min_high:
        reasons.append(f"最高较开盘 {high_from_open:.2f}% < {min_high:.2f}%")

    if not entry_ok:
        return "candidate", "T 和 T+1 已成立，等待 T+2 放量突破", reasons or ["等待盘中突破"]

    if after_tail_window and current_vs_buy >= 0 and current_from_open >= min_high:
        return "tail_ready", "尾盘仍站在介入价上方，可重点确认", ["T+2 盘中突破已触发", "尾盘价格仍有效"]
    if current_vs_buy < 0:
        return "triggered", "盘中突破过，但现价回落到介入价下方", ["T+2 盘中突破已触发", f"现价较介入价 {current_vs_buy:.2f}%"]
    return "triggered", "盘中突破提醒：已达到模式1介入条件，继续观察尾盘强度", ["T+2 盘中突破已触发"]


def _status_label(status: str) -> str:
    return {
        "candidate": "候选中",
        "triggered": "盘中突破",
        "tail_ready": "尾盘可买",
        "invalid": "已失效",
    }.get(status, status)


def _is_after_tail_window(value: str) -> bool:
    try:
        parsed = datetime.strptime(value[:8], "%H:%M:%S").time()
    except ValueError:
        return False
    return parsed >= dt_time(14, 45)


def _to_float(value: Any) -> float:
    try:
        if value in ("", "-"):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0
