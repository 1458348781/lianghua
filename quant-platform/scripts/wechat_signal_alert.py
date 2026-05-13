from __future__ import annotations

import argparse
import json
import os
import smtplib
import sys
import time
import urllib.parse
import urllib.request
import warnings
from datetime import datetime, time as dt_time
from email.header import Header
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any


warnings.filterwarnings("ignore", message="X does not have valid feature names.*")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant_platform.data_sources import normalize_symbol  # noqa: E402
from quant_platform.realtime import fetch_sina_realtime, scan_realtime_signal_engine  # noqa: E402
from quant_platform.storage import MarketDatabase, board_label  # noqa: E402


RUNTIME_DIR = ROOT / "data" / "runtime"
INITIAL_CAPITAL = 1_000_000.0

DEFAULT_PARAMS: dict[str, Any] = {
    "max_positions": 2,
    "hold_days": 5,
    "stop_loss": -0.02,
    "strong_close_pct_chg": 5.0,
    "min_price": 3,
    "max_price": 500,
    "min_turnover": 1.9,
    "max_turnover": 33.2,
    "day1_min_volume_ratio": 1.03,
    "day1_max_volume_ratio": 3.2,
    "range_min_amplitude_30": 0.214,
    "range_min_return_20": -0.005,
    "day2_min_pct_chg": -4.0,
    "day2_max_pct_chg": 8.0,
    "day2_max_volume_ratio": 1.94,
    "day2_min_close_position": 0.41,
    "day2_max_upper_shadow": 0.086,
    "day2_min_close_vs_day1_close": 0.963,
    "entry_min_open_gap_pct_chg": 1.3,
    "entry_max_open_gap_pct_chg": 6.5,
    "entry_min_high_from_open_pct_chg": 3.3,
}

HARD_EXIT_ALERTS = {"stop_open", "stop", "limit_failed", "expiry"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Send realtime divergence signal snapshots.")
    parser.add_argument("--times", default="14:30,14:45")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--breakout-watch", action="store_true", help="Send one intraday alert when a symbol first reaches mode-1 breakout.")
    parser.add_argument("--auto-track-breakouts", action="store_true", help="Add breakout symbols to the local position watch file.")
    parser.add_argument("--breakout-start", default="09:30")
    parser.add_argument("--breakout-end", default="14:45")
    parser.add_argument("--position-watch", action="store_true", help="Watch local positions for stop/profit/expiry alerts.")
    parser.add_argument("--position-file", default=str(ROOT / "config" / "watch_positions.json"))
    parser.add_argument("--risk-start", default="09:30")
    parser.add_argument("--risk-end", default="15:00")
    parser.add_argument("--risk-buffer-pct", type=float, default=0.3)
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--interval", type=int, default=20)
    parser.add_argument("--candidate-dir", default=str(RUNTIME_DIR))
    parser.add_argument("--use-candidate-pool", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--fallback-full-scan", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--channel", choices=["wecom", "serverchan", "pushplus", "email"], default=os.getenv("WECHAT_ALERT_CHANNEL", "wecom"))
    parser.add_argument("--webhook-url", default=os.getenv("WECHAT_ALERT_WEBHOOK", ""))
    parser.add_argument("--serverchan-sendkey", default=os.getenv("SERVERCHAN_SENDKEY", ""))
    parser.add_argument("--pushplus-token", default=os.getenv("PUSHPLUS_TOKEN", ""))
    parser.add_argument("--email-to", default=os.getenv("ALERT_EMAIL_TO", "202212620012@nuist.edu.cn"))
    parser.add_argument("--smtp-host", default=os.getenv("SMTP_HOST", ""))
    parser.add_argument("--smtp-port", type=int, default=int(os.getenv("SMTP_PORT", "465")))
    parser.add_argument("--smtp-user", default=os.getenv("SMTP_USER", ""))
    parser.add_argument("--smtp-password", default=os.getenv("SMTP_PASSWORD", ""))
    args = parser.parse_args()

    alert_times = {item.strip() for item in args.times.split(",") if item.strip()}
    sent_keys: set[str] = set()
    sent_breakouts: set[str] = set()
    sent_position_alerts: set[str] = set()
    print(
        f"[signal-alert] board={args.board} times={','.join(sorted(alert_times))} channel={args.channel} breakout_watch={args.breakout_watch} once={args.once} dry_run={args.dry_run}",
        flush=True,
    )

    while True:
        now = datetime.now()
        slot = now.strftime("%H:%M")
        key = f"{now.date()} {slot}"
        if args.breakout_watch and is_time_between(now.time(), args.breakout_start, args.breakout_end):
            data = scan_signal_data(args)
            breakout_message = build_breakout_message(
                data,
                sent_breakouts,
                now,
                Path(args.position_file),
                args.auto_track_breakouts,
            )
            if breakout_message:
                if args.dry_run:
                    print(breakout_message, flush=True)
                else:
                    send_alert(args, "分歧战法盘中突破提醒", breakout_message)
                    print(f"[signal-alert] sent breakout {now:%Y-%m-%d %H:%M:%S}", flush=True)
        if args.position_watch and is_time_between(now.time(), args.risk_start, args.risk_end):
            position_message = build_position_alert_message(Path(args.position_file), sent_position_alerts, now, args.risk_buffer_pct)
            if position_message:
                if args.dry_run:
                    print(position_message, flush=True)
                else:
                    send_alert(args, "分歧战法持仓风控提醒", position_message)
                    print(f"[signal-alert] sent position-risk {now:%Y-%m-%d %H:%M:%S}", flush=True)
        if args.once or (slot in alert_times and key not in sent_keys):
            message = build_signal_message(args)
            if args.dry_run:
                print(message, flush=True)
            else:
                send_alert(args, "分歧战法实时信号", message)
                print(f"[signal-alert] sent {now:%Y-%m-%d %H:%M:%S} slot={slot}", flush=True)
            sent_keys.add(key)
        if args.once:
            break
        time.sleep(max(1, int(args.interval)))


def build_signal_message(args: argparse.Namespace) -> str:
    return build_signal_message_from_data(scan_signal_data(args), args.board, args.limit)


def scan_signal_data(args: argparse.Namespace) -> dict[str, Any]:
    db = MarketDatabase()
    symbols = None
    if args.use_candidate_pool:
        pool = load_candidate_pool_for_date(Path(args.candidate_dir), datetime.now().date().isoformat(), db)
        if pool:
            symbols = [normalize_symbol(str(item.get("symbol") or "")) for item in pool.get("candidates", [])]
            symbols = [symbol for symbol in symbols if symbol]
        elif not args.fallback_full_scan:
            return {
                "source": "sina",
                "mode": "signal_engine_v1",
                "universe": args.board,
                "universe_label": board_label(args.board),
                "quote_datetime": "",
                "candidate_symbols": 0,
                "quoted_symbols": 0,
                "scanned_symbols": 0,
                "setup_symbols": 0,
                "counts": {"candidate": 0, "triggered": 0, "tail_ready": 0, "invalid": 0},
                "items": [],
                "signals": [],
                "message": "no valid candidate pool",
                "elapsed_seconds": 0,
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
    return scan_realtime_signal_engine(db, DEFAULT_PARAMS, limit=args.limit, board=args.board, symbols=symbols)


def load_candidate_pool_for_date(candidate_dir: Path, target_date: str, db: MarketDatabase) -> dict[str, Any] | None:
    paths = [
        candidate_dir / f"tomorrow_candidates_{target_date}.json",
        candidate_dir / "latest_tomorrow_candidates.json",
    ]
    latest_before = latest_daily_date_before(db, target_date)
    for index, path in enumerate(paths):
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if index == 0 and payload.get("target_date") not in ("", None, target_date):
            continue
        setup_date = str(payload.get("setup_date") or "")
        if setup_date and latest_before and setup_date != latest_before:
            continue
        if payload.get("candidates") and candidate_pool_daily_complete(db, payload):
            return payload
    return None


def candidate_pool_daily_complete(db: MarketDatabase, payload: dict[str, Any]) -> bool:
    setup_date = str(payload.get("setup_date") or "")
    if not setup_date:
        return True
    source_symbols = int(payload.get("source_symbols") or len(payload.get("candidates") or []))
    min_rows = daily_complete_threshold(source_symbols)
    return daily_rows_for_date(db, setup_date) >= min_rows


def daily_complete_threshold(source_symbols: int) -> int:
    if source_symbols <= 0:
        return 4800
    return min(source_symbols, max(4800, int(source_symbols * 0.98)))


def daily_rows_for_date(db: MarketDatabase, trade_date: str) -> int:
    with db.connect() as conn:
        row = conn.execute("select count(*) as count from stock_daily where trade_date = ?", (trade_date,)).fetchone()
    return int(row["count"] if row else 0)


def latest_daily_date_before(db: MarketDatabase, target_date: str) -> str:
    with db.connect() as conn:
        row = conn.execute(
            "select max(trade_date) as trade_date from stock_daily where trade_date < ?",
            (target_date,),
        ).fetchone()
    return str(row["trade_date"] or "") if row else ""


def build_signal_message_from_data(data: dict[str, Any], board: str, limit: int) -> str:
    counts = data.get("counts") or {}
    rows = data.get("items") or []
    title = [
        f"分歧战法实时信号 {datetime.now():%H:%M}",
        f"行情时间：{data.get('quote_datetime') or '-'}",
        f"股票池：{data.get('universe_label') or board_label(board)}",
        f"扫描：{data.get('scanned_symbols', 0)} 只；T/T+1候选：{data.get('setup_symbols', 0)} 只",
        f"尾盘可买 {counts.get('tail_ready', 0)}，盘中突破 {counts.get('triggered', 0)}，候选中 {counts.get('candidate', 0)}，已失效 {counts.get('invalid', 0)}",
    ]
    if not rows:
        return "\n".join(title + ["", "当前没有进入 T/T+1 候选链路的股票。"])

    lines = title + [""]
    for index, item in enumerate(rows[:limit], start=1):
        lines.append(
            " | ".join(
                [
                    f"{index}. {item.get('status_label', '')}",
                    f"{item.get('symbol', '')} {item.get('name', '')}",
                    f"现价 {num(item.get('price'))}",
                    f"涨跌 {pct(item.get('pct_chg'))}",
                    f"开盘 {num(item.get('open'))}",
                    f"T {item.get('day1_date', '')} {pct(item.get('day1_pct_chg'))}",
                    f"T+1 {item.get('day2_date', '')} {pct(item.get('day2_pct_chg'))}",
                    f"介入 {num(item.get('buy_price'))}",
                    f"价值 {prob(item.get('trade_worth_probability'))}",
                    str(item.get("action") or item.get("reason") or ""),
                ]
            )
        )
    return "\n".join(lines)


def build_breakout_message(
    data: dict[str, Any],
    sent_breakouts: set[str],
    now: datetime,
    position_file: Path | None = None,
    auto_track: bool = False,
) -> str:
    quote_datetime = str(data.get("quote_datetime") or "")
    quote_date = quote_datetime[:10]
    today = now.strftime("%Y-%m-%d")
    if quote_date != today:
        return ""

    rows = [
        item
        for item in (data.get("items") or [])
        if item.get("status") in ("triggered", "tail_ready")
    ]
    new_rows = []
    for item in rows:
        symbol = str(item.get("symbol") or "")
        if not symbol:
            continue
        key = f"{quote_date}:breakout:{symbol}"
        if key in sent_breakouts:
            continue
        sent_breakouts.add(key)
        new_rows.append(item)

    if not new_rows:
        return ""

    if auto_track and position_file is not None:
        tracked = auto_track_positions(position_file, new_rows, quote_date)
        print(
            f"[signal-alert] auto-track candidates={len(new_rows)} added={tracked} date={quote_date} file={position_file}",
            flush=True,
        )

    lines = [
        f"分歧战法盘中突破提醒 {now:%H:%M:%S}",
        f"行情时间：{quote_datetime or '-'}",
        "以下股票已经达到模式1：T+2 高开并盘中突破 T+1 高点。",
        "",
    ]
    for index, item in enumerate(new_rows, start=1):
        lines.append(
            " | ".join(
                [
                    f"{index}. {item.get('status_label', '')}",
                    f"{item.get('symbol', '')} {item.get('name', '')}",
                    f"现价 {num(item.get('price'))}",
                    f"涨跌 {pct(item.get('pct_chg'))}",
                    f"介入 {num(item.get('buy_price'))}",
                    f"价值 {prob(item.get('trade_worth_probability'))}",
                    str(item.get("action") or ""),
                ]
            )
        )
    return "\n".join(lines)


def auto_track_positions(position_file: Path, rows: list[dict[str, Any]], entry_date: str) -> int:
    payload = load_position_payload(position_file)
    positions = payload.setdefault("positions", [])
    max_positions = max(1, int(DEFAULT_PARAMS.get("max_positions", 4)))
    existing = {
        normalize_symbol(str(item.get("symbol") or ""))
        for item in positions
        if item.get("active", True) and item.get("symbol")
    }
    slots = max(0, max_positions - len(existing))
    if slots <= 0:
        return 0
    db = MarketDatabase()
    enriched_rows = []
    for item in rows:
        copied = dict(item)
        symbol = normalize_symbol(str(copied.get("symbol") or ""))
        entry_price = to_float(copied.get("buy_price"))
        trigger_time = first_minute_trigger_time(db, symbol, entry_date, entry_price)
        copied["trigger_time"] = trigger_time or f"{entry_date} {copied.get('quote_time') or ''}".strip()
        enriched_rows.append(copied)
    changed = False
    added = 0
    for item in sorted(enriched_rows, key=lambda row: (str(row.get("trigger_time") or ""), normalize_symbol(str(row.get("symbol") or "")))):
        if added >= slots:
            break
        symbol = normalize_symbol(str(item.get("symbol") or ""))
        if not symbol or symbol in existing:
            continue
        entry_price = to_float(item.get("buy_price"))
        if entry_price <= 0:
            continue
        amount = next_position_amount(payload, max_positions, INITIAL_CAPITAL)
        lot_size = board_lot_size(symbol)
        quantity = int(amount / entry_price / lot_size) * lot_size
        if amount <= 0 or quantity <= 0:
            continue
        amount = quantity * entry_price
        positions.append(
            {
                "symbol": symbol,
                "name": item.get("name") or "",
                "entry_date": entry_date,
                "entry_price": round(entry_price, 4),
                "quantity": quantity,
                "lot_size": lot_size,
                "amount": round(amount, 2),
                "entry_amount": round(amount, 2),
                "hold_days": int(DEFAULT_PARAMS.get("hold_days", 5)),
                "active": True,
                "source": "auto_breakout",
                "trigger_time": item.get("trigger_time") or "",
            }
        )
        existing.add(symbol)
        added += 1
        changed = True
    if changed:
        save_position_payload(position_file, payload)
    return added


def next_position_amount(payload: dict[str, Any], max_positions: int, initial_capital: float) -> float:
    active_positions = [
        item
        for item in payload.get("positions", [])
        if item.get("active", True) and item.get("symbol")
    ]
    remaining_slots = max(1, max_positions - len(active_positions))
    used_amount = sum(position_entry_amount(item) for item in active_positions)
    realized_pnl = sum(closed_realized_pnl(item) for item in payload.get("closed_positions", []))
    available_cash = max(0.0, float(initial_capital or INITIAL_CAPITAL) + realized_pnl - used_amount)
    return available_cash / remaining_slots


def position_entry_amount(position: dict[str, Any]) -> float:
    amount = to_float(position.get("entry_amount")) or to_float(position.get("amount"))
    if amount > 0:
        return amount
    quantity = to_float(position.get("quantity"))
    entry_price = to_float(position.get("entry_price"))
    return max(0.0, quantity * entry_price)


def board_lot_size(symbol: str) -> int:
    normalized = normalize_symbol(symbol)
    code, exchange = normalized.split(".")
    return 200 if exchange == "SZ" and code.startswith(("300", "301")) else 100


def closed_realized_pnl(position: dict[str, Any]) -> float:
    pnl = to_float(position.get("realized_pnl"))
    if pnl:
        return pnl
    exit_amount = to_float(position.get("exit_amount"))
    entry_amount = position_entry_amount(position)
    return exit_amount - entry_amount if exit_amount > 0 and entry_amount > 0 else 0.0


def first_minute_trigger_time(db: MarketDatabase, symbol: str, trade_date: str, entry_price: float) -> str:
    if not symbol or not trade_date or entry_price <= 0:
        return ""
    with db.connect() as conn:
        row = conn.execute(
            """
            select min(trade_time) as trade_time
            from stock_minute
            where symbol = ? and trade_date = ? and high >= ?
            """,
            (symbol, trade_date, entry_price),
        ).fetchone()
    return str(row["trade_time"] or "") if row else ""


def strategy_exit_alerts(
    symbol: str,
    position: dict[str, Any],
    quote: dict[str, Any],
    today: str,
    db: MarketDatabase,
    calendar_cache: dict[str, int],
    buffer_ratio: float,
) -> list[tuple[str, str]]:
    entry_price = to_float(position.get("entry_price"))
    price = to_float(quote.get("price"))
    open_ = to_float(quote.get("open"))
    high = to_float(quote.get("high"))
    low = to_float(quote.get("low"))
    pre_close = to_float(quote.get("pre_close"))
    quote_time = str(quote.get("quote_time") or "")
    if entry_price <= 0 or price <= 0:
        return []

    stop_price = entry_price * (1 + float(DEFAULT_PARAMS["stop_loss"]))
    hold_days = int(position.get("hold_days") or DEFAULT_PARAMS.get("hold_days", 5))
    entry_date = str(position.get("entry_date") or today)
    held_days = position_held_trading_days(db, symbol, entry_date, today, calendar_cache)
    is_entry_day = entry_date == today

    if not is_entry_day and open_ > 0 and open_ <= stop_price:
        return [("stop_open", f"开盘破止损：开盘 {num(open_)} <= 止损价 {num(stop_price)}")]
    if not is_entry_day and low > 0 and low <= stop_price:
        return [("stop", f"已触发止损：最低 {num(low)} <= 止损价 {num(stop_price)}")]
    if not is_entry_day and price <= stop_price * (1 + buffer_ratio):
        return [("stop_near", f"接近止损：现价 {num(price)}，止损价 {num(stop_price)}")]

    hit_limit = is_limit_up_intraday(symbol, high, pre_close)
    close_limit = is_limit_up_intraday(symbol, price, pre_close) and high > 0 and price >= high * 0.995
    if close_limit:
        return []
    close_execution_window = is_time_at_or_after(quote_time, "14:55")
    if hit_limit and close_execution_window:
        return [("limit_failed", "涨停炸板未回封：按回算规则卖出")]
    held_after_entry = max(0, held_days - 1)
    if hold_days > 0 and held_after_entry >= hold_days and close_execution_window:
        return [("expiry", f"持仓天数到期：买入后已持有 {held_after_entry} 个交易日，策略持有 {hold_days} 日，按回算规则卖出")]
    return []


def close_position_for_strategy_exit(
    payload: dict[str, Any],
    position: dict[str, Any],
    quote: dict[str, Any],
    alert_type: str,
    now: datetime,
) -> bool:
    if alert_type not in HARD_EXIT_ALERTS:
        return False
    symbol = normalize_symbol(str(position.get("symbol") or ""))
    entry_price = to_float(position.get("entry_price"))
    entry_amount = to_float(position.get("entry_amount")) or to_float(position.get("amount"))
    quantity = to_float(position.get("quantity"))
    if quantity <= 0 and entry_amount > 0 and entry_price > 0:
        quantity = entry_amount / entry_price
    if entry_amount <= 0 and quantity > 0 and entry_price > 0:
        entry_amount = quantity * entry_price
    if alert_type == "stop_open":
        exit_price = to_float(quote.get("open"))
    elif alert_type == "stop":
        exit_price = entry_price * (1 + float(DEFAULT_PARAMS["stop_loss"]))
    else:
        exit_price = to_float(quote.get("price"))
    if not symbol or entry_price <= 0 or exit_price <= 0 or entry_amount <= 0:
        return False

    positions = payload.get("positions", [])
    if not any(item is position or normalize_symbol(str(item.get("symbol") or "")) == symbol for item in positions):
        return False

    exit_amount = quantity * exit_price if quantity > 0 else entry_amount * exit_price / entry_price
    realized_pnl = exit_amount - entry_amount
    closed_item = {
        **position,
        "quantity": quantity,
        "entry_amount": entry_amount,
        "active": False,
        "exit_date": now.date().isoformat(),
        "exit_price": round(exit_price, 4),
        "exit_amount": exit_amount,
        "realized_pnl": realized_pnl,
        "realized_return": realized_pnl / entry_amount if entry_amount > 0 else 0,
        "close_reason": alert_type,
        "closed_at": now.isoformat(timespec="seconds"),
    }
    payload["positions"] = [item for item in positions if normalize_symbol(str(item.get("symbol") or "")) != symbol]
    payload.setdefault("closed_positions", []).append(closed_item)
    return True


def build_position_alert_message(position_file: Path, sent_alerts: set[str], now: datetime, buffer_pct: float) -> str:
    payload = load_position_payload(position_file)
    positions = [
        item
        for item in payload.get("positions", [])
        if item.get("active", True) and item.get("symbol") and to_float(item.get("entry_price")) > 0
    ]
    if not positions:
        return ""

    symbols = [normalize_symbol(str(item["symbol"])) for item in positions]
    quotes = fetch_sina_realtime(symbols)
    today = now.strftime("%Y-%m-%d")
    lines: list[str] = []
    buffer_ratio = max(0.0, buffer_pct) / 100
    calendar_cache: dict[str, int] = {}
    db = MarketDatabase()
    position_payload_changed = False

    for position in positions:
        symbol = normalize_symbol(str(position["symbol"]))
        quote = quotes.get(symbol)
        if not quote or str(quote.get("quote_date") or "") != today:
            continue
        entry_price = to_float(position.get("entry_price"))
        price = to_float(quote.get("price"))
        if entry_price <= 0 or price <= 0:
            continue

        alerts = strategy_exit_alerts(symbol, position, quote, today, db, calendar_cache, buffer_ratio)
        for alert_type, message in alerts:
            auto_closed = close_position_for_strategy_exit(payload, position, quote, alert_type, now)
            position_payload_changed = position_payload_changed or auto_closed
            if auto_closed:
                print(
                    f"[signal-alert] auto-close symbol={symbol} reason={alert_type} price={num(quote.get('price'))} time={now:%Y-%m-%d %H:%M:%S}",
                    flush=True,
                )
            key = f"{today}:position:{symbol}:{alert_type}"
            if key in sent_alerts:
                continue
            sent_alerts.add(key)
            if auto_closed:
                message = f"{message}；已记录本地模拟清仓"
            lines.append(
                " | ".join(
                    [
                        f"{symbol} {position.get('name') or quote.get('name') or ''}",
                        f"现价 {num(price)}",
                        f"成本 {num(entry_price)}",
                        f"收益 {pct((price / entry_price - 1) * 100)}",
                        message,
                    ]
                )
            )

    if position_payload_changed:
        save_position_payload(position_file, payload)

    if not lines:
        return ""
    return "\n".join([f"分歧战法持仓风控提醒 {now:%H:%M:%S}", ""] + lines)


def position_held_trading_days(
    db: MarketDatabase,
    symbol: str,
    entry_date: str,
    today: str,
    cache: dict[str, int],
) -> int:
    key = f"{symbol}:{entry_date}:{today}"
    if key not in cache:
        cache[key] = len(db.get_calendar([symbol], entry_date, today))
    return cache[key]


def is_limit_up_intraday(symbol: str, price: float, pre_close: float) -> bool:
    if price <= 0 or pre_close <= 0:
        return False
    threshold = 19.5 if symbol.startswith(("300", "301", "688", "689")) else 9.75
    return (price / pre_close - 1) * 100 >= threshold


def load_position_payload(position_file: Path) -> dict[str, Any]:
    if not position_file.exists():
        return {"positions": []}
    try:
        payload = json.loads(position_file.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {"positions": []}
    if isinstance(payload, list):
        return {"positions": payload}
    if not isinstance(payload, dict):
        return {"positions": []}
    payload.setdefault("positions", [])
    return payload


def save_position_payload(position_file: Path, payload: dict[str, Any]) -> None:
    position_file.parent.mkdir(parents=True, exist_ok=True)
    position_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def is_time_between(value: dt_time, start: str, end: str) -> bool:
    try:
        start_time = datetime.strptime(start, "%H:%M").time()
        end_time = datetime.strptime(end, "%H:%M").time()
    except ValueError:
        return False
    return start_time <= value <= end_time


def is_time_at_or_after(value: str, start: str) -> bool:
    try:
        text = value.strip()
        fmt = "%H:%M:%S" if len(text) >= 8 else "%H:%M"
        value_time = datetime.strptime(text[:8] if fmt == "%H:%M:%S" else text[:5], fmt).time()
        start_time = datetime.strptime(start, "%H:%M").time()
    except ValueError:
        return False
    return value_time >= start_time


def to_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def send_alert(args: argparse.Namespace, title: str, content: str) -> None:
    if args.channel == "email":
        send_email(args, title, content)
        return
    if args.channel == "wecom":
        if not args.webhook_url:
            raise RuntimeError("缺少企业微信机器人地址：设置 WECHAT_ALERT_WEBHOOK 或传 --webhook-url")
        post_json(args.webhook_url, {"msgtype": "markdown", "markdown": {"content": f"**{title}**\n\n{content}"}})
        return
    if args.channel == "serverchan":
        if not args.serverchan_sendkey:
            raise RuntimeError("缺少 Server酱 SendKey：设置 SERVERCHAN_SENDKEY")
        post_form(f"https://sctapi.ftqq.com/{args.serverchan_sendkey}.send", {"title": title, "desp": content})
        return
    if args.channel == "pushplus":
        if not args.pushplus_token:
            raise RuntimeError("缺少 PushPlus token：设置 PUSHPLUS_TOKEN")
        post_json("https://www.pushplus.plus/send", {"token": args.pushplus_token, "title": title, "content": content, "template": "txt"})
        return
    raise RuntimeError(f"未知推送通道：{args.channel}")


def send_email(args: argparse.Namespace, title: str, content: str) -> None:
    if not args.smtp_host or not args.smtp_user or not args.smtp_password:
        raise RuntimeError("缺少邮箱 SMTP 配置：需要 SMTP_HOST、SMTP_USER、SMTP_PASSWORD")
    message = MIMEText(content, "plain", "utf-8")
    message["Subject"] = Header(title, "utf-8")
    message["From"] = args.smtp_user
    message["To"] = args.email_to
    with smtplib.SMTP_SSL(args.smtp_host, int(args.smtp_port), timeout=20) as server:
        server.login(args.smtp_user, args.smtp_password)
        server.sendmail(args.smtp_user, [args.email_to], message.as_string())


def post_json(url: str, payload: dict[str, Any]) -> None:
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json; charset=utf-8"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    if "errcode" in body and '"errcode":0' not in body and '"errcode": 0' not in body:
        raise RuntimeError(body[:500])


def post_form(url: str, payload: dict[str, str]) -> None:
    data = urllib.parse.urlencode(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
    if '"code":0' not in body and '"errno":0' not in body:
        raise RuntimeError(body[:500])


def num(value: Any) -> str:
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return "-"


def pct(value: Any) -> str:
    try:
        return f"{float(value):.2f}%"
    except (TypeError, ValueError):
        return "-"


def prob(value: Any) -> str:
    try:
        return f"{float(value) * 100:.0f}%"
    except (TypeError, ValueError):
        return "-"


if __name__ == "__main__":
    main()
