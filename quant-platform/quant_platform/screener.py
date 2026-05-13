from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from .ml_signal import score_divergence_signal
from .strategy import Context, DivergenceStrategy, create_strategy


def scan_strategy_signals(
    strategy_name: str,
    params: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    profiles: dict[str, dict[str, Any]],
    start_date: str,
    end_date: str,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    if strategy_name == "divergence_tactic":
        return scan_divergence(params, history, profiles, start_date, end_date, limit)
    if strategy_name in {"divergence_flow", "gap_t_tactic"}:
        return scan_precomputed_entry_strategy(strategy_name, params, history, profiles, start_date, end_date, limit)
    if strategy_name == "moving_average":
        return scan_moving_average(params, history, profiles, start_date, end_date, limit)
    if strategy_name == "momentum":
        return scan_momentum(params, history, profiles, start_date, end_date, limit)
    raise ValueError(f"未知策略：{strategy_name}")


def scan_start_with_buffer(start_date: str, days: int = 140) -> str:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    return (start - timedelta(days=days)).isoformat()


def scan_precomputed_entry_strategy(
    strategy_name: str,
    params: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    profiles: dict[str, dict[str, Any]],
    start_date: str,
    end_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    strategy = create_strategy(strategy_name, params)
    context = Context({"start_date": start_date, "end_date": end_date, "symbols": list(history)}, history)
    strategy.init(context)
    signals: list[dict[str, Any]] = []
    signals_by_date = getattr(strategy, "signals_by_date", {})
    for signal_date in sorted(signals_by_date):
        if signal_date < start_date or signal_date > end_date:
            continue
        for symbol, buy_price in signals_by_date.get(signal_date, []):
            row = next((item for item in history.get(symbol, []) if item["trade_date"] == signal_date), None)
            signals.append(
                {
                    "symbol": symbol,
                    "name": profiles.get(symbol, {}).get("name", ""),
                    "signal_date": signal_date,
                    "entry_date": signal_date,
                    "entry_open": round(float(row["open"]), 3) if row else "",
                    "buy_price": round(float(buy_price), 3),
                    "close": round(float(row["close"]), 3) if row else "",
                    "reason": "策略触发",
                }
            )
            if len(signals) >= limit:
                return signals
    return signals


def scan_divergence(
    params: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    profiles: dict[str, dict[str, Any]],
    start_date: str,
    end_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    strategy = DivergenceStrategy(**params)
    signals: list[dict[str, Any]] = []
    for symbol, rows in history.items():
        if len(rows) < 35:
            continue
        name = profiles.get(symbol, {}).get("name", "")
        for index in range(34, len(rows)):
            t = rows[index - 2]
            t1 = rows[index - 1]
            t2 = rows[index]
            signal_date = t2["trade_date"]
            if signal_date < start_date or signal_date > end_date:
                continue
            if not strategy._matches(symbol, t, t1, t2, rows[index - 34 : index + 1]):  # type: ignore[attr-defined]
                continue
            previous_close = float(t2.get("pre_close") or t1["close"])
            entry_open = float(t2["open"])
            entry_high = float(t2["high"])
            buy_price = strategy._entry_price(t1, t2)  # type: ignore[attr-defined]
            signal = {
                "symbol": symbol,
                "name": name,
                "signal_date": signal_date,
                "entry_date": signal_date,
                "day1_date": t["trade_date"],
                "day2_date": t1["trade_date"],
                "day1_pct_chg": round(strategy._pct_chg(t), 4),  # type: ignore[attr-defined]
                "day2_pct_chg": round(strategy._pct_chg(t1), 4),  # type: ignore[attr-defined]
                "day2_low_pct_chg": round((float(t1["low"]) / (float(t1.get("pre_close") or t["close"])) - 1) * 100, 4),
                "day2_high_pct_chg": round((float(t1["high"]) / (float(t1.get("pre_close") or t["close"])) - 1) * 100, 4),
                "entry_open": round(entry_open, 3),
                "buy_price": round(buy_price, 3),
                "entry_open_gap_pct_chg": round((entry_open / previous_close - 1) * 100, 4) if previous_close else 0,
                "entry_high_from_open_pct_chg": round((entry_high / entry_open - 1) * 100, 4) if entry_open else 0,
                "close": round(float(t2["close"]), 3),
                "reason": "分歧战法触发",
            }
            signal.update(score_divergence_signal(symbol, rows, index))
            signals.append(signal)
            if len(signals) >= limit:
                return signals
    return signals


def scan_moving_average(
    params: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    profiles: dict[str, dict[str, Any]],
    start_date: str,
    end_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    short = int(params.get("short_window", 5))
    long = int(params.get("long_window", 20))
    signals: list[dict[str, Any]] = []
    for symbol, rows in history.items():
        if len(rows) < long + 1:
            continue
        name = profiles.get(symbol, {}).get("name", "")
        for index in range(long, len(rows)):
            row = rows[index]
            signal_date = row["trade_date"]
            if signal_date < start_date or signal_date > end_date:
                continue
            closes = [float(item["close"]) for item in rows[index - long : index + 1]]
            prev_short = sum(closes[-short - 1 : -1]) / short
            prev_long = sum(closes[-long - 1 : -1]) / long
            now_short = sum(closes[-short:]) / short
            now_long = sum(closes[-long:]) / long
            if prev_short <= prev_long and now_short > now_long:
                signals.append(
                    {
                        "symbol": symbol,
                        "name": name,
                        "signal_date": signal_date,
                        "entry_date": signal_date,
                        "close": round(float(row["close"]), 3),
                        "reason": f"{short}日均线上穿{long}日均线",
                    }
                )
                if len(signals) >= limit:
                    return signals
    return signals


def scan_momentum(
    params: dict[str, Any],
    history: dict[str, list[dict[str, Any]]],
    profiles: dict[str, dict[str, Any]],
    start_date: str,
    end_date: str,
    limit: int,
) -> list[dict[str, Any]]:
    lookback = int(params.get("lookback", 60))
    top_k = int(params.get("top_k", 3))
    calendar = sorted({row["trade_date"] for rows in history.values() for row in rows if start_date <= row["trade_date"] <= end_date})
    row_by_symbol_date = {symbol: {row["trade_date"]: row for row in rows} for symbol, rows in history.items()}
    signals: list[dict[str, Any]] = []
    previous_month = ""
    for trade_date in calendar:
        month = trade_date[:7]
        if month == previous_month:
            continue
        previous_month = month
        scores: list[tuple[float, str, dict[str, Any]]] = []
        for symbol, rows in history.items():
            dates = [row["trade_date"] for row in rows]
            try:
                index = dates.index(trade_date)
            except ValueError:
                continue
            if index <= lookback:
                continue
            old = float(rows[index - lookback]["close"])
            now = float(rows[index]["close"])
            if old > 0:
                scores.append((now / old - 1, symbol, rows[index]))
        for score, symbol, row in sorted(scores, reverse=True)[:top_k]:
            signals.append(
                {
                    "symbol": symbol,
                    "name": profiles.get(symbol, {}).get("name", ""),
                    "signal_date": trade_date,
                    "entry_date": trade_date,
                    "close": round(float(row_by_symbol_date[symbol][trade_date]["close"]), 3),
                    "momentum_return": round(score * 100, 4),
                    "reason": f"{lookback}日动量排名前{top_k}",
                }
            )
            if len(signals) >= limit:
                return signals
    return signals
