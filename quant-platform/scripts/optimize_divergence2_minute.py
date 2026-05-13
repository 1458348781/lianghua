from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from optimize_divergence2_params import (  # noqa: E402
    BASE_PARAMS,
    PARAM_COLUMNS,
    load_base_params,
    prefix_metrics,
    result_fieldnames,
    sample_params,
    score_metrics,
    write_yearly_report,
)
from quant_platform.metrics import calculate_metrics  # noqa: E402
from quant_platform.screener import scan_start_with_buffer  # noqa: E402
from quant_platform.storage import MarketDatabase  # noqa: E402
from quant_platform.strategy import DivergenceStrategy  # noqa: E402


DEFAULT_MINUTE_PARQUET_ROOT = Path(r"D:\BaiduNetdiskDownload\1m_price")
DEFAULT_RESULT_DIR = Path(r"D:\lianghua\result3\minute_opt")
_WORKER_YEARS_DATA: dict[str, "YearData"] = {}
_WORKER_ARGS: dict[str, Any] = {}


@dataclass
class YearData:
    year: str
    start_date: str
    end_date: str
    symbols: list[str]
    history: dict[str, list[dict[str, Any]]]
    calendar: list[str]


@dataclass
class Position:
    symbol: str
    entry_date: str
    entry_time: str
    entry_price: float
    amount: float
    quantity: float


def main() -> None:
    parser = argparse.ArgumentParser(description="Minute-level optimizer for divergence2 using realtime-like execution.")
    parser.add_argument("--trials", type=int, default=100)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--output-dir", default=str(DEFAULT_RESULT_DIR))
    parser.add_argument("--seed", type=int, default=20260512)
    parser.add_argument("--min-trades", type=int, default=40)
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--symbol-limit", type=int, default=0)
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--slippage-rate", type=float, default=0.001)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--minute-parquet-root", default=str(DEFAULT_MINUTE_PARQUET_ROOT))
    parser.add_argument("--minute-db-template", default="", help="Optional legacy SQLite template. Parquet root is used by default.")
    parser.add_argument("--base-params-json", default="")
    parser.add_argument("--local-search", action="store_true")
    parser.add_argument("--write-trades", action="store_true", help="Write trades for top rows after optimization.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    base_params = load_base_params(Path(args.base_params_json)) if args.base_params_json else dict(BASE_PARAMS)
    params_list = sample_params(args.trials, args.seed, base_params=base_params, local_search=args.local_search)
    years = list(range(int(args.start_date[:4]), int(args.end_date[:4]) + 1))
    years_data = load_years_data(years, args.start_date, args.end_date, args.board, args.symbol_limit)
    if not years_data:
        raise SystemExit("No daily data loaded.")

    stamp = time.strftime("%Y%m%d_%H%M%S")
    summary_path = output_dir / f"divergence2_minute_opt_{stamp}.csv"
    top_path = output_dir / f"divergence2_minute_opt_top_{stamp}.csv"
    yearly_path = output_dir / f"divergence2_minute_opt_yearly_top_{stamp}.csv"
    config_path = output_dir / f"divergence2_minute_opt_config_{stamp}.json"
    config = {
        "trials": args.trials,
        "workers": args.workers,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "board": args.board,
        "years": list(years_data),
        "symbols_by_year": {year: len(item.symbols) for year, item in years_data.items()},
        "seed": args.seed,
        "min_trades": args.min_trades,
        "top_n": args.top_n,
        "minute_parquet_root": args.minute_parquet_root,
        "minute_db_template": args.minute_db_template,
        "base_params_json": args.base_params_json,
        "local_search": args.local_search,
        "base_params": base_params,
        "costs": {
            "initial_cash": args.initial_cash,
            "commission_rate": args.commission_rate,
            "slippage_rate": args.slippage_rate,
            "stamp_tax_rate": args.stamp_tax_rate,
        },
    }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"[minute-opt] trials={len(params_list)} workers={args.workers} years={','.join(years_data)} "
        f"{args.start_date}..{args.end_date} output={summary_path}",
        flush=True,
    )
    started = time.perf_counter()
    rows: list[dict[str, Any]] = []
    fieldnames = result_fieldnames(list(years_data))
    with summary_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        with ProcessPoolExecutor(max_workers=max(1, args.workers), initializer=init_worker, initargs=(years_data, vars(args))) as pool:
            futures = [pool.submit(run_combo, idx, params) for idx, params in enumerate(params_list, start=1)]
            best_score = None
            for completed, future in enumerate(as_completed(futures), start=1):
                row = future.result()
                rows.append(row)
                writer.writerow({key: row.get(key, "") for key in fieldnames})
                score = float(row.get("score") or -999)
                best_score = score if best_score is None else max(best_score, score)
                if completed == 1 or completed % 10 == 0 or completed == len(futures):
                    elapsed = time.perf_counter() - started
                    print(f"[minute-opt] done={completed}/{len(futures)} elapsed={elapsed:.1f}s best_score={best_score:.4f}", flush=True)

    top_rows = sorted(rows, key=lambda item: float(item.get("score") or -999), reverse=True)[: max(1, args.top_n)]
    with top_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in top_rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})
    write_yearly_report(yearly_path, top_rows, list(years_data))
    print(f"[minute-opt] top={top_path}", flush=True)
    print(f"[minute-opt] yearly top={yearly_path}", flush=True)


def load_years_data(
    years: list[int],
    start_date: str,
    end_date: str,
    board: str,
    symbol_limit: int,
) -> dict[str, YearData]:
    db = MarketDatabase()
    loaded: dict[str, YearData] = {}
    for year in years:
        year_start = max(start_date, f"{year}-01-01")
        year_end = min(end_date, f"{year}-12-31")
        if year_start > year_end:
            continue
        pool = db.list_backtest_symbols(year_start, year_end, 30, board)
        symbols = [item["symbol"] for item in pool]
        if symbol_limit > 0:
            symbols = symbols[:symbol_limit]
        warmup_start = scan_start_with_buffer(year_start)
        print(f"[load] {year} pool={len(symbols)} warmup={warmup_start} backtest={year_start}..{year_end}", flush=True)
        history = db.query_many(symbols, warmup_start, year_end)
        symbols = [symbol for symbol in symbols if symbol in history]
        calendar = db.get_calendar(symbols, year_start, year_end)
        loaded[str(year)] = YearData(str(year), year_start, year_end, symbols, history, calendar)
    return loaded


def init_worker(years_data: dict[str, YearData], args: dict[str, Any]) -> None:
    global _WORKER_YEARS_DATA, _WORKER_ARGS
    _WORKER_YEARS_DATA = years_data
    _WORKER_ARGS = args


def run_combo(combo_id: int, params: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {"combo_id": combo_id, **params}
    try:
        yearly: dict[str, dict[str, Any]] = {}
        for year, data in _WORKER_YEARS_DATA.items():
            result = run_year_minute_backtest(data, params, _WORKER_ARGS)
            yearly[year] = result["metrics"]
            row.update(prefix_metrics(year, yearly[year]))
        metrics = aggregate_yearly_metrics(yearly)
        row.update(prefix_metrics("all", metrics))
        row["score"] = score_metrics(metrics, int(_WORKER_ARGS.get("min_trades") or 0))
        row["error"] = ""
    except Exception as exc:
        row.update(prefix_metrics("all", {}))
        row["score"] = -999.0
        row["error"] = repr(exc)
    return row


def run_year_minute_backtest(
    data: YearData,
    params: dict[str, Any],
    args: dict[str, Any],
) -> dict[str, Any]:
    minute_source_ok = bool(args.get("minute_db_template")) or Path(str(args.get("minute_parquet_root") or "")).exists()
    if not minute_source_ok:
        return {"metrics": {}, "trades": [], "equity_curve": []}
    strategy = DivergenceStrategy(**params)
    candidates_by_date = build_candidates_by_date(data, strategy)
    if not candidates_by_date:
        return empty_result(float(args.get("initial_cash") or 1_000_000))

    cash = float(args.get("initial_cash") or 1_000_000)
    commission_rate = float(args["commission_rate"]) if "commission_rate" in args else 0.0003
    slippage_rate = float(args["slippage_rate"]) if "slippage_rate" in args else 0.001
    stamp_tax_rate = float(args["stamp_tax_rate"]) if "stamp_tax_rate" in args else 0.001
    max_positions = max(1, int(params.get("max_positions") or 4))
    hold_days = int(params.get("hold_days") or 0)
    positions: dict[str, Position] = {}
    trades: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = []
    calendar_index = {trade_date: index for index, trade_date in enumerate(data.calendar)}
    daily_rows = {symbol: {row["trade_date"]: row for row in rows} for symbol, rows in data.history.items()}

    for trade_date in data.calendar:
        day_candidates = candidates_by_date.get(trade_date, {})
        minute_symbols = sorted(set(day_candidates) | set(positions))
        minute_rows = query_minutes(args, data.year, trade_date, minute_symbols)
        if day_candidates:
            triggers = find_intraday_triggers(minute_rows, day_candidates, strategy, trade_date)
            for trigger in triggers:
                symbol = trigger["symbol"]
                if symbol in positions or len(positions) >= max_positions:
                    continue
                remaining_slots = max(1, max_positions - len(positions))
                budget = cash / remaining_slots
                entry_price = float(trigger["entry_price"]) * (1 + slippage_rate)
                lot_size = board_lot_size(symbol)
                quantity = int(budget / entry_price / lot_size) * lot_size if entry_price > 0 else 0
                if quantity <= 0:
                    continue
                gross = quantity * entry_price
                commission = gross * commission_rate
                if gross + commission > cash and entry_price > 0:
                    quantity = int(cash / (entry_price * (1 + commission_rate)) / lot_size) * lot_size
                    gross = quantity * entry_price
                    commission = gross * commission_rate
                if quantity <= 0:
                    continue
                cash -= gross + commission
                positions[symbol] = Position(symbol, trade_date, trigger["trade_time"], entry_price, gross + commission, quantity)
                trades.append(
                    {
                        "trade_date": trade_date,
                        "trade_time": trigger["trade_time"],
                        "symbol": symbol,
                        "side": "buy",
                        "quantity": quantity,
                        "price": round(entry_price, 4),
                        "amount": gross,
                        "commission": commission,
                        "tax": 0.0,
                        "reason": "minute breakout entry",
                    }
                )

        if positions:
            cash += process_intraday_exits(
                minute_rows=minute_rows,
                positions=positions,
                trades=trades,
                trade_date=trade_date,
                daily_rows=daily_rows,
                strategy=strategy,
                params=params,
                calendar_index=calendar_index,
                hold_days=hold_days,
                commission_rate=commission_rate,
                stamp_tax_rate=stamp_tax_rate,
                slippage_rate=slippage_rate,
            )

        close_prices = close_prices_for_date(daily_rows, trade_date, positions)
        total_value = cash + sum(pos.quantity * close_prices.get(symbol, pos.entry_price) for symbol, pos in positions.items())
        previous = equity_curve[-1]["total_value"] if equity_curve else float(args.get("initial_cash") or 1_000_000)
        equity_curve.append(
            {
                "trade_date": trade_date,
                "cash": round(cash, 2),
                "market_value": round(total_value - cash, 2),
                "total_value": round(total_value, 2),
                "daily_return": total_value / previous - 1 if previous else 0.0,
            }
        )
    metrics = calculate_metrics(equity_curve, trades)
    metrics["buy_count"] = sum(1 for trade in trades if trade.get("side") == "buy")
    metrics["sell_count"] = sum(1 for trade in trades if trade.get("side") == "sell")
    return {"metrics": metrics, "trades": trades, "equity_curve": equity_curve}


def build_candidates_by_date(data: YearData, strategy: DivergenceStrategy) -> dict[str, dict[str, dict[str, Any]]]:
    candidates: dict[str, dict[str, dict[str, Any]]] = {}
    for symbol, rows in data.history.items():
        if len(rows) < 35:
            continue
        for index in range(34, len(rows)):
            t = rows[index - 2]
            t1 = rows[index - 1]
            t2 = rows[index]
            trade_date = str(t2["trade_date"])
            if trade_date < data.start_date or trade_date > data.end_date:
                continue
            if strategy._setup_ok(symbol, t, t1, rows[index - 34 : index + 1]):  # type: ignore[attr-defined]
                candidates.setdefault(trade_date, {})[symbol] = {"previous_row": t1}
    return candidates


def query_minutes(args: dict[str, Any], year: str, trade_date: str, symbols: list[str]) -> list[dict[str, Any]]:
    if not symbols:
        return []
    if args.get("minute_db_template"):
        return query_minutes_sqlite(Path(str(args["minute_db_template"]).format(year=year)), trade_date, symbols)
    return query_minutes_parquet(Path(str(args.get("minute_parquet_root") or DEFAULT_MINUTE_PARQUET_ROOT)), trade_date, symbols)


def query_minutes_sqlite(db_path: Path, trade_date: str, symbols: list[str]) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        for chunk in chunks(sorted(set(symbols)), 700):
            marks = ",".join("?" for _ in chunk)
            sql = f"""
                select symbol, trade_time, trade_date, open, high, low, close, volume, amount
                from stock_minute
                where trade_date = ? and symbol in ({marks})
                order by trade_time, symbol
            """
            rows.extend(dict(row) for row in conn.execute(sql, [trade_date, *chunk]))
    return rows


def query_minutes_parquet(root: Path, trade_date: str, symbols: list[str]) -> list[dict[str, Any]]:
    date_key = trade_date.replace("-", "")
    path = root / trade_date[:4] / f"{date_key}.parquet"
    if not path.exists():
        return []
    wanted = sorted(set(symbols))
    columns = ["code", "trade_time", "date", "open", "high", "low", "close", "vol", "amount"]
    try:
        frame = pd.read_parquet(path, columns=columns, filters=[("code", "in", wanted)])
    except Exception:
        frame = pd.read_parquet(path, columns=columns)
        frame = frame[frame["code"].isin(wanted)]
    if frame.empty:
        return []
    frame = frame.rename(columns={"code": "symbol", "vol": "volume"})
    frame["trade_date"] = (
        frame["date"].astype(str).str.slice(0, 4)
        + "-"
        + frame["date"].astype(str).str.slice(4, 6)
        + "-"
        + frame["date"].astype(str).str.slice(6, 8)
    )
    frame = frame[["symbol", "trade_time", "trade_date", "open", "high", "low", "close", "volume", "amount"]]
    frame = frame.sort_values(["trade_time", "symbol"])
    return frame.to_dict("records")


def find_intraday_triggers(
    minute_rows: list[dict[str, Any]],
    candidates: dict[str, dict[str, Any]],
    strategy: DivergenceStrategy,
    trade_date: str,
) -> list[dict[str, Any]]:
    state: dict[str, dict[str, float]] = {}
    triggers: list[dict[str, Any]] = []
    triggered_symbols: set[str] = set()
    for row in minute_rows:
        symbol = str(row["symbol"])
        if symbol in triggered_symbols or symbol not in candidates:
            continue
        previous = candidates[symbol]["previous_row"]
        item = state.setdefault(
            symbol,
            {
                "open": float(row["open"] or 0),
                "high": float(row["high"] or 0),
                "low": float(row["low"] or 0),
                "volume": 0.0,
                "amount": 0.0,
            },
        )
        item["high"] = max(float(item["high"]), float(row["high"] or 0))
        low = float(row["low"] or 0)
        item["low"] = min(float(item["low"]), low) if item["low"] else low
        item["volume"] += float(row.get("volume") or 0)
        item["amount"] += float(row.get("amount") or 0)
        pre_close = float(previous.get("close") or 0)
        close = float(row["close"] or 0)
        entry_row = {
            "symbol": symbol,
            "trade_date": trade_date,
            "open": item["open"],
            "high": item["high"],
            "low": item["low"],
            "close": close,
            "pre_close": pre_close,
            "volume": item["volume"],
            "amount": item["amount"],
            "turnover": 0.0,
            "pct_chg": close / pre_close * 100 - 100 if pre_close else 0.0,
            "is_st": 0,
        }
        if strategy._entry_day_ok(previous, entry_row):  # type: ignore[attr-defined]
            entry_price = strategy._entry_price(previous, entry_row)  # type: ignore[attr-defined]
            triggers.append({"symbol": symbol, "trade_time": str(row["trade_time"]), "entry_price": entry_price})
            triggered_symbols.add(symbol)
    return sorted(triggers, key=lambda item: (item["trade_time"], item["symbol"]))


def process_intraday_exits(
    minute_rows: list[dict[str, Any]],
    positions: dict[str, Position],
    trades: list[dict[str, Any]],
    trade_date: str,
    daily_rows: dict[str, dict[str, dict[str, Any]]],
    strategy: DivergenceStrategy,
    params: dict[str, Any],
    calendar_index: dict[str, int],
    hold_days: int,
    commission_rate: float,
    stamp_tax_rate: float,
    slippage_rate: float,
) -> float:
    cash_delta = 0.0
    state: dict[str, dict[str, float]] = {}
    for row in minute_rows:
        symbol = str(row["symbol"])
        position = positions.get(symbol)
        if position is None:
            continue
        item = state.setdefault(
            symbol,
            {
                "high": float(row["high"] or 0),
                "low": float(row["low"] or 0),
                "first_open": float(row["open"] or 0),
                "first_seen": 1.0,
            },
        )
        item["high"] = max(float(item["high"]), float(row["high"] or 0))
        low = float(row["low"] or 0)
        item["low"] = min(float(item["low"]), low) if item["low"] else low
        current_time = str(row["trade_time"])
        if trade_date == position.entry_date:
            continue
        exit_price = 0.0
        reason = ""
        stop_price = position.entry_price * (1 + float(params["stop_loss"]))
        if item.get("first_seen") == 1.0 and float(item.get("first_open") or 0) > 0 and float(item["first_open"]) <= stop_price:
            exit_price = float(item["first_open"])
            reason = "stop_open"
        elif item["low"] <= stop_price:
            exit_price = stop_price
            reason = "stop"
        elif current_time[-8:] >= "14:55:00":
            item["first_seen"] = 0.0
            daily_row = daily_rows.get(symbol, {}).get(trade_date, {})
            pre_close = float(daily_row.get("pre_close") or daily_row.get("open") or 0)
            minute_day = {
                "trade_date": trade_date,
                "open": 0,
                "high": item["high"],
                "low": item["low"],
                "close": float(row["close"] or 0),
                "pre_close": pre_close,
                "pct_chg": float(row["close"] or 0) / pre_close * 100 - 100 if pre_close else 0.0,
            }
            if strategy._is_limit_up(symbol, minute_day):  # type: ignore[attr-defined]
                continue
            if strategy._hit_limit_up(symbol, minute_day):  # type: ignore[attr-defined]
                exit_price = float(row["close"] or 0)
                reason = "limit_failed"
            elif hold_days > 0 and held_days_after_entry(calendar_index, position.entry_date, trade_date) >= hold_days:
                exit_price = float(row["close"] or 0)
                reason = "expiry"
        item["first_seen"] = 0.0
        if exit_price <= 0:
            continue
        exit_price *= 1 - slippage_rate
        gross = position.quantity * exit_price
        commission = gross * commission_rate
        tax = gross * stamp_tax_rate
        proceeds = gross - commission - tax
        pnl = proceeds - position.amount
        trades.append(
            {
                "trade_date": trade_date,
                "trade_time": current_time,
                "symbol": symbol,
                "side": "sell",
                "quantity": position.quantity,
                "price": round(exit_price, 4),
                "amount": gross,
                "commission": commission,
                "tax": tax,
                "reason": reason,
                "pnl": pnl,
                "pnl_pct": pnl / position.amount if position.amount else 0.0,
                "price_return": exit_price / position.entry_price - 1 if position.entry_price else 0.0,
            }
        )
        cash_delta += proceeds
        positions.pop(symbol, None)
    return cash_delta


def close_prices_for_date(
    daily_rows: dict[str, dict[str, dict[str, Any]]],
    trade_date: str,
    positions: dict[str, Position],
) -> dict[str, float]:
    prices: dict[str, float] = {}
    for symbol, position in positions.items():
        row = daily_rows.get(symbol, {}).get(trade_date)
        prices[symbol] = float(row["close"]) if row else position.entry_price
    return prices


def aggregate_yearly_metrics(yearly: dict[str, dict[str, Any]]) -> dict[str, Any]:
    valid = [metrics for metrics in yearly.values() if metrics]
    if not valid:
        return {}
    cumulative = 1.0
    for metrics in valid:
        cumulative *= 1 + float(metrics.get("cumulative_return") or 0)
    trade_count = sum(int(metrics.get("trade_count") or 0) for metrics in valid)
    buy_count = sum(int(metrics.get("buy_count") or 0) for metrics in valid)
    sell_count = sum(int(metrics.get("sell_count") or 0) for metrics in valid)
    weighted_avg_trade = 0.0
    if sell_count:
        weighted_avg_trade = sum(
            float(metrics.get("avg_trade_return") or 0) * int(metrics.get("sell_count") or 0)
            for metrics in valid
        ) / sell_count
    return {
        "cumulative_return": round(cumulative - 1, 6),
        "annual_return": round(sum(float(metrics.get("annual_return") or 0) for metrics in valid) / len(valid), 6),
        "max_drawdown": round(min(float(metrics.get("max_drawdown") or 0) for metrics in valid), 6),
        "sharpe": round(sum(float(metrics.get("sharpe") or 0) for metrics in valid) / len(valid), 4),
        "win_rate": round(sum(float(metrics.get("win_rate") or 0) for metrics in valid) / len(valid), 6),
        "avg_trade_return": round(weighted_avg_trade, 6),
        "best_trade_return": round(max(float(metrics.get("best_trade_return") or 0) for metrics in valid), 6),
        "worst_trade_return": round(min(float(metrics.get("worst_trade_return") or 0) for metrics in valid), 6),
        "trade_count": trade_count,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "final_value": "",
    }


def empty_result(initial_cash: float) -> dict[str, Any]:
    equity_curve = [{"trade_date": "", "cash": initial_cash, "market_value": 0.0, "total_value": initial_cash, "daily_return": 0.0}]
    return {"metrics": calculate_metrics(equity_curve, []), "trades": [], "equity_curve": equity_curve}


def chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def held_days_after_entry(calendar_index: dict[str, int], entry_date: str, trade_date: str) -> int:
    return max(0, int(calendar_index.get(trade_date, 0)) - int(calendar_index.get(entry_date, 0)))


def board_lot_size(symbol: str) -> int:
    normalized = str(symbol).upper()
    if "." in normalized:
        code, exchange = normalized.split(".", 1)
    else:
        code, exchange = normalized[:6], ""
    return 200 if exchange == "SZ" and code.startswith(("300", "301")) else 100


if __name__ == "__main__":
    main()
