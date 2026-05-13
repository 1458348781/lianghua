from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant_platform.backtest import BacktestEngine  # noqa: E402
from quant_platform.screener import scan_start_with_buffer  # noqa: E402
from quant_platform.storage import DataPortal, MarketDatabase  # noqa: E402
from quant_platform.strategy import create_strategy  # noqa: E402


RESULT_DIR = Path(r"D:\lianghua\result")

BASE_PARAMS: dict[str, Any] = {
    "max_positions": 3,
    "hold_days": 3,
    "stop_loss": -0.03,
    "strong_close_pct_chg": 5.0,
    "min_price": 3,
    "max_price": 500,
    "min_turnover": 2.0,
    "max_turnover": 25.0,
    "day1_min_volume_ratio": 1.0,
    "day1_max_volume_ratio": 4.0,
    "range_min_amplitude_30": 0.18,
    "range_min_return_20": 0.0,
    "day2_min_pct_chg": -3.0,
    "day2_max_pct_chg": 8.0,
    "entry_min_open_gap_pct_chg": 1.0,
    "entry_max_open_gap_pct_chg": 5.0,
    "entry_min_high_from_open_pct_chg": 4.0,
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Enumerate divergence tactic parameters and rank them.")
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--preset", choices=["focused", "fine"], default="focused")
    parser.add_argument("--max-combos", type=int, default=300, help="0 means run all generated combinations; keep modest on this machine.")
    parser.add_argument("--min-trades", type=int, default=20)
    parser.add_argument("--output-dir", default=str(RESULT_DIR))
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--symbol-limit", type=int, default=0, help="Only for smoke tests; 0 means full market.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    params_list = build_param_grid(args.preset, args.max_combos)

    symbols = load_symbols(args.start_date, args.end_date, args.board)
    if args.symbol_limit > 0:
        symbols = symbols[: args.symbol_limit]
    years = list(range(int(args.start_date[:4]), int(args.end_date[:4]) + 1))
    stamp = time.strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"divergence_param_enum_{args.preset}_{stamp}.csv"
    best_path = output_dir / f"divergence_param_enum_best_{args.preset}_{stamp}.json"

    print(f"[enum] symbols={len(symbols)} combos={len(params_list)} years={years[0]}..{years[-1]} workers={args.workers}")
    print(f"[enum] output={csv_path}")

    fieldnames = result_fieldnames(years)
    started = time.perf_counter()
    completed = 0
    best: dict[str, Any] | None = None
    with csv_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        with ProcessPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [
                pool.submit(run_combo, idx, params, symbols, args.start_date, args.end_date, years, args.initial_cash)
                for idx, params in enumerate(params_list, start=1)
            ]
            for future in as_completed(futures):
                row = future.result()
                writer.writerow({key: row.get(key, "") for key in fieldnames})
                completed += 1
                if int(row.get("all_trade_count") or 0) >= args.min_trades:
                    if best is None or float(row["score"]) > float(best["score"]):
                        best = row
                if completed == 1 or completed % 20 == 0 or completed == len(futures):
                    elapsed = time.perf_counter() - started
                    top_score = best.get("score") if best else "-"
                    print(f"[enum] done={completed}/{len(futures)} elapsed={elapsed:.1f}s best_score={top_score}", flush=True)

    if best:
        best_path.write_text(json.dumps(best, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[enum] best={best_path}")


def build_param_grid(preset: str, max_combos: int = 0) -> list[dict[str, Any]]:
    grid = {
        "max_positions": [3, 5],
        "hold_days": [2, 3],
        "stop_loss": [-0.03],
        "strong_close_pct_chg": [4.0, 5.0, 6.0],
        "min_turnover": [2.0],
        "max_turnover": [25.0, 30.0],
        "day1_min_volume_ratio": [1.0, 1.2],
        "day1_max_volume_ratio": [4.0, 6.0],
        "range_min_amplitude_30": [0.15, 0.18, 0.22],
        "range_min_return_20": [0.0, 0.03, 0.05],
        "day2_max_pct_chg": [7.0, 8.0],
        "entry_min_open_gap_pct_chg": [1.0],
        "entry_max_open_gap_pct_chg": [5.0],
        "entry_min_high_from_open_pct_chg": [3.5, 4.0, 4.5],
    }
    if preset == "fine":
        grid.update(
            {
                "max_positions": [2, 3, 4, 5, 6, 8],
                "hold_days": [1, 2, 3, 4, 5],
                "stop_loss": [-0.02, -0.025, -0.03, -0.035, -0.04],
                "strong_close_pct_chg": [3.0, 4.0, 5.0, 6.0, 7.0],
                "min_turnover": [2.0, 3.0, 4.0],
                "max_turnover": [20.0, 25.0, 30.0, 35.0],
                "day1_min_volume_ratio": [1.0, 1.2, 1.5],
                "day1_max_volume_ratio": [4.0, 5.0, 6.0, 8.0],
                "range_min_amplitude_30": [0.12, 0.15, 0.18, 0.22, 0.26],
                "range_min_return_20": [0.0, 0.03, 0.05, 0.08],
                "day2_max_pct_chg": [6.0, 7.0, 8.0],
                "entry_min_open_gap_pct_chg": [0.5, 1.0, 1.5, 2.0],
                "entry_max_open_gap_pct_chg": [4.0, 5.0, 6.0],
                "entry_min_high_from_open_pct_chg": [3.0, 3.5, 4.0, 4.5, 5.0],
            }
        )

    keys = list(grid)
    combos: list[dict[str, Any]] = []
    for values in itertools.product(*(grid[key] for key in keys)):
        params = dict(BASE_PARAMS)
        params.update(dict(zip(keys, values)))
        if params["min_turnover"] > params["max_turnover"]:
            continue
        if params["day1_min_volume_ratio"] > params["day1_max_volume_ratio"]:
            continue
        if params["entry_min_open_gap_pct_chg"] > params["entry_max_open_gap_pct_chg"]:
            continue
        combos.append(params)
        if max_combos > 0 and len(combos) >= max_combos:
            break
    return combos


def load_symbols(start_date: str, end_date: str, board: str) -> list[str]:
    db = MarketDatabase()
    history_start = scan_start_with_buffer(start_date)
    rows = db.list_backtest_symbols(history_start, end_date, 30, board)
    return [row["symbol"] for row in rows]


def run_combo(
    index: int,
    params: dict[str, Any],
    symbols: list[str],
    start_date: str,
    end_date: str,
    years: list[int],
    initial_cash: float,
) -> dict[str, Any]:
    row: dict[str, Any] = {"combo_id": index, **params}
    all_metrics = run_backtest(symbols, params, start_date, end_date, initial_cash)
    add_metrics(row, "all", all_metrics)
    annual_returns: list[float] = []
    annual_drawdowns: list[float] = []
    active_years = 0
    for year in years:
        year_start = max(start_date, f"{year}-01-01")
        year_end = min(end_date, f"{year}-12-31")
        metrics = run_backtest(symbols, params, year_start, year_end, initial_cash)
        add_metrics(row, str(year), metrics)
        if int(metrics.get("trade_count") or 0) > 0:
            active_years += 1
            annual_returns.append(float(metrics.get("cumulative_return") or 0))
            annual_drawdowns.append(abs(float(metrics.get("max_drawdown") or 0)))
    row["active_years"] = active_years
    row["positive_years"] = sum(1 for value in annual_returns if value > 0)
    row["score"] = rank_score(row, annual_returns, annual_drawdowns)
    return row


def run_backtest(symbols: list[str], params: dict[str, Any], start_date: str, end_date: str, initial_cash: float) -> dict[str, Any]:
    config = {
        "strategy_name": "divergence_tactic",
        "symbols": symbols,
        "start_date": start_date,
        "end_date": end_date,
        "data_start_date": scan_start_with_buffer(start_date),
        "initial_cash": initial_cash,
        "commission_rate": 0.0003,
        "slippage_rate": 0.0005,
        "stamp_tax_rate": 0.001,
        "params": params,
    }
    try:
        result = BacktestEngine(DataPortal(), create_strategy("divergence_tactic", params), config).run()
        return result.get("metrics", {})
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "trade_count": 0}


def add_metrics(row: dict[str, Any], prefix: str, metrics: dict[str, Any]) -> None:
    for key in ("cumulative_return", "annual_return", "max_drawdown", "sharpe", "win_rate", "trade_count", "avg_trade_return"):
        row[f"{prefix}_{key}"] = metrics.get(key, "")
    if metrics.get("error"):
        row[f"{prefix}_error"] = metrics["error"]


def rank_score(row: dict[str, Any], annual_returns: list[float], annual_drawdowns: list[float]) -> float:
    all_return = float(row.get("all_cumulative_return") or 0)
    all_dd = abs(float(row.get("all_max_drawdown") or 0))
    win_rate = float(row.get("all_win_rate") or 0)
    sharpe = float(row.get("all_sharpe") or 0)
    min_year = min(annual_returns) if annual_returns else -1
    avg_year = sum(annual_returns) / len(annual_returns) if annual_returns else 0
    volatility_penalty = pstdev(annual_returns) if len(annual_returns) > 1 else 0
    dd_penalty = max(annual_drawdowns) if annual_drawdowns else all_dd
    return round(all_return * 100 + avg_year * 30 + min_year * 25 + sharpe * 2 + win_rate * 10 - dd_penalty * 35 - volatility_penalty * 20, 6)


def pstdev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    avg = sum(values) / len(values)
    return math.sqrt(sum((value - avg) ** 2 for value in values) / len(values))


def result_fieldnames(years: list[int]) -> list[str]:
    params = list(BASE_PARAMS)
    metric_keys = ["cumulative_return", "annual_return", "max_drawdown", "sharpe", "win_rate", "trade_count", "avg_trade_return"]
    fields = ["combo_id", "score", "active_years", "positive_years", *params]
    fields.extend(f"all_{key}" for key in metric_keys)
    for year in years:
        fields.extend(f"{year}_{key}" for key in metric_keys)
    return fields


if __name__ == "__main__":
    main()
