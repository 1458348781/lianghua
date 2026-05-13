from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import multiprocessing as mp
import os
import sys
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Any


ROOT = Path(__file__).resolve().parent
PROJECT = ROOT / "quant-platform"
RESULT_DIR = ROOT / "result"
sys.path.insert(0, str(PROJECT))

from quant_platform.backtest import BacktestEngine  # noqa: E402
from quant_platform.metrics import equity_with_drawdown, monthly_returns  # noqa: E402
from quant_platform.storage import MarketDatabase  # noqa: E402
from quant_platform.strategy import create_strategy  # noqa: E402


_WORKER_YEARS_DATA: dict[str, "YearData"] = {}


YEARS = {
    "2024": ("2024-01-01", "2024-12-31"),
    "2025": ("2025-01-01", "2025-12-31"),
    "2026": ("2026-01-01", "2026-05-09"),
}

WARMUP_STARTS = {
    "2024": "2023-09-01",
    "2025": "2024-09-01",
    "2026": "2025-09-01",
}

DEFAULT_PARAMS: dict[str, Any] = {
    "max_positions": 5,
    "stop_loss": -0.03,
    "strong_close_pct_chg": 3.0,
    "min_price": 3,
    "max_price": 500,
    "min_turnover": 3.0,
    "max_turnover": 25.0,
    "day1_min_volume_ratio": 1.2,
    "day1_max_volume_ratio": 6.0,
    "range_min_amplitude_30": 0.18,
    "range_min_return_20": 0.05,
    "day2_min_pct_chg": -3.0,
    "day2_max_pct_chg": 8.0,
    "entry_min_open_gap_pct_chg": 1.0,
    "entry_max_open_gap_pct_chg": 5.0,
    "entry_min_high_from_open_pct_chg": 3.0,
}

QUICK_PRESETS = {
    "baseline": {},
    "loose_volume": {"day1_min_volume_ratio": 1.0, "day1_max_volume_ratio": 8.0},
    "strict_volume": {"day1_min_volume_ratio": 1.5, "day1_max_volume_ratio": 4.0},
    "loose_entry": {"entry_min_open_gap_pct_chg": 0.0, "entry_min_high_from_open_pct_chg": 2.0},
    "strict_entry": {"entry_min_open_gap_pct_chg": 2.0, "entry_min_high_from_open_pct_chg": 4.0},
    "loose_confirm": {"day2_min_pct_chg": -5.0, "day2_max_pct_chg": 12.0},
    "strict_confirm": {"day2_min_pct_chg": 0.0, "day2_max_pct_chg": 5.0},
    "defensive_exit": {"stop_loss": -0.03, "strong_close_pct_chg": 5.0},
}

BATCH_A_GRID = {
    "min_turnover": [2.0, 3.0, 5.0],
    "max_turnover": [20.0, 25.0, 35.0],
    "day1_min_volume_ratio": [1.0, 1.2, 1.5],
    "day1_max_volume_ratio": [4.0, 6.0, 8.0],
    "range_min_amplitude_30": [0.12, 0.18, 0.25],
    "range_min_return_20": [0.0, 0.05, 0.10],
}

BATCH_B_GRID = {
    "day2_min_pct_chg": [-5.0, -3.0, 0.0],
    "day2_max_pct_chg": [5.0, 8.0, 12.0],
    "entry_min_open_gap_pct_chg": [0.0, 1.0, 2.0],
    "entry_max_open_gap_pct_chg": [4.0, 5.0, 7.0],
    "entry_min_high_from_open_pct_chg": [2.0, 3.0, 4.0],
}

BATCH_C_GRID = {
    "stop_loss": [-0.03, -0.05, -0.08],
    "strong_close_pct_chg": [1.0, 3.0, 5.0],
    "max_positions": [3, 5, 8],
}


@dataclass
class YearData:
    year: str
    start_date: str
    end_date: str
    symbols: list[str]
    history: dict[str, list[dict[str, Any]]]
    calendar: list[str]
    names: dict[str, str]


class InMemoryDb:
    def __init__(self, names: dict[str, str]) -> None:
        self._names = names

    def symbol_profiles(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        return {symbol: {"name": self._names.get(symbol, "")} for symbol in symbols}


class InMemoryPortal:
    def __init__(self, data: YearData) -> None:
        self.data = data
        self.db = InMemoryDb(data.names)

    def get_trade_calendar(self, symbols: list[str], start_date: str, end_date: str) -> list[str]:
        return self.data.calendar

    def get_prices(self, symbols: list[str], start_date: str, end_date: str) -> dict[str, list[dict[str, Any]]]:
        wanted = set(symbols)
        return {symbol: rows for symbol, rows in self.data.history.items() if symbol in wanted}


def merged_params(overrides: dict[str, Any]) -> dict[str, Any]:
    params = deepcopy(DEFAULT_PARAMS)
    params.update(overrides)
    return params


def grid_combos(grid: dict[str, list[Any]], base: dict[str, Any] | None = None, prefix: str = "run") -> list[dict[str, Any]]:
    keys = list(grid)
    combos = []
    for index, values in enumerate(itertools.product(*(grid[key] for key in keys)), start=1):
        overrides = dict(zip(keys, values))
        params = merged_params(base or {})
        params.update(overrides)
        if params["min_turnover"] > params["max_turnover"]:
            continue
        if params["day1_min_volume_ratio"] > params["day1_max_volume_ratio"]:
            continue
        if params["day2_min_pct_chg"] > params["day2_max_pct_chg"]:
            continue
        if params["entry_min_open_gap_pct_chg"] > params["entry_max_open_gap_pct_chg"]:
            continue
        combos.append({"run_id": f"{prefix}_{index:05d}", "params": params})
    return combos


def quick_combos() -> list[dict[str, Any]]:
    return [{"run_id": name, "params": merged_params(overrides)} for name, overrides in QUICK_PRESETS.items()]


def load_year_data(db: MarketDatabase, years: list[str], min_rows: int) -> dict[str, YearData]:
    loaded = {}
    for year in years:
        start_date, end_date = YEARS[year]
        warmup_start = WARMUP_STARTS.get(year, start_date)
        pool = db.list_backtest_symbols(start_date, end_date, min_rows)
        symbols = [item["symbol"] for item in pool]
        print(
            f"[load] {year} pool={len(symbols)} warmup={warmup_start} backtest={start_date}..{end_date}",
            flush=True,
        )
        history = db.query_many(symbols, warmup_start, end_date)
        symbols = [symbol for symbol in symbols if symbol in history]
        calendar = db.get_calendar(symbols, start_date, end_date)
        profiles = db.symbol_profiles(symbols)
        loaded[year] = YearData(
            year=year,
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            history=history,
            calendar=calendar,
            names={symbol: profiles.get(symbol, {}).get("name", "") for symbol in symbols},
        )
    return loaded


def run_one(combo: dict[str, Any], year_data: YearData) -> dict[str, Any]:
    config = {
        "strategy_name": "divergence_tactic",
        "symbols": year_data.symbols,
        "start_date": year_data.start_date,
        "end_date": year_data.end_date,
        "initial_cash": 1_000_000,
        "commission_rate": 0.0003,
        "slippage_rate": 0.0005,
        "stamp_tax_rate": 0.001,
        "params": combo["params"],
        "universe": "all",
        "universe_symbol_count": len(year_data.symbols),
    }
    strategy = create_strategy("divergence_tactic", combo["params"])
    result = BacktestEngine(InMemoryPortal(year_data), strategy, config).run()
    result["equity_curve"] = equity_with_drawdown(result["equity_curve"])
    result["monthly_returns"] = monthly_returns(result["equity_curve"])
    metrics = result["metrics"]
    buy_count = sum(1 for trade in result["trades"] if trade["side"] == "buy")
    closed_trades = sum(1 for trade in result["trades"] if trade["side"] == "sell")
    row = {
        "run_id": combo["run_id"],
        "year": year_data.year,
        "start_date": year_data.start_date,
        "end_date": year_data.end_date,
        "final_value": metrics.get("final_value", 0),
        "cumulative_return": metrics.get("cumulative_return", 0),
        "annual_return": metrics.get("annual_return", 0),
        "max_drawdown": metrics.get("max_drawdown", 0),
        "volatility": metrics.get("volatility", 0),
        "sharpe": metrics.get("sharpe", 0),
        "buy_count": buy_count,
        "closed_trades": closed_trades,
        "win_rate": metrics.get("win_rate", 0),
        "avg_trade_return": metrics.get("avg_trade_return", 0),
    }
    row.update(combo["params"])
    return row


def init_worker(years_data: dict[str, YearData]) -> None:
    global _WORKER_YEARS_DATA
    _WORKER_YEARS_DATA = years_data


def run_one_task(task: tuple[dict[str, Any], str]) -> dict[str, Any]:
    combo, year = task
    year_data = _WORKER_YEARS_DATA[year]
    try:
        return run_one(combo, year_data)
    except Exception as exc:
        row = {
            "run_id": combo["run_id"],
            "year": year_data.year,
            "start_date": year_data.start_date,
            "end_date": year_data.end_date,
            "error": str(exc),
        }
        row.update(combo["params"])
        return row


def aggregate(summary_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in summary_rows:
        grouped.setdefault(row["run_id"], []).append(row)

    top_rows = []
    for run_id, rows in grouped.items():
        annuals = [float(row["annual_return"]) for row in rows]
        drawdowns = [float(row["max_drawdown"]) for row in rows]
        sharpes = [float(row["sharpe"]) for row in rows]
        buy_counts = [int(row["buy_count"]) for row in rows]
        sum_buy = sum(buy_counts)
        worst_drawdown = min(drawdowns) if drawdowns else 0
        min_annual = min(annuals) if annuals else 0
        trade_penalty = 0.2 if sum_buy < 20 else 0
        score = mean(annuals) + 0.5 * min_annual + 0.1 * mean(sharpes) - 0.8 * abs(worst_drawdown) - trade_penalty
        out = {
            "run_id": run_id,
            "years": ",".join(row["year"] for row in rows),
            "sum_buy_count": sum_buy,
            "min_year_buy_count": min(buy_counts) if buy_counts else 0,
            "mean_annual_return": mean(annuals) if annuals else 0,
            "median_annual_return": median(annuals) if annuals else 0,
            "min_annual_return": min_annual,
            "mean_max_drawdown": mean(drawdowns) if drawdowns else 0,
            "worst_max_drawdown": worst_drawdown,
            "mean_sharpe": mean(sharpes) if sharpes else 0,
            "score": score,
        }
        for key in DEFAULT_PARAMS:
            out[key] = rows[0].get(key)
        top_rows.append(out)
    return sorted(top_rows, key=lambda row: row["score"], reverse=True)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    fieldnames = list(rows[0])
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_combos(
    combos: list[dict[str, Any]],
    years_data: dict[str, YearData],
    label: str,
    workers: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    summary = []
    tasks = [(combo, year) for combo in combos for year in years_data]
    total = len(tasks)
    done = 0
    started = time.time()

    if workers <= 1:
        for combo, year in tasks:
            year_data = years_data[year]
            done += 1
            try:
                row = run_one(combo, year_data)
            except Exception as exc:
                row = {
                    "run_id": combo["run_id"],
                    "year": year_data.year,
                    "start_date": year_data.start_date,
                    "end_date": year_data.end_date,
                    "error": str(exc),
                }
                row.update(combo["params"])
            summary.append(row)
            if done == 1 or done % 25 == 0 or done == total:
                elapsed = time.time() - started
                print(f"[{label}] {done}/{total} elapsed={elapsed:.1f}s", flush=True)
        return summary, aggregate([row for row in summary if "error" not in row])

    with mp.get_context("spawn").Pool(
        processes=workers,
        initializer=init_worker,
        initargs=(years_data,),
    ) as pool:
        for row in pool.imap_unordered(run_one_task, tasks, chunksize=1):
            done += 1
            summary.append(row)
            if done == 1 or done % 25 == 0 or done == total:
                elapsed = time.time() - started
                print(f"[{label}] {done}/{total} workers={workers} elapsed={elapsed:.1f}s", flush=True)
    return summary, aggregate([row for row in summary if "error" not in row])


def select_candidates(top_rows: list[dict[str, Any]], keep: int) -> list[dict[str, Any]]:
    filtered = [
        row
        for row in top_rows
        if int(row["sum_buy_count"]) >= 20
        and int(row["min_year_buy_count"]) > 0
        and float(row["worst_max_drawdown"]) > -0.25
    ]
    source = filtered or top_rows
    return source[:keep]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run staged parameter sweep for the divergence strategy.")
    parser.add_argument("--mode", choices=["quick", "batch-a", "staged"], default="quick")
    parser.add_argument("--years", default="2024,2025,2026")
    parser.add_argument("--result-dir", default=str(RESULT_DIR))
    parser.add_argument("--keep-a", type=int, default=30)
    parser.add_argument("--keep-b", type=int, default=30)
    parser.add_argument("--min-rows", type=int, default=30)
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(4, (os.cpu_count() or 2) // 2)),
        help="Parallel worker processes. Use 1 to disable multiprocessing.",
    )
    args = parser.parse_args()

    result_dir = Path(args.result_dir)
    result_dir.mkdir(parents=True, exist_ok=True)
    years = [item.strip() for item in args.years.split(",") if item.strip()]
    invalid = [year for year in years if year not in YEARS]
    if invalid:
        raise SystemExit(f"Unsupported years: {invalid}; valid={sorted(YEARS)}")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db = MarketDatabase()
    years_data = load_year_data(db, years, args.min_rows)

    all_summary: list[dict[str, Any]] = []
    final_top: list[dict[str, Any]] = []
    config_out = {
        "mode": args.mode,
        "years": years,
        "workers": args.workers,
        "default_params": DEFAULT_PARAMS,
        "grids": {"batch_a": BATCH_A_GRID, "batch_b": BATCH_B_GRID, "batch_c": BATCH_C_GRID},
    }

    if args.mode == "quick":
        combos = quick_combos()
        summary, final_top = run_combos(combos, years_data, "quick", args.workers)
        all_summary.extend(summary)
    elif args.mode == "batch-a":
        combos = grid_combos(BATCH_A_GRID, prefix="A")
        summary, final_top = run_combos(combos, years_data, "batch-a", args.workers)
        all_summary.extend(summary)
    else:
        combos_a = grid_combos(BATCH_A_GRID, prefix="A")
        summary_a, top_a = run_combos(combos_a, years_data, "A", args.workers)
        all_summary.extend(summary_a)
        keep_a = select_candidates(top_a, args.keep_a)

        combos_b = []
        for base in keep_a:
            base_params = {key: base[key] for key in DEFAULT_PARAMS}
            combos_b.extend(grid_combos(BATCH_B_GRID, base=base_params, prefix=f"B_{base['run_id']}"))
        summary_b, top_b = run_combos(combos_b, years_data, "B", args.workers)
        all_summary.extend(summary_b)
        keep_b = select_candidates(top_b, args.keep_b)

        combos_c = []
        for base in keep_b:
            base_params = {key: base[key] for key in DEFAULT_PARAMS}
            combos_c.extend(grid_combos(BATCH_C_GRID, base=base_params, prefix=f"C_{base['run_id']}"))
        summary_c, final_top = run_combos(combos_c, years_data, "C", args.workers)
        all_summary.extend(summary_c)

    summary_path = result_dir / f"parameter_sweep_summary_{stamp}.csv"
    top_path = result_dir / f"parameter_sweep_top_{stamp}.csv"
    config_path = result_dir / f"parameter_sweep_config_{stamp}.json"
    write_csv(summary_path, all_summary)
    write_csv(top_path, final_top)
    config_path.write_text(json.dumps(config_out, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nDone")
    print(f"summary: {summary_path}")
    print(f"top:     {top_path}")
    print(f"config:  {config_path}")


if __name__ == "__main__":
    main()
