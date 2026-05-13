from __future__ import annotations

import argparse
import csv
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
PARAM_KEYS = [
    "max_positions",
    "hold_days",
    "stop_loss",
    "strong_close_pct_chg",
    "min_price",
    "max_price",
    "min_turnover",
    "max_turnover",
    "day1_min_volume_ratio",
    "day1_max_volume_ratio",
    "range_min_amplitude_30",
    "range_min_return_20",
    "day2_min_pct_chg",
    "day2_max_pct_chg",
    "entry_min_open_gap_pct_chg",
    "entry_max_open_gap_pct_chg",
    "entry_min_high_from_open_pct_chg",
]

DEFAULTS: dict[str, Any] = {
    "hold_days": 3,
    "max_positions": 3,
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
    parser = argparse.ArgumentParser(description="Retest legacy top sweep parameters on the current strategy logic.")
    parser.add_argument("--input", default=str(RESULT_DIR / "parameter_sweep_top_20260509_223624.csv"))
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--output-dir", default=str(RESULT_DIR))
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    combos = load_legacy_params(input_path, args.limit)
    symbols = load_symbols(args.start_date, args.end_date, args.board)
    years = list(range(int(args.start_date[:4]), int(args.end_date[:4]) + 1))
    stamp = time.strftime("%Y%m%d_%H%M%S")
    summary_path = output_dir / f"legacy_top_retest_summary_{stamp}.csv"
    top_path = output_dir / f"legacy_top_retest_top_{stamp}.csv"
    config_path = output_dir / f"legacy_top_retest_config_{stamp}.json"

    print(f"[retest] input={input_path}")
    print(f"[retest] symbols={len(symbols)} combos={len(combos)} years={years[0]}..{years[-1]} workers={args.workers}")

    summary_fields = result_fieldnames(years)
    results: list[dict[str, Any]] = []
    started = time.perf_counter()
    done = 0
    with summary_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=summary_fields)
        writer.writeheader()
        with ProcessPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = [
                pool.submit(run_combo, combo, symbols, args.start_date, args.end_date, years, args.initial_cash)
                for combo in combos
            ]
            for future in as_completed(futures):
                row = future.result()
                writer.writerow({key: row.get(key, "") for key in summary_fields})
                results.append(row)
                done += 1
                if done == 1 or done % 10 == 0 or done == len(futures):
                    elapsed = time.perf_counter() - started
                    best = max((float(item.get("score") or -999) for item in results), default="-")
                    print(f"[retest] done={done}/{len(futures)} elapsed={elapsed:.1f}s best_score={best}", flush=True)

    top_rows = sorted(results, key=lambda row: float(row.get("score") or -999), reverse=True)
    write_csv(top_path, top_rows)
    config_path.write_text(
        json.dumps(
            {
                "input": str(input_path),
                "start_date": args.start_date,
                "end_date": args.end_date,
                "board": args.board,
                "workers": args.workers,
                "limit": args.limit,
                "note": "Legacy parameters are preserved; this retest uses the current corrected entry price and hold_days default when missing.",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"[retest] summary={summary_path}")
    print(f"[retest] top={top_path}")


def load_legacy_params(input_path: Path, limit: int) -> list[dict[str, Any]]:
    combos: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8-sig", newline="") as fh:
        for index, row in enumerate(csv.DictReader(fh), start=1):
            params = dict(DEFAULTS)
            for key in PARAM_KEYS:
                if key in row and row[key] not in ("", None):
                    params[key] = cast_value(key, row[key])
            combos.append({"legacy_rank": index, "legacy_run_id": row.get("run_id") or f"legacy_{index}", "params": params})
            if limit > 0 and len(combos) >= limit:
                break
    return combos


def cast_value(key: str, value: Any) -> Any:
    if key in {"max_positions", "hold_days"}:
        return int(float(value))
    return float(value)


def load_symbols(start_date: str, end_date: str, board: str) -> list[str]:
    db = MarketDatabase()
    rows = db.list_backtest_symbols(scan_start_with_buffer(start_date), end_date, 30, board)
    return [row["symbol"] for row in rows]


def run_combo(
    combo: dict[str, Any],
    symbols: list[str],
    start_date: str,
    end_date: str,
    years: list[int],
    initial_cash: float,
) -> dict[str, Any]:
    params = combo["params"]
    row: dict[str, Any] = {"legacy_rank": combo["legacy_rank"], "legacy_run_id": combo["legacy_run_id"], **params}
    all_metrics = run_backtest(symbols, params, start_date, end_date, initial_cash)
    add_metrics(row, "all", all_metrics)
    annual_returns: list[float] = []
    annual_drawdowns: list[float] = []
    for year in years:
        metrics = run_backtest(symbols, params, max(start_date, f"{year}-01-01"), min(end_date, f"{year}-12-31"), initial_cash)
        add_metrics(row, str(year), metrics)
        if int(metrics.get("trade_count") or 0) > 0:
            annual_returns.append(float(metrics.get("cumulative_return") or 0))
            annual_drawdowns.append(abs(float(metrics.get("max_drawdown") or 0)))
    row["active_years"] = len(annual_returns)
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
        "slippage_rate": 0.001,
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
    win_rate = float(row.get("all_win_rate") or 0)
    sharpe = float(row.get("all_sharpe") or 0)
    avg_year = sum(annual_returns) / len(annual_returns) if annual_returns else 0
    min_year = min(annual_returns) if annual_returns else -1
    worst_dd = max(annual_drawdowns) if annual_drawdowns else abs(float(row.get("all_max_drawdown") or 0))
    return round(all_return * 100 + avg_year * 30 + min_year * 25 + sharpe * 2 + win_rate * 10 - worst_dd * 35 - pstdev(annual_returns) * 20, 6)


def pstdev(values: list[float]) -> float:
    if len(values) <= 1:
        return 0.0
    avg = sum(values) / len(values)
    return math.sqrt(sum((value - avg) ** 2 for value in values) / len(values))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def result_fieldnames(years: list[int]) -> list[str]:
    metric_keys = ["cumulative_return", "annual_return", "max_drawdown", "sharpe", "win_rate", "trade_count", "avg_trade_return"]
    fields = ["legacy_rank", "legacy_run_id", "score", "active_years", "positive_years", *PARAM_KEYS]
    fields.extend(f"all_{key}" for key in metric_keys)
    for year in years:
        fields.extend(f"{year}_{key}" for key in metric_keys)
    return fields


if __name__ == "__main__":
    main()
