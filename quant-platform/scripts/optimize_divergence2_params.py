from __future__ import annotations

import argparse
import csv
import json
import math
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant_platform.backtest import BacktestEngine  # noqa: E402
from quant_platform.screener import scan_start_with_buffer  # noqa: E402
from quant_platform.storage import MarketDatabase  # noqa: E402
from quant_platform.strategy import create_strategy  # noqa: E402


RESULT_DIR = Path(r"D:\lianghua\result2")
_WORKER_YEARS_DATA: dict[str, "YearData"] = {}
_WORKER_ARGS: dict[str, Any] = {}

BASE_PARAMS: dict[str, Any] = {
    "max_positions": 2,
    "hold_days": 2,
    "stop_loss": -0.025,
    "min_price": 3,
    "max_price": 500,
    "min_turnover": 3.0,
    "max_turnover": 24.4,
    "day1_min_volume_ratio": 0.85,
    "day1_max_volume_ratio": 5.59,
    "range_min_amplitude_30": 0.108,
    "range_min_return_20": 0.039,
    "day2_min_pct_chg": -1.6,
    "day2_max_pct_chg": 8.4,
    "day2_max_volume_ratio": 2.15,
    "day2_min_close_position": 0.51,
    "day2_max_upper_shadow": 0.075,
    "day2_min_close_vs_day1_close": 0.954,
    "entry_min_open_gap_pct_chg": 1.2,
    "entry_max_open_gap_pct_chg": 6.3,
    "entry_min_high_from_open_pct_chg": 2.7,
}

MODE_DEFAULTS = {
    "smoke": {"trials": 100, "workers": 4},
    "direction": {"trials": 500, "workers": 6},
    "formal": {"trials": 2000, "workers": 8},
    "overnight": {"trials": 8000, "workers": 8},
}

PARAM_COLUMNS = list(BASE_PARAMS)


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Optimize divergence2 parameters.")
    parser.add_argument("--mode", choices=list(MODE_DEFAULTS), default="smoke")
    parser.add_argument("--trials", type=int, default=0, help="Override mode trials.")
    parser.add_argument("--workers", type=int, default=0, help="Override mode workers.")
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--output-dir", default=str(RESULT_DIR))
    parser.add_argument("--seed", type=int, default=20260510)
    parser.add_argument("--min-trades", type=int, default=80)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--symbol-limit", type=int, default=0, help="Only for smoke testing.")
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--slippage-rate", type=float, default=0.0005)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--base-params-json", default="", help="JSON with best_params, base_params, or raw params.")
    parser.add_argument("--local-search", action="store_true", help="Sample near --base-params-json instead of broad random search.")
    args = parser.parse_args()

    defaults = MODE_DEFAULTS[args.mode]
    trials = args.trials or defaults["trials"]
    workers = args.workers or defaults["workers"]
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_params = load_base_params(Path(args.base_params_json)) if args.base_params_json else dict(BASE_PARAMS)
    params_list = sample_params(trials, args.seed, base_params=base_params, local_search=args.local_search)
    years = list(range(int(args.start_date[:4]), int(args.end_date[:4]) + 1))
    years_data = load_years_data(
        years=years,
        start_date=args.start_date,
        end_date=args.end_date,
        board=args.board,
        min_rows=30,
        symbol_limit=args.symbol_limit,
    )
    symbols_count = sum(len(item.symbols) for item in years_data.values())
    if not years_data or symbols_count <= 0:
        raise SystemExit("No symbols found. Please check database and date range.")
    stamp = time.strftime("%Y%m%d_%H%M%S")
    summary_path = output_dir / f"divergence2_opt_{args.mode}_{stamp}.csv"
    top_path = output_dir / f"divergence2_opt_top_{args.mode}_{stamp}.csv"
    config_path = output_dir / f"divergence2_opt_config_{args.mode}_{stamp}.json"
    yearly_path = output_dir / f"divergence2_opt_yearly_top_{args.mode}_{stamp}.csv"

    config = {
        "mode": args.mode,
        "trials": trials,
        "workers": workers,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "board": args.board,
        "years": list(years_data),
        "symbols_by_year": {year: len(item.symbols) for year, item in years_data.items()},
        "seed": args.seed,
        "min_trades": args.min_trades,
        "top_n": args.top_n,
        "costs": {
            "initial_cash": args.initial_cash,
            "commission_rate": args.commission_rate,
            "slippage_rate": args.slippage_rate,
            "stamp_tax_rate": args.stamp_tax_rate,
        },
        "base_params_json": args.base_params_json,
        "local_search": args.local_search,
        "base_params": base_params,
    }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        f"[opt] mode={args.mode} trials={trials} workers={workers} years={','.join(years_data)} "
        f"{args.start_date}..{args.end_date} output={summary_path}",
        flush=True,
    )

    started = time.perf_counter()
    rows: list[dict[str, Any]] = []
    fieldnames = result_fieldnames(list(years_data))
    with summary_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        with ProcessPoolExecutor(max_workers=max(1, workers), initializer=init_worker, initargs=(years_data, vars(args))) as pool:
            futures = [
                pool.submit(run_combo, idx, params)
                for idx, params in enumerate(params_list, start=1)
            ]
            completed = 0
            best_score = None
            for future in as_completed(futures):
                row = future.result()
                rows.append(row)
                writer.writerow({key: row.get(key, "") for key in fieldnames})
                completed += 1
                score = float(row.get("score") or -999)
                best_score = score if best_score is None else max(best_score, score)
                if completed == 1 or completed % 20 == 0 or completed == len(futures):
                    elapsed = time.perf_counter() - started
                    print(f"[opt] done={completed}/{len(futures)} elapsed={elapsed:.1f}s best_score={best_score:.4f}", flush=True)

    top_rows = sorted(rows, key=lambda item: float(item.get("score") or -999), reverse=True)[: max(1, args.top_n)]
    with top_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in top_rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})

    print(f"[opt] top={top_path}", flush=True)
    print(f"[opt] yearly top -> {yearly_path}", flush=True)
    write_yearly_report(yearly_path, top_rows, list(years_data))
    print(f"[opt] done. summary={summary_path}", flush=True)


def load_base_params(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise ValueError(f"Base params JSON must contain an object: {path}")
    if isinstance(payload.get("best_params"), dict):
        payload = payload["best_params"]
    elif isinstance(payload.get("base_params"), dict):
        payload = payload["base_params"]

    params = dict(BASE_PARAMS)
    for key in PARAM_COLUMNS:
        if key in payload:
            params[key] = payload[key]
    return params


def sample_params(
    trials: int,
    seed: int,
    base_params: dict[str, Any] | None = None,
    local_search: bool = False,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    base = dict(base_params or BASE_PARAMS)
    if local_search:
        add_params(rows, seen, base)

    attempts = 0
    while len(rows) < trials and attempts < trials * 50:
        attempts += 1
        params = sample_local_params(rng, base) if local_search else sample_global_params(rng, base)
        add_params(rows, seen, params)
    return rows


def add_params(rows: list[dict[str, Any]], seen: set[str], params: dict[str, Any]) -> bool:
    if params["min_turnover"] >= params["max_turnover"]:
        return False
    if params["day1_min_volume_ratio"] >= params["day1_max_volume_ratio"]:
        return False
    if params["day2_min_pct_chg"] >= params["day2_max_pct_chg"]:
        return False
    if params["entry_min_open_gap_pct_chg"] >= params["entry_max_open_gap_pct_chg"]:
        return False
    key = json.dumps(params, sort_keys=True)
    if key in seen:
        return False
    seen.add(key)
    rows.append(params)
    return True


def sample_global_params(rng: random.Random, base_params: dict[str, Any]) -> dict[str, Any]:
    params = dict(base_params)
    params.update(
        {
            "max_positions": rng.choice([2, 3, 4, 5]),
            "hold_days": rng.choice([2, 3, 4, 5]),
            "stop_loss": round(rng.choice([-0.02, -0.025, -0.03, -0.035, -0.04, -0.05]), 4),
            "min_turnover": round(rng.uniform(1.0, 5.0), 1),
            "max_turnover": round(rng.uniform(18.0, 35.0), 1),
            "day1_min_volume_ratio": round(rng.uniform(0.8, 1.8), 2),
            "day1_max_volume_ratio": round(rng.uniform(3.0, 7.0), 2),
            "range_min_amplitude_30": round(rng.uniform(0.10, 0.30), 3),
            "range_min_return_20": round(rng.uniform(-0.05, 0.12), 3),
            "day2_min_pct_chg": round(rng.uniform(-5.0, -1.0), 1),
            "day2_max_pct_chg": round(rng.uniform(5.5, 8.5), 1),
            "day2_max_volume_ratio": round(rng.uniform(1.3, 2.2), 2),
            "day2_min_close_position": round(rng.uniform(0.35, 0.70), 2),
            "day2_max_upper_shadow": round(rng.uniform(0.03, 0.10), 3),
            "day2_min_close_vs_day1_close": round(rng.uniform(0.94, 1.01), 3),
            "entry_min_open_gap_pct_chg": round(rng.uniform(0.0, 2.5), 1),
            "entry_max_open_gap_pct_chg": round(rng.uniform(3.0, 7.0), 1),
            "entry_min_high_from_open_pct_chg": round(rng.uniform(2.5, 6.0), 1),
        }
    )
    return params


def sample_local_params(rng: random.Random, base_params: dict[str, Any]) -> dict[str, Any]:
    params = dict(base_params)
    params.update(
        {
            "max_positions": sample_int_near(rng, base_params["max_positions"], 1, 2, 5),
            "hold_days": sample_int_near(rng, base_params["hold_days"], 1, 2, 5),
            "stop_loss": sample_discrete_near(
                rng,
                float(base_params["stop_loss"]),
                [-0.02, -0.025, -0.03, -0.035, -0.04, -0.05],
                radius=1,
            ),
            "min_turnover": sample_float_near(rng, base_params["min_turnover"], 1.0, 1.0, 5.0, 1),
            "max_turnover": sample_float_near(rng, base_params["max_turnover"], 4.0, 18.0, 35.0, 1),
            "day1_min_volume_ratio": sample_float_near(rng, base_params["day1_min_volume_ratio"], 0.25, 0.8, 1.8, 2),
            "day1_max_volume_ratio": sample_float_near(rng, base_params["day1_max_volume_ratio"], 0.8, 3.0, 7.0, 2),
            "range_min_amplitude_30": sample_float_near(rng, base_params["range_min_amplitude_30"], 0.04, 0.10, 0.30, 3),
            "range_min_return_20": sample_float_near(rng, base_params["range_min_return_20"], 0.03, -0.05, 0.12, 3),
            "day2_min_pct_chg": sample_float_near(rng, base_params["day2_min_pct_chg"], 0.8, -5.0, -1.0, 1),
            "day2_max_pct_chg": sample_float_near(rng, base_params["day2_max_pct_chg"], 0.8, 5.5, 8.5, 1),
            "day2_max_volume_ratio": sample_float_near(rng, base_params["day2_max_volume_ratio"], 0.25, 1.3, 2.2, 2),
            "day2_min_close_position": sample_float_near(rng, base_params["day2_min_close_position"], 0.08, 0.35, 0.70, 2),
            "day2_max_upper_shadow": sample_float_near(rng, base_params["day2_max_upper_shadow"], 0.02, 0.03, 0.10, 3),
            "day2_min_close_vs_day1_close": sample_float_near(rng, base_params["day2_min_close_vs_day1_close"], 0.02, 0.94, 1.01, 3),
            "entry_min_open_gap_pct_chg": sample_float_near(rng, base_params["entry_min_open_gap_pct_chg"], 0.4, 0.0, 2.5, 1),
            "entry_max_open_gap_pct_chg": sample_float_near(rng, base_params["entry_max_open_gap_pct_chg"], 0.7, 3.0, 7.0, 1),
            "entry_min_high_from_open_pct_chg": sample_float_near(rng, base_params["entry_min_high_from_open_pct_chg"], 0.6, 2.5, 6.0, 1),
        }
    )
    return params


def sample_int_near(rng: random.Random, center: Any, radius: int, low: int, high: int) -> int:
    value = int(center)
    choices = [item for item in range(value - radius, value + radius + 1) if low <= item <= high]
    return rng.choice(choices or [min(max(value, low), high)])


def sample_discrete_near(rng: random.Random, center: float, choices: list[float], radius: int) -> float:
    nearest = min(range(len(choices)), key=lambda idx: abs(choices[idx] - center))
    start = max(0, nearest - radius)
    end = min(len(choices), nearest + radius + 1)
    return round(rng.choice(choices[start:end]), 4)


def sample_float_near(
    rng: random.Random,
    center: Any,
    radius: float,
    low: float,
    high: float,
    digits: int,
) -> float:
    value = float(center)
    sampled = rng.uniform(max(low, value - radius), min(high, value + radius))
    return round(sampled, digits)


def load_years_data(
    years: list[int],
    start_date: str,
    end_date: str,
    board: str,
    min_rows: int,
    symbol_limit: int,
) -> dict[str, YearData]:
    db = MarketDatabase()
    loaded: dict[str, YearData] = {}
    for year in years:
        year_start = max(start_date, f"{year}-01-01")
        year_end = min(end_date, f"{year}-12-31")
        if year_start > year_end:
            continue
        warmup_start = scan_start_with_buffer(year_start)
        pool = db.list_backtest_symbols(year_start, year_end, min_rows, board)
        symbols = [item["symbol"] for item in pool]
        if symbol_limit > 0:
            symbols = symbols[:symbol_limit]
        print(f"[load] {year} pool={len(symbols)} warmup={warmup_start} backtest={year_start}..{year_end}", flush=True)
        history = db.query_many(symbols, warmup_start, year_end)
        symbols = [symbol for symbol in symbols if symbol in history]
        calendar = db.get_calendar(symbols, year_start, year_end)
        profiles = db.symbol_profiles(symbols)
        loaded[str(year)] = YearData(
            year=str(year),
            start_date=year_start,
            end_date=year_end,
            symbols=symbols,
            history=history,
            calendar=calendar,
            names={symbol: profiles.get(symbol, {}).get("name", "") for symbol in symbols},
        )
    return loaded


def init_worker(years_data: dict[str, YearData], args: dict[str, Any]) -> None:
    global _WORKER_YEARS_DATA, _WORKER_ARGS
    _WORKER_YEARS_DATA = years_data
    _WORKER_ARGS = args


def run_combo(
    combo_id: int,
    params: dict[str, Any],
) -> dict[str, Any]:
    row: dict[str, Any] = {"combo_id": combo_id, **params}
    try:
        yearly: dict[str, dict[str, Any]] = {}
        for year, data in _WORKER_YEARS_DATA.items():
            yearly[year] = run_year_backtest(data, params, _WORKER_ARGS)
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


def run_year_backtest(
    data: YearData,
    params: dict[str, Any],
    args: dict[str, Any],
) -> dict[str, Any]:
    body = {
        "strategy_name": "divergence_tactic",
        "symbols": data.symbols,
        "start_date": data.start_date,
        "end_date": data.end_date,
        "data_start_date": scan_start_with_buffer(data.start_date),
        "initial_cash": float(args.get("initial_cash") or 1_000_000),
        "commission_rate": float(args.get("commission_rate") or 0.0003),
        "slippage_rate": float(args.get("slippage_rate") or 0.0005),
        "stamp_tax_rate": float(args.get("stamp_tax_rate") or 0.001),
        "params": params,
        "universe": "all",
        "universe_symbol_count": len(data.symbols),
    }
    engine = BacktestEngine(InMemoryPortal(data), create_strategy("divergence_tactic", params), body)
    result = engine.run()
    metrics = dict(result.get("metrics", {}))
    metrics["buy_count"] = sum(1 for trade in result.get("trades", []) if trade.get("side") == "buy")
    metrics["sell_count"] = sum(1 for trade in result.get("trades", []) if trade.get("side") == "sell")
    return metrics


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


def score_metrics(metrics: dict[str, Any], min_trades: int) -> float:
    annual = float(metrics.get("annual_return") or 0)
    cumulative = float(metrics.get("cumulative_return") or 0)
    drawdown = abs(float(metrics.get("max_drawdown") or 0))
    sharpe = float(metrics.get("sharpe") or 0)
    win_rate = float(metrics.get("win_rate") or 0)
    avg_trade = float(metrics.get("avg_trade_return") or 0)
    trades = int(metrics.get("trade_count") or 0)
    trade_penalty = max(0.0, (min_trades - trades) / max(1, min_trades))
    if trades <= 0:
        return -999.0
    score = (
        annual * 35
        + cumulative * 10
        + sharpe * 3
        + win_rate * 8
        + avg_trade * 80
        - drawdown * 35
        - trade_penalty * 25
    )
    return round(score, 6)


def write_yearly_report(
    path: Path,
    top_rows: list[dict[str, Any]],
    years: list[str],
) -> None:
    fields = ["rank", "combo_id", *PARAM_COLUMNS]
    for year in years:
        fields.extend([f"{year}_return", f"{year}_drawdown", f"{year}_win_rate", f"{year}_trades", f"{year}_score"])
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for rank, row in enumerate(top_rows, start=1):
            out = {"rank": rank, "combo_id": row["combo_id"]}
            out.update({key: row.get(key, "") for key in PARAM_COLUMNS})
            for year in years:
                metrics = unprefix_metrics(year, row)
                out[f"{year}_return"] = metrics.get("cumulative_return", 0)
                out[f"{year}_drawdown"] = metrics.get("max_drawdown", 0)
                out[f"{year}_win_rate"] = metrics.get("win_rate", 0)
                out[f"{year}_trades"] = metrics.get("trade_count", 0)
                out[f"{year}_score"] = score_metrics(metrics, 5)
            writer.writerow(out)


def unprefix_metrics(prefix: str, row: dict[str, Any]) -> dict[str, Any]:
    names = [
        "cumulative_return",
        "annual_return",
        "max_drawdown",
        "sharpe",
        "win_rate",
        "avg_trade_return",
        "best_trade_return",
        "worst_trade_return",
        "trade_count",
        "buy_count",
        "sell_count",
        "final_value",
    ]
    return {name: row.get(f"{prefix}_{name}", "") for name in names}


def prefix_metrics(prefix: str, metrics: dict[str, Any]) -> dict[str, Any]:
    names = [
        "cumulative_return",
        "annual_return",
        "max_drawdown",
        "sharpe",
        "win_rate",
        "avg_trade_return",
        "best_trade_return",
        "worst_trade_return",
        "trade_count",
        "buy_count",
        "sell_count",
        "final_value",
    ]
    return {f"{prefix}_{name}": metrics.get(name, "") for name in names}


def result_fieldnames(year_prefixes: list[str]) -> list[str]:
    return [
        "combo_id",
        *PARAM_COLUMNS,
        "score",
        *prefix_metrics("all", {}).keys(),
        *(key for year in year_prefixes for key in prefix_metrics(year, {}).keys()),
        "error",
    ]


if __name__ == "__main__":
    main()
