from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from optimize_divergence2_minute import (  # noqa: E402
    DEFAULT_MINUTE_PARQUET_ROOT,
    init_worker,
    load_years_data,
    run_combo,
)
from optimize_divergence2_params import (  # noqa: E402
    BASE_PARAMS,
    PARAM_COLUMNS,
    result_fieldnames,
    write_yearly_report,
)


DEFAULT_OUTPUT_DIR = Path(r"D:\lianghua\result\ml_candidate_verify")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run real minute backtests for params suggested by ML.")
    parser.add_argument("--candidate-csv", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--min-trades", type=int, default=40)
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--symbol-limit", type=int, default=0)
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--slippage-rate", type=float, default=0.001)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--minute-parquet-root", default=str(DEFAULT_MINUTE_PARQUET_ROOT))
    parser.add_argument("--minute-db-template", default="")
    args = parser.parse_args()

    candidate_path = Path(args.candidate_csv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    candidates = load_candidates(candidate_path, int(args.limit))
    if not candidates:
        raise SystemExit(f"No usable candidates found: {candidate_path}")

    years = list(range(int(args.start_date[:4]), int(args.end_date[:4]) + 1))
    years_data = load_years_data(years, args.start_date, args.end_date, args.board, args.symbol_limit)
    if not years_data:
        raise SystemExit("No daily data loaded.")

    stamp = time.strftime("%Y%m%d_%H%M%S")
    fieldnames = ["candidate_rank", "predicted_score", *result_fieldnames(list(years_data))]
    summary_path = output_dir / f"ml_candidate_verify_{stamp}.csv"
    top_path = output_dir / f"ml_candidate_verify_top_{stamp}.csv"
    yearly_path = output_dir / f"ml_candidate_verify_yearly_top_{stamp}.csv"
    config_path = output_dir / f"ml_candidate_verify_config_{stamp}.json"
    config = {
        "candidate_csv": str(candidate_path),
        "candidate_count": len(candidates),
        "limit": args.limit,
        "workers": args.workers,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "board": args.board,
        "years": list(years_data),
        "symbols_by_year": {year: len(item.symbols) for year, item in years_data.items()},
        "min_trades": args.min_trades,
        "top_n": args.top_n,
        "minute_parquet_root": args.minute_parquet_root,
        "minute_db_template": args.minute_db_template,
        "costs": {
            "initial_cash": args.initial_cash,
            "commission_rate": args.commission_rate,
            "slippage_rate": args.slippage_rate,
            "stamp_tax_rate": args.stamp_tax_rate,
        },
    }
    config_path.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")

    worker_args = vars(args).copy()
    worker_args["local_search"] = False
    worker_args["seed"] = 0
    print(
        f"[candidate-verify] candidates={len(candidates)} workers={args.workers} "
        f"{args.start_date}..{args.end_date} output={summary_path}",
        flush=True,
    )
    started = time.perf_counter()
    rows: list[dict[str, Any]] = []
    with summary_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        with ProcessPoolExecutor(max_workers=max(1, int(args.workers)), initializer=init_worker, initargs=(years_data, worker_args)) as pool:
            futures = {}
            for index, item in enumerate(candidates, start=1):
                combo_id = 9_000_000 + index
                futures[pool.submit(run_combo, combo_id, item["params"])] = item
            best_score: float | None = None
            for completed, future in enumerate(as_completed(futures), start=1):
                meta = futures[future]
                row = future.result()
                row["candidate_rank"] = meta.get("candidate_rank", "")
                row["predicted_score"] = meta.get("predicted_score", "")
                rows.append(row)
                writer.writerow({key: row.get(key, "") for key in fieldnames})
                score = float(row.get("score") or -999)
                best_score = score if best_score is None else max(best_score, score)
                if completed == 1 or completed % 25 == 0 or completed == len(futures):
                    elapsed = time.perf_counter() - started
                    print(
                        f"[candidate-verify] done={completed}/{len(futures)} elapsed={elapsed:.1f}s best_score={best_score:.6f}",
                        flush=True,
                    )

    top_rows = sorted(rows, key=lambda item: float(item.get("score") or -999), reverse=True)[: max(1, int(args.top_n))]
    write_rows(top_path, fieldnames, top_rows)
    write_yearly_report(yearly_path, top_rows, list(years_data))
    print(f"[candidate-verify] top={top_path}", flush=True)
    print(f"[candidate-verify] yearly top={yearly_path}", flush=True)


def load_candidates(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames or any(column not in reader.fieldnames for column in PARAM_COLUMNS):
            raise ValueError(f"Candidate CSV must contain all parameter columns: {path}")
        for row in reader:
            params = coerce_params(row)
            key = json.dumps(params, sort_keys=True)
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "candidate_rank": row.get("rank", ""),
                    "predicted_score": row.get("predicted_score", ""),
                    "params": params,
                }
            )
            if len(rows) >= limit:
                break
    return rows


def coerce_params(row: dict[str, Any]) -> dict[str, Any]:
    params = dict(BASE_PARAMS)
    for key in PARAM_COLUMNS:
        value = row.get(key, params.get(key))
        if isinstance(params.get(key), int):
            params[key] = int(float(value))
        else:
            params[key] = float(value)
    return params


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


if __name__ == "__main__":
    main()
