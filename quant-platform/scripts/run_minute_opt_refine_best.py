from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
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
    sample_params,
    write_yearly_report,
)


DEFAULT_INPUT_ROOT = Path(r"D:\lianghua\result3")
DEFAULT_OUTPUT_DIR = Path(r"D:\lianghua\result3\minute_opt_refine_best")
DEFAULT_SEEDS = [
    130013,
    130127,
    130241,
    130363,
    130489,
    130607,
    130729,
    130853,
    130987,
    131101,
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Refine top existing minute-level divergence2 params.")
    parser.add_argument("--input-root", default=str(DEFAULT_INPUT_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--top-bases", type=int, default=5, help="How many unique top parameter sets to refine.")
    parser.add_argument("--trials-per-base", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--seeds", nargs="*", type=int, default=DEFAULT_SEEDS)
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--min-trades", type=int, default=40)
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--symbol-limit", type=int, default=0)
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--slippage-rate", type=float, default=0.0)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--minute-parquet-root", default=str(DEFAULT_MINUTE_PARQUET_ROOT))
    parser.add_argument("--minute-db-template", default="")
    args = parser.parse_args()

    input_root = Path(args.input_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_rows = load_top_unique_params(input_root, int(args.top_bases))
    if not base_rows:
        raise SystemExit(f"No scored params found under {input_root}")

    years = list(range(int(args.start_date[:4]), int(args.end_date[:4]) + 1))
    years_data = load_years_data(years, args.start_date, args.end_date, args.board, args.symbol_limit)
    if not years_data:
        raise SystemExit("No daily data loaded.")

    stamp = time.strftime("%Y%m%d_%H%M%S")
    fieldnames = result_fieldnames(list(years_data))
    master_top_path = output_dir / f"minute_refine_best_top_{stamp}.csv"
    base_report_path = output_dir / f"minute_refine_best_bases_{stamp}.csv"
    config_path = output_dir / f"minute_refine_best_config_{stamp}.json"
    config_path.write_text(
        json.dumps(
            {
                "input_root": str(input_root),
                "output_dir": str(output_dir),
                "top_bases": args.top_bases,
                "trials_per_base": args.trials_per_base,
                "workers": args.workers,
                "seeds": args.seeds,
                "start_date": args.start_date,
                "end_date": args.end_date,
                "board": args.board,
                "years": list(years_data),
                "symbols_by_year": {year: len(item.symbols) for year, item in years_data.items()},
                "minute_parquet_root": args.minute_parquet_root,
                "minute_db_template": args.minute_db_template,
                "costs": {
                    "initial_cash": args.initial_cash,
                    "commission_rate": args.commission_rate,
                    "slippage_rate": args.slippage_rate,
                    "stamp_tax_rate": args.stamp_tax_rate,
                },
                "base_rows": base_rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        f"[refine-best] bases={len(base_rows)} trials_per_base={args.trials_per_base} "
        f"workers={args.workers} output={output_dir}",
        flush=True,
    )
    all_rows: list[dict[str, Any]] = []
    reports: list[dict[str, Any]] = []
    for rank, base_row in enumerate(base_rows, start=1):
        seed = int(args.seeds[(rank - 1) % len(args.seeds)]) + rank * 10_000
        report, rows = run_base(rank, seed, base_row, args, years_data, fieldnames, output_dir, stamp)
        reports.append(report)
        all_rows.extend(rows)
        print(
            f"[refine-best] base_rank={rank} seed={seed} completed={report['completed']} "
            f"best_score={report['best_score']}",
            flush=True,
        )

    top_rows = sorted(all_rows, key=lambda item: float(item.get("score") or -999), reverse=True)[: max(1, args.top_n)]
    write_rows(master_top_path, fieldnames, top_rows)
    write_base_report(base_report_path, reports)
    write_yearly_report(output_dir / f"minute_refine_best_yearly_top_{stamp}.csv", top_rows, list(years_data))
    print(f"[refine-best] master top={master_top_path}", flush=True)
    print(f"[refine-best] base report={base_report_path}", flush=True)


def load_top_unique_params(input_root: Path, limit: int) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for path in input_root.rglob("*.csv"):
        if path.stat().st_size <= 0:
            continue
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.DictReader(fh)
                if not reader.fieldnames or "score" not in reader.fieldnames:
                    continue
                if any(column not in reader.fieldnames for column in PARAM_COLUMNS):
                    continue
                for row in reader:
                    try:
                        score = float(row.get("score") or -999)
                    except ValueError:
                        continue
                    if score <= -999:
                        continue
                    params = coerce_params(row)
                    candidates.append(
                        {
                            "source_path": str(path),
                            "source_combo_id": row.get("combo_id", ""),
                            "source_score": score,
                            "source_all_cumulative_return": row.get("all_cumulative_return", ""),
                            "source_all_annual_return": row.get("all_annual_return", ""),
                            "source_all_max_drawdown": row.get("all_max_drawdown", ""),
                            "source_all_trade_count": row.get("all_trade_count", ""),
                            "params": params,
                        }
                    )
        except OSError:
            continue

    candidates.sort(key=lambda item: float(item["source_score"]), reverse=True)
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for item in candidates:
        key = json.dumps(item["params"], sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
        if len(unique) >= limit:
            break
    return unique


def coerce_params(row: dict[str, Any]) -> dict[str, Any]:
    params = dict(BASE_PARAMS)
    for key in PARAM_COLUMNS:
        value = row.get(key, params.get(key))
        if isinstance(params.get(key), int):
            params[key] = int(float(value))
        else:
            params[key] = float(value)
    return params


def run_base(
    rank: int,
    seed: int,
    base_row: dict[str, Any],
    args: argparse.Namespace,
    years_data: dict[str, Any],
    fieldnames: list[str],
    output_dir: Path,
    stamp: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    params_list = sample_params(
        int(args.trials_per_base),
        seed,
        base_params=base_row["params"],
        local_search=True,
    )
    worker_args = vars(args).copy()
    worker_args["seed"] = seed
    worker_args["local_search"] = True
    summary_path = output_dir / f"minute_refine_base{rank:02d}_seed{seed}_{stamp}.csv"
    top_path = output_dir / f"minute_refine_base{rank:02d}_seed{seed}_top_{stamp}.csv"
    yearly_path = output_dir / f"minute_refine_base{rank:02d}_seed{seed}_yearly_top_{stamp}.csv"
    max_workers = max(1, int(args.workers))
    rows: list[dict[str, Any]] = []
    submitted = 0
    completed = 0
    best_score: float | None = None
    best_combo = ""
    started = time.perf_counter()

    print(
        f"[refine-best] base_rank={rank} source_score={base_row['source_score']} "
        f"source_combo={base_row['source_combo_id']} seed={seed} combos={len(params_list)}",
        flush=True,
    )
    with summary_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        pool = ProcessPoolExecutor(max_workers=max_workers, initializer=init_worker, initargs=(years_data, worker_args))
        futures: dict[Any, int] = {}
        try:
            submitted = submit_more(pool, futures, params_list, submitted, rank, max_workers)
            while futures:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    combo_number = futures.pop(future)
                    row = future.result()
                    rows.append(row)
                    writer.writerow({key: row.get(key, "") for key in fieldnames})
                    completed += 1
                    score = float(row.get("score") or -999)
                    if best_score is None or score > best_score:
                        best_score = score
                        best_combo = str(row.get("combo_id", combo_number))
                    if completed == 1 or completed % 25 == 0 or completed == len(params_list):
                        print(
                            f"[refine-best] base_rank={rank} done={completed}/{len(params_list)} "
                            f"submitted={submitted} best={best_score:.6f}",
                            flush=True,
                        )
                fh.flush()
                submitted = submit_more(pool, futures, params_list, submitted, rank, max_workers)
        finally:
            pool.shutdown(wait=True, cancel_futures=True)

    top_rows = sorted(rows, key=lambda item: float(item.get("score") or -999), reverse=True)[: max(1, args.top_n)]
    write_rows(top_path, fieldnames, top_rows)
    write_yearly_report(yearly_path, top_rows, list(years_data))
    report = {
        "base_rank": rank,
        "seed": seed,
        "completed": completed,
        "submitted": submitted,
        "source_score": base_row["source_score"],
        "source_combo_id": base_row["source_combo_id"],
        "source_path": base_row["source_path"],
        "best_score": "" if best_score is None else round(best_score, 6),
        "best_combo_id": best_combo,
        "elapsed_seconds": round(time.perf_counter() - started, 1),
        "summary_path": str(summary_path),
        "top_path": str(top_path),
        "yearly_path": str(yearly_path),
    }
    return report, rows


def submit_more(
    pool: ProcessPoolExecutor,
    futures: dict[Any, int],
    params_list: list[dict[str, Any]],
    submitted: int,
    base_rank: int,
    max_workers: int,
) -> int:
    while submitted < len(params_list) and len(futures) < max_workers:
        submitted += 1
        combo_id = base_rank * 1_000_000 + submitted
        future = pool.submit(run_combo, combo_id, params_list[submitted - 1])
        futures[future] = combo_id
    return submitted


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_base_report(path: Path, reports: list[dict[str, Any]]) -> None:
    fields = [
        "base_rank",
        "seed",
        "completed",
        "submitted",
        "source_score",
        "source_combo_id",
        "source_path",
        "best_score",
        "best_combo_id",
        "elapsed_seconds",
        "summary_path",
        "top_path",
        "yearly_path",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(reports)


if __name__ == "__main__":
    main()
