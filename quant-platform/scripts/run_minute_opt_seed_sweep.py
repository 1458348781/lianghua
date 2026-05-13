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
    load_base_params,
    result_fieldnames,
    sample_params,
    write_yearly_report,
)


DEFAULT_SEEDS = [
    910231,
    918577,
    927113,
    935981,
    944729,
    952763,
    961337,
    973451,
    984127,
    995669,
]
DEFAULT_OUTPUT_DIR = Path(r"D:\lianghua\result3\minute_opt_seed_sweep")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run minute-level divergence2 optimization across multiple seeds with early stop."
    )
    parser.add_argument("--trials-per-seed", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--early-stop", type=int, default=250, help="Stop a seed after this many completed combos without a new best score.")
    parser.add_argument("--seeds", nargs="*", type=int, default=DEFAULT_SEEDS)
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--min-trades", type=int, default=40)
    parser.add_argument("--top-n", type=int, default=30)
    parser.add_argument("--symbol-limit", type=int, default=0)
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--slippage-rate", type=float, default=0.001)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--minute-parquet-root", default=str(DEFAULT_MINUTE_PARQUET_ROOT))
    parser.add_argument("--minute-db-template", default="")
    parser.add_argument("--base-params-json", default="")
    parser.add_argument("--local-search", action="store_true")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    base_params = load_base_params(Path(args.base_params_json)) if args.base_params_json else dict(BASE_PARAMS)
    years = list(range(int(args.start_date[:4]), int(args.end_date[:4]) + 1))
    years_data = load_years_data(years, args.start_date, args.end_date, args.board, args.symbol_limit)
    if not years_data:
        raise SystemExit("No daily data loaded.")

    stamp = time.strftime("%Y%m%d_%H%M%S")
    fieldnames = result_fieldnames(list(years_data))
    config_path = output_dir / f"minute_seed_sweep_config_{stamp}.json"
    master_top_path = output_dir / f"minute_seed_sweep_top_{stamp}.csv"
    seed_report_path = output_dir / f"minute_seed_sweep_seeds_{stamp}.csv"
    config = {
        "trials_per_seed": args.trials_per_seed,
        "workers": args.workers,
        "early_stop": args.early_stop,
        "seeds": args.seeds,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "board": args.board,
        "years": list(years_data),
        "symbols_by_year": {year: len(item.symbols) for year, item in years_data.items()},
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
        f"[seed-sweep] seeds={len(args.seeds)} trials_per_seed={args.trials_per_seed} "
        f"workers={args.workers} early_stop={args.early_stop} output={output_dir}",
        flush=True,
    )
    all_rows: list[dict[str, Any]] = []
    seed_reports: list[dict[str, Any]] = []
    for seed_index, seed in enumerate(args.seeds, start=1):
        report, rows = run_seed(seed_index, seed, args, years_data, fieldnames, base_params, output_dir, stamp)
        seed_reports.append(report)
        all_rows.extend(rows)
        best = report.get("best_score", "")
        print(
            f"[seed-sweep] seed={seed} finished completed={report['completed']} "
            f"early_stopped={report['early_stopped']} best_score={best}",
            flush=True,
        )

    top_rows = sorted(all_rows, key=lambda item: float(item.get("score") or -999), reverse=True)[: max(1, args.top_n)]
    write_rows(master_top_path, fieldnames, top_rows)
    write_seed_report(seed_report_path, seed_reports)
    write_yearly_report(output_dir / f"minute_seed_sweep_yearly_top_{stamp}.csv", top_rows, list(years_data))
    print(f"[seed-sweep] master top={master_top_path}", flush=True)
    print(f"[seed-sweep] seed report={seed_report_path}", flush=True)


def run_seed(
    seed_index: int,
    seed: int,
    args: argparse.Namespace,
    years_data: dict[str, Any],
    fieldnames: list[str],
    base_params: dict[str, Any],
    output_dir: Path,
    stamp: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    params_list = sample_params(
        int(args.trials_per_seed),
        seed,
        base_params=base_params,
        local_search=bool(args.local_search),
    )
    summary_path = output_dir / f"minute_seed_{seed}_{stamp}.csv"
    top_path = output_dir / f"minute_seed_{seed}_top_{stamp}.csv"
    yearly_path = output_dir / f"minute_seed_{seed}_yearly_top_{stamp}.csv"
    worker_args = vars(args).copy()
    worker_args["seed"] = seed

    started = time.perf_counter()
    rows: list[dict[str, Any]] = []
    best_score: float | None = None
    best_combo = ""
    stale_completed = 0
    submitted = 0
    completed = 0
    early_stopped = False
    max_workers = max(1, int(args.workers))

    print(f"[seed-sweep] seed={seed} start combos={len(params_list)}", flush=True)
    with summary_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        pool = ProcessPoolExecutor(max_workers=max_workers, initializer=init_worker, initargs=(years_data, worker_args))
        futures: dict[Any, int] = {}
        try:
            submitted = submit_more(pool, futures, params_list, submitted, seed_index, max_workers)
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
                        stale_completed = 0
                    else:
                        stale_completed += 1

                    if completed == 1 or completed % 25 == 0:
                        print(
                            f"[seed-sweep] seed={seed} done={completed}/{len(params_list)} "
                            f"submitted={submitted} best={best_score:.6f} stale={stale_completed}",
                            flush=True,
                        )

                    if stale_completed >= int(args.early_stop):
                        early_stopped = True
                        break
                fh.flush()
                if early_stopped:
                    for future in futures:
                        future.cancel()
                    break
                submitted = submit_more(pool, futures, params_list, submitted, seed_index, max_workers)
        finally:
            pool.shutdown(wait=True, cancel_futures=True)

    top_rows = sorted(rows, key=lambda item: float(item.get("score") or -999), reverse=True)[: max(1, args.top_n)]
    write_rows(top_path, fieldnames, top_rows)
    write_yearly_report(yearly_path, top_rows, list(years_data))
    elapsed = time.perf_counter() - started
    report = {
        "seed": seed,
        "completed": completed,
        "submitted": submitted,
        "early_stopped": early_stopped,
        "best_score": "" if best_score is None else round(best_score, 6),
        "best_combo_id": best_combo,
        "stale_completed": stale_completed,
        "elapsed_seconds": round(elapsed, 1),
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
    seed_index: int,
    max_workers: int,
) -> int:
    while submitted < len(params_list) and len(futures) < max_workers:
        submitted += 1
        combo_id = seed_index * 1_000_000 + submitted
        future = pool.submit(run_combo, combo_id, params_list[submitted - 1])
        futures[future] = combo_id
    return submitted


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def write_seed_report(path: Path, reports: list[dict[str, Any]]) -> None:
    fields = [
        "seed",
        "completed",
        "submitted",
        "early_stopped",
        "best_score",
        "best_combo_id",
        "stale_completed",
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
