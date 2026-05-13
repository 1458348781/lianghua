from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = Path(r"D:\lianghua\result2")


@dataclass
class Stage:
    name: str
    mode: str
    trials: int
    workers: int
    seed_offset: int


def main() -> None:
    parser = argparse.ArgumentParser(description="Automatically run staged divergence2 parameter optimization.")
    parser.add_argument("--start-date", default="2015-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--base-seed", type=int, default=20260510)
    parser.add_argument("--workers", type=int, default=0, help="Override worker count for every stage.")
    parser.add_argument("--min-score", type=float, default=0.0)
    parser.add_argument("--min-trades", type=int, default=80)
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--symbol-limit", type=int, default=0)
    parser.add_argument("--include-overnight", action="store_true", help="Allow automatic 8000/10000 trial overnight runs.")
    parser.add_argument("--stop-after", choices=["smoke", "direction", "formal", "formal3000", "overnight"], default="formal3000")
    args = parser.parse_args()

    run_dir = Path(args.output_dir) / f"auto_divergence2_{time.strftime('%Y%m%d_%H%M%S')}"
    run_dir.mkdir(parents=True, exist_ok=True)
    print(f"[auto] output={run_dir}", flush=True)

    summary: list[dict[str, Any]] = []
    best_overall: dict[str, Any] | None = None

    smoke = run_stage(
        Stage("smoke_100", "smoke", 100, 4, 0),
        args,
        run_dir,
    )
    summary.append(smoke)
    best_overall = better(best_overall, smoke)
    write_auto_files(run_dir, summary, best_overall)
    if args.stop_after == "smoke":
        return

    direction_a = run_stage(
        Stage("direction_500_a", "direction", 500, 6, 1),
        args,
        run_dir,
    )
    summary.append(direction_a)
    best_overall = better(best_overall, direction_a)
    write_auto_files(run_dir, summary, best_overall)
    if args.stop_after == "direction":
        return

    best_direction = direction_a
    if should_run_second_direction(smoke, direction_a):
        print("[auto] direction_500_a did not clearly beat smoke; run another 500 with a different seed.", flush=True)
        direction_b = run_stage(
            Stage("direction_500_b", "direction", 500, 6, 2),
            args,
            run_dir,
        )
        summary.append(direction_b)
        best_overall = better(best_overall, direction_b)
        best_direction = better(direction_a, direction_b) or direction_a
        write_auto_files(run_dir, summary, best_overall)

    if not is_candidate_good_enough(best_direction, args.min_score, args.min_trades):
        print("[auto] stop: direction stage is not good enough for larger runs.", flush=True)
        write_auto_files(run_dir, summary, best_overall)
        return

    formal = run_stage(
        Stage("formal_2000", "formal", 2000, 8, 3),
        args,
        run_dir,
    )
    summary.append(formal)
    best_overall = better(best_overall, formal)
    write_auto_files(run_dir, summary, best_overall)
    if args.stop_after == "formal":
        return

    if should_run_formal3000(best_direction, formal):
        print("[auto] formal_2000 still improved or needs more coverage; run formal_3000.", flush=True)
        formal3000 = run_stage(
            Stage("formal_3000", "formal", 3000, 8, 4),
            args,
            run_dir,
        )
        summary.append(formal3000)
        best_overall = better(best_overall, formal3000)
        write_auto_files(run_dir, summary, best_overall)
    else:
        print("[auto] formal_2000 did not improve much; stop before formal_3000.", flush=True)
        return

    if args.stop_after == "formal3000":
        return

    if args.include_overnight:
        overnight = run_stage(
            Stage("overnight_8000", "overnight", 8000, 8, 5),
            args,
            run_dir,
        )
        summary.append(overnight)
        best_overall = better(best_overall, overnight)
        write_auto_files(run_dir, summary, best_overall)


def run_stage(stage: Stage, args: argparse.Namespace, run_dir: Path) -> dict[str, Any]:
    stage_dir = run_dir / stage.name
    stage_dir.mkdir(parents=True, exist_ok=True)
    seed = args.base_seed + stage.seed_offset
    workers = int(args.workers or stage.workers)
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "optimize_divergence2_params.py"),
        "--mode",
        stage.mode,
        "--trials",
        str(stage.trials),
        "--workers",
        str(workers),
        "--seed",
        str(seed),
        "--start-date",
        args.start_date,
        "--end-date",
        args.end_date,
        "--board",
        args.board,
        "--top-n",
        str(args.top_n),
        "--min-trades",
        str(args.min_trades),
        "--symbol-limit",
        str(args.symbol_limit),
        "--output-dir",
        str(stage_dir),
    ]
    print(f"[auto] run {stage.name}: trials={stage.trials} workers={workers} seed={seed}", flush=True)
    started = time.perf_counter()
    subprocess.run(cmd, cwd=str(ROOT), check=True)
    elapsed = time.perf_counter() - started
    top_path = newest(stage_dir, "divergence2_opt_top_*.csv")
    summary_path = newest(stage_dir, "divergence2_opt_*.csv", exclude="top")
    best = read_best(top_path)
    result = {
        "stage": stage.name,
        "mode": stage.mode,
        "trials": stage.trials,
        "workers": workers,
        "seed": seed,
        "elapsed_seconds": round(elapsed, 1),
        "top_path": str(top_path),
        "summary_path": str(summary_path),
        "best_score": best.get("score", -999.0),
        "best_trades": best.get("all_trade_count", 0),
        "best_return": best.get("all_cumulative_return", 0.0),
        "best_drawdown": best.get("all_max_drawdown", 0.0),
        "best_params": {key: best[key] for key in best if key in PARAM_KEYS},
    }
    print(
        f"[auto] {stage.name} best_score={result['best_score']} trades={result['best_trades']} "
        f"return={result['best_return']} drawdown={result['best_drawdown']}",
        flush=True,
    )
    return result


PARAM_KEYS = {
    "max_positions",
    "hold_days",
    "stop_loss",
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
    "day2_max_volume_ratio",
    "day2_min_close_position",
    "day2_max_upper_shadow",
    "day2_min_close_vs_day1_close",
    "entry_min_open_gap_pct_chg",
    "entry_max_open_gap_pct_chg",
    "entry_min_high_from_open_pct_chg",
}


def newest(folder: Path, pattern: str, exclude: str | None = None) -> Path:
    files = list(folder.glob(pattern))
    if exclude:
        files = [path for path in files if exclude not in path.name]
    if not files:
        raise FileNotFoundError(f"No {pattern} in {folder}")
    return max(files, key=lambda path: path.stat().st_mtime)


def read_best(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return {"score": -999.0}
    row = rows[0]
    for key, value in list(row.items()):
        if value is None or value == "":
            continue
        try:
            if "." in value or "e" in value.lower():
                row[key] = float(value)
            else:
                row[key] = int(value)
        except ValueError:
            pass
    return row


def better(left: dict[str, Any] | None, right: dict[str, Any] | None) -> dict[str, Any] | None:
    if left is None:
        return right
    if right is None:
        return left
    return right if float(right.get("best_score") or -999) > float(left.get("best_score") or -999) else left


def should_run_second_direction(smoke: dict[str, Any], direction: dict[str, Any]) -> bool:
    smoke_score = float(smoke.get("best_score") or -999)
    direction_score = float(direction.get("best_score") or -999)
    if direction_score <= 0:
        return True
    if smoke_score <= 0:
        return False
    return direction_score < smoke_score * 1.10


def is_candidate_good_enough(row: dict[str, Any], min_score: float, min_trades: int) -> bool:
    return float(row.get("best_score") or -999) >= min_score and int(float(row.get("best_trades") or 0)) >= min_trades


def should_run_formal3000(direction: dict[str, Any], formal: dict[str, Any]) -> bool:
    direction_score = float(direction.get("best_score") or -999)
    formal_score = float(formal.get("best_score") or -999)
    if formal_score <= 0:
        return False
    if direction_score <= 0:
        return True
    return formal_score >= direction_score * 0.95


def write_auto_files(run_dir: Path, summary: list[dict[str, Any]], best: dict[str, Any] | None) -> None:
    summary_path = run_dir / "auto_stage_summary.csv"
    fields = [
        "stage",
        "mode",
        "trials",
        "workers",
        "seed",
        "elapsed_seconds",
        "best_score",
        "best_trades",
        "best_return",
        "best_drawdown",
        "top_path",
        "summary_path",
    ]
    with summary_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in summary:
            writer.writerow({field: row.get(field, "") for field in fields})
    if best:
        (run_dir / "auto_best_overall.json").write_text(json.dumps(best, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
