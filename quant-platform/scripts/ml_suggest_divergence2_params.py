from __future__ import annotations

import argparse
import csv
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from lightgbm import LGBMRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from optimize_divergence2_params import (  # noqa: E402
    BASE_PARAMS,
    PARAM_COLUMNS,
    add_params,
    sample_global_params,
    sample_local_params,
)


DEFAULT_INPUT_ROOT = Path(r"D:\lianghua\result")
DEFAULT_OUTPUT_DIR = Path(r"D:\lianghua\result\ml_suggest")


@dataclass
class HistoryLoadResult:
    frame: pd.DataFrame
    files_scanned: int
    rows_seen: int
    rows_used: int


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train a model on historical divergence2 optimization results and suggest new params."
    )
    parser.add_argument("--input-root", default=str(DEFAULT_INPUT_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--candidate-count", type=int, default=100_000)
    parser.add_argument("--top-n", type=int, default=1000)
    parser.add_argument("--top-bases", type=int, default=25, help="How many best historical params to search around.")
    parser.add_argument("--global-ratio", type=float, default=0.35, help="Share of candidates sampled from broad space.")
    parser.add_argument("--model", choices=["random_forest", "lightgbm"], default="random_forest")
    parser.add_argument("--device", choices=["cpu", "gpu"], default="cpu", help="Only used by --model lightgbm.")
    parser.add_argument("--trees", type=int, default=500)
    parser.add_argument("--learning-rate", type=float, default=0.05, help="Only used by --model lightgbm.")
    parser.add_argument("--num-leaves", type=int, default=31, help="Only used by --model lightgbm.")
    parser.add_argument("--min-score", type=float, default=-998.0)
    parser.add_argument("--seed", type=int, default=20260514)
    parser.add_argument("--allow-seen", action="store_true", help="Allow suggesting params already present in history.")
    parser.add_argument("--save-all", action="store_true", help="Also write all generated candidates with predictions.")
    args = parser.parse_args()

    input_root = Path(args.input_root)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    loaded = load_history(input_root, float(args.min_score))
    if loaded.frame.empty:
        raise SystemExit(f"No usable scored rows found under {input_root}")
    if len(loaded.frame) < 30:
        raise SystemExit(f"Need at least 30 usable rows, found {len(loaded.frame)}")

    model, metrics, importances = train_model(loaded.frame, args)
    candidates = generate_candidates(
        history=loaded.frame,
        count=int(args.candidate_count),
        seed=int(args.seed),
        top_bases=int(args.top_bases),
        global_ratio=float(args.global_ratio),
        allow_seen=bool(args.allow_seen),
    )
    if candidates.empty:
        raise SystemExit("No candidates generated.")

    candidates["predicted_score"] = model.predict(candidates[PARAM_COLUMNS])
    top_rows = candidates.sort_values("predicted_score", ascending=False).head(max(1, int(args.top_n))).copy()
    top_rows.insert(0, "rank", range(1, len(top_rows) + 1))
    top_rows["predicted_score"] = top_rows["predicted_score"].round(6)

    stamp = time.strftime("%Y%m%d_%H%M%S")
    top_path = output_dir / f"ml_suggest_top_{stamp}.csv"
    all_path = output_dir / f"ml_suggest_all_{stamp}.csv"
    feature_path = output_dir / f"ml_suggest_feature_importance_{stamp}.csv"
    best_json_path = output_dir / f"ml_suggest_best_params_{stamp}.json"
    report_path = output_dir / f"ml_suggest_report_{stamp}.json"

    top_rows.to_csv(top_path, index=False, encoding="utf-8-sig")
    importances.to_csv(feature_path, index=False, encoding="utf-8-sig")
    if args.save_all:
        candidates.sort_values("predicted_score", ascending=False).to_csv(all_path, index=False, encoding="utf-8-sig")

    best = top_rows.iloc[0]
    best_params = coerce_params(best.to_dict())
    best_json_path.write_text(
        json.dumps(
            {
                "best_params": best_params,
                "predicted_score": float(best["predicted_score"]),
                "generated_at": date.today().isoformat(),
                "source": "ml_suggest_divergence2_params.py",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    report = {
        "input_root": str(input_root),
        "output_dir": str(output_dir),
        "files_scanned": loaded.files_scanned,
        "rows_seen": loaded.rows_seen,
        "rows_used": loaded.rows_used,
        "unique_train_rows": len(loaded.frame),
        "candidate_count_requested": int(args.candidate_count),
        "candidate_count_generated": len(candidates),
        "top_n": int(args.top_n),
        "top_bases": int(args.top_bases),
        "global_ratio": float(args.global_ratio),
        "trees": int(args.trees),
        "seed": int(args.seed),
        "metrics": metrics,
        "top_path": str(top_path),
        "feature_importance_path": str(feature_path),
        "best_params_json": str(best_json_path),
        "all_path": str(all_path) if args.save_all else "",
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[ml-suggest] rows_used={loaded.rows_used} unique_train={len(loaded.frame)} files={loaded.files_scanned}", flush=True)
    print(f"[ml-suggest] model r2_test={metrics.get('test_r2')} mae_test={metrics.get('test_mae')}", flush=True)
    print(f"[ml-suggest] candidates={len(candidates)} top={top_path}", flush=True)
    print(f"[ml-suggest] best predicted={float(best['predicted_score']):.6f} json={best_json_path}", flush=True)


def load_history(input_root: Path, min_score: float) -> HistoryLoadResult:
    rows: list[dict[str, Any]] = []
    files_scanned = 0
    rows_seen = 0
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
                files_scanned += 1
                for row in reader:
                    rows_seen += 1
                    try:
                        score = float(row.get("score") or -999.0)
                    except (TypeError, ValueError):
                        continue
                    if score <= min_score:
                        continue
                    try:
                        params = coerce_params(row)
                    except (TypeError, ValueError):
                        continue
                    params["score"] = score
                    rows.append(params)
        except OSError:
            continue

    if not rows:
        return HistoryLoadResult(pd.DataFrame(), files_scanned, rows_seen, 0)

    frame = pd.DataFrame(rows)
    frame["_param_key"] = frame[PARAM_COLUMNS].apply(lambda item: json.dumps(item.to_dict(), sort_keys=True), axis=1)
    frame = frame.sort_values("score", ascending=False).drop_duplicates("_param_key", keep="first")
    frame = frame.drop(columns=["_param_key"]).reset_index(drop=True)
    return HistoryLoadResult(frame, files_scanned, rows_seen, len(rows))


def train_model(frame: pd.DataFrame, args: argparse.Namespace) -> tuple[Any, dict[str, Any], pd.DataFrame]:
    x = frame[PARAM_COLUMNS]
    y = frame["score"].astype(float)
    test_size = 0.2 if len(frame) >= 100 else 0.25
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=test_size, random_state=int(args.seed))
    device_effective = str(args.device)
    if args.model == "lightgbm":
        model = LGBMRegressor(
            n_estimators=max(50, int(args.trees)),
            learning_rate=float(args.learning_rate),
            num_leaves=max(8, int(args.num_leaves)),
            min_child_samples=10,
            subsample=0.9,
            colsample_bytree=0.9,
            reg_lambda=1.0,
            objective="regression",
            device_type=str(args.device),
            gpu_use_dp=False,
            n_jobs=-1,
            random_state=int(args.seed),
            verbosity=-1,
        )
        try:
            model.fit(x_train, y_train)
        except Exception as exc:
            if args.device != "gpu":
                raise
            print(f"[ml-suggest] LightGBM GPU failed, falling back to CPU: {type(exc).__name__}: {exc}", flush=True)
            device_effective = "cpu"
            model = LGBMRegressor(
                n_estimators=max(50, int(args.trees)),
                learning_rate=float(args.learning_rate),
                num_leaves=max(8, int(args.num_leaves)),
                min_child_samples=10,
                subsample=0.9,
                colsample_bytree=0.9,
                reg_lambda=1.0,
                objective="regression",
                device_type="cpu",
                n_jobs=-1,
                random_state=int(args.seed),
                verbosity=-1,
            )
            model.fit(x_train, y_train)
    else:
        model = RandomForestRegressor(
            n_estimators=max(50, int(args.trees)),
            max_features="sqrt",
            min_samples_leaf=2,
            n_jobs=-1,
            random_state=int(args.seed),
        )
        model.fit(x_train, y_train)
    train_pred = model.predict(x_train)
    test_pred = model.predict(x_test)
    metrics = {
        "model": str(args.model),
        "requested_device": str(args.device),
        "effective_device": device_effective,
        "train_r2": round(float(r2_score(y_train, train_pred)), 6),
        "test_r2": round(float(r2_score(y_test, test_pred)), 6),
        "train_mae": round(float(mean_absolute_error(y_train, train_pred)), 6),
        "test_mae": round(float(mean_absolute_error(y_test, test_pred)), 6),
        "train_rows": int(len(x_train)),
        "test_rows": int(len(x_test)),
        "best_history_score": round(float(y.max()), 6),
        "median_history_score": round(float(y.median()), 6),
    }
    importances = pd.DataFrame(
        {
            "param": PARAM_COLUMNS,
            "importance": model.feature_importances_,
        }
    ).sort_values("importance", ascending=False)
    importances["importance"] = importances["importance"].round(8)
    return model, metrics, importances


def generate_candidates(
    history: pd.DataFrame,
    count: int,
    seed: int,
    top_bases: int,
    global_ratio: float,
    allow_seen: bool,
) -> pd.DataFrame:
    rng = random.Random(seed)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    if not allow_seen:
        for _, row in history.iterrows():
            seen.add(json.dumps(coerce_params(row.to_dict()), sort_keys=True))

    bases = [coerce_params(row.to_dict()) for _, row in history.sort_values("score", ascending=False).head(max(1, top_bases)).iterrows()]
    global_target = int(count * max(0.0, min(1.0, global_ratio)))
    attempts = 0
    while len(rows) < count and attempts < count * 100:
        attempts += 1
        if len(rows) < global_target:
            params = sample_global_params(rng, BASE_PARAMS)
        else:
            params = sample_local_params(rng, rng.choice(bases))
        add_params(rows, seen, params)
    return pd.DataFrame(rows, columns=PARAM_COLUMNS)


def coerce_params(row: dict[str, Any]) -> dict[str, Any]:
    params = dict(BASE_PARAMS)
    for key in PARAM_COLUMNS:
        value = row.get(key, params.get(key))
        if isinstance(params.get(key), int):
            params[key] = int(float(value))
        else:
            params[key] = float(value)
    return params


if __name__ == "__main__":
    main()
