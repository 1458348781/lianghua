from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import average_precision_score, mean_absolute_error, roc_auc_score

from feature_utils import FEATURE_NAMES


TARGETS = {
    "trade_worth": ("label_trade_worth", "classifier"),
    "limit_up_3d": ("label_limit_up_3d", "classifier"),
    "big_loss": ("label_big_loss", "classifier"),
    "expected_return_3d": ("target_return_3d", "regressor"),
}


def train(dataset_path: Path, model_dir: Path, use_gpu: bool) -> dict:
    data = pd.read_csv(dataset_path)
    if data.empty:
        raise SystemExit("Dataset is empty. Build dataset with a wider date range first.")
    data = data.sort_values("trade_date").reset_index(drop=True)
    split_index = max(1, int(len(data) * 0.8))
    train_df = data.iloc[:split_index].copy()
    test_df = data.iloc[split_index:].copy()
    if test_df.empty:
        test_df = train_df.copy()

    x_train = train_df[FEATURE_NAMES].fillna(0)
    x_test = test_df[FEATURE_NAMES].fillna(0)
    model_dir.mkdir(parents=True, exist_ok=True)

    model_payload = {
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "feature_names": FEATURE_NAMES,
        "targets": {},
        "dataset_rows": int(len(data)),
        "train_rows": int(len(train_df)),
        "test_rows": int(len(test_df)),
        "use_gpu_requested": use_gpu,
    }
    metrics: dict[str, dict] = {}

    for name, (target, kind) in TARGETS.items():
        y_train = train_df[target]
        y_test = test_df[target]
        if kind == "classifier":
            model, used_gpu = fit_classifier(x_train, y_train, use_gpu)
            if y_train.nunique() == 2 and len(train_df) >= 80:
                try:
                    model = CalibratedClassifierCV(model, method="isotonic", cv=3)
                    model.fit(x_train, y_train)
                except Exception:
                    pass
            prob = model.predict_proba(x_test)[:, 1] if hasattr(model, "predict_proba") else model.predict(x_test)
            metrics[name] = {
                "positive_rate_train": round(float(y_train.mean()), 6),
                "positive_rate_test": round(float(y_test.mean()), 6),
                "average_precision": safe_metric(lambda: average_precision_score(y_test, prob)),
                "roc_auc": safe_metric(lambda: roc_auc_score(y_test, prob)),
                "used_gpu": used_gpu,
            }
        else:
            model, used_gpu = fit_regressor(x_train, y_train, use_gpu)
            pred = model.predict(x_test)
            metrics[name] = {
                "target_mean_train": round(float(y_train.mean()), 6),
                "target_mean_test": round(float(y_test.mean()), 6),
                "mae": safe_metric(lambda: mean_absolute_error(y_test, pred)),
                "used_gpu": used_gpu,
            }
        model_payload["targets"][name] = model

    model_payload["metrics"] = metrics
    model_path = model_dir / "divergence_trade_model.joblib"
    metrics_path = model_dir / "divergence_trade_metrics.json"
    joblib.dump(model_payload, model_path)
    metrics_path.write_text(json.dumps({k: v for k, v in model_payload.items() if k != "targets"}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[train] model: {model_path}")
    print(f"[train] metrics: {metrics_path}")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
    return model_payload


def fit_classifier(x, y, use_gpu: bool):
    params = classifier_params()
    if use_gpu:
        try:
            model = LGBMClassifier(**params, device_type="gpu")
            model.fit(x, y)
            return model, True
        except Exception as exc:
            print(f"[train] GPU classifier fallback to CPU: {exc}")
    model = LGBMClassifier(**params)
    model.fit(x, y)
    return model, False


def fit_regressor(x, y, use_gpu: bool):
    params = regressor_params()
    if use_gpu:
        try:
            model = LGBMRegressor(**params, device_type="gpu")
            model.fit(x, y)
            return model, True
        except Exception as exc:
            print(f"[train] GPU regressor fallback to CPU: {exc}")
    model = LGBMRegressor(**params)
    model.fit(x, y)
    return model, False


def classifier_params() -> dict:
    return {
        "n_estimators": 300,
        "learning_rate": 0.035,
        "num_leaves": 15,
        "max_depth": 5,
        "min_child_samples": 12,
        "subsample": 0.85,
        "colsample_bytree": 0.85,
        "reg_alpha": 0.2,
        "reg_lambda": 1.0,
        "class_weight": "balanced",
        "random_state": 20260510,
        "n_jobs": -1,
        "verbosity": -1,
    }


def regressor_params() -> dict:
    params = classifier_params()
    params.pop("class_weight", None)
    params["objective"] = "regression_l1"
    return params


def safe_metric(fn):
    try:
        return round(float(fn()), 6)
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Train divergence trade worth model.")
    parser.add_argument("--dataset", default=str(Path(__file__).resolve().parent / "data" / "divergence_trade_dataset.csv"))
    parser.add_argument("--model-dir", default=str(Path(__file__).resolve().parent / "models"))
    parser.add_argument("--gpu", action="store_true", help="Try LightGBM GPU. Falls back to CPU if unavailable.")
    args = parser.parse_args()
    train(Path(args.dataset), Path(args.model_dir), args.gpu)


if __name__ == "__main__":
    main()
