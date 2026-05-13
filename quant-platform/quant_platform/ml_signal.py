from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
DEEP_DIR = ROOT / "deep_learing"
MODEL_PATH = DEEP_DIR / "models" / "divergence_trade_model.joblib"
if str(DEEP_DIR) not in sys.path:
    sys.path.insert(0, str(DEEP_DIR))

from feature_utils import FEATURE_NAMES, extract_features, fallback_trade_probability  # type: ignore  # noqa: E402


_MODEL_CACHE: dict[str, Any] = {"mtime": None, "model": None, "load_failed": False}


def score_divergence_signal(symbol: str, rows: list[dict[str, Any]], entry_index: int) -> dict[str, Any]:
    features = extract_features(symbol, rows, entry_index)
    model = load_model()
    if not model:
        return {
            "trade_worth_probability": fallback_trade_probability(features),
            "limit_up_3d_probability": None,
            "big_loss_probability": None,
            "expected_return_3d": None,
            "ml_model_available": False,
            "ml_score_source": "rule_fallback",
        }

    x = [[features.get(name, 0.0) for name in model.get("feature_names", FEATURE_NAMES)]]
    targets = model.get("targets", {})
    trade_prob = predict_probability(targets.get("trade_worth"), x)
    limit_prob = predict_probability(targets.get("limit_up_3d"), x)
    loss_prob = predict_probability(targets.get("big_loss"), x)
    expected_return = predict_value(targets.get("expected_return_3d"), x)
    if trade_prob is None:
        trade_prob = fallback_trade_probability(features)
    return {
        "trade_worth_probability": round(float(trade_prob), 4),
        "limit_up_3d_probability": round(float(limit_prob), 4) if limit_prob is not None else None,
        "big_loss_probability": round(float(loss_prob), 4) if loss_prob is not None else None,
        "expected_return_3d": round(float(expected_return), 6) if expected_return is not None else None,
        "ml_model_available": True,
        "ml_score_source": "lightgbm",
    }


def load_model() -> dict[str, Any] | None:
    if not MODEL_PATH.exists():
        return None
    mtime = MODEL_PATH.stat().st_mtime
    if _MODEL_CACHE["model"] is not None and _MODEL_CACHE["mtime"] == mtime:
        return _MODEL_CACHE["model"]
    try:
        import joblib

        model = joblib.load(MODEL_PATH)
        _MODEL_CACHE.update({"mtime": mtime, "model": model, "load_failed": False})
        return model
    except Exception:
        _MODEL_CACHE.update({"mtime": mtime, "model": None, "load_failed": True})
        return None


def predict_probability(model: Any, x: list[list[float]]) -> float | None:
    if model is None:
        return None
    try:
        if hasattr(model, "predict_proba"):
            return float(model.predict_proba(x)[0][1])
        return float(model.predict(x)[0])
    except Exception:
        return None


def predict_value(model: Any, x: list[list[float]]) -> float | None:
    if model is None:
        return None
    try:
        return float(model.predict(x)[0])
    except Exception:
        return None
