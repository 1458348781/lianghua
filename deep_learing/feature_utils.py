from __future__ import annotations

import math
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "quant-platform"
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from quant_platform.strategy import DivergenceStrategy  # noqa: E402


FEATURE_NAMES = [
    "is_20cm",
    "price",
    "day1_pct_chg",
    "day1_volume_ratio_5",
    "day1_turnover",
    "day1_close_position",
    "day1_upper_shadow",
    "day1_amplitude",
    "day2_pct_chg",
    "day2_volume_ratio_day1",
    "day2_turnover",
    "day2_close_position",
    "day2_upper_shadow",
    "day2_lower_shadow",
    "day2_amplitude",
    "day2_close_vs_day1_close",
    "entry_open_gap_pct_chg",
    "entry_high_from_open_pct_chg",
    "entry_breakout_pct",
    "ret_3",
    "ret_5",
    "ret_10",
    "ret_20",
    "amp_10",
    "amp_20",
    "amp_30",
    "vol_ratio_3",
    "vol_ratio_5",
    "vol_ratio_10",
    "turnover_3",
    "turnover_5",
    "turnover_10",
    "limit_up_count_5",
    "limit_up_count_10",
    "limit_up_count_20",
    "days_since_limit_up",
]


def extract_features(symbol: str, rows: list[dict[str, Any]], entry_index: int) -> dict[str, float]:
    t = rows[entry_index - 2]
    t1 = rows[entry_index - 1]
    t2 = rows[entry_index]
    strategy = DivergenceStrategy()
    pre_t = rows[: entry_index - 2]
    pre_entry = rows[: entry_index + 1]

    previous_close = safe_float(t2.get("pre_close") or t1["close"])
    entry_open = safe_float(t2["open"])
    entry_high = safe_float(t2["high"])
    day1_volume_base = mean([safe_float(row["volume"]) for row in pre_t[-5:]])
    day2_volume_base = safe_float(t["volume"])
    previous_high = safe_float(t1["high"])

    features = {
        "is_20cm": 1.0 if is_20cm_symbol(symbol) else 0.0,
        "price": safe_float(t["close"]),
        "day1_pct_chg": pct_chg(t),
        "day1_volume_ratio_5": safe_float(t["volume"]) / day1_volume_base if day1_volume_base else 0.0,
        "day1_turnover": safe_float(t.get("turnover", 0)),
        "day1_close_position": close_position(t),
        "day1_upper_shadow": upper_shadow(t),
        "day1_amplitude": amplitude(t),
        "day2_pct_chg": pct_chg(t1),
        "day2_volume_ratio_day1": safe_float(t1["volume"]) / day2_volume_base if day2_volume_base else 0.0,
        "day2_turnover": safe_float(t1.get("turnover", 0)),
        "day2_close_position": close_position(t1),
        "day2_upper_shadow": upper_shadow(t1),
        "day2_lower_shadow": lower_shadow(t1),
        "day2_amplitude": amplitude(t1),
        "day2_close_vs_day1_close": safe_float(t1["close"]) / safe_float(t["close"]) - 1 if safe_float(t["close"]) else 0.0,
        "entry_open_gap_pct_chg": (entry_open / previous_close - 1) * 100 if previous_close else 0.0,
        "entry_high_from_open_pct_chg": (entry_high / entry_open - 1) * 100 if entry_open else 0.0,
        "entry_breakout_pct": entry_high / previous_high - 1 if previous_high else 0.0,
        "ret_3": rolling_return(pre_entry, 3),
        "ret_5": rolling_return(pre_entry, 5),
        "ret_10": rolling_return(pre_entry, 10),
        "ret_20": rolling_return(pre_entry, 20),
        "amp_10": rolling_amplitude(pre_entry, 10),
        "amp_20": rolling_amplitude(pre_entry, 20),
        "amp_30": rolling_amplitude(pre_entry, 30),
        "vol_ratio_3": volume_ratio(pre_entry, 3),
        "vol_ratio_5": volume_ratio(pre_entry, 5),
        "vol_ratio_10": volume_ratio(pre_entry, 10),
        "turnover_3": rolling_mean(pre_entry, "turnover", 3),
        "turnover_5": rolling_mean(pre_entry, "turnover", 5),
        "turnover_10": rolling_mean(pre_entry, "turnover", 10),
        "limit_up_count_5": limit_up_count(symbol, pre_entry, 5, strategy),
        "limit_up_count_10": limit_up_count(symbol, pre_entry, 10, strategy),
        "limit_up_count_20": limit_up_count(symbol, pre_entry, 20, strategy),
        "days_since_limit_up": days_since_limit_up(symbol, pre_entry, strategy),
    }
    return {name: clean(features.get(name, 0.0)) for name in FEATURE_NAMES}


def make_labels(symbol: str, rows: list[dict[str, Any]], entry_index: int, horizon: int = 3) -> dict[str, float]:
    strategy = DivergenceStrategy()
    entry_row = rows[entry_index]
    entry_price = strategy._entry_price(rows[entry_index - 1], entry_row)  # type: ignore[attr-defined]
    future = rows[entry_index + 1 : entry_index + 1 + horizon]
    if not future or entry_price <= 0:
        return {}

    max_high_return = max(safe_float(row["high"]) / entry_price - 1 for row in future)
    min_low_return = min(safe_float(row["low"]) / entry_price - 1 for row in future)
    final_close_return = safe_float(future[-1]["close"]) / entry_price - 1
    limit_up_1d = 1.0 if future[:1] and strategy._hit_limit_up(symbol, future[0]) else 0.0  # type: ignore[attr-defined]
    limit_up_2d = 1.0 if any(strategy._hit_limit_up(symbol, row) for row in future[:2]) else 0.0  # type: ignore[attr-defined]
    limit_up_3d = 1.0 if any(strategy._hit_limit_up(symbol, row) for row in future[:3]) else 0.0  # type: ignore[attr-defined]
    repair_3d = 1.0 if max_high_return >= 0.07 or limit_up_3d else 0.0
    big_loss = 1.0 if min_low_return <= -0.05 else 0.0
    trade_worth = 1.0 if (max_high_return >= 0.06 or final_close_return >= 0.03 or limit_up_3d) and not big_loss else 0.0
    return {
        "label_trade_worth": trade_worth,
        "label_limit_up_1d": limit_up_1d,
        "label_limit_up_2d": limit_up_2d,
        "label_limit_up_3d": limit_up_3d,
        "label_repair_3d": repair_3d,
        "label_big_loss": big_loss,
        "target_return_3d": clean(final_close_return),
        "target_max_high_return_3d": clean(max_high_return),
        "target_min_low_return_3d": clean(min_low_return),
    }


def fallback_trade_probability(features: dict[str, float]) -> float:
    score = 0.0
    score += clamp((features.get("day2_close_position", 0) - 0.45) / 0.35, -1, 1) * 0.18
    score += clamp((0.06 - features.get("day2_upper_shadow", 0)) / 0.06, -1, 1) * 0.14
    score += clamp((features.get("entry_high_from_open_pct_chg", 0) - 3.0) / 5.0, -1, 1) * 0.16
    score += clamp((features.get("ret_20", 0) - 0.05) / 0.25, -1, 1) * 0.10
    score += clamp((features.get("amp_30", 0) - 0.18) / 0.30, -1, 1) * 0.08
    score -= clamp((features.get("day2_volume_ratio_day1", 0) - 1.4) / 0.8, 0, 1) * 0.10
    score -= clamp((features.get("entry_open_gap_pct_chg", 0) - 5.0) / 3.0, 0, 1) * 0.08
    return round(clamp(0.48 + score, 0.05, 0.92), 4)


def safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def clean(value: float) -> float:
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return float(value)


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def pct_chg(row: dict[str, Any]) -> float:
    pre_close = safe_float(row.get("pre_close", 0))
    return (safe_float(row["close"]) / pre_close - 1) * 100 if pre_close else safe_float(row.get("pct_chg", 0))


def close_position(row: dict[str, Any]) -> float:
    high = safe_float(row["high"])
    low = safe_float(row["low"])
    return (safe_float(row["close"]) - low) / (high - low) if high > low else 0.0


def upper_shadow(row: dict[str, Any]) -> float:
    close = safe_float(row["close"])
    return (safe_float(row["high"]) - close) / close if close else 0.0


def lower_shadow(row: dict[str, Any]) -> float:
    open_ = safe_float(row["open"])
    close = safe_float(row["close"])
    low = safe_float(row["low"])
    body_low = min(open_, close)
    return (body_low - low) / close if close else 0.0


def amplitude(row: dict[str, Any]) -> float:
    pre_close = safe_float(row.get("pre_close", 0))
    return (safe_float(row["high"]) / pre_close - safe_float(row["low"]) / pre_close) if pre_close else 0.0


def rolling_return(rows: list[dict[str, Any]], window: int) -> float:
    if len(rows) <= window:
        return 0.0
    old = safe_float(rows[-window - 1]["close"])
    now = safe_float(rows[-1]["close"])
    return now / old - 1 if old else 0.0


def rolling_amplitude(rows: list[dict[str, Any]], window: int) -> float:
    part = rows[-window:]
    lows = [safe_float(row["low"]) for row in part if safe_float(row["low"]) > 0]
    highs = [safe_float(row["high"]) for row in part]
    return max(highs) / min(lows) - 1 if lows and highs else 0.0


def rolling_mean(rows: list[dict[str, Any]], field: str, window: int) -> float:
    return mean([safe_float(row.get(field, 0)) for row in rows[-window:]])


def volume_ratio(rows: list[dict[str, Any]], window: int) -> float:
    if len(rows) <= window:
        return 0.0
    base = mean([safe_float(row["volume"]) for row in rows[-window - 1 : -1]])
    return safe_float(rows[-1]["volume"]) / base if base else 0.0


def limit_up_count(symbol: str, rows: list[dict[str, Any]], window: int, strategy: DivergenceStrategy) -> float:
    return float(sum(1 for row in rows[-window:] if strategy._is_limit_up(symbol, row)))  # type: ignore[attr-defined]


def days_since_limit_up(symbol: str, rows: list[dict[str, Any]], strategy: DivergenceStrategy) -> float:
    for distance, row in enumerate(reversed(rows), start=0):
        if strategy._is_limit_up(symbol, row):  # type: ignore[attr-defined]
            return float(distance)
    return 999.0


def is_20cm_symbol(symbol: str) -> bool:
    code = normalize_code(symbol)
    return code.startswith(("300", "301", "688", "689"))


def normalize_code(symbol: str) -> str:
    return symbol.split(".")[0]
