from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "quant-platform"
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))
if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from feature_utils import FEATURE_NAMES, extract_features, make_labels  # noqa: E402
from quant_platform.storage import MarketDatabase  # noqa: E402
from quant_platform.strategy import DivergenceStrategy  # noqa: E402


DEFAULT_PARAMS: dict[str, Any] = {
    **DivergenceStrategy.params,
    "strong_close_pct_chg": 5.0,
}


def buffered_start(start_date: str, days: int = 220) -> str:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    return (start - timedelta(days=days)).isoformat()


def build_dataset(start_date: str, end_date: str, output: Path, min_rows: int, board: str) -> int:
    db = MarketDatabase()
    history_start = buffered_start(start_date)
    pool = db.list_backtest_symbols(history_start, end_date, min_rows, board)
    symbols = [item["symbol"] for item in pool]
    print(f"[dataset] pool={len(symbols)} board={board} history={history_start}..{end_date}", flush=True)
    history = db.query_many(symbols, history_start, end_date)
    strategy = DivergenceStrategy(**DEFAULT_PARAMS)
    rows_out: list[dict[str, Any]] = []

    for n, (symbol, rows) in enumerate(history.items(), start=1):
        if n == 1 or n % 500 == 0:
            print(f"[dataset] {n}/{len(history)} samples={len(rows_out)}", flush=True)
        if len(rows) < 38:
            continue
        for index in range(34, len(rows) - 3):
            t = rows[index - 2]
            t1 = rows[index - 1]
            t2 = rows[index]
            signal_date = t2["trade_date"]
            if signal_date < start_date or signal_date > end_date:
                continue
            window = rows[index - 34 : index + 1]
            if not strategy._matches(symbol, t, t1, t2, window):  # type: ignore[attr-defined]
                continue
            labels = make_labels(symbol, rows, index)
            if not labels:
                continue
            features = extract_features(symbol, rows, index)
            row = {
                "symbol": symbol,
                "trade_date": signal_date,
                "entry_price": strategy._entry_price(t1, t2),  # type: ignore[attr-defined]
            }
            row.update(features)
            row.update(labels)
            rows_out.append(row)

    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["symbol", "trade_date", "entry_price", *FEATURE_NAMES, *label_names()]
    with output.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_out)
    print(f"[dataset] wrote {len(rows_out)} rows -> {output}", flush=True)
    return len(rows_out)


def label_names() -> list[str]:
    return [
        "label_trade_worth",
        "label_limit_up_1d",
        "label_limit_up_2d",
        "label_limit_up_3d",
        "label_repair_3d",
        "label_big_loss",
        "target_return_3d",
        "target_max_high_return_3d",
        "target_min_low_return_3d",
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build divergence trade ML dataset.")
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default=datetime.now().date().isoformat())
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--min-rows", type=int, default=120)
    parser.add_argument("--output", default=str(Path(__file__).resolve().parent / "data" / "divergence_trade_dataset.csv"))
    args = parser.parse_args()
    build_dataset(args.start_date, args.end_date, Path(args.output), args.min_rows, args.board)


if __name__ == "__main__":
    main()
