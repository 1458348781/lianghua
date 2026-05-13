from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quant_platform.data_sources import get_source, normalize_symbol
from quant_platform.storage import MarketDatabase


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", default="000001.SZ,600519.SH,000300.SH")
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--source", default="auto")
    args = parser.parse_args()

    db = MarketDatabase()
    source = get_source(args.source)
    for raw_symbol in args.symbols.split(","):
        symbol = normalize_symbol(raw_symbol)
        bars = source.fetch_daily(symbol, args.start_date, args.end_date)
        count = db.upsert_daily(bars)
        print(f"{symbol}: {count} rows from {bars[0].source if bars else args.source}")


if __name__ == "__main__":
    main()
