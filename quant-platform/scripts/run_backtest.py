from __future__ import annotations

import json
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quant_platform.backtest import BacktestEngine
from quant_platform.storage import DataPortal
from quant_platform.strategy import create_strategy


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy", default="moving_average")
    parser.add_argument("--symbols", default="000001.SZ")
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--short-window", type=int, default=5)
    parser.add_argument("--long-window", type=int, default=20)
    parser.add_argument("--lookback", type=int, default=60)
    parser.add_argument("--top-k", type=int, default=3)
    args = parser.parse_args()
    params = (
        {"lookback": args.lookback, "top_k": args.top_k}
        if args.strategy == "momentum"
        else {"short_window": args.short_window, "long_window": args.long_window}
    )
    config = {
        "strategy_name": args.strategy,
        "symbols": [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()],
        "start_date": args.start_date,
        "end_date": args.end_date,
        "initial_cash": args.initial_cash,
        "commission_rate": 0.0003,
        "slippage_rate": 0.001,
        "stamp_tax_rate": 0.001,
        "params": params,
    }
    strategy = create_strategy(config["strategy_name"], config["params"])
    result = BacktestEngine(DataPortal(), strategy, config).run()
    print(json.dumps({"metrics": result["metrics"], "trades": len(result["trades"])}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
