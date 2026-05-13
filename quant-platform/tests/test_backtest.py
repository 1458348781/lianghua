from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quant_platform.backtest import BacktestEngine
from quant_platform.config import DATABASE_DIR
from quant_platform.data_sources import SampleSource
from quant_platform.storage import DataPortal, MarketDatabase
from quant_platform.strategy import create_strategy


class BacktestSmokeTest(unittest.TestCase):
    def test_moving_average_backtest_runs(self) -> None:
        db = MarketDatabase(DATABASE_DIR / "test_backtest.sqlite")
        bars = SampleSource().fetch_daily("000001.SZ", "2020-01-01", "2020-12-31")
        db.upsert_daily(bars)
        config = {
            "strategy_name": "moving_average",
            "symbols": ["000001.SZ"],
            "start_date": "2020-01-01",
            "end_date": "2020-12-31",
            "initial_cash": 1_000_000,
            "params": {"short_window": 5, "long_window": 20},
        }
        result = BacktestEngine(DataPortal(db), create_strategy("moving_average", config["params"]), config).run()
        self.assertGreater(len(result["equity_curve"]), 100)
        self.assertIn("cumulative_return", result["metrics"])


if __name__ == "__main__":
    unittest.main()
