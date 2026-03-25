import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

from backtest_engine import Backtester

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DATA = REPO_ROOT / "data" / "sample_backtest.csv"


class OfflineBacktestTests(unittest.TestCase):
    def test_cli_help_returns_success(self):
        result = subprocess.run(
            [sys.executable, "cli.py", "--help"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("backtest", result.stdout)

    def test_sample_backtest_generates_trades(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle)

        backtester = Backtester(config, initial_capital=1000.0)
        df = backtester.load_historical_orderbooks(str(SAMPLE_DATA))
        result = backtester.simulate_mean_reversion(df, market_id="example-market", outcome="YES")

        self.assertGreater(result.total_trades, 0)
        self.assertGreater(len(result.equity_curve), 1)
        self.assertTrue(any(trade.size > 0 for trade in result.trades))

    def test_cli_backtest_runs_on_sample_data(self):
        result = subprocess.run(
            [sys.executable, "cli.py", "backtest", "--data", str(SAMPLE_DATA)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("AGGREGATE", result.stdout)

    def test_backtest_runs_are_independent(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle)

        backtester = Backtester(config, initial_capital=1000.0)
        df = backtester.load_historical_orderbooks(str(SAMPLE_DATA))
        first = backtester.simulate_mean_reversion(df, market_id="example-market", outcome="YES")
        second = backtester.simulate_mean_reversion(df, market_id="example-market", outcome="YES")

        self.assertEqual(first.total_trades, second.total_trades)
        self.assertAlmostEqual(first.total_pnl, second.total_pnl, places=9)

    def test_loader_rejects_missing_required_columns(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle)

        backtester = Backtester(config, initial_capital=1000.0)
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_csv = Path(tmpdir) / "bad.csv"
            bad_csv.write_text(
                "timestamp,market_id,outcome,best_bid,best_ask,bid_size,ask_size,mid_price\n"
                "2026-03-01T00:00:00,example-market,YES,0.49,0.51,100,100,0.50\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "missing required columns"):
                backtester.load_historical_orderbooks(str(bad_csv))

    def test_loader_rejects_crossed_books(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle)

        backtester = Backtester(config, initial_capital=1000.0)
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_csv = Path(tmpdir) / "crossed.csv"
            bad_csv.write_text(
                "timestamp,market_id,outcome,best_bid,best_ask,bid_size,ask_size,mid_price,volume\n"
                "2026-03-01T00:00:00,example-market,YES,0.60,0.55,100,100,0.575,20000\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "crossed books"):
                backtester.load_historical_orderbooks(str(bad_csv))


if __name__ == "__main__":
    unittest.main()
