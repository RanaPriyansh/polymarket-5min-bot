import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from cli import select_markets_for_trading
from market_filter import rank_markets, should_trade_market
from research import run_polymarket_research_cycle

REPO_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_DATA = REPO_ROOT / "data" / "sample_backtest.csv"


class MarketFilterAndResearchTests(unittest.TestCase):
    def test_market_filter_blocks_meme_market(self):
        market = {
            "id": "meme-1",
            "question": "Will Elon tweet about Dogecoin tonight?",
            "price": 0.42,
            "volume": 25000,
            "endDate": "2026-04-01T00:00:00Z",
        }
        should_trade, reason = should_trade_market(market)
        self.assertFalse(should_trade)
        self.assertIn("blocked keyword", reason)

    def test_market_filter_prefers_crypto_market(self):
        crypto_market = {
            "id": "btc-1",
            "question": "Will Bitcoin close above $100k this week?",
            "price": 0.52,
            "volume": 60000,
            "endDate": "2026-04-04T00:00:00Z",
        }
        generic_market = {
            "id": "generic-1",
            "question": "Will a new product launch this month?",
            "price": 0.52,
            "volume": 6000,
            "endDate": "2026-04-20T00:00:00Z",
        }
        ranked = rank_markets([generic_market, crypto_market])
        self.assertEqual(ranked[0][0]["id"], "btc-1")
        self.assertTrue(ranked[0][1])

    def test_select_markets_for_trading_applies_scan_limit(self):
        markets = [
            {
                "id": f"btc-{idx}",
                "question": f"Will Bitcoin do thing {idx}?",
                "price": 0.50,
                "volume": 20000 + idx,
                "endDate": "2026-04-04T00:00:00Z",
            }
            for idx in range(5)
        ]
        selected, summary = select_markets_for_trading(markets, {"market_filter": {"enabled": True, "scan_limit": 2}})
        self.assertEqual(len(selected), 2)
        self.assertEqual(summary["passed"], 5)

    def test_research_cycle_generates_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = run_polymarket_research_cycle(
                config_path=REPO_ROOT / "config.yaml",
                data_path=SAMPLE_DATA,
                output_dir=tmpdir,
                objective="find the best starting paper-trading configuration",
                max_hypotheses=2,
                max_markets=2,
            )
            self.assertTrue(result["cycle_id"])
            self.assertTrue(Path(result["report"]["path"]).exists())
            self.assertTrue(Path(result["report"]["artifact_path"]).exists())
            self.assertTrue(Path(result["report"]["subagents_path"]).exists())
            self.assertGreaterEqual(len(result["experiments"]), 1)

    def test_cli_research_runs_on_sample_data(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [
                    sys.executable,
                    "cli.py",
                    "research",
                    "--data",
                    str(SAMPLE_DATA),
                    "--output-dir",
                    tmpdir,
                    "--max-hypotheses",
                    "2",
                    "--max-markets",
                    "2",
                ],
                cwd=REPO_ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, msg=result.stderr)
            self.assertIn("Research cycle:", result.stdout)
            self.assertIn("Top insight:", result.stdout)


if __name__ == "__main__":
    unittest.main()
