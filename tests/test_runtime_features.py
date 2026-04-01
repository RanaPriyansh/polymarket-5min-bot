import json
import tempfile
import unittest

from book_quality import assess_book_quality
from execution import PolymarketExecutor
from market_data import OrderBook
from research.loop import ResearchLoop
from research.polymarket import PolymarketRuntimeResearchAdapter
from runtime_telemetry import RuntimeTelemetry


class RuntimeFeatureTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            "polymarket": {
                "clob_api_url": "https://clob.polymarket.com",
                "wallet_address": "paper-wallet",
                "private_key": "paper-key",
            },
            "execution": {
                "quote_refresh_seconds": 30,
            },
        }

    async def test_executor_tracks_family_counters_and_realized_pnl(self):
        executor = PolymarketExecutor(self.config, market_data=None, mode="paper")
        await executor.__aenter__()
        try:
            buy_id = await executor.place_order("m1", "YES", "BUY", 10, 0.55, strategy_family="toxicity_mm", order_kind="quote")
            sell_id = await executor.place_order("m1", "YES", "SELL", 10, 0.70, strategy_family="toxicity_mm", order_kind="quote")
            buy_fill = executor.fill_order(buy_id, fill_price=0.54)
            sell_fill = executor.fill_order(sell_id, fill_price=0.71)
            metrics = executor.get_family_metrics()["toxicity_mm"]
            self.assertTrue(buy_fill["filled"])
            self.assertTrue(sell_fill["filled"])
            self.assertEqual(metrics["quotes_submitted"], 2)
            self.assertEqual(metrics["orders_filled"], 2)
            self.assertEqual(metrics["orders_resting"], 0)
            self.assertAlmostEqual(metrics["realized_pnl"], 1.7, places=6)
        finally:
            await executor.__aexit__(None, None, None)

    async def test_assess_book_quality_rejects_toxic_books(self):
        ob = OrderBook(
            market_id="m2",
            yes_asks=[(0.9, 1.0)],
            yes_bids=[(0.1, 1.0)],
            no_asks=[(0.9, 1.0)],
            no_bids=[(0.1, 1.0)],
            timestamp=0.0,
            sequence=1,
        )
        quality = assess_book_quality(ob, "YES", max_spread_bps=500, min_top_depth=10, min_top_notional=10)
        self.assertFalse(quality.is_tradeable)
        self.assertIn("wide_spread>500", quality.reasons)
        self.assertIn("thin_depth<10", quality.reasons)

    async def test_research_loop_consumes_live_runtime_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="paper-test", loop_count=3)
            telemetry.write_strategy_metrics({
                "mean_reversion_5min": {
                    "quotes_submitted": 0,
                    "orders_resting": 0,
                    "orders_filled": 1,
                    "cancellations": 0,
                    "realized_pnl": 2.5,
                    "markets_seen": 10,
                    "toxic_book_skips": 3,
                }
            })
            telemetry.append_event("order.filled", {
                "strategy_family": "mean_reversion_5min",
                "market_id": "m3",
                "realized_pnl_delta": 2.5,
            })
            telemetry.append_market_sample({
                "market_id": "m3",
                "book_reasons": ["wide_spread>250"],
            })
            loop = ResearchLoop(f"{tmpdir}/research")
            result = loop.run_cycle(PolymarketRuntimeResearchAdapter(tmpdir))
            self.assertEqual(result.source, "live-runtime-artifacts")
            self.assertTrue(any("runtime quality" in insight.title for insight in result.insights))
            report_path = f"{tmpdir}/research/{result.cycle_id}.json"
            with open(report_path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            self.assertEqual(payload["source"], "live-runtime-artifacts")


if __name__ == "__main__":
    unittest.main()
