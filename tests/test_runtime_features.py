import json
import tempfile
import unittest
from pathlib import Path

from book_quality import assess_book_quality
from execution import PolymarketExecutor
from market_data import OrderBook
from research.loop import ResearchLoop
from research.polymarket import PolymarketRuntimeResearchAdapter
from runtime_telemetry import RuntimeTelemetry
from status_utils import render_status_text, runtime_health_payload


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
            self.assertEqual(result.context.metadata["run_scope"], "paper-test")
            self.assertIn("run scope paper-test", result.summary)
            self.assertTrue(any("runtime quality" in insight.title for insight in result.insights))
            report_path = f"{tmpdir}/research/{result.cycle_id}.json"
            with open(report_path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            self.assertEqual(payload["source"], "live-runtime-artifacts")

    async def test_status_utils_render_runtime_summary_and_health(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.write_strategy_metrics({
                "toxicity_mm": {
                    "quotes_submitted": 4,
                    "orders_resting": 2,
                    "orders_filled": 1,
                    "cancellations": 1,
                    "realized_pnl": 0.5,
                    "markets_seen": 8,
                    "toxic_book_skips": 3,
                }
            })
            telemetry.update_status(
                run_id="paper-test",
                phase="running",
                mode="paper",
                baseline_strategy="toxicity_mm",
                research_candidates=["mean_reversion_5min", "opening_range", "time_decay"],
                loop_count=7,
                fetched_markets=8,
                processed_markets=5,
                toxic_skips=3,
                bankroll=500.0,
                open_position_count=1,
                resolved_trade_count=2,
                win_rate=0.5,
                risk={
                    "capital": 501.25,
                    "daily_pnl": 1.25,
                    "max_drawdown": 0.02,
                    "open_order_count": 2,
                    "gross_position_exposure": 4.0,
                    "gross_open_order_exposure": 3.0,
                    "total_gross_exposure": 7.0,
                },
            )
            status = telemetry.read_status()
            self.assertNotIn("strategy_metrics", status)
            rendered = render_status_text(tmpdir)
            self.assertIn("Run id: paper-test", rendered)
            self.assertIn("Baseline strategy: toxicity_mm", rendered)
            self.assertIn("Research candidates: mean_reversion_5min, opening_range, time_decay", rendered)
            self.assertIn("Strategy metrics:", rendered)
            self.assertIn("toxicity_mm", rendered)
            self.assertTrue(Path(tmpdir, "latest-status.txt").exists())
            health = runtime_health_payload(tmpdir, max_heartbeat_age=180)
            self.assertTrue(health["healthy"])


if __name__ == "__main__":
    unittest.main()
