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

    async def test_partial_fill_counts_as_filled_telemetry(self):
        executor = PolymarketExecutor(self.config, market_data=None, mode="paper")
        await executor.__aenter__()
        try:
            order_id = await executor.place_order("m1", "YES", "BUY", 10, 0.55, strategy_family="toxicity_mm", order_kind="quote")
            fill = executor.fill_order(order_id, fill_price=0.54, fill_size=2.5)
            metrics = executor.get_family_metrics()["toxicity_mm"]
            self.assertTrue(fill["filled"])
            self.assertEqual(metrics["orders_filled"], 1)
            self.assertEqual(metrics["orders_resting"], 1)
            self.assertEqual(executor.orders[order_id]["status"], "partially_filled")
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
                gate_state="RED",
                gate_reasons=["win_rate=0.090 < 0.20 with resolved_count=35 >= 20"],
                new_order_pause=True,
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
            self.assertIn("Runtime gate: RED | New orders paused: True", rendered)
            self.assertIn("Gate reasons: win_rate=0.090 < 0.20 with resolved_count=35 >= 20", rendered)
            self.assertIn("Strategy metrics:", rendered)
            self.assertIn("toxicity_mm", rendered)
            self.assertTrue(Path(tmpdir, "latest-status.txt").exists())
            health = runtime_health_payload(tmpdir, max_heartbeat_age=180)
            self.assertTrue(health["healthy"])

    async def test_runtime_telemetry_treats_empty_status_file_as_empty_dict(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            (runtime_dir / "status.json").write_text("", encoding="utf-8")
            telemetry = RuntimeTelemetry(runtime_dir)
            self.assertEqual(telemetry.read_status(), {})
            rendered = render_status_text(runtime_dir)
            self.assertIn("Run id: unknown", rendered)

    async def test_runtime_health_payload_handles_invalid_status_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            (runtime_dir / "status.json").write_text("{", encoding="utf-8")
            telemetry = RuntimeTelemetry(runtime_dir)
            self.assertEqual(telemetry.read_status(), {})
            payload = runtime_health_payload(runtime_dir, max_heartbeat_age=180)
            self.assertFalse(payload["healthy"])

    async def test_runtime_status_payload_skips_corrupt_event_lines(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            telemetry = RuntimeTelemetry(runtime_dir)
            telemetry.update_status(run_id="paper-test", phase="running", mode="paper")
            telemetry.events_path.write_text(
                '\n'.join([
                    '{"event_type":"runtime.started","payload":{"run_id":"paper-test"},"run_id":"paper-test","ts":1}',
                    '{"event_type":"runtime.started","payload":{"run_id":"paper-test"}',
                    '{"event_type":"runtime.loop","payload":{"loop":1},"run_id":"paper-test","ts":2}',
                ]) + '\n',
                encoding="utf-8",
            )
            payload = runtime_health_payload(runtime_dir, max_heartbeat_age=180)
            self.assertTrue(payload["healthy"])
            self.assertEqual(len(payload["recent_events"]), 2)

    async def test_preserve_run_evidence_copies_runtime_and_optional_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            data_dir = Path(tmpdir) / "data"
            runtime_dir = data_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            telemetry = RuntimeTelemetry(runtime_dir)
            telemetry.update_status(
                run_id="paper-stop",
                phase="running",
                mode="paper",
                loop_count=9,
                win_rate=0.5,
                resolved_trade_count=2,
                bankroll=497.5,
                risk={
                    "capital": 497.5,
                    "realized_pnl_total": -2.5,
                },
            )
            telemetry.append_event("runtime.started", {"phase": "running"}, run_id="paper-stop")
            telemetry.append_market_sample({"market_id": "m1", "mid": 0.42}, run_id="paper-stop")
            telemetry.write_strategy_metrics({"toxicity_mm": {"orders_filled": 2}})
            (runtime_dir / "ledger.db").write_bytes(b"sqlite-bytes")
            (runtime_dir / "ops_status.txt").write_text("ops ok\n", encoding="utf-8")
            (runtime_dir / "ops_evidence.txt").write_text("evidence ok\n", encoding="utf-8")
            (runtime_dir / "ops_settlement_diagnostics.txt").write_text("settlement ok\n", encoding="utf-8")
            (runtime_dir / "fill_markout_audit_latest.md").write_text("# fill audit\n", encoding="utf-8")
            (runtime_dir / "settlement_latency_audit_latest.md").write_text("# settlement audit\n", encoding="utf-8")
            (runtime_dir / "reconcile_metrics_latest.txt").write_text("reconcile ok\n", encoding="utf-8")
            research_dir = data_dir / "research"
            research_dir.mkdir(parents=True, exist_ok=True)
            (research_dir / "latest.json").write_text('{"gate_state":"YELLOW"}\n', encoding="utf-8")
            (research_dir / "latest.md").write_text("# latest\n", encoding="utf-8")

            manifest = telemetry.preserve_run_evidence(trigger="circuit_breaker", snapshot_ts=1_744_333_600)

            snapshot_dir = Path(manifest["snapshot_dir"])
            self.assertTrue(snapshot_dir.exists())
            self.assertEqual(manifest["run_id"], "paper-stop")
            self.assertEqual(manifest["trigger"], "circuit_breaker")
            self.assertEqual(manifest["bankroll"], 497.5)
            self.assertEqual(manifest["resolved_trade_count"], 2)
            self.assertTrue((snapshot_dir / "status.json").exists())
            self.assertTrue((snapshot_dir / "events.jsonl").exists())
            self.assertTrue((snapshot_dir / "market_samples.jsonl").exists())
            self.assertTrue((snapshot_dir / "strategy_metrics.json").exists())
            self.assertTrue((snapshot_dir / "ledger.db").exists())
            self.assertTrue((snapshot_dir / "latest-status.txt").exists())
            self.assertTrue((snapshot_dir / "ops_status.txt").exists())
            self.assertTrue((snapshot_dir / "ops_evidence.txt").exists())
            self.assertTrue((snapshot_dir / "ops_settlement_diagnostics.txt").exists())
            self.assertTrue((snapshot_dir / "fill_markout_audit_latest.md").exists())
            self.assertTrue((snapshot_dir / "settlement_latency_audit_latest.md").exists())
            self.assertTrue((snapshot_dir / "reconcile_metrics_latest.txt").exists())
            self.assertTrue((snapshot_dir / "research-latest.json").exists())
            self.assertTrue((snapshot_dir / "research-latest.md").exists())
            artifact_names = {Path(artifact["dest"]).name for artifact in manifest["artifacts"]}
            self.assertTrue({
                "status.json",
                "events.jsonl",
                "market_samples.jsonl",
                "strategy_metrics.json",
                "ledger.db",
                "latest-status.txt",
                "ops_status.txt",
                "ops_evidence.txt",
                "ops_settlement_diagnostics.txt",
                "fill_markout_audit_latest.md",
                "settlement_latency_audit_latest.md",
                "reconcile_metrics_latest.txt",
                "research-latest.json",
                "research-latest.md",
            }.issubset(artifact_names))


if __name__ == "__main__":
    unittest.main()
