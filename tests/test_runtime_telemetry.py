import asyncio
import json
import tempfile
import time
import unittest
from pathlib import Path

import yaml

from api_wrapper import runtime_health_payload, runtime_status_payload
from execution import create_broker
from market_data import OrderBook
from runtime_telemetry import RuntimeTelemetryStore

REPO_ROOT = Path(__file__).resolve().parents[1]


class RuntimeTelemetryTests(unittest.TestCase):
    def setUp(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle)

    def make_orderbook(self):
        return OrderBook(
            market_id="crypto-1",
            yes_asks=[(0.52, 200.0)],
            yes_bids=[(0.50, 150.0)],
            no_asks=[(0.50, 200.0)],
            no_bids=[(0.48, 150.0)],
            timestamp=1_700_300_000.0,
            sequence=11,
        )

    def test_runtime_store_writes_status_and_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = RuntimeTelemetryStore(Path(tmpdir))
            store.append_event("runtime.started", {"run_id": "run-1"})
            store.update_status(
                run_id="run-1",
                mode="paper",
                phase="running",
                loop_count=3,
                markets_fetched=9,
                markets_selected=4,
                strategies=["mean_reversion_5min"],
                broker_summary={"equity": 1001.5, "open_orders": 1},
            )

            status = json.loads((Path(tmpdir) / "status.json").read_text(encoding="utf-8"))
            self.assertEqual(status["run_id"], "run-1")
            self.assertEqual(status["phase"], "running")
            self.assertEqual(status["loop_count"], 3)
            self.assertEqual(status["broker_summary"]["equity"], 1001.5)

            events = (Path(tmpdir) / "events.jsonl").read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(events), 1)
            payload = json.loads(events[0])
            self.assertEqual(payload["event_type"], "runtime.started")

    def test_paper_broker_snapshot_roundtrip(self):
        broker = create_broker("paper", self.config, market_data=None)
        orderbook = self.make_orderbook()

        async def run_flow():
            await broker.__aenter__()
            broker.md = type("MD", (), {"orderbooks": {"crypto-1": orderbook}})()
            order_id = await broker.place_order("crypto-1", "YES", "BUY", 10, 0.53, post_only=True)
            await broker.__aexit__(None, None, None)
            return order_id

        order_id = asyncio.run(run_flow())
        snapshot = broker.snapshot_state()

        restored = create_broker("paper", self.config, market_data=None)
        restored.restore_state(snapshot)

        self.assertEqual(restored.cash, broker.cash)
        self.assertEqual(restored.positions, broker.positions)
        self.assertIn(order_id, restored.orders)
        self.assertEqual(restored.orders[order_id].status, "filled")

    def test_api_wrapper_reads_runtime_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = RuntimeTelemetryStore(Path(tmpdir))
            store.update_status(
                run_id="run-2",
                mode="paper",
                phase="running",
                loop_count=1,
                markets_fetched=2,
                markets_selected=1,
                strategies=["shock_reversion"],
                broker_summary={"equity": 999.0},
            )
            status = runtime_status_payload(Path(tmpdir))
            health = runtime_health_payload(Path(tmpdir), max_heartbeat_age=60)
            self.assertEqual(status["run_id"], "run-2")
            self.assertEqual(status["broker_summary"]["equity"], 999.0)
            self.assertEqual(health["status"], "healthy")

            stale = json.loads((Path(tmpdir) / "status.json").read_text(encoding="utf-8"))
            stale["heartbeat_ts"] = time.time() - 3600
            (Path(tmpdir) / "status.json").write_text(json.dumps(stale), encoding="utf-8")
            health = runtime_health_payload(Path(tmpdir), max_heartbeat_age=60)
            self.assertEqual(health["status"], "stale")


if __name__ == "__main__":
    unittest.main()
