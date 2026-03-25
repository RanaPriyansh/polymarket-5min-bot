import asyncio
import json
import tempfile
import unittest
from pathlib import Path

import yaml

from event_recorder import EventRecorder
from execution import create_broker
from market_data import OrderBook
from resolver_map import ResolverMap

REPO_ROOT = Path(__file__).resolve().parents[1]


class ResolverAndPaperSimTests(unittest.TestCase):
    def setUp(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle)

    def make_orderbook(self):
        return OrderBook(
            market_id="sports-1",
            yes_asks=[(0.52, 200.0)],
            yes_bids=[(0.50, 150.0)],
            no_asks=[(0.50, 200.0)],
            no_bids=[(0.48, 150.0)],
            timestamp=1_700_300_000.0,
            sequence=11,
        )

    def test_resolver_map_infers_source_family_and_persists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "resolver_map.json"
            rm = ResolverMap(path)
            info = rm.upsert_market({"id": "sports-1", "question": "Will the NBA game go to overtime?"})
            self.assertEqual(info.source_family, "sports")
            self.assertTrue(path.exists())
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertIn("sports-1", saved)

    def test_event_recorder_includes_resolver_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "events.csv"
            recorder = EventRecorder(output)
            resolver = ResolverMap(Path(tmpdir) / "resolver.json").infer_source_family(
                {"id": "sports-1", "question": "Will the NBA game go to overtime?"}
            )
            event = recorder.record(self.make_orderbook(), volume=22000, regime="calm", resolver_info=resolver)
            self.assertEqual(event.source_family, "sports")
            self.assertGreater(event.resolver_confidence, 0)

    def test_paper_broker_can_rest_and_fill_later(self):
        broker = create_broker("paper", self.config, market_data=None)
        orderbook_before = self.make_orderbook()
        orderbook_after = OrderBook(
            market_id="sports-1",
            yes_asks=[(0.49, 200.0)],
            yes_bids=[(0.48, 150.0)],
            no_asks=[(0.52, 200.0)],
            no_bids=[(0.50, 150.0)],
            timestamp=1_700_300_060.0,
            sequence=12,
        )

        async def run_flow():
            await broker.__aenter__()
            broker.md = type("MD", (), {"orderbooks": {"sports-1": orderbook_before}})()
            order_id = await broker.place_order("sports-1", "YES", "BUY", 10, 0.49, post_only=True)
            order = broker.orders[order_id]
            self.assertEqual(order.status, "open")
            await broker.process_orderbook("sports-1", orderbook_after)
            await broker.__aexit__(None, None, None)
            return order_id

        order_id = asyncio.run(run_flow())
        order = broker.orders[order_id]
        self.assertEqual(order.status, "filled")
        self.assertEqual(broker.positions["sports-1"]["YES"], 10.0)
        summary = asyncio.run(broker.refresh_positions())
        self.assertIn("equity", summary)

    def test_paper_broker_crossing_buy_fills_at_ask_not_mid(self):
        broker = create_broker("paper", self.config, market_data=None)
        orderbook = self.make_orderbook()

        async def run_flow():
            await broker.__aenter__()
            broker.md = type("MD", (), {"orderbooks": {"sports-1": orderbook}})()
            order_id = await broker.place_order("sports-1", "YES", "BUY", 10, 0.53, post_only=True)
            await broker.__aexit__(None, None, None)
            return order_id

        order_id = asyncio.run(run_flow())
        order = broker.orders[order_id]
        self.assertEqual(order.status, "filled")
        self.assertAlmostEqual(order.average_fill_price, 0.52)


if __name__ == "__main__":
    unittest.main()
