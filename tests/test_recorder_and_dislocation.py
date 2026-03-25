import csv
import tempfile
import unittest
from pathlib import Path

import yaml

from event_recorder import EventRecorder
from market_data import OrderBook
from strategies.dislocation_arb import ComplementaryDislocationStrategy

REPO_ROOT = Path(__file__).resolve().parents[1]


class RecorderAndDislocationTests(unittest.TestCase):
    def setUp(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle)

    def make_orderbook(self):
        return OrderBook(
            market_id="arb-market",
            yes_asks=[(0.62, 200.0)],
            yes_bids=[(0.60, 150.0)],
            no_asks=[(0.46, 180.0)],
            no_bids=[(0.44, 120.0)],
            timestamp=1_700_200_000.0,
            sequence=7,
        )

    def test_event_recorder_writes_feature_row(self):
        orderbook = self.make_orderbook()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "events.csv"
            recorder = EventRecorder(output)
            event = recorder.record(orderbook, volume=25000, regime="calm")
            self.assertTrue(output.exists())
            with output.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_id"], "arb-market")
            self.assertAlmostEqual(float(rows[0]["dislocation"]), event.dislocation)

    def test_dislocation_strategy_generates_signal(self):
        strategy = ComplementaryDislocationStrategy(self.config)
        signal = strategy.generate_signal("arb-market", self.make_orderbook(), volume=25000)
        self.assertIsNotNone(signal)
        self.assertIn(signal.outcome, {"YES", "NO"})
        self.assertIn(signal.action, {"BUY", "SELL"})
        self.assertGreater(abs(signal.dislocation), self.config["strategies"]["dislocation_arb"]["min_dislocation"])
        self.assertGreater(signal.expected_edge, 0)


if __name__ == "__main__":
    unittest.main()
