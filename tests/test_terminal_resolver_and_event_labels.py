import tempfile
import unittest
from pathlib import Path

import yaml

from cli import seconds_to_resolution
from event_recorder import EventRecorder
from market_data import OrderBook
from resolver_map import ResolverInfo
from strategies.terminal_resolver import TerminalResolverStrategy

REPO_ROOT = Path(__file__).resolve().parents[1]


class TerminalResolverAndEventLabelTests(unittest.TestCase):
    def setUp(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle)

    def make_market(self):
        return {
            "id": "sports-2",
            "question": "Will the NBA game go to overtime?",
            "end_date_iso": "2099-01-01T00:00:30Z",
            "volume": 25000,
        }

    def make_orderbook(self):
        return OrderBook(
            market_id="sports-2",
            yes_asks=[(0.561, 140.0)],
            yes_bids=[(0.555, 260.0)],
            no_asks=[(0.445, 120.0)],
            no_bids=[(0.439, 220.0)],
            timestamp=1_700_400_000.0,
            sequence=15,
        )

    def test_event_recorder_writes_richer_labels(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "events.csv"
            recorder = EventRecorder(output)
            resolver = ResolverInfo(
                market_id="sports-2",
                source_family="sports",
                resolver="official_sports_feed",
                confidence=0.7,
            )
            event = recorder.record(
                self.make_orderbook(),
                volume=25000,
                regime="terminal",
                resolver_info=resolver,
                seconds_to_resolution=28.0,
                active_signal_family="terminal_resolver",
            )
            self.assertEqual(event.source_family, "sports")
            self.assertEqual(event.active_signal_family, "terminal_resolver")
            self.assertAlmostEqual(event.seconds_to_resolution, 28.0)
            self.assertGreater(event.yes_spread_bps, 0)
            self.assertGreater(event.yes_microprice, 0)

    def test_terminal_resolver_generates_signal(self):
        strategy = TerminalResolverStrategy(self.config)
        resolver = ResolverInfo(
            market_id="sports-2",
            source_family="sports",
            resolver="official_sports_feed",
            confidence=0.7,
        )
        signal = strategy.generate_signal(
            market_id="sports-2",
            market=self.make_market(),
            orderbook=self.make_orderbook(),
            volume=25000,
            resolver_info=resolver,
            seconds_to_resolution=25.0,
        )
        self.assertIsNotNone(signal)
        self.assertEqual(signal.outcome, "YES")
        self.assertIn(signal.action, {"BUY", "SELL"})
        self.assertGreater(signal.expected_edge, 0)


if __name__ == "__main__":
    unittest.main()
