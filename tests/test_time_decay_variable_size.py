import unittest
from pathlib import Path

import yaml

from market_data import OrderBook
from strategies.time_decay import TimeDecay


class TimeDecayVariableSizeTests(unittest.TestCase):
    def _cfg(self, **overrides):
        params = {
            "min_seconds_left": 10,
            "max_seconds_left": 60,
            "min_price": 0.55,
            "max_price": 0.92,
            "min_notional_usd": 1.0,
            "max_notional_usd": 6.0,
        }
        params.update(overrides)
        return {
            "strategies": {"time_decay": params},
            "filters": {
                "max_book_spread_bps": 1000,
                "min_top_depth": 1,
                "min_top_notional": 0,
                "max_depth_ratio": 100,
            },
        }

    def _market(self, **overrides):
        market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "slot_id": "btc:5:100",
            "end_ts": 130.0,
            "outcomes": ["Up", "Down"],
        }
        market.update(overrides)
        return market

    def _book(self, *, yes_bid=0.69, yes_ask=0.71, no_bid=0.29, no_ask=0.31):
        return OrderBook(
            market_id="m1",
            yes_asks=[(yes_ask, 10)],
            yes_bids=[(yes_bid, 10)],
            no_asks=[(no_ask, 10)],
            no_bids=[(no_bid, 10)],
            timestamp=100.0,
            sequence=1,
            outcome_labels=("Up", "Down"),
            slot_id="btc:5:100",
            end_ts=130.0,
        )

    def _book_with_sizes(
        self,
        *,
        yes_bid=0.69,
        yes_ask=0.71,
        yes_bid_size=10,
        yes_ask_size=10,
        no_bid=0.69,
        no_ask=0.71,
        no_bid_size=10,
        no_ask_size=10,
    ):
        return OrderBook(
            market_id="m1",
            yes_asks=[(yes_ask, yes_ask_size)],
            yes_bids=[(yes_bid, yes_bid_size)],
            no_asks=[(no_ask, no_ask_size)],
            no_bids=[(no_bid, no_bid_size)],
            timestamp=100.0,
            sequence=1,
            outcome_labels=("Up", "Down"),
            slot_id="btc:5:100",
            end_ts=130.0,
        )

    def test_mid_070_with_30s_left_fires_buy_with_confidence_scaled_size(self):
        strategy = TimeDecay(self._cfg())

        signal = strategy.generate_signal("m1", self._market(), self._book(), current_time=100.0)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "BUY")
        self.assertEqual(signal.outcome, "Up")
        self.assertAlmostEqual(signal.confidence, 0.6)
        expected_final_size = 1.0 / signal.price
        self.assertAlmostEqual(signal.size, expected_final_size)

    def test_mid_above_max_price_returns_none(self):
        strategy = TimeDecay(self._cfg())

        signal = strategy.generate_signal(
            "m1",
            self._market(),
            self._book(yes_bid=0.94, yes_ask=0.96, no_bid=0.04, no_ask=0.06),
            current_time=100.0,
        )

        self.assertIsNone(signal)

    def test_same_slot_fires_only_once_per_outcome(self):
        strategy = TimeDecay(self._cfg())
        market = self._market()
        book = self._book()

        first = strategy.generate_signal("m1", market, book, current_time=100.0)
        self.assertIsNotNone(first)
        strategy.mark_fired(market["slot_id"], first.outcome)
        second = strategy.generate_signal("m1", market, book, current_time=101.0)
        next_slot = strategy.generate_signal(
            "m2",
            self._market(id="m2", slot_id="btc:5:400", end_ts=430.0),
            self._book(),
            current_time=400.0,
        )

        self.assertIsNone(second)
        self.assertIsNotNone(next_slot)

    def test_generate_signal_does_not_mark_fired_until_caller_confirms_execution(self):
        strategy = TimeDecay(self._cfg())
        market = self._market()
        book = self._book()

        first = strategy.generate_signal("m1", market, book, current_time=100.0)
        second = strategy.generate_signal("m1", market, book, current_time=101.0)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        strategy.mark_fired(market["slot_id"], first.outcome)
        self.assertIsNone(strategy.generate_signal("m1", market, book, current_time=102.0))

    def test_first_outcome_untradeable_can_still_select_tradeable_second_outcome(self):
        strategy = TimeDecay(self._cfg(min_notional_usd=1.0, max_notional_usd=6.0))

        signal = strategy.generate_signal(
            "m1",
            self._market(),
            self._book_with_sizes(yes_bid=0.69, yes_ask=0.71, yes_bid_size=0.1, yes_ask_size=0.1),
            current_time=100.0,
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.outcome, "Down")
        self.assertTrue(signal.book_quality["is_tradeable"])

    def test_second_outcome_untradeable_does_not_beat_tradeable_first_outcome(self):
        strategy = TimeDecay(self._cfg(min_notional_usd=1.0, max_notional_usd=6.0))

        signal = strategy.generate_signal(
            "m1",
            self._market(),
            self._book_with_sizes(
                yes_bid=0.59,
                yes_ask=0.61,
                no_bid=0.79,
                no_ask=0.81,
                no_bid_size=0.1,
                no_ask_size=0.1,
            ),
            current_time=100.0,
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.outcome, "Up")
        self.assertTrue(signal.book_quality["is_tradeable"])

    def test_sizing_respects_min_and_max_notional_bounds(self):
        min_strategy = TimeDecay(self._cfg(min_notional_usd=4.0, max_notional_usd=10.0))
        min_signal = min_strategy.generate_signal("m1", self._market(), self._book(), current_time=100.0)
        self.assertIsNotNone(min_signal)
        self.assertAlmostEqual(
            min_signal.size,
            4.0 / min_signal.price,
        )

        max_strategy = TimeDecay(self._cfg(min_notional_usd=4.0, max_notional_usd=2.0))
        max_signal = max_strategy.generate_signal("m1", self._market(), self._book(), current_time=100.0)
        self.assertIsNotNone(max_signal)
        self.assertAlmostEqual(
            max_signal.size,
            2.0 / max_signal.price,
        )

    def test_config_activates_time_decay_with_controlled_sizing_defaults(self):
        cfg = yaml.safe_load((Path(__file__).resolve().parents[1] / "config.yaml").read_text(encoding="utf-8"))

        self.assertIn("time_decay", cfg["strategies"]["active"])
        self.assertEqual(cfg["strategies"]["time_decay"]["min_notional_usd"], 1.0)
        self.assertEqual(cfg["strategies"]["time_decay"]["max_notional_usd"], 6.0)
        self.assertEqual(cfg["strategies"]["time_decay"]["max_price"], 0.92)
        self.assertEqual(cfg["strategies"]["time_decay"]["directional_signal_entry_style"], "resting_limit")


if __name__ == "__main__":
    unittest.main()
