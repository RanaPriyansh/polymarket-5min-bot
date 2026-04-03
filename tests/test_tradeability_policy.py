import unittest

from market_data import OrderBook
from tradeability_policy import assess_tradeability, tradeability_policy


class TradeabilityPolicyTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "filters": {
                "max_book_spread_bps": 500,
                "min_top_depth": 2,
                "min_top_notional": 0.5,
                "max_depth_ratio": 12,
            },
            "tradeability": {
                "per_strategy": {
                    "time_decay": {
                        "max_book_spread_bps": 800,
                    }
                }
            },
        }
        self.orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.53, 2.0)],
            yes_bids=[(0.50, 2.0)],
            no_asks=[(0.50, 2.0)],
            no_bids=[(0.47, 2.0)],
            timestamp=100.0,
            sequence=1,
            outcome_labels=("Up", "Down"),
        )

    def test_default_policy_uses_global_filters(self):
        policy = tradeability_policy(self.config, "mean_reversion_5min")
        self.assertEqual(policy.max_spread_bps, 500)
        self.assertEqual(policy.min_top_depth, 2)
        self.assertEqual(policy.min_top_notional, 0.5)
        self.assertEqual(policy.max_depth_ratio, 12)

    def test_strategy_override_can_change_thresholds(self):
        policy = tradeability_policy(self.config, "time_decay")
        self.assertEqual(policy.max_spread_bps, 800)
        quality = policy.assess(self.orderbook, "Up")
        self.assertTrue(quality.is_tradeable)

    def test_assess_tradeability_matches_policy_assess(self):
        via_helper = assess_tradeability(self.config, "mean_reversion_5min", self.orderbook, "Up")
        via_policy = tradeability_policy(self.config, "mean_reversion_5min").assess(self.orderbook, "Up")
        self.assertEqual(via_helper.is_tradeable, via_policy.is_tradeable)
        self.assertEqual(via_helper.reasons, via_policy.reasons)
        self.assertAlmostEqual(via_helper.spread_bps, via_policy.spread_bps)


if __name__ == "__main__":
    unittest.main()
