import unittest

from risk import RiskManager


class RiskSizingPolicyTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "strategies": {
                "mean_reversion_5min": {"kelly_fraction": 0.25},
                "opening_range": {"kelly_fraction": 0.10},
            },
            "risk": {
                "circuit_breaker_dd": 0.1,
                "max_daily_loss": 0.05,
                "max_position_size": 1.0,
                "max_risk_per_trade_usd": 100,
            },
        }
        self.rm = RiskManager(self.config, initial_capital=500.0)

    def test_bounded_size_returns_zero_when_edge_non_positive(self):
        sizing = self.rm.calculate_bounded_size(
            "mean_reversion_5min",
            edge=0.0,
            price=0.50,
            stop_loss_distance=0.10,
        )
        self.assertEqual(sizing.size, 0.0)
        self.assertIn("edge<=0", sizing.reason)

    def test_bounded_size_shrinks_when_stop_loss_distance_grows(self):
        tight = self.rm.calculate_bounded_size(
            "mean_reversion_5min",
            edge=0.04,
            price=0.50,
            stop_loss_distance=0.05,
        )
        wide = self.rm.calculate_bounded_size(
            "mean_reversion_5min",
            edge=0.04,
            price=0.50,
            stop_loss_distance=0.20,
        )
        self.assertGreater(tight.size, wide.size)

    def test_bounded_size_respects_notional_cap(self):
        capped_config = {
            **self.config,
            "risk": {
                **self.config["risk"],
                "max_position_size": 0.1,
                "max_risk_per_trade_usd": 10,
            },
        }
        capped_rm = RiskManager(capped_config, initial_capital=500.0)
        sizing = capped_rm.calculate_bounded_size(
            "mean_reversion_5min",
            edge=0.20,
            price=0.25,
            stop_loss_distance=0.01,
        )
        self.assertLessEqual(sizing.size * 0.25, 10.0 + 1e-9)

    def test_bounded_size_honors_strategy_kelly_fraction(self):
        high = self.rm.calculate_bounded_size(
            "mean_reversion_5min",
            edge=0.03,
            price=0.50,
            stop_loss_distance=0.10,
        )
        low = self.rm.calculate_bounded_size(
            "opening_range",
            edge=0.03,
            price=0.50,
            stop_loss_distance=0.10,
        )
        self.assertGreater(high.size, low.size)


if __name__ == "__main__":
    unittest.main()
