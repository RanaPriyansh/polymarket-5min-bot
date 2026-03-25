import unittest
from pathlib import Path

import yaml

from market_data import OrderBook
from risk import RiskManager
from strategies.mean_reversion_5min import MeanReversion5Min

REPO_ROOT = Path(__file__).resolve().parents[1]


class RiskAndStrategyTests(unittest.TestCase):
    def setUp(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle)

    def test_daily_profit_does_not_trigger_loss_breaker(self):
        rm = RiskManager(self.config, initial_capital=1000.0)
        rm.update_capital(80.0)
        self.assertFalse(rm.check_circuit_breakers())

    def test_position_sizing_respects_capital_risk_budget(self):
        rm = RiskManager(self.config, initial_capital=1000.0)
        sizing = rm.calculate_position_size(
            "mean_reversion_5min",
            confidence=0.95,
            price=0.60,
            volatility=0.08,
            stop_loss=0.05,
            edge=0.02,
        )
        self.assertGreater(sizing.size, 0)
        self.assertLessEqual(sizing.target_notional, 100.0)
        self.assertLessEqual(sizing.max_loss, 12.5)

    def test_mean_reversion_signal_uses_risk_based_size(self):
        strat = MeanReversion5Min(self.config)
        market_id = "example-market"
        prices = [0.58] * 20 + [0.42]
        timestamp = 1_700_000_000
        for idx, price in enumerate(prices):
            strat.update_price(market_id, price, timestamp + idx * 60, volume=20_000)

        orderbook = OrderBook(
            market_id=market_id,
            yes_asks=[(0.43, 100.0)],
            yes_bids=[(0.41, 250.0)],
            no_asks=[(0.59, 100.0)],
            no_bids=[(0.57, 200.0)],
            timestamp=timestamp + len(prices) * 60,
            sequence=1,
        )
        rm = RiskManager(self.config, initial_capital=1000.0)
        signal = strat.generate_signal(
            market_id=market_id,
            outcome="YES",
            price=0.42,
            orderbook=orderbook,
            volume=20_000,
            risk_manager=rm,
        )

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "BUY")
        self.assertLessEqual(signal.size * signal.price, 100.0)
        self.assertGreater(signal.expected_edge, 0)
        self.assertLess(signal.zscore, 0)


if __name__ == "__main__":
    unittest.main()
