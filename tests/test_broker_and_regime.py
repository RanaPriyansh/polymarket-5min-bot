import asyncio
import unittest
from pathlib import Path

import yaml

from cli import classify_runtime_regime, merge_unique_markets
from execution import PaperBroker, create_broker
from market_data import OrderBook
from strategies.shock_reversion import ShockReversionStrategy

REPO_ROOT = Path(__file__).resolve().parents[1]


class BrokerAndRegimeTests(unittest.TestCase):
    def setUp(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle)

    def test_merge_unique_markets_dedupes_ids(self):
        merged = merge_unique_markets(
            [{"id": "a"}, {"id": "b"}],
            [{"id": "b"}, {"id": "c"}],
        )
        self.assertEqual([m["id"] for m in merged], ["a", "b", "c"])

    def test_paper_broker_places_in_memory_fill(self):
        broker = create_broker("paper", self.config, market_data=None)
        self.assertIsInstance(broker, PaperBroker)

        async def run_order():
            await broker.__aenter__()
            order_id = await broker.place_order("market-1", "YES", "BUY", 10, 0.45)
            await broker.__aexit__(None, None, None)
            return order_id

        order_id = asyncio.run(run_order())
        self.assertTrue(order_id.startswith("paper-"))
        self.assertIn(order_id, broker.orders)
        self.assertEqual(broker.orders[order_id].status, "open")
        self.assertEqual(broker.open_orders[order_id].remaining_size, 10.0)

    def test_runtime_regime_classification(self):
        regime = classify_runtime_regime({}, spread_bps=350, imbalance=0.2, realized_vol=0.04)
        self.assertEqual(regime, "stressed")
        regime = classify_runtime_regime({}, spread_bps=100, imbalance=0.1, realized_vol=0.15)
        self.assertEqual(regime, "volatile")
        regime = classify_runtime_regime({}, spread_bps=50, imbalance=0.7, realized_vol=0.03)
        self.assertEqual(regime, "one_sided")

    def test_shock_reversion_generates_signal(self):
        strat = ShockReversionStrategy(self.config)
        market_id = "shock-market"
        prices = [0.50] * 20 + [0.64]
        ts = 1_700_100_000
        for idx, price in enumerate(prices):
            strat.update_price(market_id, price, ts + idx * 60, volume=20_000)

        orderbook = OrderBook(
            market_id=market_id,
            yes_asks=[(0.63, 240.0)],
            yes_bids=[(0.61, 120.0)],
            no_asks=[(0.39, 200.0)],
            no_bids=[(0.37, 80.0)],
            timestamp=ts + len(prices) * 60,
            sequence=1,
        )
        signal = strat.generate_signal(market_id, "YES", 0.64, orderbook, 20_000)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "SELL")
        self.assertGreater(signal.expected_edge, 0)
        self.assertEqual(signal.regime, "shock")


if __name__ == "__main__":
    unittest.main()
