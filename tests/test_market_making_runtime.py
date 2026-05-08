import unittest
from pathlib import Path

import yaml

from cli import place_mm_quotes
from market_data import OrderBook
from strategies.toxicity_mm import MMQuote, ToxicityMM

REPO_ROOT = Path(__file__).resolve().parents[1]


class FakeBroker:
    def __init__(self):
        self.cancelled = []
        self.orders = []

    async def cancel_all_market(self, market_id: str):
        self.cancelled.append(market_id)

    async def place_order(self, market_id: str, outcome: str, side: str, size: float, price: float, post_only: bool = True):
        self.orders.append({
            "market_id": market_id,
            "outcome": outcome,
            "side": side,
            "size": size,
            "price": price,
            "post_only": post_only,
        })
        return f"oid-{len(self.orders)}"

    async def process_orderbook(self, market_id: str, orderbook):
        return None


class MarketMakingRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_place_mm_quotes_posts_bid_and_ask(self):
        broker = FakeBroker()
        quote = MMQuote(
            market_id="m1",
            outcome="YES",
            bid_price=0.47,
            ask_price=0.49,
            bid_size=10.0,
            ask_size=10.0,
            reason="test",
        )
        orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.50, 100.0)],
            yes_bids=[(0.46, 100.0)],
            no_asks=[(0.54, 100.0)],
            no_bids=[(0.50, 100.0)],
            timestamp=1.0,
            sequence=1,
        )

        await place_mm_quotes(broker, "m1", quote, orderbook)

        self.assertEqual(broker.cancelled, ["m1"])
        self.assertEqual(len(broker.orders), 2)
        self.assertEqual(broker.orders[0]["side"], "BUY")
        self.assertEqual(broker.orders[1]["side"], "SELL")
        self.assertEqual(broker.orders[0]["price"], 0.47)
        self.assertEqual(broker.orders[1]["price"], 0.49)


class ToxicityMMSizingTests(unittest.TestCase):
    def setUp(self):
        with open(REPO_ROOT / "config.yaml", "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle)

    def test_generate_quotes_respects_paper_capital_budget(self):
        strategy = ToxicityMM(self.config)
        orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.76, 100.0)],
            yes_bids=[(0.75, 100.0)],
            no_asks=[(0.25, 100.0)],
            no_bids=[(0.24, 100.0)],
            timestamp=1.0,
            sequence=1,
        )

        quote, _ = strategy.generate_quotes("m1", orderbook)

        self.assertIsNotNone(quote)
        self.assertLess(quote.bid_size, 100)
        self.assertLess(quote.ask_size, 100)


if __name__ == "__main__":
    unittest.main()
