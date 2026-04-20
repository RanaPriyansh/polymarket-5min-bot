import time
import unittest
from types import SimpleNamespace

from cli import _strategy_directional_signal_entry_style
from execution import PolymarketExecutor, resolve_signal_fill_behavior
from market_data import OrderBook


class FakeMarketData:
    def best_bid(self, orderbook, outcome):
        if outcome == orderbook.outcome_labels[0]:
            return orderbook.yes_bids[0][0]
        return orderbook.no_bids[0][0]

    def best_ask(self, orderbook, outcome):
        if outcome == orderbook.outcome_labels[0]:
            return orderbook.yes_asks[0][0]
        return orderbook.no_asks[0][0]


class ExecutionSignalPolicyTests(unittest.IsolatedAsyncioTestCase):
    def _config(self, *, global_style=None, strategy_styles=None):
        cfg = {
            "polymarket": {
                "clob_api_url": "https://clob.polymarket.com",
                "wallet_address": "paper-wallet",
                "private_key": "paper-key",
            },
            "execution": {},
            "strategies": {},
        }
        if global_style is not None:
            cfg["execution"]["directional_signal_entry_style"] = global_style
        for family, style in (strategy_styles or {}).items():
            cfg["strategies"][family] = {"directional_signal_entry_style": style}
        return cfg

    def _market(self):
        return {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "slot_id": "btc:5:100",
            "end_ts": time.time() + 300.0,
        }

    def _book(self, *, yes_bid=0.60, yes_ask=0.61, no_bid=0.39, no_ask=0.40, ts=100.0):
        return OrderBook(
            market_id="m1",
            yes_asks=[(yes_ask, 10)],
            yes_bids=[(yes_bid, 10)],
            no_asks=[(no_ask, 10)],
            no_bids=[(no_bid, 10)],
            timestamp=ts,
            sequence=1,
            outcome_labels=("Up", "Down"),
        )

    def _signal(self, *, outcome="Up", action="BUY", price=0.60, size=2.0, reason="test-entry"):
        return SimpleNamespace(
            market_id="m1",
            outcome=outcome,
            action=action,
            price=price,
            confidence=0.9,
            size=size,
            reason=reason,
        )

    def test_cli_strategy_entry_style_uses_strategy_override_before_global_default(self):
        cfg = self._config(global_style="marketable", strategy_styles={"opening_range": "resting_limit"})
        self.assertEqual(_strategy_directional_signal_entry_style(cfg, "opening_range"), "resting_limit")
        self.assertEqual(_strategy_directional_signal_entry_style(cfg, "mean_reversion_5min"), "marketable")

    def test_resolve_signal_fill_behavior_defaults_to_marketable(self):
        behavior = resolve_signal_fill_behavior(
            self._config(),
            "mean_reversion_5min",
            self._signal(price=0.60),
            best_bid=0.60,
            best_ask=0.61,
        )
        self.assertEqual(behavior["entry_style"], "marketable")
        self.assertEqual(behavior["order_price"], 0.61)
        self.assertTrue(behavior["should_fill_immediately"])

    def test_resolve_signal_fill_behavior_uses_resting_limit_for_non_crossing_buy(self):
        behavior = resolve_signal_fill_behavior(
            self._config(strategy_styles={"opening_range": "resting_limit"}),
            "opening_range",
            self._signal(price=0.60),
            best_bid=0.60,
            best_ask=0.61,
        )
        self.assertEqual(behavior["entry_style"], "resting_limit")
        self.assertEqual(behavior["order_price"], 0.60)
        self.assertFalse(behavior["should_fill_immediately"])
        self.assertIsNone(behavior["fill_price"])

    async def test_marketable_default_preserves_immediate_fill_even_if_signal_price_is_inside_book(self):
        executor = PolymarketExecutor(self._config(), FakeMarketData(), mode="paper")
        await executor.__aenter__()
        try:
            result = await executor.execute_signal_trade(self._market(), self._book(), self._signal(price=0.60))
            self.assertTrue(result["opened"])
            self.assertTrue(result["filled"])
            self.assertEqual(len(executor.signal_slots), 1)
            self.assertAlmostEqual(executor.positions[("mean_reversion_5min", "m1", "Up")].quantity, 2.0)
            self.assertIn("order.filled", [event["event_type"] for event in result["events"]])
        finally:
            await executor.__aexit__(None, None, None)

    async def test_resting_limit_leaves_non_crossing_directional_order_open(self):
        executor = PolymarketExecutor(
            self._config(strategy_styles={"opening_range": "resting_limit"}),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            signal = self._signal(price=0.60, reason="respect-limit")
            result = await executor.execute_signal_trade(self._market(), self._book(), signal, strategy_family="opening_range")
            self.assertTrue(result["opened"])
            self.assertFalse(result["filled"])
            self.assertEqual(result["resting_order_price"], 0.60)
            self.assertEqual(result["entry_style"], "resting_limit")
            self.assertEqual(len(executor.signal_slots), 0)
            self.assertNotIn(("opening_range", "m1", "Up"), executor.positions)
            order = executor.orders[result["order_id"]]
            self.assertEqual(order["status"], "open")
            self.assertAlmostEqual(order["price"], 0.60)
            self.assertEqual([event["event_type"] for event in result["events"]], ["order.opened"])
        finally:
            await executor.__aexit__(None, None, None)

    async def test_resting_limit_fills_immediately_when_signal_limit_crosses_book(self):
        executor = PolymarketExecutor(
            self._config(global_style="resting_limit"),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            result = await executor.execute_signal_trade(self._market(), self._book(), self._signal(price=0.63))
            self.assertTrue(result["opened"])
            self.assertTrue(result["filled"])
            self.assertEqual(result["entry_style"], "resting_limit")
            self.assertAlmostEqual(executor.orders[result["order_id"]]["price"], 0.63)
            self.assertAlmostEqual(result["fill_price"], 0.61)
            self.assertIn("order.filled", [event["event_type"] for event in result["events"]])
        finally:
            await executor.__aexit__(None, None, None)

    async def test_resting_limit_passive_fill_promotes_signal_slot(self):
        executor = PolymarketExecutor(
            self._config(strategy_styles={"opening_range": "resting_limit"}),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            result = await executor.execute_signal_trade(
                self._market(),
                self._book(yes_bid=0.58, yes_ask=0.61),
                self._signal(price=0.59, reason="rest-then-fill"),
                strategy_family="opening_range",
            )
            self.assertTrue(result["opened"])
            self.assertFalse(result["filled"])
            created_ts = executor.orders[result["order_id"]]["timestamp"]
            fills = executor.evaluate_market_orders(
                "m1",
                self._book(yes_bid=0.58, yes_ask=0.59, ts=created_ts + 2.0),
            )
            self.assertEqual(len(fills), 1)
            slot = executor.signal_slots["btc:5:100"]
            self.assertEqual(slot.strategy_family, "opening_range")
            self.assertEqual(slot.outcome, "Up")
            self.assertGreater(slot.quantity, 0)
        finally:
            await executor.__aexit__(None, None, None)

    async def test_resting_limit_blocks_duplicate_open_order_same_direction(self):
        executor = PolymarketExecutor(
            self._config(strategy_styles={"opening_range": "resting_limit"}),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            first = await executor.execute_signal_trade(
                self._market(),
                self._book(),
                self._signal(price=0.60),
                strategy_family="opening_range",
            )
            self.assertTrue(first["opened"])
            self.assertFalse(first["filled"])
            second = await executor.execute_signal_trade(
                self._market(),
                self._book(),
                self._signal(price=0.60),
                strategy_family="opening_range",
            )
            self.assertFalse(second["opened"])
            self.assertEqual(second["reason"], "existing_open_order_same_direction")
        finally:
            await executor.__aexit__(None, None, None)

    async def test_rebuild_signal_slots_restores_non_mean_reversion_directional_family(self):
        executor = PolymarketExecutor(
            self._config(global_style="resting_limit"),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            result = await executor.execute_signal_trade(
                self._market(),
                self._book(),
                self._signal(price=0.63),
                strategy_family="time_decay",
            )
            self.assertTrue(result["filled"])
            executor.signal_slots = {}
            executor._rebuild_signal_slots_from_orders()
            self.assertIn("btc:5:100", executor.signal_slots)
            self.assertEqual(executor.signal_slots["btc:5:100"].strategy_family, "time_decay")
        finally:
            await executor.__aexit__(None, None, None)


if __name__ == "__main__":
    unittest.main()
