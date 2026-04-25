import time
import unittest
from types import SimpleNamespace

from cli import _filled_event_from_execution_result, _mark_directional_fired_on_fill, _strategy_directional_signal_entry_style
from execution import PolymarketExecutor, resolve_signal_fill_behavior
from market_data import OrderBook
from strategies.opening_range import OpeningRangeBreakout


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
            "execution": {
                "directional_order_ttl_enabled": True,
                "directional_order_ttl_seconds": {
                    "time_decay": 3,
                    "spot_momentum": 5,
                    "opening_range": 10,
                    "mean_reversion_5min": 10,
                },
            },
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

    def test_time_decay_strategy_override_uses_resting_limit_despite_marketable_global_default(self):
        cfg = self._config(global_style="marketable", strategy_styles={"time_decay": "resting_limit"})
        self.assertEqual(_strategy_directional_signal_entry_style(cfg, "time_decay"), "resting_limit")

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

    async def test_expired_directional_order_is_cancelled_and_retry_allowed(self):
        executor = PolymarketExecutor(
            self._config(strategy_styles={"opening_range": "resting_limit"}),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            first = await executor.execute_signal_trade(
                self._market(),
                self._book(ts=100.0),
                self._signal(price=0.60),
                strategy_family="opening_range",
            )
            order_id = first["order_id"]
            executor.orders[order_id]["timestamp"] = 100.0
            events = await executor.expire_directional_signal_orders(market_id="m1", now_ts=111.0)
            self.assertEqual(executor.orders[order_id]["status"], "cancelled")
            self.assertIn("signal.order_expired", [event["event_type"] for event in events])
            self.assertIn("signal.retry_allowed", [event["event_type"] for event in events])

            retry = await executor.execute_signal_trade(
                self._market(),
                self._book(ts=112.0),
                self._signal(price=0.60),
                strategy_family="opening_range",
            )
            self.assertTrue(retry["opened"])
            self.assertNotEqual(retry["order_id"], order_id)
        finally:
            await executor.__aexit__(None, None, None)

    async def test_immediate_fill_consumes_signal_using_order_filled_payload(self):
        executor = PolymarketExecutor(
            self._config(strategy_styles={"time_decay": "marketable"}),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            result = await executor.execute_signal_trade(
                self._market(),
                self._book(yes_ask=0.61, ts=100.0),
                self._signal(price=0.61),
                strategy_family="time_decay",
            )
            fired_fill = _filled_event_from_execution_result(result)
            self.assertIsNotNone(fired_fill)
            marker = SimpleNamespace(_fired_slots=set())

            def mark_fired(slot_id, outcome):
                marker._fired_slots.add((slot_id, outcome))

            marker.mark_fired = mark_fired
            event = _mark_directional_fired_on_fill(fired_fill, time_decay=marker)

            self.assertIsNotNone(event)
            self.assertEqual(event["event_type"], "signal.fired_on_fill")
            self.assertIn(("btc:5:100", "Up"), marker._fired_slots)
        finally:
            await executor.__aexit__(None, None, None)

    def test_close_fill_payload_does_not_fire_signal_state(self):
        result = {
            "events": [
                {
                    "event_type": "order.filled",
                    "order_kind": "signal_close",
                    "strategy_family": "time_decay",
                    "slot_id": "btc:5:100",
                    "outcome": "Up",
                }
            ]
        }
        self.assertIsNone(_filled_event_from_execution_result(result))
        marker = SimpleNamespace(_fired_slots=set(), mark_fired=lambda slot_id, outcome: marker._fired_slots.add((slot_id, outcome)))
        self.assertIsNone(_mark_directional_fired_on_fill(result["events"][0], time_decay=marker))
        self.assertEqual(marker._fired_slots, set())

    def test_opening_range_retries_until_fill_then_consumes_slot(self):
        cfg = self._config(strategy_styles={"opening_range": "resting_limit"})
        cfg["strategies"]["opening_range"].update({
            "opening_range_ticks": 2,
            "breakout_pct": 0.01,
            "min_volume": 0,
        })
        strategy = OpeningRangeBreakout(cfg)
        slot_id = "btc:5:100"
        market_id = "m1"
        book = self._book(yes_ask=0.61)
        strategy.update_price(market_id, 0.50, volume=1000)
        strategy.update_price(market_id, 0.51, volume=1000)

        first = strategy.generate_signal(market_id, "Up", 0.52, book, volume=1000, slot_id=slot_id)
        second = strategy.generate_signal(market_id, "Up", 0.52, book, volume=1000, slot_id=slot_id)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertFalse(strategy.is_fired(market_id))

        event = _mark_directional_fired_on_fill(
            {
                "strategy_family": "opening_range",
                "slot_id": slot_id,
                "market_id": "m1",
                "market_slug": "btc-updown-5m-100",
                "outcome": "Up",
                "order_id": "o1",
                "order_kind": "signal",
            },
            opening_range=strategy,
        )
        self.assertIsNotNone(event)
        self.assertTrue(strategy.is_fired(market_id))
        self.assertIsNone(strategy.generate_signal(market_id, "Up", 0.52, book, volume=1000, slot_id=slot_id))

    async def test_unfilled_order_does_not_consume_strategy_fired_state_until_fill(self):
        class FakeTimeDecay:
            def __init__(self):
                self.calls = []
                self._fired_slots = set()

            def mark_fired(self, slot_id, outcome):
                self.calls.append((slot_id, outcome))
                self._fired_slots.add((slot_id, outcome))

        marker = FakeTimeDecay()
        executor = PolymarketExecutor(
            self._config(strategy_styles={"time_decay": "resting_limit"}),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            result = await executor.execute_signal_trade(
                self._market(),
                self._book(yes_bid=0.58, yes_ask=0.61, ts=100.0),
                self._signal(price=0.59),
                strategy_family="time_decay",
            )
            self.assertTrue(result["opened"])
            self.assertFalse(result["filled"])
            self.assertIsNone(_mark_directional_fired_on_fill(result, time_decay=marker))
            self.assertEqual(marker.calls, [])

            executor.orders[result["order_id"]]["timestamp"] = 100.0
            fills = executor.evaluate_market_orders("m1", self._book(yes_bid=0.58, yes_ask=0.59, ts=102.0))
            self.assertEqual(len(fills), 1)
            event = _mark_directional_fired_on_fill(fills[0], time_decay=marker)
            self.assertIsNotNone(event)
            self.assertEqual(event["event_type"], "signal.fired_on_fill")
            self.assertEqual(marker.calls, [("btc:5:100", "Up")])
            event2 = _mark_directional_fired_on_fill(fills[0], time_decay=marker)
            self.assertIsNone(event2)
            self.assertEqual(marker.calls, [("btc:5:100", "Up")])
        finally:
            await executor.__aexit__(None, None, None)

    async def test_ttl_does_not_cancel_partially_filled_or_quote_orders(self):
        executor = PolymarketExecutor(
            self._config(strategy_styles={"opening_range": "resting_limit"}),
            FakeMarketData(),
            mode="paper",
        )
        await executor.__aenter__()
        try:
            signal_order = await executor.place_order(
                "m1", "Up", "BUY", 2.0, 0.60, strategy_family="opening_range", order_kind="signal", market=self._market()
            )
            executor.orders[signal_order]["timestamp"] = 100.0
            executor.orders[signal_order]["status"] = "partially_filled"
            executor.orders[signal_order]["filled_qty"] = 0.5
            quote_order = await executor.place_order(
                "m1", "Up", "BUY", 2.0, 0.60, strategy_family="toxicity_mm", order_kind="quote", market=self._market()
            )
            executor.orders[quote_order]["timestamp"] = 100.0
            events = await executor.expire_directional_signal_orders(market_id="m1", now_ts=200.0)
            self.assertEqual(events, [])
            self.assertEqual(executor.orders[signal_order]["status"], "partially_filled")
            self.assertEqual(executor.orders[quote_order]["status"], "open")
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

    async def test_cancel_order_cancels_open_like_statuses_only(self):
        executor = PolymarketExecutor(self._config(), FakeMarketData(), mode="paper")
        await executor.__aenter__()
        try:
            open_like_ids = []
            for status in ("open", "partially_filled", "acknowledged"):
                order_id = await executor.place_order(
                    "m1", "Up", "BUY", 2.0, 0.60, strategy_family="toxicity_mm", market=self._market()
                )
                executor.orders[order_id]["status"] = status
                open_like_ids.append(order_id)

            final_ids = []
            for status in ("filled", "cancelled", "rejected"):
                order_id = await executor.place_order(
                    "m1", "Up", "BUY", 2.0, 0.60, strategy_family="toxicity_mm", market=self._market()
                )
                executor.orders[order_id]["status"] = status
                final_ids.append(order_id)

            for order_id in open_like_ids:
                self.assertTrue(await executor.cancel_order(order_id))
                self.assertEqual(executor.orders[order_id]["status"], "cancelled")

            for order_id in final_ids:
                self.assertFalse(await executor.cancel_order(order_id))

            self.assertEqual([executor.orders[order_id]["status"] for order_id in final_ids], ["filled", "cancelled", "rejected"])
            self.assertEqual(executor.family_metrics["toxicity_mm"]["cancellations"], 3)
        finally:
            await executor.__aexit__(None, None, None)

    async def test_cancel_family_market_actually_cancels_partially_filled_and_acknowledged_orders(self):
        executor = PolymarketExecutor(self._config(), FakeMarketData(), mode="paper")
        await executor.__aenter__()
        try:
            order_ids = []
            for status in ("open", "partially_filled", "acknowledged"):
                order_id = await executor.place_order(
                    "m1", "Up", "BUY", 2.0, 0.60, strategy_family="toxicity_mm", market=self._market()
                )
                executor.orders[order_id]["status"] = status
                order_ids.append(order_id)
            filled_order_id = await executor.place_order(
                "m1", "Up", "BUY", 2.0, 0.60, strategy_family="toxicity_mm", market=self._market()
            )
            executor.orders[filled_order_id]["status"] = "filled"

            cancelled = await executor.cancel_family_market("m1", "toxicity_mm")

            self.assertEqual(cancelled, 3)
            self.assertTrue(all(executor.orders[order_id]["status"] == "cancelled" for order_id in order_ids))
            self.assertEqual(executor.orders[filled_order_id]["status"], "filled")
            self.assertEqual(executor.family_metrics["toxicity_mm"]["cancellations"], 3)
        finally:
            await executor.__aexit__(None, None, None)

    async def test_cancel_market_orders_cancels_open_like_statuses_only_and_returns_actual_count(self):
        executor = PolymarketExecutor(self._config(), FakeMarketData(), mode="paper")
        await executor.__aenter__()
        try:
            cancellable_ids = []
            for status in ("open", "partially_filled", "acknowledged"):
                order_id = await executor.place_order(
                    "m1", "Up", "BUY", 2.0, 0.60, strategy_family="toxicity_mm", market=self._market()
                )
                executor.orders[order_id]["status"] = status
                cancellable_ids.append(order_id)

            final_ids = []
            for status in ("filled", "cancelled", "rejected"):
                order_id = await executor.place_order(
                    "m1", "Up", "BUY", 2.0, 0.60, strategy_family="toxicity_mm", market=self._market()
                )
                executor.orders[order_id]["status"] = status
                final_ids.append(order_id)

            other_market_id = await executor.place_order(
                "m2", "Up", "BUY", 2.0, 0.60, strategy_family="toxicity_mm", market={**self._market(), "id": "m2"}
            )
            executor.orders[other_market_id]["status"] = "open"

            cancelled = await executor.cancel_market_orders("m1")

            self.assertEqual(cancelled, 3)
            self.assertTrue(all(executor.orders[order_id]["status"] == "cancelled" for order_id in cancellable_ids))
            self.assertEqual(
                [executor.orders[order_id]["status"] for order_id in final_ids],
                ["filled", "cancelled", "rejected"],
            )
            self.assertEqual(executor.orders[other_market_id]["status"], "open")
            self.assertEqual(executor.family_metrics["toxicity_mm"]["cancellations"], 3)
        finally:
            await executor.__aexit__(None, None, None)


if __name__ == "__main__":
    unittest.main()
