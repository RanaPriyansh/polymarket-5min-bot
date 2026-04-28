import unittest

from ledger import LedgerEvent
from paper_exchange import ConservativeFillEngine, FillPolicy, OrderBookSnapshot
from replay import replay_ledger


class ConservativeFillEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.order = {
            "order_id": "order-1",
            "market_id": "m1",
            "slot_id": "btc:5:100",
            "outcome": "Up",
            "side": "BUY",
            "size": 10.0,
            "price": 0.52,
            "strategy_family": "toxicity_mm",
            "status": "open",
            "created_ts": 100.0,
            "market_end_ts": 200.0,
            "filled_qty": 0.0,
            "queue_ahead_shares": 8.0,
        }
        self.engine = ConservativeFillEngine(
            FillPolicy(min_rest_seconds=5.0, max_fill_fraction_per_snapshot=0.5)
        )

    def test_midpoint_touch_does_not_fill_maker_order(self):
        touched_snapshot = OrderBookSnapshot(timestamp=106.0, best_bid=0.51, best_ask=0.52)
        self.assertIsNone(self.engine.observe_fill(self.order, touched_snapshot))

    def test_trade_through_fills_maker_order(self):
        trade_through = OrderBookSnapshot(
            timestamp=106.0,
            best_bid=0.50,
            best_ask=0.53,
            last_trade_price=0.51,
            last_trade_size=20.0,
        )
        observed = self.engine.observe_fill(self.order, trade_through)
        self.assertIsNotNone(observed)
        self.assertEqual(observed.event_type, "fill_observed")
        self.assertEqual(observed.payload["fill_size"], 5.0)
        self.assertEqual(observed.payload["fill_price"], 0.52)
        self.assertEqual(observed.payload["fill_reason"], "trade_through")

    def test_queue_ahead_blocks_until_consumed_volume_exceeds_queue(self):
        blocked = OrderBookSnapshot(
            timestamp=106.0,
            best_bid=0.52,
            best_ask=0.54,
            last_trade_price=0.52,
            last_trade_size=5.0,
        )
        self.assertIsNone(self.engine.observe_fill(self.order, blocked))

        consumed = OrderBookSnapshot(
            timestamp=106.0,
            best_bid=0.52,
            best_ask=0.54,
            last_trade_price=0.52,
            last_trade_size=12.0,
        )
        observed = self.engine.observe_fill(self.order, consumed)
        self.assertIsNotNone(observed)
        self.assertEqual(observed.payload["fill_reason"], "queue_exhausted_at_price")
        self.assertGreater(observed.payload["queue_fill_probability"], 1.0 - 1e-9)

    def test_stale_book_blocks_fills(self):
        stale = OrderBookSnapshot(
            timestamp=106.0,
            best_bid=0.50,
            best_ask=0.53,
            last_trade_price=0.51,
            last_trade_size=20.0,
            book_age_ms=5000.0,
        )
        self.assertIsNone(self.engine.observe_fill(self.order, stale))

    def test_cancellation_latency_allows_fill_before_effective_time(self):
        order = dict(self.order)
        order.update({"status": "cancel_pending", "cancel_effective_at": 107.0})
        before_cancel_effective = OrderBookSnapshot(
            timestamp=106.5,
            best_bid=0.50,
            best_ask=0.53,
            last_trade_price=0.51,
            last_trade_size=20.0,
        )
        self.assertIsNotNone(self.engine.observe_fill(order, before_cancel_effective))

        after_cancel_effective = OrderBookSnapshot(
            timestamp=108.0,
            best_bid=0.50,
            best_ask=0.53,
            last_trade_price=0.51,
            last_trade_size=20.0,
        )
        self.assertIsNone(self.engine.observe_fill(order, after_cancel_effective))

    def test_taker_walks_depth_and_models_partial_slippage_and_fees(self):
        order = {**self.order, "side": "BUY", "price": 0.55, "post_only": False, "queue_ahead_shares": 0.0}
        result = self.engine.walk_taker_book(
            order,
            [(0.53, 4.0), (0.54, 3.0), (0.56, 100.0)],
            snapshot_ts=106.0,
            fees_enabled=True,
            fee_bps=10.0,
        )
        self.assertTrue(result["filled"])
        self.assertEqual(result["fill_size"], 7.0)
        self.assertEqual(result["fill_reason"], "taker_book_walk_partial")
        self.assertGreater(result["slippage"], 0.0)
        self.assertGreater(result["fees_estimated"], 0.0)

    def test_fill_events_replay_into_partial_then_full_position_state(self):
        created = LedgerEvent(
            event_id="evt-1",
            stream="order",
            aggregate_id="order-1",
            sequence_num=1,
            event_type="order_created",
            event_ts=100.0,
            recorded_ts=100.0,
            run_id="run-1",
            idempotency_key="order_created:order-1",
            causation_id=None,
            correlation_id="corr-1",
            schema_version=1,
            payload={
                "market_id": "m1",
                "slot_id": "btc:5:100",
                "outcome": "Up",
                "side": "BUY",
                "size": 10.0,
                "price": 0.52,
                "strategy_family": "toxicity_mm",
                "created_ts": 100.0,
                "market_end_ts": 200.0,
            },
        )
        acknowledged = LedgerEvent(
            event_id="evt-2",
            stream="order",
            aggregate_id="order-1",
            sequence_num=2,
            event_type="order_acknowledged",
            event_ts=100.1,
            recorded_ts=100.1,
            run_id="run-1",
            idempotency_key="order_ack:order-1",
            causation_id="evt-1",
            correlation_id="corr-1",
            schema_version=1,
            payload={"status": "open"},
        )

        no_queue_order = {**self.order, "queue_ahead_shares": 0.0}
        first_observed = self.engine.observe_fill(
            no_queue_order,
            OrderBookSnapshot(timestamp=106.0, best_bid=0.50, best_ask=0.53, last_trade_price=0.51, last_trade_size=20.0),
            event_id="evt-3",
            sequence_num=3,
            run_id="run-1",
            correlation_id="corr-1",
        )
        first_applied = self.engine.apply_fill(
            no_queue_order,
            first_observed,
            event_id="evt-4",
            sequence_num=4,
            run_id="run-1",
            correlation_id="corr-1",
        )

        order_after_first = {
            **no_queue_order,
            "filled_qty": first_applied.payload["filled_qty"],
            "average_fill_price": first_applied.payload["average_fill_price"],
        }
        second_observed = self.engine.observe_fill(
            order_after_first,
            OrderBookSnapshot(timestamp=112.0, best_bid=0.49, best_ask=0.53, last_trade_price=0.50, last_trade_size=20.0),
            event_id="evt-5",
            sequence_num=5,
            run_id="run-1",
            correlation_id="corr-1",
        )
        second_applied = self.engine.apply_fill(
            order_after_first,
            second_observed,
            event_id="evt-6",
            sequence_num=6,
            run_id="run-1",
            correlation_id="corr-1",
        )

        projection = replay_ledger([
            created,
            acknowledged,
            first_observed,
            first_applied,
            second_observed,
            second_applied,
        ])

        order_state = projection.orders["order-1"]
        self.assertEqual(order_state["status"], "filled")
        self.assertEqual(order_state["filled_qty"], 10.0)
        self.assertEqual(order_state["remaining_qty"], 0.0)
        self.assertAlmostEqual(order_state["average_fill_price"], 0.52)

        position = projection.positions[("toxicity_mm", "m1", "Up")]
        self.assertEqual(position["quantity"], 10.0)
        self.assertAlmostEqual(position["average_price"], 0.52)


if __name__ == "__main__":
    unittest.main()
