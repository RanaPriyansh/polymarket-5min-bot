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
        }
        self.engine = ConservativeFillEngine(
            FillPolicy(min_rest_seconds=5.0, max_fill_fraction_per_snapshot=0.5)
        )

    def test_observe_fill_requires_rest_and_crossed_book(self):
        early_snapshot = OrderBookSnapshot(timestamp=103.0, best_bid=0.48, best_ask=0.50)
        self.assertIsNone(self.engine.observe_fill(self.order, early_snapshot))

        uncrossed_snapshot = OrderBookSnapshot(timestamp=106.0, best_bid=0.48, best_ask=0.53)
        self.assertIsNone(self.engine.observe_fill(self.order, uncrossed_snapshot))

        crossed_snapshot = OrderBookSnapshot(timestamp=106.0, best_bid=0.48, best_ask=0.50)
        observed = self.engine.observe_fill(self.order, crossed_snapshot)
        self.assertIsNotNone(observed)
        self.assertEqual(observed.event_type, "fill_observed")
        self.assertEqual(observed.payload["fill_size"], 5.0)
        self.assertEqual(observed.payload["fill_price"], 0.50)

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

        first_observed = self.engine.observe_fill(
            self.order,
            OrderBookSnapshot(timestamp=106.0, best_bid=0.48, best_ask=0.50),
            event_id="evt-3",
            sequence_num=3,
            run_id="run-1",
            correlation_id="corr-1",
        )
        first_applied = self.engine.apply_fill(
            self.order,
            first_observed,
            event_id="evt-4",
            sequence_num=4,
            run_id="run-1",
            correlation_id="corr-1",
        )

        order_after_first = {
            **self.order,
            "filled_qty": first_applied.payload["filled_qty"],
            "average_fill_price": first_applied.payload["average_fill_price"],
        }
        second_observed = self.engine.observe_fill(
            order_after_first,
            OrderBookSnapshot(timestamp=112.0, best_bid=0.49, best_ask=0.49),
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
        self.assertAlmostEqual(order_state["average_fill_price"], 0.495)

        position = projection.positions[("toxicity_mm", "m1", "Up")]
        self.assertEqual(position["quantity"], 10.0)
        self.assertAlmostEqual(position["average_price"], 0.495)

    def test_fill_engine_blocks_same_snapshot_and_post_expiry_fills(self):
        same_tick = OrderBookSnapshot(timestamp=100.0, best_bid=0.48, best_ask=0.50)
        expired = OrderBookSnapshot(timestamp=201.0, best_bid=0.48, best_ask=0.50)

        self.assertIsNone(self.engine.observe_fill(self.order, same_tick))
        self.assertIsNone(self.engine.observe_fill(self.order, expired))


if __name__ == "__main__":
    unittest.main()
