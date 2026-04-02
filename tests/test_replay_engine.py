import tempfile
import unittest
from pathlib import Path

from ledger import LedgerEvent, SQLiteLedger
from replay import replay_ledger


class ReplayEngineTests(unittest.TestCase):
    def test_replay_projects_open_and_cancelled_orders(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = SQLiteLedger(Path(tmpdir) / "ledger.db")
            ledger.append_event(
                LedgerEvent(
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
                    payload={"market_id": "m1", "size": 10, "price": 0.45},
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-2",
                    stream="order",
                    aggregate_id="order-1",
                    sequence_num=2,
                    event_type="order_acknowledged",
                    event_ts=101.0,
                    recorded_ts=101.0,
                    run_id="run-1",
                    idempotency_key="order_ack:order-1",
                    causation_id="evt-1",
                    correlation_id="corr-1",
                    schema_version=1,
                    payload={"status": "open"},
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-3",
                    stream="order",
                    aggregate_id="order-2",
                    sequence_num=1,
                    event_type="order_created",
                    event_ts=102.0,
                    recorded_ts=102.0,
                    run_id="run-1",
                    idempotency_key="order_created:order-2",
                    causation_id=None,
                    correlation_id="corr-2",
                    schema_version=1,
                    payload={"market_id": "m2", "size": 5, "price": 0.55},
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-4",
                    stream="order",
                    aggregate_id="order-2",
                    sequence_num=2,
                    event_type="order_cancelled",
                    event_ts=103.0,
                    recorded_ts=103.0,
                    run_id="run-1",
                    idempotency_key="order_cancelled:order-2",
                    causation_id="evt-3",
                    correlation_id="corr-2",
                    schema_version=1,
                    payload={"reason": "manual"},
                )
            )

            projection = replay_ledger(ledger.list_events())
            self.assertEqual(projection.orders["order-1"]["status"], "open")
            self.assertEqual(projection.orders["order-2"]["status"], "cancelled")
            self.assertEqual(sorted(projection.open_orders), ["order-1"])

    def test_replay_tracks_pending_and_settled_slots(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = SQLiteLedger(Path(tmpdir) / "ledger.db")
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-1",
                    stream="market_slot",
                    aggregate_id="btc:5:100",
                    sequence_num=1,
                    event_type="slot_resolution_pending",
                    event_ts=150.0,
                    recorded_ts=150.0,
                    run_id="run-1",
                    idempotency_key="pending:btc:5:100:150",
                    causation_id=None,
                    correlation_id="corr-1",
                    schema_version=1,
                    payload={"market_id": "m1", "next_poll_ts": 160.0},
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-2",
                    stream="market_slot",
                    aggregate_id="eth:15:100",
                    sequence_num=1,
                    event_type="slot_resolution_pending",
                    event_ts=151.0,
                    recorded_ts=151.0,
                    run_id="run-1",
                    idempotency_key="pending:eth:15:100:151",
                    causation_id=None,
                    correlation_id="corr-2",
                    schema_version=1,
                    payload={"market_id": "m2", "next_poll_ts": 170.0},
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-3",
                    stream="market_slot",
                    aggregate_id="btc:5:100",
                    sequence_num=2,
                    event_type="slot_settled",
                    event_ts=161.0,
                    recorded_ts=161.0,
                    run_id="run-1",
                    idempotency_key="settled:btc:5:100:Up:161",
                    causation_id="evt-1",
                    correlation_id="corr-1",
                    schema_version=1,
                    payload={"market_id": "m1", "winning_outcome": "Up"},
                )
            )

            projection = replay_ledger(ledger.list_events())
            self.assertNotIn("btc:5:100", projection.pending_slots)
            self.assertIn("eth:15:100", projection.pending_slots)
            self.assertEqual(projection.settled_slots["btc:5:100"]["winning_outcome"], "Up")

    def test_replay_builds_exposure_snapshot_from_positions_and_open_orders(self):
        events = [
            LedgerEvent(
                event_id="evt-1",
                stream="order",
                aggregate_id="order-long",
                sequence_num=1,
                event_type="order_created",
                event_ts=100.0,
                recorded_ts=100.0,
                run_id="run-1",
                idempotency_key="order_created:order-long",
                causation_id=None,
                correlation_id="corr-1",
                schema_version=1,
                payload={
                    "market_id": "m1",
                    "slot_id": "btc:5:100",
                    "outcome": "Up",
                    "side": "BUY",
                    "size": 10.0,
                    "price": 0.4,
                    "strategy_family": "toxicity_mm",
                },
            ),
            LedgerEvent(
                event_id="evt-2",
                stream="order",
                aggregate_id="order-long",
                sequence_num=2,
                event_type="fill_applied",
                event_ts=101.0,
                recorded_ts=101.0,
                run_id="run-1",
                idempotency_key="fill_apply:order-long:obs-1",
                causation_id="obs-1",
                correlation_id="corr-1",
                schema_version=1,
                payload={
                    "market_id": "m1",
                    "slot_id": "btc:5:100",
                    "outcome": "Up",
                    "side": "BUY",
                    "strategy_family": "toxicity_mm",
                    "fill_size": 10.0,
                    "fill_price": 0.4,
                    "filled_qty": 10.0,
                    "remaining_qty": 0.0,
                    "average_fill_price": 0.4,
                },
            ),
            LedgerEvent(
                event_id="evt-3",
                stream="order",
                aggregate_id="order-open",
                sequence_num=1,
                event_type="order_created",
                event_ts=102.0,
                recorded_ts=102.0,
                run_id="run-1",
                idempotency_key="order_created:order-open",
                causation_id=None,
                correlation_id="corr-2",
                schema_version=1,
                payload={
                    "market_id": "m2",
                    "slot_id": "eth:15:200",
                    "outcome": "Down",
                    "side": "BUY",
                    "size": 5.0,
                    "price": 0.6,
                    "strategy_family": "mean_reversion_5min",
                },
            ),
            LedgerEvent(
                event_id="evt-4",
                stream="order",
                aggregate_id="order-open",
                sequence_num=2,
                event_type="order_acknowledged",
                event_ts=102.1,
                recorded_ts=102.1,
                run_id="run-1",
                idempotency_key="order_ack:order-open",
                causation_id="evt-3",
                correlation_id="corr-2",
                schema_version=1,
                payload={"status": "open", "remaining_qty": 5.0},
            ),
        ]

        projection = replay_ledger(events)
        self.assertEqual(projection.exposure["open_position_count"], 1)
        self.assertEqual(projection.exposure["open_order_count"], 1)
        self.assertAlmostEqual(projection.exposure["gross_position_exposure"], 4.0)
        self.assertAlmostEqual(projection.exposure["gross_open_order_exposure"], 3.0)
        self.assertAlmostEqual(projection.exposure["reserved_buy_order_notional"], 3.0)
        self.assertAlmostEqual(projection.exposure["total_gross_exposure"], 7.0)
        self.assertAlmostEqual(projection.exposure["by_asset"]["btc"]["position_exposure"], 4.0)
        self.assertAlmostEqual(projection.exposure["by_interval"]["15"]["open_order_exposure"], 3.0)


if __name__ == "__main__":
    unittest.main()
