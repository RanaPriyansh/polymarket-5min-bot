import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from ledger import LedgerEvent, SQLiteLedger


class SQLiteLedgerTests(unittest.TestCase):
    def test_append_and_list_events_in_replay_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ledger.db"
            ledger = SQLiteLedger(db_path)

            ledger.append_event(
                LedgerEvent(
                    event_id="evt-2",
                    stream="order",
                    aggregate_id="order-1",
                    sequence_num=2,
                    event_type="order_acknowledged",
                    event_ts=101.0,
                    recorded_ts=200.0,
                    run_id="run-1",
                    idempotency_key="order_ack:order-1",
                    causation_id="evt-1",
                    correlation_id="corr-1",
                    schema_version=1,
                    payload={"status": "acked"},
                )
            )
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
                    payload={"size": 10, "price": 0.45},
                )
            )

            events = ledger.list_events()
            self.assertEqual([event.event_id for event in events], ["evt-1", "evt-2"])
            self.assertEqual(events[0].payload["size"], 10)
            self.assertEqual(events[1].payload["status"], "acked")

    def test_duplicate_idempotency_key_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ledger.db"
            ledger = SQLiteLedger(db_path)
            event = LedgerEvent(
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
                payload={"size": 10},
            )

            ledger.append_event(event)
            with self.assertRaises(sqlite3.IntegrityError):
                ledger.append_event(
                    LedgerEvent(
                        event_id="evt-2",
                        stream="order",
                        aggregate_id="order-1",
                        sequence_num=2,
                        event_type="order_created",
                        event_ts=101.0,
                        recorded_ts=101.0,
                        run_id="run-1",
                        idempotency_key="order_created:order-1",
                        causation_id=None,
                        correlation_id="corr-1",
                        schema_version=1,
                        payload={"size": 20},
                    )
                )

    def test_projection_reset_does_not_delete_ledger_events(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ledger.db"
            ledger = SQLiteLedger(db_path)
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-1",
                    stream="run",
                    aggregate_id="run-1",
                    sequence_num=1,
                    event_type="run_started",
                    event_ts=100.0,
                    recorded_ts=100.0,
                    run_id="run-1",
                    idempotency_key="run_started:run-1",
                    causation_id=None,
                    correlation_id="corr-1",
                    schema_version=1,
                    payload={"mode": "paper"},
                )
            )
            ledger.reset_projections()
            events = ledger.list_events()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].event_type, "run_started")

    def test_raw_payload_is_stored_as_json_text(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ledger.db"
            ledger = SQLiteLedger(db_path)
            event = LedgerEvent(
                event_id="evt-1",
                stream="risk",
                aggregate_id="run-1",
                sequence_num=1,
                event_type="risk_snapshot_recorded",
                event_ts=100.0,
                recorded_ts=100.0,
                run_id="run-1",
                idempotency_key="risk:run-1:100",
                causation_id=None,
                correlation_id="corr-1",
                schema_version=1,
                payload={"reserved_cash": 12.5, "bucket": {"btc": 5.0}},
            )
            ledger.append_event(event)

            with sqlite3.connect(db_path) as conn:
                payload_json = conn.execute(
                    "SELECT payload_json FROM ledger_events WHERE event_id = ?",
                    ("evt-1",),
                ).fetchone()[0]
            self.assertEqual(json.loads(payload_json), event.payload)


if __name__ == "__main__":
    unittest.main()
