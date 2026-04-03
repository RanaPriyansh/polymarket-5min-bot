import sqlite3
import tempfile
import unittest
from pathlib import Path

from ledger import LedgerEvent, SQLiteLedger
from replay import replay_ledger
from settlement_engine import SettlementEngine


class SettlementEngineTests(unittest.TestCase):
    def test_pending_and_settled_events_use_stable_idempotency_keys(self):
        engine = SettlementEngine()
        pending = engine.pending_event(
            event_id="evt-1",
            slot_id="btc:5:100",
            market_id="m1",
            run_id="run-1",
            sequence_num=1,
            recorded_ts=150.0,
            next_poll_ts=160.0,
            correlation_id="corr-1",
        )
        settled = engine.settled_event(
            event_id="evt-2",
            slot_id="btc:5:100",
            market_id="m1",
            market_slug="btc-updown-5m-100",
            winning_outcome="Up",
            settled_ts=170.0,
            run_id="run-1",
            sequence_num=2,
            correlation_id="corr-1",
        )

        self.assertEqual(pending.idempotency_key, "pending:btc:5:100:160")
        self.assertEqual(settled.idempotency_key, "settled:btc:5:100:Up:170")

        with tempfile.TemporaryDirectory() as tmpdir:
            ledger = SQLiteLedger(Path(tmpdir) / "ledger.db")
            ledger.append_event(pending)
            ledger.append_event(settled)
            with self.assertRaises(sqlite3.IntegrityError):
                ledger.append_event(
                    engine.settled_event(
                        event_id="evt-3",
                        slot_id="btc:5:100",
                        market_id="m1",
                        market_slug="btc-updown-5m-100",
                        winning_outcome="Up",
                        settled_ts=170.0,
                        run_id="run-1",
                        sequence_num=3,
                        correlation_id="corr-1",
                    )
                )

    def test_slot_settlement_replay_closes_positions_idempotently(self):
        events = [
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
                payload={
                    "market_id": "m1",
                    "slot_id": "btc:5:100",
                    "outcome": "Up",
                    "side": "BUY",
                    "size": 10.0,
                    "price": 0.40,
                    "strategy_family": "toxicity_mm",
                    "created_ts": 100.0,
                    "market_end_ts": 150.0,
                },
            ),
            LedgerEvent(
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
            ),
            LedgerEvent(
                event_id="evt-3",
                stream="order",
                aggregate_id="order-1",
                sequence_num=3,
                event_type="fill_applied",
                event_ts=106.0,
                recorded_ts=106.0,
                run_id="run-1",
                idempotency_key="fill_apply:order-1:evt-obs-1",
                causation_id="evt-obs-1",
                correlation_id="corr-1",
                schema_version=1,
                payload={
                    "market_id": "m1",
                    "slot_id": "btc:5:100",
                    "outcome": "Up",
                    "side": "BUY",
                    "strategy_family": "toxicity_mm",
                    "fill_size": 10.0,
                    "fill_price": 0.40,
                    "filled_qty": 10.0,
                    "remaining_qty": 0.0,
                    "average_fill_price": 0.40,
                    "observed_event_id": "evt-obs-1",
                },
            ),
            SettlementEngine().settled_event(
                event_id="evt-4",
                slot_id="btc:5:100",
                market_id="m1",
                market_slug="btc-updown-5m-100",
                winning_outcome="Down",
                settled_ts=170.0,
                run_id="run-1",
                sequence_num=1,
                correlation_id="corr-2",
            ),
        ]

        projection_one = replay_ledger(events)
        projection_two = replay_ledger(events)

        position = projection_one.positions[("toxicity_mm", "m1", "Up")]
        self.assertEqual(position["quantity"], 0.0)
        self.assertAlmostEqual(position["realized_pnl"], -4.0)
        self.assertEqual(projection_one.pending_slots, {})
        self.assertEqual(projection_one.settled_slots["btc:5:100"]["winning_outcome"], "Down")
        self.assertEqual(projection_one.positions, projection_two.positions)
        self.assertEqual(projection_one.realized_pnl_total, projection_two.realized_pnl_total)


if __name__ == "__main__":
    unittest.main()
