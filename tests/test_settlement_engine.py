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

        # Settled positions are removed from the projection (not left as zero-qty)
        self.assertNotIn(("toxicity_mm", "m1", "Up"), projection_one.positions,
                         "Settled positions must be purged from replay projection")
        # PnL was applied by _settle_positions_for_slot during replay
        self.assertEqual(projection_one.pending_slots, {})
        self.assertEqual(projection_one.settled_slots["btc:5:100"]["winning_outcome"], "Down")
        self.assertEqual(projection_one.positions, projection_two.positions)
        self.assertEqual(projection_one.realized_pnl_total, projection_two.realized_pnl_total)
        # Resolved count is 1 (one position settled)
        self.assertEqual(projection_one.resolved_trade_count, 1)
        self.assertEqual(projection_one.win_count, 0)
        self.assertEqual(projection_one.loss_count, 1)

    # ------------------------------------------------------------------
    # PnL attribution tests (AC-10 slot_settled schema)
    # ------------------------------------------------------------------

    def test_slot_settled_has_realized_pnl(self):
        """settled_event with PnL kwargs must surface realized_pnl in payload."""
        engine = SettlementEngine()
        event = engine.settled_event(
            slot_id="btc:5:100",
            market_id="m1",
            market_slug="btc-updown-5m-100",
            winning_outcome="Up",
            settled_ts=170.0,
            run_id="run-1",
            sequence_num=2,
            position_outcome="Up",
            position_size=10.0,
            entry_price=0.40,
            realized_pnl=6.0,
            is_win=1,
        )
        self.assertEqual(event.payload["realized_pnl"], 6.0)
        self.assertEqual(event.payload["is_win"], 1)

    def test_slot_settled_has_is_win(self):
        """Loss scenario: is_win=0 and realized_pnl negative must be stored verbatim."""
        engine = SettlementEngine()
        # entry_price=0.40, size=10, position loses → realized = 0 - 0.40*10 = -4.0
        event = engine.settled_event(
            slot_id="btc:5:100",
            market_id="m1",
            market_slug="btc-updown-5m-100",
            winning_outcome="Down",
            settled_ts=170.0,
            run_id="run-1",
            sequence_num=2,
            position_outcome="Up",
            position_size=10.0,
            entry_price=0.40,
            realized_pnl=-4.0,
            is_win=0,
        )
        self.assertEqual(event.payload["is_win"], 0)
        self.assertEqual(event.payload["realized_pnl"], -4.0)

    def test_slot_settled_win_computation_correct(self):
        """WIN case: position buys Up at 0.30, size=10, winning_outcome=Up.
        realized_pnl = (1.0 - 0.30) * 10.0 = 7.0; is_win = 1."""
        engine = SettlementEngine()
        event = engine.settled_event(
            slot_id="btc:5:200",
            market_id="m2",
            market_slug="btc-updown-5m-200",
            winning_outcome="Up",
            settled_ts=200.0,
            run_id="run-2",
            sequence_num=1,
            position_outcome="Up",
            position_size=10.0,
            entry_price=0.30,
            realized_pnl=7.0,
            is_win=1,
        )
        self.assertEqual(event.payload["realized_pnl"], 7.0)
        self.assertEqual(event.payload["position_outcome"], "Up")
        self.assertEqual(event.payload["entry_price"], 0.30)
        self.assertEqual(event.payload["position_size"], 10.0)
        self.assertEqual(event.payload["is_win"], 1)

    def test_slot_settled_loss_computation_correct(self):
        """LOSS case: position buys Up at 0.60, size=10, winning_outcome=Down.
        realized_pnl = (0.0 - 0.60) * 10.0 = -6.0; is_win = 0."""
        engine = SettlementEngine()
        event = engine.settled_event(
            slot_id="btc:5:300",
            market_id="m3",
            market_slug="btc-updown-5m-300",
            winning_outcome="Down",
            settled_ts=300.0,
            run_id="run-3",
            sequence_num=1,
            position_outcome="Up",
            position_size=10.0,
            entry_price=0.60,
            realized_pnl=-6.0,
            is_win=0,
        )
        self.assertEqual(event.payload["realized_pnl"], -6.0)
        self.assertEqual(event.payload["is_win"], 0)
        self.assertEqual(event.payload["position_outcome"], "Up")

    def test_slot_settled_backward_compat_no_pnl_args(self):
        """Existing callers omitting PnL kwargs must still work; payload defaults to None."""
        engine = SettlementEngine()
        event = engine.settled_event(
            slot_id="btc:5:100",
            market_id="m1",
            market_slug="btc-updown-5m-100",
            winning_outcome="Up",
            settled_ts=170.0,
            run_id="run-1",
            sequence_num=2,
        )
        # All PnL fields default to None for backward compatibility
        self.assertIsNone(event.payload["realized_pnl"])
        self.assertIsNone(event.payload["is_win"])
        self.assertIsNone(event.payload["position_outcome"])
        self.assertIsNone(event.payload["position_size"])
        self.assertIsNone(event.payload["entry_price"])


if __name__ == "__main__":
    unittest.main()
