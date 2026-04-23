import unittest

from ledger import LedgerEvent
from replay import replay_ledger


class ReplaySettlementTruthTests(unittest.TestCase):
    def test_latest_position_settlement_ignores_later_null_attribution_resolution(self):
        events = [
            LedgerEvent(
                event_id="evt-fill-1",
                stream="order",
                aggregate_id="order-1",
                sequence_num=1,
                event_type="fill_applied",
                event_ts=100.0,
                recorded_ts=100.0,
                run_id="run-replay",
                idempotency_key="fill-1",
                causation_id=None,
                correlation_id="order-1",
                schema_version=1,
                payload={
                    "strategy_family": "toxicity_mm",
                    "market_id": "m1",
                    "outcome": "Up",
                    "side": "BUY",
                    "fill_size": 4.0,
                    "fill_price": 0.4,
                    "filled_qty": 4.0,
                    "remaining_qty": 0.0,
                    "average_fill_price": 0.4,
                    "slot_id": "btc:5:100",
                },
            ),
            LedgerEvent(
                event_id="evt-settle-1",
                stream="market_slot",
                aggregate_id="btc:5:100",
                sequence_num=1,
                event_type="slot_settled",
                event_ts=150.0,
                recorded_ts=150.0,
                run_id="run-replay",
                idempotency_key="settle-1",
                causation_id=None,
                correlation_id="btc:5:100",
                schema_version=1,
                payload={
                    "market_id": "m1",
                    "market_slug": "btc-updown-5m-100",
                    "winning_outcome": "Up",
                    "settled_ts": 150.0,
                    "position_outcome": "Up",
                    "position_size": 4.0,
                    "entry_price": 0.4,
                    "realized_pnl": 2.4,
                    "is_win": 1,
                },
            ),
            LedgerEvent(
                event_id="evt-settle-2",
                stream="market_slot",
                aggregate_id="btc:5:105",
                sequence_num=1,
                event_type="slot_settled",
                event_ts=200.0,
                recorded_ts=200.0,
                run_id="run-replay",
                idempotency_key="settle-2",
                causation_id=None,
                correlation_id="btc:5:105",
                schema_version=1,
                payload={
                    "market_id": "m2",
                    "market_slug": "btc-updown-5m-105",
                    "winning_outcome": "Down",
                    "settled_ts": 200.0,
                    "position_outcome": None,
                    "position_size": None,
                    "entry_price": None,
                    "realized_pnl": None,
                    "is_win": None,
                },
            ),
        ]

        projection = replay_ledger(events)

        self.assertEqual(projection.latest_slot_resolution["slot_id"], "btc:5:105")
        self.assertEqual(projection.latest_position_settlement["slot_id"], "btc:5:100")
        self.assertEqual(projection.latest_settlement["slot_id"], "btc:5:100")

    def test_slot_resolution_with_multiple_legs_stays_slot_level_only(self):
        projection = replay_ledger(
            [
                LedgerEvent(
                    event_id="evt-settle-multi",
                    stream="market_slot",
                    aggregate_id="btc:5:200",
                    sequence_num=1,
                    event_type="slot_settled",
                    event_ts=220.0,
                    recorded_ts=220.0,
                    run_id="run-replay",
                    idempotency_key="settle-multi",
                    causation_id=None,
                    correlation_id="btc:5:200",
                    schema_version=1,
                    payload={
                        "market_id": "m3",
                        "market_slug": "btc-updown-5m-200",
                        "winning_outcome": "Up",
                        "settled_ts": 220.0,
                        "position_count": 2,
                        "realized_pnl": 1.5,
                        "position_outcome": None,
                        "position_size": None,
                        "entry_price": None,
                        "is_win": 1,
                    },
                )
            ]
        )

        self.assertEqual(projection.latest_slot_resolution["position_count"], 2)
        self.assertIsNone(projection.latest_position_settlement)

    def test_slot_closed_updates_latest_slot_resolution_without_position_settlement(self):
        projection = replay_ledger(
            [
                LedgerEvent(
                    event_id="evt-close-flat",
                    stream="market_slot",
                    aggregate_id="btc:5:210",
                    sequence_num=1,
                    event_type="slot_closed",
                    event_ts=230.0,
                    recorded_ts=230.0,
                    run_id="run-replay",
                    idempotency_key="close-flat-1",
                    causation_id=None,
                    correlation_id="btc:5:210",
                    schema_version=1,
                    payload={
                        "market_id": "m4",
                        "market_slug": "btc-updown-5m-210",
                        "winning_outcome": "Down",
                        "settled_ts": 230.0,
                        "position_count": 0,
                    },
                )
            ]
        )

        self.assertEqual(projection.latest_slot_resolution["event_type"], "slot_closed")
        self.assertEqual(projection.latest_slot_resolution["slot_id"], "btc:5:210")
        self.assertIsNone(projection.latest_position_settlement)
        self.assertIsNone(projection.latest_settlement)


if __name__ == "__main__":
    unittest.main()
