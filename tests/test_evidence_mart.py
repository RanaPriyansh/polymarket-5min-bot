import tempfile
import unittest
from pathlib import Path

from evidence_mart import build_evidence_mart, markout_vs_settlement, validate_evidence_rows
from ledger import LedgerEvent, SQLiteLedger


class EvidenceMartTests(unittest.TestCase):
    def test_settlement_linkage_gate_requires_strategy_and_slot(self):
        gate = validate_evidence_rows([
            {"paper_fill_reason": "trade_through", "strategy_family": None, "slot_id": "btc:5:100"},
            {"settled_pnl": -1.0, "strategy_family": "terminal_fair_value", "slot_id": None},
        ])
        self.assertEqual(gate.state, "RED")
        self.assertIn("row_0_missing_strategy_family", gate.reasons)
        self.assertIn("row_1_missing_slot_id", gate.reasons)

    def test_evidence_mart_builds_from_ledger_fixture(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir) / "runtime"
            runtime_dir.mkdir()
            ledger = SQLiteLedger(runtime_dir / "ledger.db")
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
                    correlation_id="order-1",
                    schema_version=1,
                    payload={
                        "market_id": "m1",
                        "slot_id": "btc:5:100",
                        "market_slug": "btc-updown-5m-100",
                        "outcome": "Up",
                        "side": "BUY",
                        "size": 10,
                        "price": 0.40,
                        "strategy_family": "terminal_fair_value",
                        "model_fair": 0.55,
                        "model_edge": 0.15,
                    },
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-2",
                    stream="order",
                    aggregate_id="order-1",
                    sequence_num=2,
                    event_type="fill_applied",
                    event_ts=101.0,
                    recorded_ts=101.0,
                    run_id="run-1",
                    idempotency_key="fill_apply:order-1:evt-fill",
                    causation_id="evt-fill",
                    correlation_id="order-1",
                    schema_version=1,
                    payload={
                        "market_id": "m1",
                        "slot_id": "btc:5:100",
                        "outcome": "Up",
                        "side": "BUY",
                        "strategy_family": "terminal_fair_value",
                        "fill_size": 10,
                        "fill_price": 0.41,
                        "filled_qty": 10,
                        "remaining_qty": 0,
                        "paper_fill_reason": "taker_book_walk_full",
                    },
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-3",
                    stream="market_slot",
                    aggregate_id="btc:5:100",
                    sequence_num=1,
                    event_type="slot_settled",
                    event_ts=170.0,
                    recorded_ts=170.0,
                    run_id="run-1",
                    idempotency_key="settled:btc:5:100:Up:170",
                    causation_id=None,
                    correlation_id="btc:5:100",
                    schema_version=1,
                    payload={
                        "slot_id": "btc:5:100",
                        "market_id": "m1",
                        "market_slug": "btc-updown-5m-100",
                        "strategy_family": "terminal_fair_value",
                        "winning_outcome": "Up",
                        "settled_ts": 170.0,
                        "settled_pnl": 5.9,
                    },
                )
            )
            artifact_dir = Path(tmpdir) / "mart"
            payload = build_evidence_mart(runtime_dir, artifact_dir=artifact_dir, run_id="run-1")
            self.assertEqual(payload["gate"]["state"], "GREEN")
            self.assertTrue((artifact_dir / "latest.json").exists())
            self.assertTrue((artifact_dir / "latest.md").exists())
            self.assertGreaterEqual(payload["row_count"], 2)

    def test_markout_vs_settlement_exposes_positive_markout_negative_settlement(self):
        report = markout_vs_settlement([
            {
                "order_id": "order-1",
                "slot_id": "btc:5:100",
                "strategy_family": "terminal_fair_value",
                "markout_60s": 0.05,
                "settled_pnl": -1.25,
            }
        ])
        self.assertEqual(report["positive_60s_negative_settlement_count"], 1)


if __name__ == "__main__":
    unittest.main()
