import tempfile
import unittest
from pathlib import Path

from baseline_evidence import build_baseline_evidence, render_baseline_evidence_text
from ledger import LedgerEvent, SQLiteLedger
from runtime_telemetry import RuntimeTelemetry


class BaselineEvidenceTests(unittest.TestCase):
    def test_builds_run_scoped_evidence_and_restart_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = RuntimeTelemetry(tmpdir)
            runtime.update_status(
                run_id="run-b",
                phase="running",
                mode="paper",
                baseline_strategy="toxicity_mm",
                research_candidates=["mean_reversion_5min"],
                loop_count=4,
                fetched_markets=8,
                processed_markets=5,
                open_position_count=1,
                resolved_trade_count=0,
                pending_resolution_slots=[],
                risk={
                    "capital": 501.0,
                    "realized_pnl_total": 0.5,
                    "unrealized_pnl_total": -0.1,
                    "mark_to_market_capital": 500.9,
                    "max_drawdown": 0.02,
                    "marked_position_count": 1,
                    "unmarked_position_count": 0,
                    "open_order_count": 2,
                    "total_gross_exposure": 7.0,
                    "exposure_by_asset": {"btc": {"total_exposure": 7.0}},
                    "exposure_by_interval": {"5": {"total_exposure": 7.0}},
                },
            )
            runtime.write_strategy_metrics(
                {
                    "toxicity_mm": {
                        "quotes_submitted": 6,
                        "orders_resting": 2,
                        "orders_filled": 1,
                        "cancellations": 1,
                        "realized_pnl": 0.5,
                        "markets_seen": 8,
                        "toxic_book_skips": 3,
                    }
                }
            )
            runtime.append_event("order.filled", {"strategy_family": "toxicity_mm", "market_id": "m-new"}, run_id="run-b")
            runtime.append_event("order.filled", {"strategy_family": "toxicity_mm", "market_id": "m-old"}, run_id="run-a")
            runtime.append_event("quote.skipped", {"reasons": ["existing_market_exposure"]}, run_id="run-b")
            runtime.append_market_sample({"market_id": "m-new", "book_reasons": ["high_vpin"]}, run_id="run-b")
            runtime.append_market_sample({"market_id": "m-old", "book_reasons": ["wide_spread>500"]}, run_id="run-a")

            ledger = SQLiteLedger(Path(tmpdir) / "ledger.db")
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-a",
                    stream="market_slot",
                    aggregate_id="slot-a",
                    sequence_num=1,
                    event_type="slot_settled",
                    event_ts=100.0,
                    recorded_ts=100.0,
                    run_id="run-a",
                    idempotency_key="settled-a",
                    causation_id=None,
                    correlation_id="slot-a",
                    schema_version=1,
                    payload={"market_id": "m-old", "market_slug": "old", "winning_outcome": "Up", "settled_ts": 100.0},
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-b",
                    stream="risk",
                    aggregate_id="run-b",
                    sequence_num=1,
                    event_type="risk_snapshot_recorded",
                    event_ts=101.0,
                    recorded_ts=101.0,
                    run_id="run-b",
                    idempotency_key="risk-b",
                    causation_id=None,
                    correlation_id="run-b",
                    schema_version=1,
                    payload={"capital": 501.0},
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-c",
                    stream="market_slot",
                    aggregate_id="slot-b",
                    sequence_num=1,
                    event_type="slot_settled",
                    event_ts=102.0,
                    recorded_ts=102.0,
                    run_id="run-b",
                    idempotency_key="settled-b",
                    causation_id=None,
                    correlation_id="slot-b",
                    schema_version=1,
                    payload={"market_id": "m-new", "market_slug": "new", "winning_outcome": "Down", "settled_ts": 102.0},
                )
            )

            payload = build_baseline_evidence(tmpdir)
            self.assertEqual(payload["current_run_id"], "run-b")
            self.assertEqual(payload["current_run"]["fill_event_count"], 1)
            self.assertEqual(payload["current_run"]["slot_settled_count"], 1)
            self.assertEqual(payload["restart_continuity"]["observed_restart_count"], 1)
            self.assertEqual(payload["skip_analysis"]["quote_skip_reasons"], {"existing_market_exposure": 1})
            self.assertEqual(payload["skip_analysis"]["sample_skip_reasons"], {"high_vpin": 1})
            rendered = render_baseline_evidence_text(payload)
            self.assertIn("Strategy family: toxicity_mm", rendered)
            self.assertIn("Restarts observed in ledger: 1", rendered)


if __name__ == "__main__":
    unittest.main()
