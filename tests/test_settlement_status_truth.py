import json
import tempfile
import unittest
from pathlib import Path

from cli import _write_runtime_status_snapshot
from execution import PolymarketExecutor
from replay import replay_ledger
from ledger import LedgerEvent, SQLiteLedger
from runtime_telemetry import RuntimeTelemetry
from scripts.ops_status import render_status
from status_utils import render_status_text


class SettlementStatusTruthTests(unittest.TestCase):
    def test_slot_close_without_position_renders_as_resolution_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            (runtime_dir / "status.json").write_text(
                json.dumps(
                    {
                        "run_id": "run-flat",
                        "mode": "paper",
                        "phase": "active",
                        "baseline_strategy": "toxicity_mm",
                        "bankroll": 500.0,
                        "heartbeat_ts": 200.0,
                        "loop_count": 1,
                        "active_slots": [],
                        "positions": {},
                        "open_position_count": 0,
                        "risk": {
                            "realized_pnl_total": 0.0,
                            "unrealized_pnl_total": 0.0,
                            "total_gross_exposure": 0.0,
                            "peak": 500.0,
                            "max_drawdown": 0.0,
                        },
                        "resolved_trade_count": 0,
                        "win_rate": 0.0,
                        "pending_resolution_slots": [],
                        "latest_slot_resolution": {
                            "event_type": "slot_settled",
                            "slot_id": "btc:5:123",
                            "market_slug": "btc-updown-5m-123",
                            "winning_outcome": "Up",
                            "settled_ts": 180.0,
                            "position_outcome": None,
                            "position_size": None,
                            "realized_pnl": None,
                        },
                        "latest_position_settlement": None,
                        "latest_settlement": None,
                    }
                ),
                encoding="utf-8",
            )

            report = render_status(runtime_dir, now_ts=210.0)
            cli_report = render_status_text(runtime_dir)

            self.assertIn("Last slot resolved: btc:5:123", report)
            self.assertIn("no held position", report)
            self.assertIn("Last position settlement: none", report)
            self.assertNotIn("payout=?", report)
            self.assertIn("Last slot resolved: btc:5:123", cli_report)
            self.assertIn("resolved with no held position", cli_report)
            self.assertIn("Last position settlement: none", cli_report)

    def test_slot_closed_flat_resolution_renders_without_position_settlement(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            (runtime_dir / "status.json").write_text(
                json.dumps(
                    {
                        "run_id": "run-flat-closed",
                        "mode": "paper",
                        "phase": "active",
                        "bankroll": 500.0,
                        "heartbeat_ts": 200.0,
                        "loop_count": 1,
                        "open_position_count": 0,
                        "risk": {
                            "realized_pnl_total": 0.0,
                            "unrealized_pnl_total": 0.0,
                            "total_gross_exposure": 0.0,
                            "peak": 500.0,
                            "max_drawdown": 0.0,
                        },
                        "resolved_trade_count": 0,
                        "win_rate": 0.0,
                        "pending_resolution_slots": [],
                        "latest_slot_resolution": {
                            "event_type": "slot_closed",
                            "slot_id": "btc:5:124",
                            "market_slug": "btc-updown-5m-124",
                            "winning_outcome": "Down",
                            "settled_ts": 181.0,
                            "position_count": 0,
                        },
                        "latest_position_settlement": None,
                        "latest_settlement": None,
                    }
                ),
                encoding="utf-8",
            )

            report = render_status(runtime_dir, now_ts=210.0)
            cli_report = render_status_text(runtime_dir)

            for rendered in (report, cli_report):
                self.assertIn("Last slot resolved: btc:5:124  closed flat at expiry (winner=Down)", rendered)
                self.assertIn("Last position settlement: none", rendered)

    def test_attributed_settlement_is_exposed_in_snapshot_and_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "ledger.db"
            ledger = SQLiteLedger(db_path)
            run_id = "run-attrib"
            slot_id = "btc:5:100"
            market_id = "m1"

            fill_payload = {
                "strategy_family": "toxicity_mm",
                "market_id": market_id,
                "outcome": "Up",
                "side": "BUY",
                "fill_size": 10.0,
                "fill_price": 0.4,
                "filled_qty": 10.0,
                "remaining_qty": 0.0,
                "average_fill_price": 0.4,
                "slot_id": slot_id,
            }
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-fill",
                    stream="order",
                    aggregate_id="order-1",
                    sequence_num=1,
                    event_type="fill_applied",
                    event_ts=100.0,
                    recorded_ts=100.0,
                    run_id=run_id,
                    idempotency_key="fill-1",
                    causation_id=None,
                    correlation_id="order-1",
                    schema_version=1,
                    payload=fill_payload,
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-settle",
                    stream="market_slot",
                    aggregate_id=slot_id,
                    sequence_num=1,
                    event_type="slot_settled",
                    event_ts=150.0,
                    recorded_ts=150.0,
                    run_id=run_id,
                    idempotency_key="settle-1",
                    causation_id=None,
                    correlation_id=slot_id,
                    schema_version=1,
                    payload={
                        "market_id": market_id,
                        "market_slug": "btc-updown-5m-100",
                        "winning_outcome": "Up",
                        "settled_ts": 150.0,
                        "position_outcome": "Up",
                        "position_size": 10.0,
                        "entry_price": 0.4,
                        "realized_pnl": 6.0,
                        "is_win": 1,
                    },
                )
            )

            executor = PolymarketExecutor(
                {
                    "polymarket": {"clob_api_url": "http://fake", "gamma_api_url": "http://fake"},
                    "execution": {"ledger_db_path": str(db_path), "paper_starting_bankroll": 500.0},
                },
                market_data=None,
                mode="paper",
                run_id=run_id,
            )
            snapshot = executor.get_runtime_snapshot(now_ts=160.0)

            self.assertEqual(snapshot["latest_position_settlement"]["slot_id"], slot_id)
            self.assertEqual(snapshot["latest_position_settlement"]["realized_pnl"], 6.0)
            self.assertEqual(snapshot["latest_settlement"]["slot_id"], slot_id)
            self.assertEqual(snapshot["latest_slot_resolution"]["slot_id"], slot_id)

            runtime_dir = Path(tmpdir) / "runtime"
            runtime_dir.mkdir()
            (runtime_dir / "status.json").write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "mode": "paper",
                        "phase": "active",
                        "baseline_strategy": "toxicity_mm",
                        "bankroll": 506.0,
                        "heartbeat_ts": 160.0,
                        "loop_count": 2,
                        "active_slots": [],
                        "positions": {},
                        "open_position_count": 0,
                        "risk": {
                            "realized_pnl_total": 6.0,
                            "unrealized_pnl_total": 0.0,
                            "total_gross_exposure": 0.0,
                            "peak": 506.0,
                            "max_drawdown": 0.0,
                        },
                        "resolved_trade_count": 1,
                        "win_rate": 1.0,
                        "pending_resolution_slots": [],
                        "latest_slot_resolution": snapshot["latest_slot_resolution"],
                        "latest_position_settlement": snapshot["latest_position_settlement"],
                        "latest_settlement": snapshot["latest_settlement"],
                    }
                ),
                encoding="utf-8",
            )

            report = render_status(runtime_dir, now_ts=170.0)
            cli_report = render_status_text(runtime_dir)
            self.assertIn("Last position settlement: btc:5:100", report)
            self.assertIn("realized=$+6.00", report)
            self.assertIn("outcome=Up", report)
            self.assertIn("Last position settlement: btc:5:100", cli_report)
            self.assertIn("realized=$+6.00", cli_report)
            self.assertIn("outcome=Up", cli_report)

    def test_runtime_status_write_path_persists_resolution_and_position_truth(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = RuntimeTelemetry(Path(tmpdir))
            executor_snapshot = {
                "open_position_count": 0,
                "resolved_trade_count": 1,
                "win_rate": 1.0,
                "pending_resolution_slots": [],
                "latest_slot_resolution": {
                    "event_type": "slot_settled",
                    "slot_id": "btc:5:100",
                    "market_id": "m1",
                    "market_slug": "btc-updown-5m-100",
                    "winning_outcome": "Up",
                    "settled_ts": 150.0,
                    "position_count": 1,
                    "position_outcome": "Up",
                    "position_size": 5.0,
                    "entry_price": 0.4,
                    "realized_pnl": 3.0,
                    "is_win": 1,
                },
                "latest_position_settlement": {
                    "event_type": "slot_settled",
                    "slot_id": "btc:5:100",
                    "market_id": "m1",
                    "market_slug": "btc-updown-5m-100",
                    "winning_outcome": "Up",
                    "settled_ts": 150.0,
                    "outcome": "Up",
                    "quantity": 5.0,
                    "average_price": 0.4,
                    "realized_pnl_delta": 3.0,
                },
                "latest_settlement": {
                    "event_type": "slot_settled",
                    "slot_id": "btc:5:100",
                },
            }
            risk_report = {
                "capital": 503.0,
                "realized_pnl_total": 3.0,
                "unrealized_pnl_total": 0.0,
                "total_gross_exposure": 0.0,
                "peak": 503.0,
                "max_drawdown": 0.0,
            }

            _write_runtime_status_snapshot(
                runtime,
                run_id="run-live",
                phase="running",
                mode="paper",
                loop_count=2,
                fetched_markets=1,
                processed_markets=1,
                toxic_skips=0,
                bankroll=503.0,
                all_markets=[],
                executor_snapshot=executor_snapshot,
                positions={},
                risk_report=risk_report,
                gate_snapshot={"gate_state": "GREEN", "gate_reasons": [], "gate_inputs": {}},
                strategy_governance={"baseline_strategy": "toxicity_mm", "research_candidates": []},
            )

            status = runtime.read_status()
            self.assertEqual(status["latest_slot_resolution"]["slot_id"], "btc:5:100")
            self.assertEqual(status["latest_position_settlement"]["slot_id"], "btc:5:100")
            self.assertEqual(status["latest_settlement"]["slot_id"], "btc:5:100")

    def test_latest_position_settlement_prefers_latest_attributed_event(self):
        events = [
            LedgerEvent(
                event_id="evt-fill",
                stream="order",
                aggregate_id="order-1",
                sequence_num=1,
                event_type="fill_applied",
                event_ts=100.0,
                recorded_ts=100.0,
                run_id="run-pref",
                idempotency_key="fill-1",
                causation_id=None,
                correlation_id="order-1",
                schema_version=1,
                payload={
                    "strategy_family": "toxicity_mm",
                    "market_id": "m1",
                    "outcome": "Up",
                    "side": "BUY",
                    "fill_size": 5.0,
                    "fill_price": 0.4,
                    "filled_qty": 5.0,
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
                run_id="run-pref",
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
                    "position_size": 5.0,
                    "entry_price": 0.4,
                    "realized_pnl": 3.0,
                    "is_win": 1,
                },
            ),
            LedgerEvent(
                event_id="evt-close-2",
                stream="market_slot",
                aggregate_id="btc:5:105",
                sequence_num=1,
                event_type="slot_settled",
                event_ts=200.0,
                recorded_ts=200.0,
                run_id="run-pref",
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

    def test_multi_leg_slot_resolution_does_not_misattribute_single_position_fields(self):
        events = [
            LedgerEvent(
                event_id="evt-settle-multi",
                stream="market_slot",
                aggregate_id="btc:5:200",
                sequence_num=1,
                event_type="slot_settled",
                event_ts=220.0,
                recorded_ts=220.0,
                run_id="run-multi",
                idempotency_key="settle-multi",
                causation_id=None,
                correlation_id="btc:5:200",
                schema_version=1,
                payload={
                    "market_id": "m-multi",
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
            ),
        ]

        projection = replay_ledger(events)

        self.assertEqual(projection.latest_slot_resolution["position_count"], 2)
        self.assertIsNone(projection.latest_slot_resolution["position_outcome"])
        self.assertIsNone(projection.latest_position_settlement)


if __name__ == "__main__":
    unittest.main()
