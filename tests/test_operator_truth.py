import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ledger import LedgerEvent, SQLiteLedger
from scripts.operator_truth import artifact_truth_context, status_truth_context
from scripts import reconcile_metrics_current
from scripts.ops_daily_summary import render_daily_summary
from scripts.ops_evidence import render_evidence
from scripts.ops_settlement_diagnostics import render_settlement_diagnostics
from scripts import fill_markout_audit


class OperatorTruthTests(unittest.TestCase):
    def test_reviewed_scripts_resolve_operator_truth_when_loaded_outside_repo_root(self):
        repo_root = Path(__file__).resolve().parent.parent
        script_names = [
            "ops_status.py",
            "ops_evidence.py",
            "ops_settlement_diagnostics.py",
            "ops_daily_summary.py",
            "fill_markout_audit.py",
            "settlement_latency_audit.py",
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            for script_name in script_names:
                script_path = repo_root / "scripts" / script_name
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        f"import runpy; runpy.run_path({str(script_path)!r}, run_name='__not_main__')",
                    ],
                    cwd=tmpdir,
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(
                    result.returncode,
                    0,
                    msg=f"{script_name} failed to import outside repo root: {result.stderr or result.stdout}",
                )

    def test_artifact_truth_context_marks_current_and_stale(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            (runtime_dir / "status.json").write_text(
                json.dumps({"run_id": "run-1", "heartbeat_ts": 120.0}),
                encoding="utf-8",
            )

            current = artifact_truth_context(runtime_dir, artifact_run_id="run-1", generated_at_ts=180.0)
            self.assertEqual(current["freshness"], "CURRENT")
            self.assertTrue(current["run_match"])
            self.assertEqual(current["stale_reasons"], [])

            stale = artifact_truth_context(runtime_dir, artifact_run_id="run-0", generated_at_ts=400.0)
            self.assertEqual(stale["freshness"], "STALE")
            self.assertIn("run_id_mismatch", stale["stale_reasons"])
            self.assertIn("heartbeat_age>180s", stale["stale_reasons"])

    def test_reconcile_report_uses_replay_projection_not_fill_only_open_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            db_path = runtime_dir / "ledger.db"
            ledger = SQLiteLedger(db_path)
            run_id = "run-reconcile"
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
                    event_id="evt-fill-observed",
                    stream="order",
                    aggregate_id="order-1",
                    sequence_num=1,
                    event_type="fill_observed",
                    event_ts=99.0,
                    recorded_ts=99.0,
                    run_id=run_id,
                    idempotency_key="fill-observed-1",
                    causation_id=None,
                    correlation_id="order-1",
                    schema_version=1,
                    payload=fill_payload,
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-fill",
                    stream="order",
                    aggregate_id="order-1",
                    sequence_num=2,
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
                        "slot_id": slot_id,
                        "market_id": market_id,
                        "winning_outcome": "Up",
                        "settled_ts": 150.0,
                    },
                )
            )

            (runtime_dir / "status.json").write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "heartbeat_ts": 200.0,
                        "baseline_strategy": "toxicity_mm",
                        "open_position_count": 0,
                        "resolved_trade_count": 1,
                        "pending_resolution_slots": [],
                        "bankroll": 506.0,
                        "risk": {
                            "realized_pnl_total": 6.0,
                            "unrealized_pnl_total": 0.0,
                        },
                    }
                ),
                encoding="utf-8",
            )
            (runtime_dir / "strategy_metrics.json").write_text(
                json.dumps({"toxicity_mm": {"orders_filled": 1, "quotes_submitted": 1, "realized_pnl": 6.0}}),
                encoding="utf-8",
            )

            old_runtime = reconcile_metrics_current.RUNTIME
            old_db = reconcile_metrics_current.DB
            old_status = reconcile_metrics_current.STATUS
            old_metrics = reconcile_metrics_current.METRICS
            try:
                reconcile_metrics_current.RUNTIME = runtime_dir
                reconcile_metrics_current.DB = runtime_dir / "ledger.db"
                reconcile_metrics_current.STATUS = runtime_dir / "status.json"
                reconcile_metrics_current.METRICS = runtime_dir / "strategy_metrics.json"
                report = reconcile_metrics_current.build_report(now_ts=210.0)
            finally:
                reconcile_metrics_current.RUNTIME = old_runtime
                reconcile_metrics_current.DB = old_db
                reconcile_metrics_current.STATUS = old_status
                reconcile_metrics_current.METRICS = old_metrics

            self.assertIn("freshness: CURRENT", report)
            self.assertIn("replay_asof_heartbeat.open_positions=0", report)
            self.assertIn("replay_asof_heartbeat.resolved_trade_count=1", report)
            self.assertIn("Verdict: PASS", report)
            self.assertNotIn("open positions mismatch", report)

    def test_status_truth_context_tracks_status_freshness_without_artifact_run_binding(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            (runtime_dir / "status.json").write_text(
                json.dumps({"run_id": "run-status", "heartbeat_ts": 120.0}),
                encoding="utf-8",
            )

            current = status_truth_context(runtime_dir, generated_at_ts=180.0)
            self.assertEqual(current["freshness"], "CURRENT")
            self.assertEqual(current["status_run_id"], "run-status")
            self.assertEqual(current["stale_reasons"], [])

            stale = status_truth_context(runtime_dir, generated_at_ts=400.0)
            self.assertEqual(stale["freshness"], "STALE")
            self.assertIn("heartbeat_age>180s", stale["stale_reasons"])

    def test_non_run_scoped_reports_label_scope_explicitly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            (runtime_dir / "status.json").write_text(
                json.dumps(
                    {
                        "run_id": "run-active",
                        "heartbeat_ts": 200.0,
                        "mode": "paper",
                        "bankroll": 500.0,
                        "resolved_trade_count": 2,
                        "win_rate": 0.5,
                        "pending_resolution_slots": [],
                    }
                ),
                encoding="utf-8",
            )
            (runtime_dir / "strategy_metrics.json").write_text(
                json.dumps({"toxicity_mm": {"orders_filled": 2, "quotes_submitted": 4, "realized_pnl": 1.25}}),
                encoding="utf-8",
            )
            (runtime_dir / "events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({"ts": 1712275201.0, "run_id": "run-a", "event_type": "quote.submitted", "payload": {"strategy_family": "toxicity_mm"}}),
                        json.dumps({"ts": 1712275261.0, "run_id": "run-b", "event_type": "fill_applied", "payload": {"strategy_family": "toxicity_mm"}}),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            evidence = render_evidence(runtime_dir)
            self.assertIn("Report scope: all-run ledger counts + runtime-wide strategy metrics + current status snapshot", evidence)
            self.assertIn("status_run_id: run-active", evidence)
            self.assertNotIn("artifact_run_id:", evidence)

            settlement = render_settlement_diagnostics(runtime_dir, now_ts=210.0)
            self.assertIn("Report scope: all-run settlement lifecycle evidence + current pending-status snapshot", settlement)
            self.assertIn("Active status run: run-active", settlement)
            self.assertNotIn("artifact_run_id:", settlement)

            daily = render_daily_summary(runtime_dir, "2024-04-05")
            self.assertIn("Report scope: UTC day 2024-04-05 from events.jsonl + current status snapshot", daily)
            self.assertIn("Unique run_ids: 2", daily)
            self.assertIn("--- CURRENT STATUS SNAPSHOT ---", daily)
            self.assertNotIn("artifact_run_id:", daily)

    def test_settlement_diagnostics_stays_artifact_truthful_and_exact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            db_path = runtime_dir / "ledger.db"
            ledger = SQLiteLedger(db_path)
            run_id = "run-active"
            slot_id = "btc:5:100"

            ledger.append_event(
                LedgerEvent(
                    event_id="evt-pending",
                    stream="market_slot",
                    aggregate_id=slot_id,
                    sequence_num=1,
                    event_type="slot_resolution_pending",
                    event_ts=100.0,
                    recorded_ts=100.0,
                    run_id=run_id,
                    idempotency_key="pending-1",
                    causation_id=None,
                    correlation_id=slot_id,
                    schema_version=1,
                    payload={"slot_id": slot_id},
                )
            )
            ledger.append_event(
                LedgerEvent(
                    event_id="evt-settled",
                    stream="market_slot",
                    aggregate_id=slot_id,
                    sequence_num=2,
                    event_type="slot_settled",
                    event_ts=140.0,
                    recorded_ts=140.0,
                    run_id=run_id,
                    idempotency_key="settled-1",
                    causation_id=None,
                    correlation_id=slot_id,
                    schema_version=1,
                    payload={"slot_id": slot_id},
                )
            )

            (runtime_dir / "events.jsonl").write_text(
                "\n".join(
                    [
                        json.dumps({
                            "ts": 110.0,
                            "run_id": run_id,
                            "event_type": "market.pending_resolution",
                            "aggregate_id": slot_id,
                            "payload": {"slot_id": slot_id, "delay_seconds": 10, "deferred": True},
                        }),
                        json.dumps({
                            "ts": 140.0,
                            "run_id": run_id,
                            "event_type": "market.settled",
                            "aggregate_id": slot_id,
                            "payload": {"slot_id": slot_id},
                        }),
                        json.dumps({
                            "ts": 141.0,
                            "run_id": run_id,
                            "event_type": "slot_settled",
                            "aggregate_id": slot_id,
                            "payload": {"slot_id": slot_id},
                        }),
                    ]
                ) + "\n",
                encoding="utf-8",
            )
            (runtime_dir / "status.json").write_text(
                json.dumps(
                    {
                        "run_id": run_id,
                        "heartbeat_ts": 150.0,
                        "pending_resolution_slots": [
                            {"slot_id": slot_id, "market_slug": "btc-updown-5m-100", "next_poll_ts": 160.0, "deferred": True}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            report = render_settlement_diagnostics(runtime_dir, now_ts=170.0)
            self.assertIn("slot_resolution_pending (ledger.db):            1", report)
            self.assertIn("slot_settled (ledger.db):                       1", report)
            self.assertIn("market.pending_resolution (events):             1", report)
            self.assertIn("market.settled (events):                        1", report)
            self.assertIn("slot_settled (events):                          1", report)
            self.assertIn("pending_resolution_slots in status.json is a current snapshot", report)
            self.assertIn("This report does not perform live API or curl verification.", report)
            self.assertNotIn("verified independently via curl", report)
            self.assertNotIn("_market_has_open_exposure", report)
            self.assertNotIn("RECOMMENDED FIX", report)

    def test_fill_markout_samples_merge_events_and_market_samples(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir)
            events_path = runtime_dir / "events.jsonl"
            samples_path = runtime_dir / "market_samples.jsonl"
            events_path.write_text(
                json.dumps(
                    {
                        "ts": 100.0,
                        "run_id": "run-1",
                        "event_type": "quote.submitted",
                        "payload": {
                            "market_id": "m1",
                            "outcome": "Up",
                            "book_quality": {"outcome": "Up", "mid_price": 0.41},
                        },
                    }
                ) + "\n",
                encoding="utf-8",
            )
            samples_path.write_text(
                "\n".join(
                    [
                        json.dumps({"ts": 120.0, "run_id": "run-1", "market_id": "m1", "outcome": "Down", "mid_price": 0.59}),
                        json.dumps({"ts": 100.0, "run_id": "run-1", "market_id": "m1", "outcome": "Up", "mid_price": 0.41}),
                    ]
                ) + "\n",
                encoding="utf-8",
            )

            old_events = fill_markout_audit.EVENTS
            old_samples = fill_markout_audit.SAMPLES
            try:
                fill_markout_audit.EVENTS = events_path
                fill_markout_audit.SAMPLES = samples_path
                merged = fill_markout_audit.load_samples("run-1")
            finally:
                fill_markout_audit.EVENTS = old_events
                fill_markout_audit.SAMPLES = old_samples

            self.assertEqual(merged[("m1", "Up")], [(100.0, 0.41)])
            self.assertEqual(merged[("m1", "Down")], [(120.0, 0.59)])


if __name__ == "__main__":
    unittest.main()
