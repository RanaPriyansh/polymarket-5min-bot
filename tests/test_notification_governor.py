import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from click.testing import CliRunner

import cli
from runtime_telemetry import RuntimeTelemetry


class NotificationGovernorTests(unittest.TestCase):
    def test_digest_sends_at_t0(self):
        from notification_governor import NotificationGovernor

        with tempfile.TemporaryDirectory() as tmpdir:
            governor = NotificationGovernor(state_path=Path(tmpdir) / "notify.json")

            decision = governor.digest_decision(now_ts=0.0)

            self.assertTrue(decision.should_send)
            self.assertEqual(decision.reason, "initial_digest")

    def test_digest_does_not_send_before_6h(self):
        from notification_governor import NotificationGovernor

        with tempfile.TemporaryDirectory() as tmpdir:
            governor = NotificationGovernor(state_path=Path(tmpdir) / "notify.json")
            governor.record_digest_sent(now_ts=0.0)

            decision = governor.digest_decision(now_ts=21599.0)

            self.assertFalse(decision.should_send)
            self.assertEqual(decision.reason, "digest_interval_not_elapsed")

    def test_digest_sends_after_6h(self):
        from notification_governor import NotificationGovernor

        with tempfile.TemporaryDirectory() as tmpdir:
            governor = NotificationGovernor(state_path=Path(tmpdir) / "notify.json")
            governor.record_digest_sent(now_ts=0.0)

            decision = governor.digest_decision(now_ts=21600.0)

            self.assertTrue(decision.should_send)
            self.assertEqual(decision.reason, "digest_interval_elapsed")

    def test_claim_digest_send_records_atomically(self):
        from notification_governor import NotificationGovernor

        with tempfile.TemporaryDirectory() as tmpdir:
            governor = NotificationGovernor(state_path=Path(tmpdir) / "notify.json")

            first = governor.claim_digest_send(now_ts=0.0)
            second = governor.claim_digest_send(now_ts=1.0)

            self.assertTrue(first.should_send)
            self.assertFalse(second.should_send)
            self.assertEqual(second.reason, "digest_claim_active")

    def test_force_digest_bypasses_interval(self):
        from notification_governor import NotificationGovernor

        with tempfile.TemporaryDirectory() as tmpdir:
            governor = NotificationGovernor(state_path=Path(tmpdir) / "notify.json")
            governor.record_digest_sent(now_ts=0.0)

            decision = governor.claim_digest_send(now_ts=1.0, force=True)

            self.assertTrue(decision.should_send)
            self.assertEqual(decision.reason, "forced_digest")

    def test_repeated_same_critical_dedupes(self):
        from notification_governor import NotificationGovernor

        with tempfile.TemporaryDirectory() as tmpdir:
            governor = NotificationGovernor(state_path=Path(tmpdir) / "notify.json")
            incident = {
                "category": "service_failure",
                "root_cause": "paper bot down",
                "loop_count": 1,
            }

            first = governor.incident_decision("critical", incident, now_ts=100.0)
            governor.record_incident_notification(first.fingerprint, "critical", incident, now_ts=100.0)
            second = governor.incident_decision("critical", incident | {"loop_count": 9}, now_ts=200.0)

            self.assertTrue(first.should_send)
            self.assertFalse(second.should_send)
            self.assertEqual(first.fingerprint, second.fingerprint)
            self.assertEqual(second.reason, "critical_dedup_active")

    def test_resolved_incident_can_alert_again(self):
        from notification_governor import NotificationGovernor

        with tempfile.TemporaryDirectory() as tmpdir:
            governor = NotificationGovernor(state_path=Path(tmpdir) / "notify.json")
            incident = {
                "category": "service_failure",
                "root_cause": "paper bot down",
            }

            first = governor.incident_decision("critical", incident, now_ts=100.0)
            governor.record_incident_notification(first.fingerprint, "critical", incident, now_ts=100.0)
            governor.resolve_incident(first.fingerprint, now_ts=150.0)
            second = governor.incident_decision("critical", incident, now_ts=200.0)

            self.assertTrue(first.should_send)
            self.assertTrue(second.should_send)
            self.assertEqual(second.reason, "incident_reappeared")


class NotifyDigestCliTests(unittest.TestCase):
    def _build_runtime(self, runtime_dir: Path) -> None:
        runtime = RuntimeTelemetry(runtime_dir)
        runtime.write_runtime_snapshot(
            run_id="paper-1",
            phase="running",
            mode="paper",
            loop_count=12,
            bankroll=500.0,
            open_position_count=0,
            resolved_trade_count=0,
            win_rate=0.0,
            fetched_markets=4,
            processed_markets=4,
            toxic_skips=0,
            gate_state="GREEN",
            gate_reasons=[],
            new_order_pause=False,
            pending_resolution_slots=[],
            risk={
                "capital": 500.0,
                "realized_pnl_total": 0.0,
                "unrealized_pnl_total": 0.0,
                "daily_pnl": 0.0,
                "max_drawdown": 0.0,
            },
            positions={},
        )

    def test_manual_and_dry_run_paths_do_not_send(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir) / "runtime"
            self._build_runtime(runtime_dir)
            cfg = cli.load_cfg()
            cfg["runtime"] = {"dir": str(runtime_dir)}
            cfg.setdefault("telegram", {})["enabled"] = True

            with mock.patch.object(cli, "load_cfg", return_value=cfg), mock.patch.object(
                cli, "send_digest_notification", side_effect=AssertionError("should not send")
            ):
                result = runner.invoke(cli.cli, ["notify-digest", "--dry-run", "--runtime-dir", str(runtime_dir)])
                self.assertEqual(result.exit_code, 0, msg=result.output)
                self.assertIn("POLYMARKET PAPER TRADING STATUS — 6H DIGEST", result.output)
                self.assertIn("Dry run", result.output)

                result = runner.invoke(cli.cli, ["notify-digest", "--runtime-dir", str(runtime_dir)])
                self.assertEqual(result.exit_code, 0, msg=result.output)
                self.assertIn("POLYMARKET PAPER TRADING STATUS — 6H DIGEST", result.output)
                self.assertIn("Send not requested", result.output)

    def test_send_path_respects_governor_suppression_and_force(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir) / "runtime"
            state_path = Path(tmpdir) / "notify.json"
            self._build_runtime(runtime_dir)
            cfg = cli.load_cfg()
            cfg["runtime"] = {"dir": str(runtime_dir)}
            cfg.setdefault("telegram", {})["enabled"] = True

            with mock.patch.object(cli, "load_cfg", return_value=cfg), mock.patch.dict(
                "os.environ", {"HERMES_NOTIFICATION_STATE_PATH": str(state_path)}, clear=False
            ), mock.patch.object(cli, "send_digest_notification", return_value=True) as send_mock:
                first = runner.invoke(cli.cli, ["notify-digest", "--runtime-dir", str(runtime_dir), "--send"])
                self.assertEqual(first.exit_code, 0, msg=first.output)
                self.assertIn("Digest sent", first.output)
                self.assertEqual(send_mock.call_count, 1)

                second = runner.invoke(cli.cli, ["notify-digest", "--runtime-dir", str(runtime_dir), "--send"])
                self.assertEqual(second.exit_code, 0, msg=second.output)
                self.assertIn("Digest suppressed by governor", second.output)
                self.assertEqual(send_mock.call_count, 1)

                forced = runner.invoke(cli.cli, ["notify-digest", "--runtime-dir", str(runtime_dir), "--send", "--force"])
                self.assertEqual(forced.exit_code, 0, msg=forced.output)
                self.assertIn("Digest sent: reason=forced_digest", forced.output)
                self.assertEqual(send_mock.call_count, 2)

    def test_failed_send_releases_claim_for_retry(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir) / "runtime"
            state_path = Path(tmpdir) / "notify.json"
            self._build_runtime(runtime_dir)
            cfg = cli.load_cfg()
            cfg["runtime"] = {"dir": str(runtime_dir)}
            cfg.setdefault("telegram", {})["enabled"] = True

            with mock.patch.object(cli, "load_cfg", return_value=cfg), mock.patch.dict(
                "os.environ", {"HERMES_NOTIFICATION_STATE_PATH": str(state_path)}, clear=False
            ), mock.patch.object(cli, "send_digest_notification", return_value=False):
                failed = runner.invoke(cli.cli, ["notify-digest", "--runtime-dir", str(runtime_dir), "--send"])
                self.assertNotEqual(failed.exit_code, 0)
                self.assertIn("Digest send failed", failed.output)

                governor = __import__("notification_governor").NotificationGovernor(state_path=state_path)
                state = governor.load_state()
                self.assertIsNone(state.get("last_digest_sent_at"))
                self.assertIsNone(state.get("digest_claimed_at"))
                self.assertIsNone(state.get("digest_claim_expires_at"))

    def test_send_blocked_when_paper_only_policy_meets_live_mode(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir = Path(tmpdir) / "runtime"
            self._build_runtime(runtime_dir)
            runtime = RuntimeTelemetry(runtime_dir)
            runtime.update_status(mode="live")
            cfg = cli.load_cfg()
            cfg["runtime"] = {"dir": str(runtime_dir)}
            cfg.setdefault("telegram", {})["enabled"] = True

            with mock.patch.object(cli, "load_cfg", return_value=cfg), mock.patch.object(
                cli, "send_digest_notification", side_effect=AssertionError("should not send while live"),
            ):
                result = runner.invoke(cli.cli, ["notify-digest", "--runtime-dir", str(runtime_dir), "--send"])
                self.assertNotEqual(result.exit_code, 0)
                self.assertIn("PAPER_ONLY policy", result.output)


if __name__ == "__main__":
    unittest.main()
