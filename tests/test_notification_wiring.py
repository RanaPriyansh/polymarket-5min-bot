import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from runtime_telemetry import RuntimeTelemetry

SUPERVISOR_PATH = Path("/root/.hermes/scripts/polymarket_72h_supervisor.py")
DIGEST_PATH = Path("/root/.hermes/scripts/polymarket_telegram_evidence.py")


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class SupervisorNotificationWiringTests(unittest.TestCase):
    def setUp(self):
        if not SUPERVISOR_PATH.exists():
            self.skipTest(f"supervisor script not present: {SUPERVISOR_PATH}")
        self.module = _load_module("polymarket_72h_supervisor_test", SUPERVISOR_PATH)

    def _snapshot(self, **overrides):
        snap = {
            "run_id": "paper-1",
            "mode": "paper",
            "phase": "running",
            "loop": 12,
            "heartbeat_age": 5.0,
            "service": {"ActiveState": "active", "SubState": "running", "MainPID": "11"},
            "timer": {"ActiveState": "active", "SubState": "waiting", "UnitFileState": "enabled"},
            "healthcheck_execstart": "python cli.py health",
            "paper_processes": 1,
            "live_processes": 0,
            "capital": 500.0,
            "realized": 0.0,
            "unrealized": 0.0,
            "drawdown": 0.0,
            "daily_pnl": 0.0,
            "resolved": 0,
            "win_rate": 0.0,
            "stop_reason": None,
            "gate_state": "GREEN",
            "gate_reasons": [],
            "pause_reason": None,
            "new_order_pause": False,
            "bucket_pause": {},
            "runtime_gate": {},
        }
        snap.update(overrides)
        return snap

    def test_repeated_same_critical_incident_sends_once_then_dedupes(self):
        state = {"seen_error_lines": {}}
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            "os.environ", {"HERMES_NOTIFICATION_STATE_PATH": str(Path(tmpdir) / "notify.json")}, clear=False
        ), mock.patch.object(self.module, "send_telegram", return_value=True) as send_mock:
            snap = self._snapshot(service={"ActiveState": "failed", "SubState": "failed", "MainPID": "0"})
            self.module.maybe_alerts(state, snap, "", "")
            self.module.maybe_alerts(state, snap, "", "")

        self.assertEqual(send_mock.call_count, 1)

    def test_resolved_critical_incident_can_alert_again_when_it_reappears(self):
        state = {"seen_error_lines": {}}
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            "os.environ", {"HERMES_NOTIFICATION_STATE_PATH": str(Path(tmpdir) / "notify.json")}, clear=False
        ), mock.patch.object(self.module, "send_telegram", return_value=True) as send_mock:
            failing = self._snapshot(service={"ActiveState": "failed", "SubState": "failed", "MainPID": "0"})
            healthy = self._snapshot()
            self.module.maybe_alerts(state, failing, "", "")
            self.module.maybe_alerts(state, healthy, "", "")
            self.module.maybe_alerts(state, failing, "", "")

        self.assertEqual(send_mock.call_count, 2)

    def test_warning_conditions_do_not_immediately_send(self):
        state = {"seen_error_lines": {}}
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            "os.environ", {"HERMES_NOTIFICATION_STATE_PATH": str(Path(tmpdir) / "notify.json")}, clear=False
        ), mock.patch.object(self.module, "send_telegram", return_value=True) as send_mock:
            snap = self._snapshot(stop_reason="run_lineage_fragmentation=4 >= 4 (experiment fragmented)")
            self.module.maybe_alerts(state, snap, "run_lineage_fragmentation hit", "")

        self.assertEqual(send_mock.call_count, 0)

    def test_stale_stopped_lines_do_not_page_when_service_is_running(self):
        state = {"seen_error_lines": {}}
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            "os.environ", {"HERMES_NOTIFICATION_STATE_PATH": str(Path(tmpdir) / "notify.json")}, clear=False
        ), mock.patch.object(self.module, "send_telegram", return_value=True) as send_mock:
            snap = self._snapshot()
            self.module.maybe_alerts(state, snap, "2026-04-27T18:00:00 service stopped", "")

        self.assertEqual(send_mock.call_count, 0)


class DigestNotificationWiringTests(unittest.TestCase):
    def setUp(self):
        if not DIGEST_PATH.exists():
            self.skipTest(f"digest script not present: {DIGEST_PATH}")
        self.module = _load_module("polymarket_telegram_evidence_test", DIGEST_PATH)

    def _build_runtime(self, runtime_dir: Path, mode: str = "paper") -> None:
        runtime = RuntimeTelemetry(runtime_dir)
        runtime.write_runtime_snapshot(
            run_id="paper-1",
            phase="running",
            mode=mode,
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

    def test_digest_command_does_not_resend_before_6h(self):
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            "os.environ", {"HERMES_NOTIFICATION_STATE_PATH": str(Path(tmpdir) / "notify.json")}, clear=False
        ), mock.patch.object(self.module, "send_telegram", return_value=True) as send_mock:
            runtime_dir = Path(tmpdir) / "runtime"
            self._build_runtime(runtime_dir)

            first = self.module.main(["--runtime-dir", str(runtime_dir), "--send"])
            second = self.module.main(["--runtime-dir", str(runtime_dir), "--send"])

        self.assertEqual(first, 0)
        self.assertEqual(second, 0)
        self.assertEqual(send_mock.call_count, 1)

    def test_digest_send_blocked_under_paper_only_for_live_mode(self):
        with tempfile.TemporaryDirectory() as tmpdir, mock.patch.dict(
            "os.environ", {"HERMES_NOTIFICATION_STATE_PATH": str(Path(tmpdir) / "notify.json")}, clear=False
        ), mock.patch.object(self.module, "send_telegram", side_effect=AssertionError("should not send while live")):
            runtime_dir = Path(tmpdir) / "runtime"
            self._build_runtime(runtime_dir, mode="live")

            rc = self.module.main(["--runtime-dir", str(runtime_dir), "--send"])

        self.assertNotEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
