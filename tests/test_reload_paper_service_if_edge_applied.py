import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.reload_paper_service_if_edge_applied import maybe_reload


class FakeCompleted:
    def __init__(self, returncode=0):
        self.returncode = returncode


class ReloadPaperServiceIfEdgeAppliedTests(unittest.TestCase):
    def _artifact(self, path: Path, *, created_at=None):
        payload = {
            "created_at": time.time() if created_at is None else created_at,
            "applied_decisions": [
                {"family": "time_decay", "action": "promote_to_paper_active", "applied": True}
            ],
        }
        path.write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def test_refuses_restart_when_service_inactive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            edge = Path(tmpdir) / "edge.json"
            state = Path(tmpdir) / "state.json"
            self._artifact(edge)
            calls = []

            def fake_run(cmd, check=False):
                calls.append(cmd)
                return FakeCompleted(1 if "is-active" in cmd else 0)

            with patch("scripts.reload_paper_service_if_edge_applied.subprocess.run", side_effect=fake_run):
                self.assertEqual(maybe_reload(edge, state), 0)
            self.assertEqual(len([cmd for cmd in calls if "restart" in cmd]), 0)
            self.assertFalse(state.exists())

    def test_restarts_only_once_for_same_fresh_artifact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            edge = Path(tmpdir) / "edge.json"
            state = Path(tmpdir) / "state.json"
            self._artifact(edge)
            calls = []

            def fake_run(cmd, check=False):
                calls.append(cmd)
                return FakeCompleted(0)

            with patch("scripts.reload_paper_service_if_edge_applied.subprocess.run", side_effect=fake_run):
                self.assertEqual(maybe_reload(edge, state), 0)
                self.assertEqual(maybe_reload(edge, state), 0)
            restart_calls = [cmd for cmd in calls if "restart" in cmd]
            self.assertEqual(len(restart_calls), 1)
            self.assertTrue(state.exists())

    def test_ignores_stale_or_malformed_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            edge = Path(tmpdir) / "edge.json"
            state = Path(tmpdir) / "state.json"
            self._artifact(edge, created_at=time.time() - 9999)
            with patch("scripts.reload_paper_service_if_edge_applied.subprocess.run") as run:
                self.assertEqual(maybe_reload(edge, state, max_age_seconds=60), 0)
                run.assert_not_called()
            edge.write_text("{not-json", encoding="utf-8")
            with patch("scripts.reload_paper_service_if_edge_applied.subprocess.run") as run:
                self.assertEqual(maybe_reload(edge, state), 0)
                run.assert_not_called()

    def test_block_decision_does_not_restart(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            edge = Path(tmpdir) / "edge.json"
            state = Path(tmpdir) / "state.json"
            edge.write_text(json.dumps({"created_at": time.time(), "applied_decisions": [{"action": "block"}]}), encoding="utf-8")
            with patch("scripts.reload_paper_service_if_edge_applied.subprocess.run") as run:
                self.assertEqual(maybe_reload(edge, state), 0)
                run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
