import unittest
from click.testing import CliRunner

import cli
from paper_health_policy import evaluate_healthcheck_restart_policy, is_protective_stop


class PaperHealthPolicyTests(unittest.TestCase):
    def test_healthcheck_does_not_restart_after_risk_stop(self):
        payload = {
            "healthy": False,
            "status": {
                "mode": "paper",
                "phase": "stopped",
                "stop_reason": "circuit_breaker",
                "gate_state": "RED",
                "gate_reasons": ["circuit_breaker_fired_unreviewed=1"],
                "pause_reason": "hard_stop_red_gate",
            },
        }

        decision = evaluate_healthcheck_restart_policy(payload, paper_process_count=0)

        self.assertFalse(decision["should_restart"])
        self.assertEqual(decision["reason"], "protective_stop")
        self.assertIn("stop_reason=circuit_breaker", decision["protective_stop_reasons"])

    def test_duplicate_paper_processes_block_restart_amplification(self):
        payload = {
            "healthy": False,
            "status": {
                "mode": "paper",
                "phase": "running",
                "stop_reason": None,
                "gate_state": "GREEN",
                "gate_reasons": [],
            },
        }

        decision = evaluate_healthcheck_restart_policy(payload, paper_process_count=2)

        self.assertFalse(decision["should_restart"])
        self.assertEqual(decision["reason"], "duplicate_paper_processes")
        self.assertEqual(decision["paper_process_count"], 2)

    def test_orphan_single_paper_process_blocks_restart(self):
        payload = {
            "healthy": False,
            "status": {
                "mode": "paper",
                "phase": "running",
                "stop_reason": None,
                "gate_state": "GREEN",
                "gate_reasons": [],
            },
        }

        decision = evaluate_healthcheck_restart_policy(
            payload,
            paper_process_count=1,
            service_main_pid=12345,
            managed_paper_process_count=0,
        )

        self.assertFalse(decision["should_restart"])
        self.assertEqual(decision["reason"], "orphan_paper_process")

    def test_genuine_unhealthy_case_still_restarts(self):
        payload = {
            "healthy": False,
            "status": {
                "mode": "paper",
                "phase": "running",
                "stop_reason": None,
                "gate_state": "GREEN",
                "gate_reasons": [],
            },
        }

        decision = evaluate_healthcheck_restart_policy(
            payload,
            paper_process_count=1,
            service_main_pid=12345,
            managed_paper_process_count=1,
        )

        self.assertTrue(decision["should_restart"])
        self.assertEqual(decision["reason"], "genuine_unhealthy")

    def test_manual_bounded_stop_counts_as_protective_stop(self):
        protective, reasons = is_protective_stop(
            {
                "phase": "stopped",
                "stop_reason": "max_loops",
                "gate_state": "YELLOW",
                "gate_reasons": [],
            }
        )

        self.assertTrue(protective)
        self.assertIn("stop_reason=max_loops", reasons)


    def test_latched_runtime_counts_as_protective_stop(self):
        payload = {
            "healthy": False,
            "status": {
                "mode": "paper",
                "phase": "paused",
                "stop_reason": "risk_stop_latch_present",
                "pause_policy": "persistent_risk_stop_latch",
                "pause_reason": "risk_stop_latch_present:drawdown_limit",
                "risk_latch_present": True,
                "risk_latch_reason": "drawdown_limit",
                "gate_state": "RED",
                "gate_reasons": ["risk_stop_latch:drawdown_limit"],
            },
        }

        decision = evaluate_healthcheck_restart_policy(payload, paper_process_count=0)

        self.assertFalse(decision["should_restart"])
        self.assertEqual(decision["reason"], "protective_stop")
        self.assertIn("stop_reason=risk_stop_latch_present", decision["protective_stop_reasons"])
        self.assertIn("risk_latch_present=drawdown_limit", decision["protective_stop_reasons"])


class CliLiveBlockTests(unittest.TestCase):
    def test_live_mode_remains_blocked_via_cli(self):
        runner = CliRunner()

        result = runner.invoke(cli.cli, ["live"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Live mode is deferred until the restored paper workflow is proven.", result.output)


if __name__ == "__main__":
    unittest.main()
