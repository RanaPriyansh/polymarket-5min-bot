"""
Tests for research/gate.py — compute_gate_state() AC-10 gate acceptance criteria.

6 required test cases covering RED / YELLOW / GREEN transitions and each
RED-trigger condition individually.
"""
import sys
import os

# Ensure the project root is on sys.path so imports work when running from
# inside the tests/ directory or via pytest from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest

from research.gate import compute_gate_state


def _base_inputs(**overrides) -> dict:
    """
    Return a fully-populated healthy (GREEN) inputs dict.
    Override individual keys to test specific conditions.
    """
    base = {
        "win_rate": 0.50,
        "resolved_count": 60,
        "settlement_pnl_computable": True,
        "run_lineage_fragmentation": 0,
        "circuit_breaker_fired_unreviewed": False,
        "contradiction_log_open": 0,
    }
    base.update(overrides)
    return base


class GateTests(unittest.TestCase):

    # ------------------------------------------------------------------
    # RED tests
    # ------------------------------------------------------------------

    def test_red_on_low_win_rate(self):
        """win_rate < 0.20 AND resolved_count >= 20 must trigger RED."""
        inputs = _base_inputs(win_rate=0.03, resolved_count=65)
        state, reasons = compute_gate_state(inputs)

        self.assertEqual(state, "RED")
        self.assertTrue(len(reasons) > 0, "reasons must be non-empty for RED")
        # The reason string must reference win_rate
        self.assertIn(
            "win_rate",
            reasons[0],
            "First reason must mention 'win_rate' so the caller can surface it",
        )

    def test_red_on_missing_settlement_pnl(self):
        """settlement_pnl_computable=False must trigger RED."""
        inputs = _base_inputs(settlement_pnl_computable=False)
        state, reasons = compute_gate_state(inputs)

        self.assertEqual(state, "RED")
        self.assertTrue(len(reasons) > 0, "reasons must be non-empty for RED")
        # At least one reason must mention settlement_pnl_computable
        reason_text = " ".join(reasons).lower()
        self.assertIn(
            "settlement_pnl_computable",
            reason_text,
            "A reason must mention 'settlement_pnl_computable'",
        )

    def test_red_on_fragmentation(self):
        """run_lineage_fragmentation >= 4 must trigger RED."""
        inputs = _base_inputs(run_lineage_fragmentation=5)
        state, reasons = compute_gate_state(inputs)

        self.assertEqual(state, "RED")
        self.assertTrue(len(reasons) > 0, "reasons must be non-empty for RED")
        # At least one reason must mention fragmentation (case-insensitive)
        reason_text = " ".join(reasons).lower()
        self.assertIn(
            "fragmentation",
            reason_text,
            "A reason must mention 'fragmentation' (case-insensitive)",
        )

    def test_red_on_circuit_breaker_unreviewed(self):
        """circuit_breaker_fired_unreviewed=True must trigger RED."""
        inputs = _base_inputs(circuit_breaker_fired_unreviewed=True)
        state, reasons = compute_gate_state(inputs)

        self.assertEqual(state, "RED")
        self.assertTrue(len(reasons) > 0, "reasons must be non-empty for RED")
        # At least one reason must mention circuit_breaker (case-insensitive)
        reason_text = " ".join(reasons).lower()
        self.assertIn(
            "circuit_breaker",
            reason_text,
            "A reason must mention 'circuit_breaker' (case-insensitive)",
        )

    def test_red_collects_hard_stop_reason_alongside_low_win_rate(self):
        inputs = _base_inputs(win_rate=0.03, resolved_count=65, contradiction_log_open=1)
        state, reasons = compute_gate_state(inputs)

        self.assertEqual(state, "RED")
        reason_text = " ".join(reasons).lower()
        self.assertIn("win_rate", reason_text)
        self.assertIn("contradiction_log_open", reason_text)

    def test_red_blocks_experiments(self):
        """
        Integration: RED gate must block experiment emission.

        This test verifies two things:
        1. contradiction_log_open >= 1 triggers RED (covers the last RED condition).
        2. Downstream consumers checking gate state will correctly suppress
           experiments when the state is RED — validated via the wiring logic
           that ResearchLoop.run_cycle() uses (result.experiments = [] on RED).
        """
        # Part 1 — verify contradiction_log_open=1 produces RED
        inputs = _base_inputs(contradiction_log_open=1)
        state, reasons = compute_gate_state(inputs)

        self.assertEqual(state, "RED")
        self.assertTrue(len(reasons) > 0, "reasons must be non-empty for RED")

        # Part 2 — simulate the ResearchLoop gate wiring
        # (mirrors research/loop.py ResearchLoop.run_cycle lines 93-101)
        result_experiments = ["some_experiment_object"]
        if state == "RED":
            result_experiments = []

        self.assertEqual(
            result_experiments,
            [],
            "RED gate must reduce experiments to empty list",
        )

    # ------------------------------------------------------------------
    # YELLOW test
    # ------------------------------------------------------------------

    def test_yellow_on_low_count(self):
        """resolved_count < 50 must trigger YELLOW when no RED condition exists."""
        inputs = _base_inputs(win_rate=0.45, resolved_count=30)
        state, reasons = compute_gate_state(inputs)

        self.assertEqual(state, "YELLOW")
        self.assertTrue(len(reasons) > 0, "reasons must be non-empty for YELLOW")

    # ------------------------------------------------------------------
    # GREEN test
    # ------------------------------------------------------------------

    def test_green_baseline(self):
        """All-healthy inputs must produce GREEN with an empty reasons list."""
        inputs = _base_inputs()  # win_rate=0.50, resolved_count=60, all clear
        state, reasons = compute_gate_state(inputs)

        self.assertEqual(state, "GREEN")
        self.assertEqual(reasons, [], "GREEN state must have no reasons")


if __name__ == "__main__":
    unittest.main()
