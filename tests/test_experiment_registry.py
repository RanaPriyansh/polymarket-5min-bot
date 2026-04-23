import json
import tempfile
import unittest
from pathlib import Path

from research.experiment_registry import (
    FamilyScore,
    build_family_scoreboard,
    decide_promotion_state,
    write_family_scoreboard,
)


class ExperimentRegistryTests(unittest.TestCase):
    def test_threshold_logic_promotes_enough_good_evidence(self):
        state = decide_promotion_state(settled_trades=30, win_rate=0.45, pnl_per_trade=0.05, configured_state="candidate")
        self.assertEqual(state, "active")

    def test_kill_threshold_demotes_bad_evidence(self):
        state = decide_promotion_state(settled_trades=20, win_rate=0.30, pnl_per_trade=-0.21, configured_state="active")
        self.assertEqual(state, "demoted")

    def test_no_settled_trades_stays_candidate(self):
        state = decide_promotion_state(settled_trades=0, win_rate=0.0, pnl_per_trade=0.0, configured_state="candidate")
        self.assertEqual(state, "candidate")

    def test_json_artifact_serializes_and_round_trips(self):
        rows = [
            FamilyScore(
                family="toxicity_mm",
                settled_trades=31,
                realized_pnl=3.1,
                pnl_per_trade=0.1,
                win_rate=0.55,
                fragmentation=1,
                promotion_state="active",
                last_evidence_ts=123.0,
            )
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_family_scoreboard(Path(tmpdir), rows)
            payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload[0]["family"], "toxicity_mm")
        self.assertEqual(payload[0]["promotion_state"], "active")
        self.assertEqual(payload[0]["promote_if"]["settled_trades_>="], 30)

    def test_build_scoreboard_from_settled_events(self):
        events = [
            {"event_type": "slot_settled", "ts": 10.0, "payload": {"strategy_family": "time_decay", "realized_pnl": 0.2, "is_win": True}},
            {"event_type": "slot_settled", "ts": 11.0, "payload": {"strategy_family": "time_decay", "realized_pnl": -0.1, "is_win": False}},
            {"event_type": "slot_settled", "ts": 12.0, "payload": {"strategy_family": "toxicity_mm", "realized_pnl": 1.0, "is_win": True}},
        ]
        rows = build_family_scoreboard(events, active_families=["toxicity_mm"], candidate_families=["time_decay", "spot_momentum"])
        by_family = {row.family: row for row in rows}

        self.assertEqual(by_family["time_decay"].settled_trades, 2)
        self.assertAlmostEqual(by_family["time_decay"].realized_pnl, 0.1)
        self.assertAlmostEqual(by_family["time_decay"].win_rate, 0.5)
        self.assertEqual(by_family["spot_momentum"].settled_trades, 0)
        self.assertEqual(by_family["spot_momentum"].promotion_state, "candidate")


if __name__ == "__main__":
    unittest.main()
