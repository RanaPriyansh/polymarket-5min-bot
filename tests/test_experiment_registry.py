import json
import tempfile
import unittest
from pathlib import Path

from research.experiment_registry import (
    BucketScore,
    FamilyScore,
    build_bucket_scoreboard,
    build_family_scoreboard,
    decide_promotion_state,
    write_bucket_scoreboard,
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
    def test_build_scoreboard_infers_family_by_joining_settlements_to_fills(self):
        events = [
            {"event_type": "order.filled", "ts": 1.0, "payload": {"strategy_family": "toxicity_mm", "market_id": "m1", "outcome": "Down"}},
            {"event_type": "slot_settled", "ts": 2.0, "payload": {"market_id": "m1", "position_outcome": "Down", "realized_pnl": -0.25, "is_win": False}},
        ]
        rows = build_family_scoreboard(events, active_families=["toxicity_mm"], candidate_families=[])
        by_family = {row.family: row for row in rows}
        self.assertEqual(by_family["toxicity_mm"].settled_trades, 1)
        self.assertAlmostEqual(by_family["toxicity_mm"].realized_pnl, -0.25)

    def test_build_bucket_scoreboard_joins_settlements_to_fill_metadata_and_pauses_negative_bucket(self):
        events = []
        for idx in range(20):
            market_id = f"m{idx}"
            events.append({
                "event_type": "fill_applied",
                "ts": float(idx),
                "payload": {
                    "strategy_family": "toxicity_mm",
                    "market_id": market_id,
                    "outcome": "Up",
                    "slot_id": "btc:5:1777000000",
                    "tte_bucket": "120-300s",
                },
            })
            events.append({
                "event_type": "slot_settled",
                "ts": float(idx + 100),
                "payload": {
                    "market_id": market_id,
                    "position_outcome": "Up",
                    "realized_pnl": -0.01,
                    "is_win": False,
                },
            })

        rows = build_bucket_scoreboard(events, min_settled_trades=20)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.family, "toxicity_mm")
        self.assertEqual(row.asset, "btc")
        self.assertEqual(row.interval, "5")
        self.assertEqual(row.tte_bucket, "120-300s")
        self.assertEqual(row.settled_trades, 20)
        self.assertTrue(row.pause)
        self.assertTrue(row.pause_reason.startswith("negative_pnl_per_trade"))

    def test_write_bucket_scoreboard_serializes_pause_rows(self):
        rows = [
            BucketScore(
                family="toxicity_mm",
                asset="btc",
                interval="5",
                tte_bucket="120-300s",
                settled_trades=20,
                realized_pnl=-1.0,
                pnl_per_trade=-0.05,
                win_rate=0.4,
                pause=True,
                pause_reason="negative_pnl_per_trade<-0.050000>",
                last_evidence_ts=123.0,
            )
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_bucket_scoreboard(Path(tmpdir), rows)
            payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload[0]["asset"], "btc")
        self.assertTrue(payload[0]["pause"])


if __name__ == "__main__":
    unittest.main()
