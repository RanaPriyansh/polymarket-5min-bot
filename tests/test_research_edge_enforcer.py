import unittest

from research.edge_enforcer import apply_edge_decisions, build_edge_decisions


class ResearchEdgeEnforcerTests(unittest.TestCase):
    def test_promotes_only_with_settled_positive_evidence_and_keeps_risk_unchanged(self):
        cfg = {
            "risk": {"circuit_breaker_dd": 0.03, "max_daily_loss": 0.01},
            "execution": {"paper_starting_bankroll": 500, "live_order_approval": False},
            "polymarket": {"clob_environment": "read_only"},
            "strategies": {
                "active": [],
                "candidates": ["time_decay"],
                "states": {"time_decay": "candidate_only"},
            },
            "research": {"edge_enforcement": {"enabled": True, "operator_approved_paper_auto_activation": True}},
        }
        rows = [
            {
                "family": "time_decay",
                "settled_trades": 31,
                "win_rate": 0.49,
                "pnl_per_trade": 0.061,
                "realized_pnl": 1.891,
            }
        ]
        decisions = build_edge_decisions(config=cfg, scoreboard_rows=rows, gate_state="GREEN")
        self.assertEqual([(d.family, d.action) for d in decisions], [("time_decay", "promote_to_paper_active")])
        updated, applied = apply_edge_decisions(cfg, decisions)
        self.assertEqual(updated["strategies"]["active"], ["time_decay"])
        self.assertEqual(updated["strategies"]["states"]["time_decay"], "paper_active")
        self.assertEqual(updated["risk"], cfg["risk"])
        self.assertTrue(applied[0].applied)

    def test_gate_blocks_promotion_and_demotes_bad_active_family(self):
        cfg = {
            "strategies": {
                "active": ["toxicity_mm"],
                "candidates": ["time_decay"],
                "states": {"toxicity_mm": "paper_active", "time_decay": "candidate_only"},
            },
            "research": {"edge_enforcement": {"enabled": True, "require_gate_green": True}},
        }
        blocked = build_edge_decisions(
            config=cfg,
            scoreboard_rows=[{"family": "time_decay", "settled_trades": 50, "win_rate": 0.6, "pnl_per_trade": 0.1}],
            gate_state="YELLOW",
            gate_reasons=["not enough evidence"],
        )
        self.assertEqual(blocked[0].action, "block")

        cfg["research"]["edge_enforcement"]["require_gate_green"] = False
        decisions = build_edge_decisions(
            config=cfg,
            scoreboard_rows=[{"family": "toxicity_mm", "settled_trades": 30, "win_rate": 0.2, "pnl_per_trade": -0.3}],
            gate_state="YELLOW",
        )
        self.assertEqual([(d.family, d.action) for d in decisions], [("toxicity_mm", "demote")])
        updated, _ = apply_edge_decisions(cfg, decisions)
        self.assertEqual(updated["strategies"]["active"], [])
        self.assertEqual(updated["strategies"]["states"]["toxicity_mm"], "disabled")


if __name__ == "__main__":
    unittest.main()
