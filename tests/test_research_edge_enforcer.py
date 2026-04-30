import json
import tempfile
import unittest
from pathlib import Path

import yaml

from research.edge_enforcer import apply_edge_decisions, build_edge_decisions, enforce_research_edges
from runtime_telemetry import RuntimeTelemetry


def _base_cfg(**edge_overrides):
    edge = {"enabled": True, "operator_approved_paper_auto_activation": True}
    edge.update(edge_overrides)
    return {
        "risk": {"circuit_breaker_dd": 0.03, "max_daily_loss": 0.01},
        "execution": {"paper_starting_bankroll": 500, "live_order_approval": False},
        "polymarket": {"clob_environment": "read_only"},
        "strategies": {
            "active": [],
            "candidates": ["time_decay"],
            "states": {"time_decay": "candidate_only"},
        },
        "research": {"edge_enforcement": edge},
    }


class ResearchEdgeEnforcerTests(unittest.TestCase):
    def test_promotes_only_with_settled_positive_evidence_and_keeps_risk_unchanged(self):
        cfg = _base_cfg()
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

    def test_gate_blocks_promotion_but_not_demotion(self):
        cfg = {
            "strategies": {
                "active": ["toxicity_mm"],
                "candidates": ["time_decay"],
                "states": {"toxicity_mm": "paper_active", "time_decay": "candidate_only"},
            },
            "research": {"edge_enforcement": {"enabled": True, "require_gate_green": True}},
        }
        decisions = build_edge_decisions(
            config=cfg,
            scoreboard_rows=[
                {"family": "time_decay", "settled_trades": 50, "win_rate": 0.6, "pnl_per_trade": 0.1},
                {"family": "toxicity_mm", "settled_trades": 30, "win_rate": 0.2, "pnl_per_trade": -0.3},
            ],
            gate_state="YELLOW",
            gate_reasons=["not enough evidence"],
        )
        self.assertEqual([(d.family, d.action) for d in decisions], [("toxicity_mm", "demote"), ("__gate__", "block")])
        updated, _ = apply_edge_decisions(cfg, decisions)
        self.assertEqual(updated["strategies"]["active"], [])
        self.assertEqual(updated["strategies"]["states"]["toxicity_mm"], "disabled")

    def test_operator_approval_blocks_promotion_but_not_demotion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runtime_dir = root / "runtime"
            artifact_dir = root / "research"
            runtime = RuntimeTelemetry(runtime_dir)
            runtime.update_status(run_id="paper-test", phase="running", mode="paper", loop_count=1, resolved_trade_count=100, win_rate=0.5)
            cfg = _base_cfg(operator_approved_paper_auto_activation=False, require_gate_green=False)
            cfg["strategies"]["active"] = ["toxicity_mm"]
            cfg["strategies"]["states"]["toxicity_mm"] = "paper_active"
            cfg["strategies"]["candidates"] = ["time_decay"]
            config_path = root / "config.yaml"
            config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
            artifact_dir.mkdir()
            (artifact_dir / "family_scoreboard.json").write_text(
                json.dumps([
                    {"family": "toxicity_mm", "settled_trades": 30, "win_rate": 0.2, "pnl_per_trade": -0.3},
                    {"family": "time_decay", "settled_trades": 40, "win_rate": 0.7, "pnl_per_trade": 0.1},
                ]),
                encoding="utf-8",
            )
            payload = enforce_research_edges(config_path=config_path, runtime_dir=runtime_dir, artifact_dir=artifact_dir)
            updated = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            self.assertEqual(updated["strategies"]["active"], [])
            self.assertEqual(updated["strategies"]["states"]["toxicity_mm"], "disabled")
            self.assertEqual(updated["strategies"]["states"]["time_decay"], "candidate_only")
            self.assertEqual(payload["apply_blocked_reason"], "operator_approval_missing")
            self.assertTrue(any(d["family"] == "toxicity_mm" and d["applied"] for d in payload["applied_decisions"]))

    def test_scanner_only_candidate_is_not_promoted(self):
        cfg = _base_cfg()
        cfg["strategies"]["candidates"] = ["terminal_fair_value"]
        cfg["strategies"]["states"] = {"terminal_fair_value": "scanner_only"}
        decisions = build_edge_decisions(
            config=cfg,
            scoreboard_rows=[{"family": "terminal_fair_value", "settled_trades": 100, "win_rate": 0.9, "pnl_per_trade": 1.0}],
            gate_state="GREEN",
        )
        self.assertEqual(decisions, [])

    def test_candidate_evidence_gap_includes_missing_scoreboard_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runtime_dir = root / "runtime"
            artifact_dir = root / "research"
            runtime = RuntimeTelemetry(runtime_dir)
            runtime.update_status(run_id="paper-test", phase="running", mode="paper", loop_count=1, resolved_trade_count=100, win_rate=0.5)
            cfg = _base_cfg()
            cfg["strategies"]["candidates"] = ["time_decay", "opening_range"]
            cfg["strategies"]["states"]["opening_range"] = "candidate_only"
            config_path = root / "config.yaml"
            config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
            artifact_dir.mkdir()
            (artifact_dir / "family_scoreboard.json").write_text(
                json.dumps([{"family": "time_decay", "settled_trades": 31, "win_rate": 0.5, "pnl_per_trade": 0.1}]),
                encoding="utf-8",
            )
            payload = enforce_research_edges(config_path=config_path, runtime_dir=runtime_dir, artifact_dir=artifact_dir, apply=False)
            self.assertEqual(payload["candidate_evidence_gap"], ["opening_range"])
            self.assertEqual(payload["next_experiment"], "run_bakeoff")

    def test_enforce_research_edges_writes_parseable_config_and_artifact_atomically(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            runtime_dir = root / "runtime"
            artifact_dir = root / "research"
            runtime = RuntimeTelemetry(runtime_dir)
            runtime.update_status(run_id="paper-test", phase="running", mode="paper", loop_count=1, resolved_trade_count=100, win_rate=0.5)
            cfg = _base_cfg(require_gate_green=False)
            config_path = root / "config.yaml"
            config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
            artifact_dir.mkdir()
            (artifact_dir / "family_scoreboard.json").write_text(
                json.dumps([{"family": "time_decay", "settled_trades": 31, "win_rate": 0.5, "pnl_per_trade": 0.1}]),
                encoding="utf-8",
            )
            payload = enforce_research_edges(config_path=config_path, runtime_dir=runtime_dir, artifact_dir=artifact_dir)
            self.assertEqual(yaml.safe_load(config_path.read_text())["strategies"]["active"], ["time_decay"])
            artifact = json.loads((artifact_dir / "edge_enforcement_latest.json").read_text(encoding="utf-8"))
            self.assertEqual(artifact["decision_hash"], payload["decision_hash"])
            self.assertFalse(list(root.glob(".config.yaml.*.tmp")))


if __name__ == "__main__":
    unittest.main()
