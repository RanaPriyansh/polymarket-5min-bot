import unittest

from strategy_state import validate_active_strategy_states


class StrategyStateTests(unittest.TestCase):
    def test_scanner_candidate_disabled_cannot_be_active_without_approval(self):
        cfg = {
            "strategies": {
                "states": {
                    "terminal_fair_value": "scanner_only",
                    "time_decay": "disabled",
                    "opening_range": "candidate_only",
                    "toxicity_mm": "paper_active",
                }
            }
        }
        violations = validate_active_strategy_states(
            cfg,
            ["terminal_fair_value", "time_decay", "opening_range", "toxicity_mm"],
        )
        self.assertEqual(
            violations,
            ["terminal_fair_value:scanner_only", "time_decay:disabled", "opening_range:candidate_only"],
        )

    def test_legacy_configs_without_states_remain_paper_active(self):
        self.assertEqual(validate_active_strategy_states({"strategies": {}}, ["toxicity_mm"]), [])


if __name__ == "__main__":
    unittest.main()
