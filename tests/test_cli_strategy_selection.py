import unittest

from cli import _resolve_active_strategies


class CliStrategySelectionTests(unittest.TestCase):
    def test_uses_configured_strategies_when_flag_omitted(self):
        cfg = {
            "strategies": {
                "active": ["mean_reversion_5min", "toxicity_mm"],
            }
        }
        self.assertEqual(
            _resolve_active_strategies(cfg, None),
            ["mean_reversion_5min", "toxicity_mm"],
        )

    def test_cli_flag_overrides_config(self):
        cfg = {
            "strategies": {
                "active": ["mean_reversion_5min", "toxicity_mm"],
            }
        }
        self.assertEqual(
            _resolve_active_strategies(cfg, "toxicity_mm,opening_range"),
            ["toxicity_mm", "opening_range"],
        )

    def test_blank_flag_falls_back_to_config(self):
        cfg = {
            "strategies": {
                "active": ["toxicity_mm"],
            }
        }
        self.assertEqual(_resolve_active_strategies(cfg, "  "), ["toxicity_mm"])


if __name__ == "__main__":
    unittest.main()
