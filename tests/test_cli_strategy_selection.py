import unittest
from unittest.mock import patch

from click.testing import CliRunner

import cli as cli_module
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

    def test_candidate_trial_requires_bounded_paper_run(self):
        runner = CliRunner()
        cfg = {
            "strategies": {
                "active": [],
                "states": {"time_decay": "disabled"},
                "candidates": ["time_decay"],
            }
        }
        with patch.object(cli_module, "load_cfg", return_value=cfg), patch.object(
            cli_module, "validate_active_strategy_states", return_value=["time_decay:disabled"]
        ):
            result = runner.invoke(
                cli_module.cli,
                ["run", "--mode", "paper", "--strategies", "time_decay", "--allow-candidate-trial"],
            )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("only valid for bounded paper runs", result.output)

    def test_candidate_trial_requires_experiment_runtime_dir(self):
        runner = CliRunner()
        cfg = {
            "strategies": {
                "active": [],
                "states": {"time_decay": "disabled"},
                "candidates": ["time_decay"],
            }
        }
        with patch.object(cli_module, "load_cfg", return_value=cfg), patch.object(
            cli_module, "validate_active_strategy_states", return_value=["time_decay:disabled"]
        ):
            result = runner.invoke(
                cli_module.cli,
                [
                    "run",
                    "--mode",
                    "paper",
                    "--strategies",
                    "time_decay",
                    "--max-loops",
                    "1",
                    "--runtime-dir",
                    "data/runtime",
                    "--allow-candidate-trial",
                ],
            )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("requires an isolated runtime under data/experiments", result.output)

    def test_candidate_trial_rejects_non_candidate_state_violations(self):
        runner = CliRunner()
        cfg = {
            "strategies": {
                "active": [],
                "states": {"opening_range": "disabled"},
                "candidates": [],
            }
        }
        with patch.object(cli_module, "load_cfg", return_value=cfg), patch.object(
            cli_module, "validate_active_strategy_states", return_value=["opening_range:disabled"]
        ):
            result = runner.invoke(
                cli_module.cli,
                [
                    "run",
                    "--mode",
                    "paper",
                    "--strategies",
                    "opening_range",
                    "--max-loops",
                    "1",
                    "--runtime-dir",
                    "data/experiments/test-candidate/trials/opening_range",
                    "--allow-candidate-trial",
                ],
            )
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("Refusing candidate trial for non-candidate strategies", result.output)


if __name__ == "__main__":
    unittest.main()
