import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from click.testing import CliRunner

import cli as cli_module
from cli import cli
from ledger import LedgerEvent, SQLiteLedger
from runtime_telemetry import RuntimeTelemetry
from strategy_bakeoff import (
    TrialOutcome,
    build_trial_command,
    build_trial_runtime_dir,
    collect_trial_outcome,
    load_bakeoff_spec,
    rank_trials,
    render_bakeoff_report,
    score_trial,
    write_bakeoff_artifacts,
)


class StrategyBakeoffTests(unittest.TestCase):
    def test_summary_ranking_prefers_settled_then_resolved_then_realized_then_mtm_then_fills_then_cleaner_risk(self):
        outcomes = [
            TrialOutcome(
                family="time_decay",
                label="candidate",
                settled_count=0,
                resolved_count=50,
                realized_pnl=9.0,
                mark_to_market_pnl=8.5,
                fill_count=20,
                toxic_skip_count=0,
                max_drawdown=0.20,
            ),
            TrialOutcome(
                family="opening_range",
                label="candidate",
                settled_count=2,
                resolved_count=2,
                realized_pnl=8.0,
                mark_to_market_pnl=8.0,
                fill_count=10,
                toxic_skip_count=0,
                max_drawdown=0.05,
            ),
            TrialOutcome(
                family="mean_reversion_5min",
                label="candidate",
                settled_count=2,
                resolved_count=5,
                realized_pnl=1.0,
                mark_to_market_pnl=2.0,
                fill_count=1,
                toxic_skip_count=10,
                max_drawdown=0.30,
            ),
            TrialOutcome(
                family="toxicity_mm",
                label="baseline",
                settled_count=2,
                resolved_count=5,
                realized_pnl=1.0,
                mark_to_market_pnl=2.0,
                fill_count=3,
                toxic_skip_count=5,
                max_drawdown=0.10,
            ),
        ]

        ranked = rank_trials(outcomes)

        self.assertEqual(
            [outcome.family for outcome in ranked],
            ["toxicity_mm", "mean_reversion_5min", "opening_range", "time_decay"],
        )
        self.assertGreater(score_trial(ranked[0]), score_trial(ranked[-1]))

    def test_summary_ranking_does_not_use_family_name_as_tiebreaker(self):
        outcomes = [
            TrialOutcome(
                family="zeta_strategy",
                label="candidate",
                settled_count=1,
                resolved_count=2,
                realized_pnl=3.0,
                fill_count=4,
                toxic_skip_count=1,
            ),
            TrialOutcome(
                family="alpha_strategy",
                label="baseline",
                settled_count=1,
                resolved_count=2,
                realized_pnl=3.0,
                fill_count=4,
                toxic_skip_count=1,
            ),
        ]

        ranked = rank_trials(outcomes)

        self.assertEqual(
            [outcome.family for outcome in ranked],
            ["zeta_strategy", "alpha_strategy"],
        )

    def test_render_bakeoff_report_keeps_baseline_when_candidate_lacks_settled_edge_and_risk_is_worse(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")
        outcomes = [
            TrialOutcome(
                family="toxicity_mm",
                label="baseline",
                settled_count=0,
                resolved_count=3,
                realized_pnl=1.0,
                mark_to_market_pnl=1.2,
                fill_count=3,
                toxic_skip_count=1,
                max_drawdown=0.02,
            ),
            TrialOutcome(
                family="opening_range",
                label="candidate",
                settled_count=0,
                resolved_count=10,
                realized_pnl=1.1,
                mark_to_market_pnl=1.3,
                fill_count=4,
                toxic_skip_count=6,
                max_drawdown=0.25,
            ),
        ]

        report = render_bakeoff_report(spec, outcomes)

        self.assertIn("Winner: opening_range", report)
        self.assertIn("Promotion recommendation: keep baseline toxicity_mm", report)

    def test_render_bakeoff_report_promotes_no_settlement_candidate_only_with_stronger_fills_pnl_and_risk(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")
        outcomes = [
            TrialOutcome(
                family="toxicity_mm",
                label="baseline",
                settled_count=0,
                resolved_count=4,
                realized_pnl=1.0,
                mark_to_market_pnl=1.1,
                fill_count=3,
                toxic_skip_count=1,
                max_drawdown=0.05,
            ),
            TrialOutcome(
                family="opening_range",
                label="candidate",
                settled_count=0,
                resolved_count=2,
                realized_pnl=2.0,
                mark_to_market_pnl=2.4,
                fill_count=8,
                toxic_skip_count=0,
                max_drawdown=0.01,
            ),
        ]

        report = render_bakeoff_report(spec, outcomes)

        self.assertIn("Winner: toxicity_mm", report)
        self.assertIn("Promotion recommendation: promote opening_range over baseline toxicity_mm", report)

    def test_render_bakeoff_report_keeps_baseline_when_settled_advantage_has_no_fills_or_pnl(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")
        outcomes = [
            TrialOutcome(
                family="toxicity_mm",
                label="baseline",
                settled_count=1,
                resolved_count=1,
                realized_pnl=0.2,
                mark_to_market_pnl=0.2,
                fill_count=1,
                toxic_skip_count=0,
                max_drawdown=0.01,
            ),
            TrialOutcome(
                family="opening_range",
                label="candidate",
                settled_count=2,
                resolved_count=2,
                realized_pnl=0.0,
                mark_to_market_pnl=0.0,
                fill_count=0,
                toxic_skip_count=0,
                max_drawdown=0.0,
            ),
        ]

        report = render_bakeoff_report(spec, outcomes)

        self.assertIn("Winner: opening_range", report)
        self.assertIn("Promotion recommendation: keep baseline toxicity_mm", report)

    def test_write_bakeoff_artifacts_keeps_summary_fields_when_zero_fill_candidate_is_not_promoted(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")
        outcomes = [
            TrialOutcome(
                family="toxicity_mm",
                label="baseline",
                settled_count=1,
                resolved_count=1,
                realized_pnl=0.2,
                mark_to_market_pnl=0.2,
                fill_count=1,
                toxic_skip_count=0,
                max_drawdown=0.01,
            ),
            TrialOutcome(
                family="opening_range",
                label="candidate",
                settled_count=2,
                resolved_count=2,
                realized_pnl=0.0,
                mark_to_market_pnl=0.0,
                fill_count=0,
                toxic_skip_count=0,
                max_drawdown=0.0,
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            artifact_paths = write_bakeoff_artifacts(tmpdir, spec, outcomes)
            payload = json.loads(Path(artifact_paths["summary_json"]).read_text(encoding="utf-8"))

        self.assertEqual(payload["promotion_recommendation"], "keep_baseline")
        self.assertIsNone(payload["promoted_family"])
        self.assertEqual(payload["baseline_family"], "toxicity_mm")
        self.assertEqual(payload["winner"]["family"], "opening_range")
        self.assertIn("Promotion recommendation: keep baseline toxicity_mm", payload["report"])

    def test_load_bakeoff_spec_reads_defaults_and_trials(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")

        self.assertEqual(spec.experiment_id, "multi-family-bakeoff")
        self.assertEqual(spec.max_loops, 20)
        self.assertEqual(spec.sleep_seconds, 5)
        self.assertEqual(
            spec.runtime_root,
            "data/experiments/multi-family-bakeoff",
        )
        self.assertEqual(len(spec.trials), 4)
        self.assertEqual([trial.family for trial in spec.trials], [
            "toxicity_mm",
            "mean_reversion_5min",
            "opening_range",
            "time_decay",
        ])
        self.assertEqual(len({trial.family for trial in spec.trials}), len(spec.trials))
        self.assertEqual(spec.trials[0].label, "baseline")
        self.assertEqual([trial.label for trial in spec.trials[1:]], [
            "mean_reversion_5min",
            "opening_range",
            "time_decay",
        ])
        self.assertEqual(len({trial.label for trial in spec.trials}), len(spec.trials))

    def test_load_bakeoff_spec_requires_trial_strategies(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "missing-strategy.yaml"
            config_path.write_text(
                """
experiment_id: bad-bakeoff
trials:
  - family: toxicity_mm
    label: baseline
  - label: candidate
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "trial family.*index 1.*non-blank"):
                load_bakeoff_spec(config_path)

    def test_build_trial_runtime_dir_nests_trial_family_under_runtime_root(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")
        runtime_dir = build_trial_runtime_dir(spec, spec.trials[2])
        self.assertEqual(
            runtime_dir,
            str((Path("data/experiments/multi-family-bakeoff") / "trials" / "opening_range").resolve()),
        )

    def test_build_trial_command_uses_isolated_runtime_dir_and_bounded_run_flags(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")
        trial = spec.trials[2]
        runtime_dir = build_trial_runtime_dir(spec, trial)

        command = build_trial_command(
            python_bin=".venv/bin/python",
            trial=trial,
            runtime_dir=runtime_dir,
            max_loops=spec.max_loops,
            sleep_seconds=spec.sleep_seconds,
        )

        self.assertEqual(
            command,
            [
                ".venv/bin/python",
                str(cli_module.CLI_SCRIPT_PATH),
                "run",
                "--mode",
                "paper",
                "--runtime-dir",
                str((Path("data/experiments/multi-family-bakeoff") / "trials" / "opening_range").resolve()),
                "--strategies",
                "opening_range",
                "--max-loops",
                "20",
                "--sleep-seconds",
                "5",
            ],
        )

    def test_load_bakeoff_spec_requires_mapping_payload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "not-a-mapping.yaml"
            config_path.write_text("- family: toxicity_mm\n", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "top-level YAML.*mapping"):
                load_bakeoff_spec(config_path)

    def test_load_bakeoff_spec_rejects_blank_required_top_level_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "blank-top-level-fields.yaml"
            config_path.write_text(
                """
experiment_id: "  "
runtime_root: "   "
trials:
  - family: toxicity_mm
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "experiment_id.*non-blank"):
                load_bakeoff_spec(config_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "blank-runtime-root.yaml"
            config_path.write_text(
                """
experiment_id: okay
runtime_root: "   "
trials:
  - family: toxicity_mm
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "runtime_root.*non-blank"):
                load_bakeoff_spec(config_path)

    def test_load_bakeoff_spec_rejects_invalid_loop_settings(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "invalid-loop-settings.yaml"
            config_path.write_text(
                """
experiment_id: bad-loops
runtime_root: data/experiments
max_loops: 0
sleep_seconds: -1
trials:
  - family: toxicity_mm
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "max_loops.*greater than 0"):
                load_bakeoff_spec(config_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "invalid-sleep-seconds.yaml"
            config_path.write_text(
                """
experiment_id: bad-sleep
runtime_root: data/experiments
max_loops: 1
sleep_seconds: -1
trials:
  - family: toxicity_mm
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "sleep_seconds.*greater than or equal to 0"):
                load_bakeoff_spec(config_path)

    def test_load_bakeoff_spec_rejects_invalid_trials_payload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "trials-not-a-list.yaml"
            config_path.write_text(
                """
experiment_id: bad-trials
runtime_root: data/experiments
trials:
  family: toxicity_mm
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "trials.*list"):
                load_bakeoff_spec(config_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "trial-not-a-mapping.yaml"
            config_path.write_text(
                """
experiment_id: bad-trial
runtime_root: data/experiments
trials:
  - toxicity_mm
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "trial.*index 0.*mapping"):
                load_bakeoff_spec(config_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "blank-family.yaml"
            config_path.write_text(
                """
experiment_id: blank-family
runtime_root: data/experiments
trials:
  - family: "   "
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "family.*index 0.*non-blank"):
                load_bakeoff_spec(config_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "path-traversal-family.yaml"
            config_path.write_text(
                """
experiment_id: bad-family
runtime_root: data/experiments
trials:
  - family: ../escape
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, r"family.*match \^\[a-z0-9_\]\+\$"):
                load_bakeoff_spec(config_path)

    def test_load_bakeoff_spec_requires_non_empty_trials_and_single_baseline_for_multi_trial_specs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "empty-trials.yaml"
            config_path.write_text(
                """
experiment_id: empty-trials
runtime_root: data/experiments
trials: []
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "at least one trial"):
                load_bakeoff_spec(config_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "missing-baseline.yaml"
            config_path.write_text(
                """
experiment_id: missing-baseline
runtime_root: data/experiments
trials:
  - family: toxicity_mm
    label: candidate_a
  - family: opening_range
    label: candidate_b
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "exactly one baseline"):
                load_bakeoff_spec(config_path)

    def test_load_bakeoff_spec_allows_single_trial_without_baseline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "single-trial.yaml"
            config_path.write_text(
                """
experiment_id: single-trial
runtime_root: data/experiments
trials:
  - family: opening_range
    label: opening_range
""".strip()
                + "\n",
                encoding="utf-8",
            )

            spec = load_bakeoff_spec(config_path)

        self.assertEqual(len(spec.trials), 1)
        self.assertEqual(spec.trials[0].label, "opening_range")

    def test_load_bakeoff_spec_rejects_duplicate_families_and_labels(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            duplicate_family_path = Path(tmpdir) / "duplicate-family.yaml"
            duplicate_family_path.write_text(
                """
experiment_id: duplicate-family
runtime_root: data/experiments
trials:
  - family: toxicity_mm
    label: baseline
  - family: toxicity_mm
    label: candidate
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "Duplicate trial family.*toxicity_mm"):
                load_bakeoff_spec(duplicate_family_path)

        with tempfile.TemporaryDirectory() as tmpdir:
            duplicate_label_path = Path(tmpdir) / "duplicate-label.yaml"
            duplicate_label_path.write_text(
                """
experiment_id: duplicate-label
runtime_root: data/experiments
trials:
  - family: toxicity_mm
    label: baseline
  - family: mean_reversion_5min
    label: baseline
""".strip()
                + "\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "Duplicate trial label.*baseline"):
                load_bakeoff_spec(duplicate_label_path)

    def test_render_bakeoff_report_includes_winner_recommendation_and_trial_stats(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")
        outcomes = [
            TrialOutcome(
                family="toxicity_mm",
                label="baseline",
                settled_count=1,
                resolved_count=2,
                realized_pnl=1.0,
                mark_to_market_pnl=0.8,
                fill_count=3,
                toxic_skip_count=4,
                max_drawdown=0.12,
            ),
            TrialOutcome(
                family="mean_reversion_5min",
                label="candidate",
                settled_count=3,
                resolved_count=7,
                realized_pnl=2.5,
                mark_to_market_pnl=2.8,
                fill_count=5,
                toxic_skip_count=1,
                max_drawdown=0.04,
            ),
            TrialOutcome(
                family="opening_range",
                label="candidate",
                settled_count=0,
                resolved_count=9,
                realized_pnl=5.0,
                mark_to_market_pnl=5.5,
                fill_count=8,
                toxic_skip_count=0,
                max_drawdown=0.08,
            ),
            TrialOutcome(
                family="time_decay",
                label="candidate",
                settled_count=1,
                resolved_count=1,
                realized_pnl=-0.5,
                mark_to_market_pnl=-0.4,
                fill_count=1,
                toxic_skip_count=2,
                max_drawdown=0.15,
            ),
        ]

        report = render_bakeoff_report(spec, outcomes)

        self.assertIn("Experiment: multi-family-bakeoff", report)
        self.assertIn("Winner: mean_reversion_5min", report)
        self.assertIn("Promotion recommendation: promote mean_reversion_5min over baseline toxicity_mm", report)
        self.assertIn(
            "toxicity_mm [baseline]: settled=1 resolved=2 realized_pnl=1.0000 mtm_pnl=0.8000 fills=3 toxic_skips=4 max_drawdown=0.1200",
            report,
        )
        self.assertIn(
            "mean_reversion_5min [candidate]: settled=3 resolved=7 realized_pnl=2.5000 mtm_pnl=2.8000 fills=5 toxic_skips=1 max_drawdown=0.0400",
            report,
        )

    def test_write_bakeoff_artifacts_records_machine_readable_promotion_fields(self):
        spec = load_bakeoff_spec("configs/strategy-bakeoff.yaml")
        outcomes = [
            TrialOutcome(
                family="toxicity_mm",
                label="baseline",
                settled_count=1,
                resolved_count=2,
                realized_pnl=1.0,
                mark_to_market_pnl=0.8,
                fill_count=3,
                toxic_skip_count=4,
                max_drawdown=0.12,
            ),
            TrialOutcome(
                family="mean_reversion_5min",
                label="candidate",
                settled_count=3,
                resolved_count=7,
                realized_pnl=2.5,
                mark_to_market_pnl=2.8,
                fill_count=5,
                toxic_skip_count=1,
                max_drawdown=0.04,
            ),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts = write_bakeoff_artifacts(tmpdir, spec, outcomes)
            payload = json.loads(Path(artifacts["summary_json"]).read_text(encoding="utf-8"))

        self.assertEqual(payload["promotion_recommendation"], "promote")
        self.assertEqual(payload["promoted_family"], "mean_reversion_5min")
        self.assertEqual(payload["baseline_family"], "toxicity_mm")
        self.assertEqual(payload["winner"]["family"], "mean_reversion_5min")

    def test_collect_trial_outcome_reads_runtime_artifacts_and_baseline_evidence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime = RuntimeTelemetry(tmpdir)
            runtime.update_status(
                run_id="run-bakeoff",
                phase="running",
                mode="paper",
                baseline_strategy="toxicity_mm",
                research_candidates=["opening_range"],
                loop_count=4,
                fetched_markets=7,
                processed_markets=5,
                resolved_trade_count=3,
                pending_resolution_slots=[],
                risk={
                    "capital": 510.0,
                    "realized_pnl_total": 2.25,
                    "unrealized_pnl_total": -0.5,
                    "mark_to_market_capital": 509.5,
                    "max_drawdown": 0.03,
                },
            )
            runtime.write_strategy_metrics(
                {
                    "opening_range": {
                        "orders_filled": 6,
                        "toxic_book_skips": 2,
                        "realized_pnl": 2.25,
                    }
                }
            )
            runtime.append_event("order.filled", {"strategy_family": "opening_range"}, run_id="run-bakeoff")
            ledger = SQLiteLedger(Path(tmpdir) / "ledger.db")
            ledger.append_event(
                LedgerEvent(
                    event_id="settled-1",
                    stream="market_slot",
                    aggregate_id="slot-1",
                    sequence_num=1,
                    event_type="slot_settled",
                    event_ts=101.0,
                    recorded_ts=101.0,
                    run_id="run-bakeoff",
                    idempotency_key="settled-1",
                    causation_id=None,
                    correlation_id="slot-1",
                    schema_version=1,
                    payload={"market_id": "m-1"},
                )
            )

            outcome = collect_trial_outcome(tmpdir, "opening_range", label="candidate")

            self.assertEqual(outcome.family, "opening_range")
            self.assertEqual(outcome.label, "candidate")
            self.assertEqual(outcome.settled_count, 1)
            self.assertEqual(outcome.resolved_count, 3)
            self.assertEqual(outcome.realized_pnl, 2.25)
            self.assertEqual(outcome.fill_count, 1)
            self.assertEqual(outcome.toxic_skip_count, 2)
            self.assertEqual(outcome.mark_to_market_pnl, 1.75)
            self.assertEqual(outcome.max_drawdown, 0.03)
            self.assertEqual(outcome.evidence["current_run"]["slot_settled_count"], 1)
            self.assertEqual(outcome.evidence["strategy_metrics"]["orders_filled"], 6)

    def test_bakeoff_command_writes_summary_json_and_md(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            spec_path = Path(tmpdir) / "bakeoff.yaml"
            experiment_root = Path(tmpdir) / "experiments" / "task-2"
            spec_path.write_text(
                """
experiment_id: task-2
max_loops: 3
sleep_seconds: 0
runtime_root: {runtime_root}
trials:
  - family: toxicity_mm
    label: baseline
  - family: opening_range
    label: opening_range
""".strip().format(runtime_root=experiment_root.as_posix())
                + "\n",
                encoding="utf-8",
            )

            def fake_collect(runtime_dir, strategy_family, *, label=None):
                return TrialOutcome(
                    family=strategy_family,
                    label=label or ("baseline" if strategy_family == "toxicity_mm" else strategy_family),
                    settled_count=2 if strategy_family == "toxicity_mm" else 3,
                    resolved_count=2 if strategy_family == "toxicity_mm" else 4,
                    realized_pnl=1.0 if strategy_family == "toxicity_mm" else 1.5,
                    fill_count=2 if strategy_family == "toxicity_mm" else 5,
                    toxic_skip_count=1 if strategy_family == "toxicity_mm" else 0,
                    evidence={"runtime_dir": runtime_dir, "strategy_family": strategy_family},
                )

            with mock.patch("cli.subprocess.run") as run_mock, mock.patch(
                "cli.collect_trial_outcome",
                side_effect=fake_collect,
            ):
                run_mock.return_value = mock.Mock(returncode=0)
                result = runner.invoke(
                    cli,
                    [
                        "bakeoff",
                        "--spec-path",
                        str(spec_path),
                        "--python-bin",
                        ".venv/bin/python",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.output)
            summary_json_path = experiment_root / "summary.json"
            summary_md_path = experiment_root / "summary.md"
            self.assertTrue(summary_json_path.exists())
            self.assertTrue(summary_md_path.exists())
            payload = json.loads(summary_json_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["winner"]["family"], "opening_range")
            self.assertEqual(payload["promotion_recommendation"], "promote")
            self.assertEqual(payload["promoted_family"], "opening_range")
            self.assertEqual(payload["baseline_family"], "toxicity_mm")
            self.assertEqual(len(payload["ranked_outcomes"]), 2)
            self.assertIn("Promotion recommendation", summary_md_path.read_text(encoding="utf-8"))

            self.assertEqual(run_mock.call_count, 2)
            first_command = run_mock.call_args_list[0].args[0]
            self.assertEqual(
                first_command,
                [
                    ".venv/bin/python",
                    str(cli_module.CLI_SCRIPT_PATH),
                    "run",
                    "--mode",
                    "paper",
                    "--runtime-dir",
                    str((experiment_root / "trials" / "toxicity_mm").resolve()),
                    "--strategies",
                    "toxicity_mm",
                    "--max-loops",
                    "3",
                    "--sleep-seconds",
                    "0",
                ],
            )
            self.assertEqual(run_mock.call_args_list[0].kwargs["cwd"], str(cli_module.PROJECT_ROOT))
            self.assertTrue(run_mock.call_args_list[0].kwargs["capture_output"])
            self.assertTrue(run_mock.call_args_list[0].kwargs["text"])
    def test_bakeoff_command_dry_run_prints_commands_only(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            spec_path = Path(tmpdir) / "bakeoff.yaml"
            experiment_root = Path(tmpdir) / "experiments" / "dry-run"
            spec_path.write_text(
                """
experiment_id: dry-run
max_loops: 2
sleep_seconds: 0
runtime_root: {runtime_root}
trials:
  - family: toxicity_mm
    label: baseline
  - family: opening_range
    label: opening_range
""".strip().format(runtime_root=experiment_root.as_posix())
                + "\n",
                encoding="utf-8",
            )

            with mock.patch("cli.subprocess.run") as run_mock, mock.patch(
                "cli.collect_trial_outcome"
            ) as collect_mock, mock.patch("cli.write_bakeoff_artifacts") as write_mock:
                result = runner.invoke(
                    cli,
                    [
                        "bakeoff",
                        "--spec-path",
                        str(spec_path),
                        "--python-bin",
                        ".venv/bin/python",
                        "--dry-run",
                    ],
                )

            self.assertEqual(result.exit_code, 0, msg=result.output)
            self.assertIn("Bakeoff: dry-run (2 trials)", result.output)
            self.assertIn(str(cli_module.CLI_SCRIPT_PATH), result.output)
            self.assertNotIn("Summary:", result.output)
            self.assertFalse((experiment_root / "summary.json").exists())
            self.assertFalse(experiment_root.exists())
            run_mock.assert_not_called()
            collect_mock.assert_not_called()
            write_mock.assert_not_called()

    def test_bakeoff_command_surfaces_failed_subprocess_context(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            spec_path = Path(tmpdir) / "bakeoff.yaml"
            experiment_root = Path(tmpdir) / "experiments" / "failed-run"
            spec_path.write_text(
                """
experiment_id: failed-run
max_loops: 1
sleep_seconds: 0
runtime_root: {runtime_root}
trials:
  - family: toxicity_mm
    label: baseline
""".strip().format(runtime_root=experiment_root.as_posix())
                + "\n",
                encoding="utf-8",
            )

            with mock.patch("cli.subprocess.run") as run_mock:
                run_mock.return_value = mock.Mock(returncode=7, stdout="hello stdout\n", stderr="boom stderr\n")
                result = runner.invoke(
                    cli,
                    [
                        "bakeoff",
                        "--spec-path",
                        str(spec_path),
                        "--python-bin",
                        ".venv/bin/python",
                    ],
                )

            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("Trial failed for toxicity_mm", result.output)
            self.assertIn("returncode=7", result.output)
            self.assertIn("stdout=hello stdout", result.output)
            self.assertIn("stderr=boom stderr", result.output)
            self.assertIn(str(cli_module.CLI_SCRIPT_PATH), result.output)

    def test_bakeoff_command_surfaces_subprocess_launch_failure_cleanly(self):
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            spec_path = Path(tmpdir) / "bakeoff.yaml"
            experiment_root = Path(tmpdir) / "experiments" / "launch-failed"
            spec_path.write_text(
                """
experiment_id: launch-failed
max_loops: 1
sleep_seconds: 0
runtime_root: {runtime_root}
trials:
  - family: toxicity_mm
    label: baseline
""".strip().format(runtime_root=experiment_root.as_posix())
                + "\n",
                encoding="utf-8",
            )

            with mock.patch("cli.subprocess.run", side_effect=FileNotFoundError("No such file or directory: '.venv/bin/missing-python'")):
                result = runner.invoke(
                    cli,
                    [
                        "bakeoff",
                        "--spec-path",
                        str(spec_path),
                        "--python-bin",
                        ".venv/bin/missing-python",
                    ],
                )

            self.assertNotEqual(result.exit_code, 0)
            self.assertNotIn("Traceback", result.output)
            self.assertIn("Unable to launch trial subprocess for toxicity_mm", result.output)
            self.assertIn(".venv/bin/missing-python", result.output)
            self.assertIn(str(cli_module.CLI_SCRIPT_PATH), result.output)


if __name__ == "__main__":
    unittest.main()
