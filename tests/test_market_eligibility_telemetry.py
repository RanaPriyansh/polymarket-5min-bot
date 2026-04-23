import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from runtime_telemetry import RuntimeTelemetry
from scripts.ops_status import render_status
from status_utils import render_status_text, runtime_status_payload


class MarketEligibilityTelemetryTests(unittest.TestCase):
    def test_aggregates_structural_governance_and_quoted_market_counts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(
                run_id="paper-eligibility",
                phase="running",
                mode="paper",
                fetched_markets=6,
            )
            for market_id in range(1, 7):
                telemetry.append_market_sample(
                    {
                        "market_id": f"m-{market_id}",
                        "market_slug": f"btc-{market_id}",
                        "slot_id": f"slot-{market_id}",
                    },
                    run_id="paper-eligibility",
                )
            telemetry.append_event(
                "market.runtime_baseline_untradeable",
                {"market_id": "m-1", "market_slug": "btc-1", "reasons": ["wide_spread>500", "thin_depth<10"]},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "market.runtime_baseline_untradeable",
                {"market_id": "m-2", "market_slug": "btc-2", "reasons": ["wide_spread>500"]},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "market.entry_blocked",
                {"market_id": "m-3", "market_slug": "btc-3", "strategy_family": "opening_range", "reasons": ["runtime_gate_red", "gate:win_rate<0.20"]},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "market.entry_blocked",
                {"market_id": "m-4", "market_slug": "btc-4", "strategy_family": "time_decay", "reasons": ["tte_lt_120s"]},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "quote.submitted",
                {"market_id": "m-5", "market_slug": "btc-5", "strategy_family": "toxicity_mm"},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "order.opened",
                {"market_id": "m-6", "market_slug": "btc-6", "strategy_family": "opening_range"},
                run_id="paper-eligibility",
            )

            payload = runtime_status_payload(tmpdir)
            eligibility = payload["market_eligibility"]

            self.assertEqual(eligibility["summary_scope"], "bounded_recent_run")
            self.assertEqual(eligibility["discovered_markets"], 6)
            self.assertEqual(eligibility["structurally_untradeable_markets"], 2)
            self.assertEqual(eligibility["governance_blocked_markets"], 2)
            self.assertEqual(eligibility["quoted_or_entered_markets"], 2)
            self.assertEqual(eligibility["top_structural_reasons"][0], ["wide_spread>500", 2])
            self.assertEqual(eligibility["top_governance_reasons"][0], ["runtime_gate_red", 1])

    def test_operator_status_text_includes_dominant_market_skip_and_block_reasons(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(
                run_id="paper-eligibility",
                phase="running",
                mode="paper",
                baseline_strategy="toxicity_mm",
                heartbeat_ts=180.0,
                fetched_markets=5,
                gate_state="RED",
                gate_reasons=["win_rate=0.090 < 0.20 with resolved_count=35 >= 20"],
                new_order_pause=False,
                pause_policy="family-aware",
                pause_scope="mixed_by_family",
                pause_reason="mixed_by_family",
                pause_family_decisions={
                    "opening_range": {"pause": True, "reason": "directional_low_win_rate_red_gate"},
                    "toxicity_mm": {"pause": False, "reason": "mm_exempt_low_win_rate_only"},
                },
            )
            for market_id in range(1, 6):
                telemetry.append_market_sample(
                    {
                        "market_id": f"m-{market_id}",
                        "market_slug": f"btc-{market_id}",
                        "slot_id": f"slot-{market_id}",
                    },
                    run_id="paper-eligibility",
                )
            telemetry.append_event(
                "market.runtime_baseline_untradeable",
                {"market_id": "m-1", "market_slug": "btc-1", "reasons": ["wide_spread>500"]},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "market.runtime_baseline_untradeable",
                {"market_id": "m-2", "market_slug": "btc-2", "reasons": ["wide_spread>500", "thin_depth<10"]},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "market.entry_blocked",
                {"market_id": "m-3", "market_slug": "btc-3", "strategy_family": "opening_range", "reasons": ["runtime_gate_red", "gate:win_rate<0.20"]},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "quote.submitted",
                {"market_id": "m-4", "market_slug": "btc-4", "strategy_family": "toxicity_mm"},
                run_id="paper-eligibility",
            )
            telemetry.append_event(
                "quote.skipped",
                {"market_id": "m-5", "market_slug": "btc-5", "strategy_family": "toxicity_mm", "reasons": ["inventory_limit"]},
                run_id="paper-eligibility",
            )

            ops_report = render_status(tmpdir, now_ts=200.0)
            latest_status = render_status_text(tmpdir)

            for rendered in (ops_report, latest_status):
                self.assertIn("Market eligibility (bounded recent-run-scoped):", rendered)
                self.assertIn("discovered=5", rendered)
                self.assertIn("structural=2", rendered)
                self.assertIn("governance=1", rendered)
                self.assertIn("quoted/entered=1", rendered)
                self.assertIn("Structural reasons (recent events): wide_spread>500 (2)", rendered)
                self.assertIn("Governance reasons (recent events): runtime_gate_red (1)", rendered)
                self.assertIn("Quote skip reasons (recent events): inventory_limit (1)", rendered)
                self.assertIn("Pause policy: family-aware | Scope: mixed_by_family | Reason: mixed_by_family", rendered)
                self.assertIn(
                    "Family-aware pause detail: opening_range=paused (directional_low_win_rate_red_gate); toxicity_mm=active (mm_exempt_low_win_rate_only)",
                    rendered,
                )

    def test_market_eligibility_precomputed_reason_dicts_render_actual_values(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(
                run_id="paper-precomputed",
                phase="running",
                mode="paper",
                market_eligibility={
                    "summary_scope": "run",
                    "reason_counts_scope": "stored_status",
                    "discovered_markets": 12,
                    "structurally_untradeable_markets": 7,
                    "governance_blocked_markets": 5,
                    "quoted_or_entered_markets": 3,
                    "top_structural_reasons": [{"reason": "wide_spread>500.0", "count": 7}],
                    "top_governance_reasons": [{"reason": "runtime_gate_red", "count": 5}],
                    "top_quote_skip_reasons": [{"reason": "high_vpin", "count": 2}],
                },
            )

            ops_report = render_status(tmpdir, now_ts=200.0)
            latest_status = render_status_text(tmpdir)

            for rendered in (ops_report, latest_status):
                self.assertIn("Structural reasons (events): wide_spread>500.0 (7)", rendered)
                self.assertIn("Governance reasons (events): runtime_gate_red (5)", rendered)
                self.assertIn("Quote skip reasons (events): high_vpin (2)", rendered)
                self.assertNotIn("count (reason)", rendered)

    def test_runtime_surfaces_prefer_persisted_market_eligibility_snapshot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            persisted_eligibility = {
                "summary_scope": "bounded_recent_run",
                "reason_counts_scope": "recent_run_events",
                "discovered_markets": 77,
                "structurally_untradeable_markets": 11,
                "governance_blocked_markets": 5,
                "quoted_or_entered_markets": 9,
                "top_structural_reasons": [["persisted_structural", 11]],
                "top_governance_reasons": [["persisted_governance", 5]],
                "top_quote_skip_reasons": [["persisted_quote_skip", 3]],
            }
            telemetry.update_status(
                run_id="paper-persisted",
                phase="running",
                mode="paper",
                market_eligibility=persisted_eligibility,
            )

            with patch.object(RuntimeTelemetry, "summarize_market_eligibility", side_effect=AssertionError("should not recompute")):
                payload = runtime_status_payload(tmpdir)
                ops_report = render_status(tmpdir, now_ts=200.0)
                latest_status = render_status_text(tmpdir)

            self.assertEqual(payload["market_eligibility"], persisted_eligibility)
            for rendered in (ops_report, latest_status):
                self.assertIn("discovered=77", rendered)
                self.assertIn("structural=11", rendered)
                self.assertIn("governance=5", rendered)
                self.assertIn("quoted/entered=9", rendered)
                self.assertIn("persisted_structural (11)", rendered)
                self.assertIn("persisted_governance (5)", rendered)
                self.assertIn("persisted_quote_skip (3)", rendered)

    def test_runtime_surfaces_recompute_market_eligibility_when_snapshot_absent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(
                run_id="paper-missing-snapshot",
                phase="running",
                mode="paper",
                fetched_markets=0,
            )
            status_path = Path(tmpdir) / "status.json"
            status = json.loads(status_path.read_text(encoding="utf-8"))
            status.pop("market_eligibility", None)
            status_path.write_text(json.dumps(status), encoding="utf-8")
            recomputed_eligibility = {
                "summary_scope": "bounded_recent_run",
                "reason_counts_scope": "recent_run_events",
                "discovered_markets": 4,
                "structurally_untradeable_markets": 1,
                "governance_blocked_markets": 2,
                "quoted_or_entered_markets": 3,
                "top_structural_reasons": [["fallback_structural", 1]],
                "top_governance_reasons": [["fallback_governance", 2]],
                "top_quote_skip_reasons": [["fallback_quote_skip", 3]],
            }

            with patch.object(RuntimeTelemetry, "summarize_market_eligibility", return_value=recomputed_eligibility) as summarize:
                payload = runtime_status_payload(tmpdir)
                ops_report = render_status(tmpdir, now_ts=200.0)
                latest_status = render_status_text(tmpdir)

            self.assertGreaterEqual(summarize.call_count, 3)
            self.assertEqual(payload["market_eligibility"], recomputed_eligibility)
            for rendered in (ops_report, latest_status):
                self.assertIn("discovered=4", rendered)
                self.assertIn("fallback_structural (1)", rendered)
                self.assertIn("fallback_governance (2)", rendered)
                self.assertIn("fallback_quote_skip (3)", rendered)

    def test_market_eligibility_discovered_counts_include_fetch_errors_without_samples(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(
                run_id="paper-fetch-errors",
                phase="running",
                mode="paper",
                fetched_markets=2,
            )
            telemetry.append_event(
                "market.discovered",
                {"market_id": "m-1", "market_slug": "btc-1", "slot_id": "slot-1"},
                run_id="paper-fetch-errors",
            )
            telemetry.append_event(
                "market.fetch_error",
                {"market_id": "m-1", "market_slug": "btc-1", "error": "boom"},
                run_id="paper-fetch-errors",
            )
            telemetry.append_event(
                "market.discovered",
                {"market_id": "m-2", "market_slug": "btc-2", "slot_id": "slot-2"},
                run_id="paper-fetch-errors",
            )
            telemetry.append_event(
                "market.fetch_error",
                {"market_id": "m-2", "market_slug": "btc-2", "error": "still boom"},
                run_id="paper-fetch-errors",
            )

            eligibility = runtime_status_payload(tmpdir)["market_eligibility"]

            self.assertEqual(eligibility["summary_scope"], "bounded_recent_run")
            self.assertEqual(eligibility["inferred_discovered_markets"], 2)
            self.assertEqual(eligibility["discovered_markets"], 2)
            self.assertEqual(eligibility["structurally_untradeable_markets"], 0)
            self.assertEqual(eligibility["governance_blocked_markets"], 0)
            self.assertEqual(eligibility["quoted_or_entered_markets"], 0)

    def test_market_eligibility_no_event_edge_case_renders_cleanly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(
                run_id="paper-empty",
                phase="running",
                mode="paper",
                fetched_markets=0,
            )

            payload = runtime_status_payload(tmpdir)
            eligibility = payload["market_eligibility"]
            ops_report = render_status(tmpdir, now_ts=200.0)

            self.assertEqual(eligibility["summary_scope"], "bounded_recent_run")
            self.assertEqual(eligibility["discovered_markets"], 0)
            self.assertEqual(eligibility["structurally_untradeable_markets"], 0)
            self.assertEqual(eligibility["governance_blocked_markets"], 0)
            self.assertEqual(eligibility["quoted_or_entered_markets"], 0)
            self.assertEqual(eligibility["top_structural_reasons"], [])
            self.assertEqual(eligibility["top_governance_reasons"], [])
            self.assertIn("Market eligibility (bounded recent-run-scoped): discovered=0 structural=0 governance=0 quoted/entered=0", ops_report)
            self.assertIn("Structural reasons (recent events): none recently", ops_report)
            self.assertIn("Governance reasons (recent events): none recently", ops_report)

    def test_market_eligibility_isolated_to_current_run_id(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="run-active", phase="running", mode="paper", fetched_markets=1)
            telemetry.append_market_sample({"market_id": "active-1", "market_slug": "active-1"}, run_id="run-active")
            telemetry.append_event(
                "market.runtime_baseline_untradeable",
                {"market_id": "active-1", "market_slug": "active-1", "reasons": ["wide_spread>500"]},
                run_id="run-active",
            )
            telemetry.append_market_sample({"market_id": "other-1", "market_slug": "other-1"}, run_id="run-old")
            telemetry.append_event(
                "market.entry_blocked",
                {"market_id": "other-1", "market_slug": "other-1", "reasons": ["runtime_gate_red"]},
                run_id="run-old",
            )
            telemetry.append_event(
                "quote.submitted",
                {"market_id": "other-2", "market_slug": "other-2"},
                run_id="run-old",
            )

            eligibility = runtime_status_payload(tmpdir)["market_eligibility"]

            self.assertEqual(eligibility["discovered_markets"], 1)
            self.assertEqual(eligibility["structurally_untradeable_markets"], 1)
            self.assertEqual(eligibility["governance_blocked_markets"], 0)
            self.assertEqual(eligibility["quoted_or_entered_markets"], 0)

    def test_inferred_discovered_uses_union_across_event_categories(self):
        eligibility = RuntimeTelemetry.summarize_market_eligibility_events(
            [
                {
                    "event_type": "market.runtime_baseline_untradeable",
                    "payload": {"market_id": "m-1", "reasons": ["wide_spread>500"]},
                },
                {
                    "event_type": "market.entry_blocked",
                    "payload": {"market_id": "m-1", "reasons": ["runtime_gate_red"]},
                },
                {
                    "event_type": "quote.submitted",
                    "payload": {"market_id": "m-1"},
                },
                {
                    "event_type": "quote.skipped",
                    "payload": {"market_id": "m-2", "reasons": ["inventory_limit"]},
                },
            ]
        )

        self.assertEqual(eligibility["inferred_discovered_markets"], 2)
        self.assertEqual(eligibility["discovered_markets"], 2)

    def test_discovered_market_identity_canonicalizes_overlapping_slug_and_slot_aliases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="run-aliases", phase="running", mode="paper", fetched_markets=1)
            telemetry.append_market_sample(
                {
                    "market_id": "m-1",
                    "market_slug": "btc-up",
                    "slot_id": "slot-1",
                },
                run_id="run-aliases",
            )
            telemetry.append_event(
                "market.discovered",
                {"market_slug": "btc-up"},
                run_id="run-aliases",
            )
            telemetry.append_event(
                "quote.submitted",
                {"slot_id": "slot-1"},
                run_id="run-aliases",
            )
            telemetry.append_event(
                "market.entry_blocked",
                {"market_id": "m-1", "reasons": ["runtime_gate_red"]},
                run_id="run-aliases",
            )

            eligibility = runtime_status_payload(tmpdir)["market_eligibility"]

            self.assertEqual(eligibility["inferred_discovered_markets"], 1)
            self.assertEqual(eligibility["discovered_markets"], 1)
            self.assertEqual(eligibility["governance_blocked_markets"], 1)

    def test_run_scoped_discovered_remains_consistent_when_current_loop_fetch_count_shrinks(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="run-consistent", phase="running", mode="paper", fetched_markets=6)
            for market_id in range(1, 7):
                telemetry.append_market_sample(
                    {
                        "market_id": f"m-{market_id}",
                        "market_slug": f"btc-{market_id}",
                        "slot_id": f"slot-{market_id}",
                    },
                    run_id="run-consistent",
                )
            telemetry.append_event(
                "market.runtime_baseline_untradeable",
                {"market_id": "m-1", "market_slug": "btc-1", "reasons": ["wide_spread>500"]},
                run_id="run-consistent",
            )
            telemetry.append_event(
                "market.entry_blocked",
                {"market_id": "m-2", "market_slug": "btc-2", "reasons": ["runtime_gate_red"]},
                run_id="run-consistent",
            )
            telemetry.append_event(
                "quote.submitted",
                {"market_id": "m-3", "market_slug": "btc-3"},
                run_id="run-consistent",
            )
            telemetry.update_status(run_id="run-consistent", phase="running", mode="paper", fetched_markets=1)

            payload = runtime_status_payload(tmpdir)
            eligibility = payload["market_eligibility"]
            rendered = render_status_text(tmpdir)

            self.assertEqual(payload["status"]["fetched_markets"], 1)
            self.assertEqual(eligibility["discovered_markets"], 6)
            self.assertEqual(eligibility["structurally_untradeable_markets"], 1)
            self.assertEqual(eligibility["governance_blocked_markets"], 1)
            self.assertEqual(eligibility["quoted_or_entered_markets"], 1)
            self.assertIn("Markets: fetched=1", rendered)
            self.assertIn("Market eligibility (bounded recent-run-scoped): discovered=6 structural=1 governance=1 quoted/entered=1", rendered)

    def test_update_status_derives_market_eligibility_for_latest_status_surface(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="run-surface", phase="running", mode="paper", fetched_markets=6)
            for market_id in range(1, 7):
                telemetry.append_market_sample(
                    {
                        "market_id": f"m-{market_id}",
                        "market_slug": f"btc-{market_id}",
                        "slot_id": f"slot-{market_id}",
                    },
                    run_id="run-surface",
                )
            telemetry.append_event(
                "market.runtime_baseline_untradeable",
                {"market_id": "m-1", "market_slug": "btc-1", "reasons": ["wide_spread>500"]},
                run_id="run-surface",
            )
            telemetry.append_event(
                "market.entry_blocked",
                {"market_id": "m-2", "market_slug": "btc-2", "reasons": ["runtime_gate_red"]},
                run_id="run-surface",
            )
            telemetry.append_event(
                "quote.submitted",
                {"market_id": "m-3", "market_slug": "btc-3"},
                run_id="run-surface",
            )

            status = telemetry.update_status(run_id="run-surface", phase="running", mode="paper", fetched_markets=1)
            latest_status = telemetry.latest_status_text_path.read_text(encoding="utf-8")

            self.assertEqual(status["market_eligibility"]["summary_scope"], "bounded_recent_run")
            self.assertEqual(status["market_eligibility"]["discovered_markets"], 6)
            self.assertIn("market_eligibility[bounded recent-run]=discovered:6 structural:1 governance:1 quoted_entered:1", latest_status)

    def test_latest_status_text_invalidates_market_eligibility_after_append(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="run-latest-status", phase="running", mode="paper", fetched_markets=1)
            telemetry.update_status(
                run_id="run-latest-status",
                phase="running",
                mode="paper",
                market_eligibility={
                    "summary_scope": "bounded_recent_run",
                    "reason_counts_scope": "recent_run_events",
                    "discovered_markets": 1,
                    "structurally_untradeable_markets": 0,
                    "governance_blocked_markets": 0,
                    "quoted_or_entered_markets": 0,
                },
            )

            telemetry.append_event(
                "market.entry_blocked",
                {"market_id": "m-2", "reasons": ["runtime_gate_red"]},
                run_id="run-latest-status",
            )
            latest_status_after_event = telemetry.latest_status_text_path.read_text(encoding="utf-8")
            telemetry.append_market_sample({"market_id": "m-2", "market_slug": "btc-2"}, run_id="run-latest-status")
            latest_status_after_sample = telemetry.latest_status_text_path.read_text(encoding="utf-8")

            self.assertIn("market_eligibility[stale]=pending_refresh", latest_status_after_event)
            self.assertIn("market_eligibility[stale]=pending_refresh", latest_status_after_sample)
            self.assertNotIn("market_eligibility[bounded recent-run]=discovered:1 structural:0 governance:0 quoted_entered:0", latest_status_after_event)
            self.assertNotIn("market_eligibility[bounded recent-run]=discovered:1 structural:0 governance:0 quoted_entered:0", latest_status_after_sample)


if __name__ == "__main__":
    unittest.main()
