import tempfile
import unittest
from pathlib import Path

from cli import (
    _aggregate_gate_pause_decision,
    _bucket_pause_decision_for_market,
    _bucket_pause_status,
    _entry_gate_for_market,
    _gate_pause_decision,
    _quote_submission_post_only,
    _runtime_gate_snapshot,
    _toxicity_mm_has_family_market_state,
    _toxicity_mm_runtime_entry_allowed,
)
from runtime_telemetry import RuntimeTelemetry


class RuntimeEntryPolicyTests(unittest.TestCase):
    def test_runtime_gate_snapshot_turns_red_on_low_win_rate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="paper-bad", win_rate=0.09, resolved_trade_count=35)
            gate = _runtime_gate_snapshot(tmpdir)
            self.assertEqual(gate["gate_state"], "RED")
            self.assertTrue(any("win_rate" in reason for reason in gate["gate_reasons"]))

    def test_entry_gate_allows_mm_when_red_gate_is_low_win_rate_only(self):
        market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": 400.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {}},
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="RED",
            gate_reasons=["win_rate=0.090 < 0.20 with resolved_count=35 >= 20"],
        )
        self.assertTrue(allowed)
        self.assertEqual(reasons, [])

    def test_directional_family_still_blocks_on_low_win_rate_red_gate(self):
        market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": 400.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {}},
            market,
            now_ts=100.0,
            strategy_family="opening_range",
            gate_state="RED",
            gate_reasons=["win_rate=0.090 < 0.20 with resolved_count=35 >= 20"],
        )
        self.assertFalse(allowed)
        self.assertIn("runtime_gate_red", reasons)

    def test_unlisted_mm_suffix_family_does_not_get_mm_exemption(self):
        market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": 400.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {}},
            market,
            now_ts=100.0,
            strategy_family="momentum_mm",
            gate_state="RED",
            gate_reasons=["win_rate=0.090 < 0.20 with resolved_count=35 >= 20"],
        )
        self.assertFalse(allowed)
        self.assertIn("runtime_gate_red", reasons)

    def test_directional_family_blocks_on_low_win_rate_mixed_with_non_blocking_reason(self):
        decision = _gate_pause_decision(
            "RED",
            [
                "win_rate=0.090 < 0.20 with resolved_count=35 >= 20",
                "settlement_pnl_computable=False: slot_settled schema missing realized_pnl",
            ],
            strategy_family="opening_range",
        )
        self.assertTrue(decision["pause"])
        self.assertEqual(decision["reason"], "directional_low_win_rate_red_gate")
        self.assertEqual(
            decision["blocking_gate_reasons"],
            ["win_rate=0.090 < 0.20 with resolved_count=35 >= 20"],
        )

    def test_mm_family_allows_low_win_rate_mixed_only_with_non_blocking_reasons(self):
        decision = _gate_pause_decision(
            "RED",
            [
                "win_rate=0.090 < 0.20 with resolved_count=35 >= 20",
                "settlement_pnl_computable=False: slot_settled schema missing realized_pnl",
            ],
            strategy_family="toxicity_mm",
        )
        self.assertFalse(decision["pause"])
        self.assertEqual(decision["reason"], "mm_exempt_low_win_rate_non_blocking_red_gate")

    def test_toxicity_mm_tte_only_block_allows_refresh_when_inventory_exists(self):
        self.assertTrue(
            _toxicity_mm_runtime_entry_allowed(
                entry_allowed=False,
                entry_reasons=["tte_lt_120s"],
                seconds_to_expiry=75.0,
                has_existing_exposure=True,
            )
        )

    def test_toxicity_mm_tte_only_block_does_not_allow_flat_fresh_exposure(self):
        self.assertFalse(
            _toxicity_mm_runtime_entry_allowed(
                entry_allowed=False,
                entry_reasons=["tte_lt_120s"],
                seconds_to_expiry=75.0,
                has_existing_exposure=False,
            )
        )

    def test_toxicity_mm_market_state_detects_open_family_orders_when_flat(self):
        class FakeExecutor:
            orders = {
                "tox-open": {"market_id": "m1", "strategy_family": "toxicity_mm", "status": "open"},
                "other-open": {"market_id": "m1", "strategy_family": "opening_range", "status": "open"},
            }

            def has_strategy_market_exposure(self, strategy_family, market_id):
                return False

        self.assertTrue(_toxicity_mm_has_family_market_state(FakeExecutor(), "m1"))

    def test_toxicity_mm_market_state_ignores_flat_markets_with_no_open_family_orders(self):
        class FakeExecutor:
            orders = {
                "tox-cancelled": {"market_id": "m1", "strategy_family": "toxicity_mm", "status": "cancelled"},
                "other-open": {"market_id": "m1", "strategy_family": "opening_range", "status": "open"},
            }

            def has_strategy_market_exposure(self, strategy_family, market_id):
                return False

        self.assertFalse(_toxicity_mm_has_family_market_state(FakeExecutor(), "m1"))

    def test_toxicity_mm_market_state_detects_existing_exposure(self):
        class FakeExecutor:
            orders = {}

            def has_strategy_market_exposure(self, strategy_family, market_id):
                return strategy_family == "toxicity_mm" and market_id == "m1"

        self.assertTrue(_toxicity_mm_has_family_market_state(FakeExecutor(), "m1"))

    def test_entry_gate_blocks_mm_on_contradiction_red_reason(self):
        market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": 400.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {}},
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="RED",
            gate_reasons=["contradiction_log_open=1: unresolved contradictions exist"],
        )
        self.assertFalse(allowed)
        self.assertIn("runtime_gate_red", reasons)

    def test_entry_gate_blocks_mm_on_unreviewed_circuit_breaker(self):
        market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": 400.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {}},
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="RED",
            gate_reasons=["circuit_breaker_fired_unreviewed=True: prior circuit breaker stop not yet reviewed"],
        )
        self.assertFalse(allowed)
        self.assertIn("runtime_gate_red", reasons)

    def test_entry_gate_does_not_pause_for_schema_only_red_reason(self):
        market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": 400.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {}},
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="RED",
            gate_reasons=["settlement_pnl_computable=False: slot_settled schema missing realized_pnl"],
        )
        self.assertTrue(allowed)
        self.assertEqual(reasons, [])

    def test_bucket_pause_blocks_new_non_risk_reducing_orders(self):
        market = {"id": "m1", "slug": "btc-updown-5m-100", "asset": "btc", "interval_minutes": 5, "end_ts": 400.0}
        pause_row = {
            "family": "time_decay",
            "asset": "btc",
            "interval": "5",
            "tte_bucket": "120-300s",
            "settled_trades": 20,
            "pause": True,
            "pause_reason": "negative_pnl_per_trade<-0.010000>",
        }
        allowed, reasons = _entry_gate_for_market(
            {"research": {"bucket_pause_enabled": True, "bucket_pause_warn_only": False}},
            market,
            now_ts=100.0,
            strategy_family="time_decay",
            gate_state="GREEN",
            gate_reasons=[],
            bucket_pause_decisions={("time_decay", "btc", "5", "120-300s"): pause_row},
            tte_bucket="120-300s",
        )
        self.assertFalse(allowed)
        self.assertIn("bucket_paused", reasons)

    def test_bucket_pause_allows_risk_reducing_orders(self):
        market = {"id": "m1", "slug": "btc-updown-5m-100", "asset": "btc", "interval_minutes": 5, "end_ts": 400.0}
        pause_row = {
            "family": "toxicity_mm",
            "asset": "btc",
            "interval": "5",
            "tte_bucket": "120-300s",
            "settled_trades": 20,
            "pause": True,
            "pause_reason": "negative_pnl_per_trade<-0.010000>",
        }
        allowed, reasons = _entry_gate_for_market(
            {"research": {"bucket_pause_enabled": True, "bucket_pause_warn_only": False}},
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="GREEN",
            gate_reasons=[],
            bucket_pause_decisions={("toxicity_mm", "btc", "5", "120-300s"): pause_row},
            tte_bucket="120-300s",
            allow_bucket_pause_for_risk_reducing=True,
        )
        self.assertTrue(allowed)
        self.assertIn("bucket_paused", reasons)

    def test_bucket_pause_status_surfaces_paused_count(self):
        row = {"family": "toxicity_mm", "asset": "btc", "interval": "5", "tte_bucket": "120-300s", "pause": True}
        status = _bucket_pause_status({("toxicity_mm", "btc", "5", "120-300s"): row})
        self.assertEqual(status["paused_bucket_count"], 1)
        self.assertEqual(status["paused_buckets"][0]["family"], "toxicity_mm")

    def test_entry_gate_can_block_for_schema_truth_when_configured(self):
        market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": 400.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {"block_on_uncomputable_settlement_truth": True}},
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="RED",
            gate_reasons=["settlement_pnl_computable=False: slot_settled schema missing realized_pnl"],
        )
        self.assertFalse(allowed)
        self.assertIn("runtime_gate_red", reasons)
        self.assertTrue(any("settlement_pnl_computable" in reason for reason in reasons))

    def test_entry_gate_blocks_near_expiry_markets(self):
        market = {
            "id": "m2",
            "slug": "eth-updown-5m-100",
            "asset": "eth",
            "interval_minutes": 5,
            "end_ts": 205.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {"min_seconds_to_expiry_for_new_orders": 120}},
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="GREEN",
            gate_reasons=[],
        )
        self.assertFalse(allowed)
        self.assertIn("tte_lt_120s", reasons)

    def test_toxicity_mm_strategy_override_allows_tte_scaled_band(self):
        market = {
            "id": "m2",
            "slug": "eth-updown-5m-100",
            "asset": "eth",
            "interval_minutes": 5,
            "end_ts": 205.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {
                "execution": {"min_seconds_to_expiry_for_new_orders": 120},
                "strategies": {"toxicity_mm": {"min_seconds_to_expiry_for_new_orders": 30}},
            },
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="GREEN",
            gate_reasons=[],
        )
        self.assertTrue(allowed)
        self.assertEqual(reasons, [])

    def test_endgame_flatten_quote_submission_is_not_post_only(self):
        self.assertFalse(_quote_submission_post_only("endgame_flatten|inventory=4.00|side=sell"))
        self.assertFalse(_quote_submission_post_only("endgame_flatten|inventory=-3.00|side=buy"))
        self.assertTrue(_quote_submission_post_only("VPIN=0.01|quote_spread=0.150%"))

    def test_entry_gate_allows_strategy_specific_late_window(self):
        market = {
            "id": "m2",
            "slug": "eth-updown-5m-100",
            "asset": "eth",
            "interval_minutes": 5,
            "end_ts": 150.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {
                "execution": {"min_seconds_to_expiry_for_new_orders": 120},
                "strategies": {
                    "time_decay": {"min_seconds_to_expiry_for_new_orders": 0},
                },
            },
            market,
            now_ts=100.0,
            strategy_family="time_decay",
            gate_state="GREEN",
            gate_reasons=[],
        )
        self.assertTrue(allowed)
        self.assertEqual(reasons, [])

    def test_entry_gate_honors_strategy_specific_tighter_window(self):
        market = {
            "id": "m2",
            "slug": "eth-updown-5m-100",
            "asset": "eth",
            "interval_minutes": 5,
            "end_ts": 250.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {
                "execution": {"min_seconds_to_expiry_for_new_orders": 120},
                "strategies": {
                    "opening_range": {"min_seconds_to_expiry_for_new_orders": 180},
                },
            },
            market,
            now_ts=100.0,
            strategy_family="opening_range",
            gate_state="GREEN",
            gate_reasons=[],
        )
        self.assertFalse(allowed)
        self.assertIn("tte_lt_180s", reasons)

    def test_entry_gate_allows_sufficient_time_when_gate_is_green(self):
        market = {
            "id": "m3",
            "slug": "btc-updown-15m-100",
            "asset": "btc",
            "interval_minutes": 15,
            "end_ts": 500.0,
        }
        allowed, reasons = _entry_gate_for_market(
            {"execution": {"min_seconds_to_expiry_for_new_orders": 120}},
            market,
            now_ts=100.0,
            strategy_family="toxicity_mm",
            gate_state="GREEN",
            gate_reasons=[],
        )
        self.assertTrue(allowed)
        self.assertEqual(reasons, [])

    def test_aggregate_pause_status_marks_mixed_strategy_runs_honestly(self):
        decision = _aggregate_gate_pause_decision(
            "RED",
            ["win_rate=0.090 < 0.20 with resolved_count=35 >= 20"],
            active_families=["toxicity_mm", "opening_range"],
        )
        self.assertFalse(decision["pause"])
        self.assertEqual(decision["reason"], "mixed_by_family")
        self.assertEqual(decision["scope"], "mixed_by_family")
        self.assertFalse(decision["family_pause_decisions"]["toxicity_mm"]["pause"])
        self.assertTrue(decision["family_pause_decisions"]["opening_range"]["pause"])


if __name__ == "__main__":
    unittest.main()
