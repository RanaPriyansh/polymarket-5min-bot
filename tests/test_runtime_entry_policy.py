import tempfile
import unittest
from pathlib import Path

from cli import _entry_gate_for_market, _runtime_gate_snapshot
from runtime_telemetry import RuntimeTelemetry


class RuntimeEntryPolicyTests(unittest.TestCase):
    def test_runtime_gate_snapshot_turns_red_on_low_win_rate(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="paper-bad", win_rate=0.09, resolved_trade_count=35)
            gate = _runtime_gate_snapshot(tmpdir)
            self.assertEqual(gate["gate_state"], "RED")
            self.assertTrue(any("win_rate" in reason for reason in gate["gate_reasons"]))

    def test_entry_gate_blocks_new_orders_when_runtime_gate_is_red(self):
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


if __name__ == "__main__":
    unittest.main()
