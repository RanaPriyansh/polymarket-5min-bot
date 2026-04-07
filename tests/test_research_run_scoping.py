import tempfile
import unittest

from research.polymarket import PolymarketRuntimeResearchAdapter
from runtime_telemetry import RuntimeTelemetry


class ResearchRunScopingTests(unittest.TestCase):
    def test_adapter_defaults_to_current_run_and_ignores_prior_run_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(run_id="run-old", loop_count=1)
            telemetry.append_event("order.filled", {"market_id": "old-fill", "strategy_family": "toxicity_mm"})
            telemetry.append_market_sample({"market_id": "old-sample", "book_reasons": ["wide_spread>500"]})

            telemetry.update_status(run_id="run-new", loop_count=2)
            telemetry.write_strategy_metrics(
                {
                    "toxicity_mm": {
                        "quotes_submitted": 2,
                        "orders_resting": 1,
                        "orders_filled": 1,
                        "cancellations": 0,
                        "realized_pnl": 0.25,
                        "markets_seen": 2,
                        "toxic_book_skips": 1,
                    }
                }
            )
            telemetry.append_event("order.filled", {"market_id": "new-fill", "strategy_family": "toxicity_mm"})
            telemetry.append_market_sample({"market_id": "new-sample", "book_reasons": ["high_vpin"]})

            adapter = PolymarketRuntimeResearchAdapter(tmpdir)
            result = adapter.run()

            self.assertEqual(adapter.run_id, "run-new")
            self.assertEqual(result.context.metadata["run_scope"], "run-new")
            self.assertEqual(result.raw_context["events_analyzed"], 1)
            self.assertEqual(result.raw_context["samples_analyzed"], 1)
            self.assertEqual(result.raw_context["fill_events"], 1)
            self.assertIn("run scope run-new", result.summary)


if __name__ == "__main__":
    unittest.main()
