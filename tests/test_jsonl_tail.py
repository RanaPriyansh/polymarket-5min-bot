import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from research.loop import read_jsonl_tail


class JsonlTailTests(unittest.TestCase):
    def test_read_jsonl_tail_returns_last_n_records_without_parsing_whole_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "events.jsonl"
            rows = [{"idx": idx, "payload": f"row-{idx}"} for idx in range(100)]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

            original_loads = json.loads
            parse_count = 0

            def counting_loads(raw, *args, **kwargs):
                nonlocal parse_count
                parse_count += 1
                if parse_count > 12:
                    raise AssertionError(f"expected bounded tail parsing, got {parse_count}")
                return original_loads(raw, *args, **kwargs)

            with patch("research.loop.json.loads", side_effect=counting_loads):
                tail_rows = list(read_jsonl_tail(path, limit=5))

            self.assertEqual([row["idx"] for row in tail_rows], [95, 96, 97, 98, 99])
            self.assertLessEqual(parse_count, 12)

    def test_runtime_telemetry_run_scoped_limited_reads_return_last_matching_rows(self):
        from runtime_telemetry import RuntimeTelemetry

        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            for idx in range(5):
                telemetry.append_event("market.discovered", {"market_id": f"old-{idx}"}, run_id="run-old")
                telemetry.append_market_sample({"market_id": f"old-{idx}"}, run_id="run-old")
            for idx in range(5):
                telemetry.append_event("market.discovered", {"market_id": f"new-{idx}"}, run_id="run-new")
                telemetry.append_market_sample({"market_id": f"new-{idx}"}, run_id="run-new")

            event_rows = telemetry.read_events(limit=3, run_id="run-old")
            sample_rows = telemetry.read_market_samples(limit=3, run_id="run-old")

            self.assertEqual([row["payload"]["market_id"] for row in event_rows], ["old-2", "old-3", "old-4"])
            self.assertEqual([row["market_id"] for row in sample_rows], ["old-2", "old-3", "old-4"])


if __name__ == "__main__":
    unittest.main()
