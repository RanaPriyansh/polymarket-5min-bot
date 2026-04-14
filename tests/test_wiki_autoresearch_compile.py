import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

from scripts.analysis_ingest_duckdb import build_duckdb
from scripts.wiki_autoresearch_compile import compile_wiki


class WikiAutoresearchCompileTests(unittest.TestCase):
    def _write_ledger(self, runtime_dir: Path, rows: list[tuple[object, ...]]) -> None:
        with sqlite3.connect(runtime_dir / "ledger.db") as conn:
            conn.execute(
                """
                CREATE TABLE ledger_events (
                    event_id TEXT PRIMARY KEY,
                    stream TEXT NOT NULL,
                    aggregate_id TEXT NOT NULL,
                    sequence_num INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    event_ts REAL NOT NULL,
                    recorded_ts REAL NOT NULL,
                    run_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    causation_id TEXT,
                    correlation_id TEXT,
                    schema_version INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.executemany(
                "INSERT INTO ledger_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            conn.commit()

    def _append_runtime_event(self, runtime_dir: Path, event: dict[str, object]) -> None:
        events_path = runtime_dir / "events.jsonl"
        with events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event) + "\n")

    def _append_ledger_row(self, runtime_dir: Path, row: tuple[object, ...]) -> None:
        with sqlite3.connect(runtime_dir / "ledger.db") as conn:
            conn.execute(
                "INSERT INTO ledger_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
            conn.commit()

    def _add_overlapping_cross_run_fallback_noise(self, runtime_dir: Path, *, base_ts: float) -> None:
        overlap_run_id = "paper-overlap-run"
        self._append_runtime_event(
            runtime_dir,
            {
                "ts": base_ts - 5,
                "run_id": overlap_run_id,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:2000000000",
                    "strategy_family": "toxicity_mm",
                    "book_quality": {
                        "outcome": "Up",
                        "mid_price": 0.44,
                        "spread_bps": 1500.0,
                        "top_depth": 180.0,
                        "top_notional": 79.2,
                        "depth_ratio": 1.0,
                        "is_tradeable": True,
                    },
                },
            },
        )
        self._append_runtime_event(
            runtime_dir,
            {
                "ts": base_ts + 70,
                "run_id": overlap_run_id,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:2000000000",
                    "strategy_family": "toxicity_mm",
                    "book_quality": {
                        "outcome": "Up",
                        "mid_price": 0.91,
                        "spread_bps": 1500.0,
                        "top_depth": 180.0,
                        "top_notional": 163.8,
                        "depth_ratio": 1.0,
                        "is_tradeable": True,
                    },
                },
            },
        )
        self._append_ledger_row(
            runtime_dir,
            (
                "evt-overlap-settle",
                "slots",
                "btc:5:2000000000",
                2,
                "slot_settled",
                base_ts + 170,
                base_ts + 170,
                overlap_run_id,
                "ik-overlap-settle",
                None,
                None,
                1,
                json.dumps({"market_id": "mkt-1", "slot_id": "btc:5:2000000000", "winning_outcome": "Down", "realized_pnl": -1.0}),
            ),
        )

    def _write_fixture_tree(self, root: Path, *, include_cross_run_noise: bool = False) -> tuple[Path, Path, Path, Path]:
        runtime_dir = root / "data" / "runtime"
        research_dir = root / "data" / "research"
        wiki_root = root / "wiki"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        research_dir.mkdir(parents=True, exist_ok=True)
        (wiki_root / "00_overview").mkdir(parents=True, exist_ok=True)
        (wiki_root / "08_decisions").mkdir(parents=True, exist_ok=True)
        (wiki_root / "07_experiments").mkdir(parents=True, exist_ok=True)

        now = time.time()
        run_id = "paper-test-run"

        status = {
            "run_id": run_id,
            "phase": "running",
            "mode": "paper",
            "heartbeat_ts": now,
            "bankroll": 500.25,
            "fetched_markets": 8,
            "processed_markets": 3,
            "loop_count": 9,
            "baseline_strategy": "toxicity_mm",
            "open_position_count": 1,
            "resolved_trade_count": 1,
            "active_slots": [{"slot_id": "btc:5:2000000000"}],
            "pending_resolution_slots": [{"slot_id": "sol:5:2000000000"}],
            "positions": {
                "toxicity_mm:mkt-1:Up": {"slot_id": "btc:5:2000000000"},
                "toxicity_mm:mkt-2:Down": {"slot_id": "eth:5:2000000000"},
            },
            "stop_reason": "circuit_breaker",
            "strategies": ["toxicity_mm"],
            "risk": {
                "open_order_count": 4,
                "marked_position_count": 2,
                "pending_settlement_count": 1,
                "pending_settlement_exposure": 6.4,
                "realized_pnl_total": 1.5,
                "unrealized_pnl_total": -0.2,
                "total_gross_exposure": 55.4,
                "max_drawdown": 0.09,
            },
        }
        (runtime_dir / "status.json").write_text(json.dumps(status), encoding="utf-8")

        metrics = {
            "toxicity_mm": {
                "markets_seen": 8,
                "quotes_submitted": 5,
                "orders_filled": 2,
                "orders_resting": 1,
                "cancellations": 1,
                "toxic_book_skips": 3,
                "realized_pnl": 2.5,
            }
        }
        (runtime_dir / "strategy_metrics.json").write_text(json.dumps(metrics), encoding="utf-8")

        events = [
            {
                "ts": now - 180,
                "event_type": "runtime.started",
                "payload": {"run_id": run_id, "mode": "paper"},
            },
            {
                "ts": now - 170,
                "run_id": run_id,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:2000000000",
                    "strategy_family": "toxicity_mm",
                    "bid_order_id": "order-1",
                    "ask_order_id": "order-2",
                    "book_quality": {
                        "outcome": "Up",
                        "mid_price": 0.48,
                        "spread_bps": 220.0,
                        "top_depth": 12.0,
                        "top_notional": 5.76,
                        "depth_ratio": 1.2,
                        "is_tradeable": True,
                    },
                },
            },
            {
                "ts": now - 160,
                "run_id": run_id,
                "event_type": "order.filled",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:2000000000",
                    "strategy_family": "toxicity_mm",
                    "order_id": "order-1",
                    "side": "BUY",
                    "outcome": "Up",
                    "fill_price": 0.50,
                    "size": 2.0,
                    "realized_pnl_delta": 0.0,
                    "time_to_expiry_seconds": 187.0,
                },
            },
            {
                "ts": now - 80,
                "run_id": run_id,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:2000000000",
                    "strategy_family": "toxicity_mm",
                    "bid_order_id": "order-3",
                    "ask_order_id": "order-4",
                    "book_quality": {
                        "outcome": "Up",
                        "mid_price": 0.58,
                        "spread_bps": 280.0,
                        "top_depth": 10.0,
                        "top_notional": 5.8,
                        "depth_ratio": 1.4,
                        "is_tradeable": True,
                    },
                },
            },
            {
                "ts": now - 140,
                "run_id": run_id,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-2",
                    "market_slug": "eth-updown",
                    "slot_id": "eth:5:2000000000",
                    "strategy_family": "toxicity_mm",
                    "bid_order_id": "order-5",
                    "ask_order_id": "order-6",
                    "book_quality": {
                        "outcome": "Down",
                        "mid_price": 0.52,
                        "spread_bps": 650.0,
                        "top_depth": 4.0,
                        "top_notional": 2.08,
                        "depth_ratio": 4.2,
                        "is_tradeable": True,
                    },
                },
            },
            {
                "ts": now - 135,
                "run_id": run_id,
                "event_type": "order.filled",
                "payload": {
                    "market_id": "mkt-2",
                    "market_slug": "eth-updown",
                    "slot_id": "eth:5:2000000000",
                    "strategy_family": "toxicity_mm",
                    "order_id": "order-5",
                    "side": "SELL",
                    "outcome": "Down",
                    "fill_price": 0.51,
                    "size": 1.0,
                    "realized_pnl_delta": 0.3,
                    "time_to_expiry_seconds": 820.0,
                },
            },
            {
                "ts": now - 40,
                "run_id": run_id,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-2",
                    "market_slug": "eth-updown",
                    "slot_id": "eth:5:2000000000",
                    "strategy_family": "toxicity_mm",
                    "bid_order_id": "order-7",
                    "ask_order_id": "order-8",
                    "book_quality": {
                        "outcome": "Down",
                        "mid_price": 0.45,
                        "spread_bps": 640.0,
                        "top_depth": 5.0,
                        "top_notional": 2.25,
                        "depth_ratio": 4.0,
                        "is_tradeable": True,
                    },
                },
            },
        ]
        if include_cross_run_noise:
            for idx in range(3):
                old_run_id = f"paper-older-fill-{idx}"
                base_ts = now - 320 + (idx * 10)
                events.extend(
                    [
                        {
                            "ts": base_ts,
                            "run_id": old_run_id,
                            "event_type": "quote.submitted",
                            "payload": {
                                "market_id": f"old-mkt-{idx}",
                                "market_slug": f"old-market-{idx}",
                                "slot_id": f"sol:15:2000000{idx}",
                                "strategy_family": "toxicity_mm",
                                "book_quality": {
                                    "outcome": "Up",
                                    "mid_price": 0.42,
                                    "spread_bps": 1200.0,
                                    "top_depth": 150.0,
                                    "top_notional": 63.0,
                                    "depth_ratio": 1.1,
                                    "is_tradeable": True,
                                },
                            },
                        },
                        {
                            "ts": base_ts + 2,
                            "run_id": old_run_id,
                            "event_type": "order.filled",
                            "payload": {
                                "market_id": f"old-mkt-{idx}",
                                "market_slug": f"old-market-{idx}",
                                "slot_id": f"sol:15:2000000{idx}",
                                "strategy_family": "toxicity_mm",
                                "order_id": f"old-order-{idx}",
                                "side": "BUY",
                                "outcome": "Up",
                                "fill_price": 0.40,
                                "size": 1.0,
                                "realized_pnl_delta": 1.0,
                                "time_to_expiry_seconds": 950.0,
                            },
                        },
                        {
                            "ts": base_ts + 70,
                            "run_id": old_run_id,
                            "event_type": "quote.submitted",
                            "payload": {
                                "market_id": f"old-mkt-{idx}",
                                "market_slug": f"old-market-{idx}",
                                "slot_id": f"sol:15:2000000{idx}",
                                "strategy_family": "toxicity_mm",
                                "book_quality": {
                                    "outcome": "Up",
                                    "mid_price": 0.55,
                                    "spread_bps": 1200.0,
                                    "top_depth": 150.0,
                                    "top_notional": 82.5,
                                    "depth_ratio": 1.1,
                                    "is_tradeable": True,
                                },
                            },
                        },
                    ]
                )
        (runtime_dir / "events.jsonl").write_text(
            "\n".join(json.dumps(event) for event in events) + "\n",
            encoding="utf-8",
        )

        samples = [
            {
                "ts": now - 200,
                "run_id": run_id,
                "market_id": "mkt-1",
                "market_slug": "btc-updown",
                "slot_id": "btc:5:2000000000",
                "is_tradeable": True,
                "book_depth": 10.0,
                "book_notional": 5.0,
                "book_spread_bps": 210.5,
                "book_reasons": [],
                "volume": 123.0,
            },
            {
                "ts": now - 190,
                "run_id": run_id,
                "market_id": "mkt-2",
                "market_slug": "eth-updown",
                "slot_id": "eth:5:2000000000",
                "is_tradeable": False,
                "book_depth": 4.0,
                "book_notional": 2.0,
                "book_spread_bps": 650.0,
                "book_reasons": ["wide_spread>500.0"],
                "volume": 50.0,
            },
        ]
        (runtime_dir / "market_samples.jsonl").write_text(
            "\n".join(json.dumps(sample) for sample in samples) + "\n",
            encoding="utf-8",
        )

        research = {
            "cycle_id": "cycle-test",
            "created_at": now,
            "gate_state": "RED",
            "source": "live-runtime-artifacts",
            "raw_context": {
                "gate_reasons": [
                    "settlement_pnl_computable=False: slot_settled schema missing realized_pnl",
                    "run_lineage_fragmentation=4 >= 4 (experiment fragmented)",
                ]
            },
        }
        research_path = research_dir / "latest.json"
        research_path.write_text(json.dumps(research), encoding="utf-8")

        rows = [
            (
                "evt-fill-1",
                "orders",
                "mkt-1",
                1,
                "fill_applied",
                now - 160,
                now - 160,
                run_id,
                "ik-1",
                None,
                None,
                1,
                json.dumps({"market_id": "mkt-1", "slot_id": "btc:5:2000000000", "strategy_family": "toxicity_mm"}),
            ),
            (
                "evt-fill-2",
                "orders",
                "mkt-2",
                1,
                "fill_applied",
                now - 135,
                now - 135,
                run_id,
                "ik-2",
                None,
                None,
                1,
                json.dumps({"market_id": "mkt-2", "slot_id": "eth:5:2000000000", "strategy_family": "toxicity_mm"}),
            ),
            (
                "evt-settle-1",
                "slots",
                "btc:5:2000000000",
                1,
                "slot_settled",
                now - 30,
                now - 30,
                run_id,
                "ik-3",
                None,
                None,
                1,
                json.dumps({"market_id": "mkt-1", "slot_id": "btc:5:2000000000", "winning_outcome": "Up", "realized_pnl": 1.0}),
            ),
            (
                "evt-settle-2",
                "slots",
                "eth:5:2000000000",
                1,
                "slot_settled",
                now - 20,
                now - 20,
                run_id,
                "ik-4",
                None,
                None,
                1,
                json.dumps({"market_id": "mkt-2", "slot_id": "eth:5:2000000000", "winning_outcome": "Up", "realized_pnl": -0.2}),
            ),
            (
                "evt-pending-1",
                "slots",
                "sol:5:2000000000",
                1,
                "slot_resolution_pending",
                now - 10,
                now - 10,
                run_id,
                "ik-5",
                None,
                None,
                1,
                json.dumps({"market_id": "mkt-3", "slot_id": "sol:5:2000000000"}),
            ),
        ]
        for idx in range(4):
            rid = f"paper-older-{idx}"
            rows.append(
                (
                    f"evt-old-{idx}",
                    "runs",
                    rid,
                    1,
                    "risk_snapshot_recorded",
                    now - 50 + idx,
                    now - 50 + idx,
                    rid,
                    f"ik-old-{idx}",
                    None,
                    None,
                    1,
                    json.dumps({"capital": 500.0 + idx}),
                )
            )
        self._write_ledger(runtime_dir, rows)

        for relative in (
            "00_overview/current_state.md",
            "08_decisions/contradiction_log.md",
            "07_experiments/experiment_registry.md",
        ):
            path = wiki_root / relative
            path.write_text(
                "---\ncreated: 2026-04-08\nupdated: 2026-04-08\n---\nplaceholder\n",
                encoding="utf-8",
            )

        duckdb_path = root / "data" / "analysis" / "base.duckdb"
        return runtime_dir, research_path, wiki_root, duckdb_path

    def test_compile_wiki_refreshes_base_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, wiki_root, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            generated_ts = 1776000000.0

            result = compile_wiki(
                repo_root=tmpdir,
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
                wiki_root=wiki_root,
                generated_ts=generated_ts,
                refresh_analysis=True,
            )

            self.assertEqual(result["contradiction_count"], 7)
            self.assertEqual(result["experiment_count"], 5)
            self.assertEqual(len(result["changed_files"]), 3)

            current_state = (wiki_root / "00_overview" / "current_state.md").read_text(encoding="utf-8")
            contradiction_log = (wiki_root / "08_decisions" / "contradiction_log.md").read_text(encoding="utf-8")
            experiment_registry = (wiki_root / "07_experiments" / "experiment_registry.md").read_text(encoding="utf-8")

            self.assertIn("created: 2026-04-08", current_state)
            self.assertIn("updated: 2026-04-12", current_state)
            self.assertIn("active_contradictions: `7`", current_state)
            self.assertIn("matured_60s_count: `2`", current_state)
            self.assertIn("matured_300s_count: `0`", current_state)
            self.assertIn("analysis_mode: `batch3_to_batch5_refresh`", current_state)
            self.assertIn("analysis_source: `batch4_batch5_views`", current_state)
            self.assertIn("analysis_scope: `current_run_only`", current_state)
            self.assertIn("validation_scope: `omitted_for_current_run_scope`", current_state)

            self.assertIn("C-001 — Runtime realized PnL and strategy snapshot disagree", contradiction_log)
            self.assertIn("C-006 — Research snapshot says settlement PnL is not computable, but live ledger says it is", contradiction_log)
            self.assertIn("C-007 — Execution is proven before 300s markout quality is proven", contradiction_log)
            self.assertIn("analysis_source: `batch4_batch5_views`", contradiction_log)

            self.assertIn("EXP-001 — Runtime truth reconciliation bridge", experiment_registry)
            self.assertIn("EXP-005 — Liquidity bucket baseline", experiment_registry)
            self.assertIn("analysis_source: `batch4_batch5_views`", experiment_registry)
            self.assertNotIn("- recommendation:", experiment_registry.lower())
            self.assertNotIn("promote", experiment_registry.lower())

    def test_compile_wiki_scopes_summary_views_to_current_run(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, wiki_root, duckdb_path = self._write_fixture_tree(
                Path(tmpdir),
                include_cross_run_noise=True,
            )

            compile_wiki(
                repo_root=tmpdir,
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
                wiki_root=wiki_root,
                generated_ts=1776000000.0,
                refresh_analysis=True,
            )

            current_state = (wiki_root / "00_overview" / "current_state.md").read_text(encoding="utf-8")

            self.assertIn("fill_count: `2`", current_state)
            self.assertIn(
                "top_liquidity_bucket: `spread:500-1000bps | depth:<5 | imbalance:skewed`",
                current_state,
            )
            self.assertNotIn("spread:1000bps+ | depth:100+ | imbalance:balanced", current_state)

    def test_compile_wiki_scopes_runtime_fallback_to_current_run_quotes_and_settlements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, wiki_root, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            self._add_overlapping_cross_run_fallback_noise(
                runtime_dir,
                base_ts=time.time() - 160,
            )

            compile_wiki(
                repo_root=tmpdir,
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
                wiki_root=wiki_root,
                generated_ts=1776000000.0,
                refresh_analysis=False,
            )

            current_state = (wiki_root / "00_overview" / "current_state.md").read_text(encoding="utf-8")

            self.assertIn("analysis_mode: `runtime_fallback_no_duckdb`", current_state)
            self.assertIn("analysis_source: `runtime_jsonl_fallback`", current_state)
            self.assertIn("avg_markout_60s: `0.0700`", current_state)
            self.assertIn("avg_markout_final: `0.5050`", current_state)
            self.assertIn(
                "top_liquidity_bucket: `spread:500-1000bps | depth:<5 | imbalance:skewed`",
                current_state,
            )
            self.assertNotIn("spread:1000bps+ | depth:100+ | imbalance:balanced", current_state)

    def test_compile_wiki_scopes_existing_duckdb_fallback_to_current_run_quotes_and_settlements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, wiki_root, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            self._add_overlapping_cross_run_fallback_noise(
                runtime_dir,
                base_ts=time.time() - 160,
            )
            build_duckdb(
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
            )

            compile_wiki(
                repo_root=tmpdir,
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
                wiki_root=wiki_root,
                generated_ts=1776000000.0,
                refresh_analysis=False,
            )

            current_state = (wiki_root / "00_overview" / "current_state.md").read_text(encoding="utf-8")

            self.assertIn("analysis_mode: `existing_duckdb`", current_state)
            self.assertIn("analysis_source: `batch3_duckdb_fallback`", current_state)
            self.assertIn("avg_markout_60s: `0.0700`", current_state)
            self.assertIn("avg_markout_final: `0.5050`", current_state)
            self.assertIn(
                "top_liquidity_bucket: `spread:500-1000bps | depth:<5 | imbalance:skewed`",
                current_state,
            )
            self.assertNotIn("spread:1000bps+ | depth:100+ | imbalance:balanced", current_state)

    def test_compile_wiki_is_deterministic_for_fixed_timestamp(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, wiki_root, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            generated_ts = 1776000000.0

            compile_wiki(
                repo_root=tmpdir,
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
                wiki_root=wiki_root,
                generated_ts=generated_ts,
                refresh_analysis=True,
            )
            first = {
                relative: (wiki_root / relative).read_text(encoding="utf-8")
                for relative in (
                    Path("00_overview/current_state.md"),
                    Path("08_decisions/contradiction_log.md"),
                    Path("07_experiments/experiment_registry.md"),
                )
            }

            second_result = compile_wiki(
                repo_root=tmpdir,
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
                wiki_root=wiki_root,
                generated_ts=generated_ts,
                refresh_analysis=True,
            )
            second = {
                relative: (wiki_root / relative).read_text(encoding="utf-8")
                for relative in first
            }

            self.assertEqual(first, second)
            self.assertEqual(second_result["changed_files"], [])


if __name__ == "__main__":
    unittest.main()
