import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import duckdb

from scripts.analysis_build_views import build_analysis_views
from scripts.analysis_ingest_duckdb import build_duckdb


class AnalysisIngestDuckDBTests(unittest.TestCase):
    def _append_runtime_events(self, runtime_dir: Path, *events: dict) -> None:
        with (runtime_dir / "events.jsonl").open("a", encoding="utf-8") as fh:
            for event in events:
                fh.write(json.dumps(event) + "\n")

    def _insert_ledger_event(self, runtime_dir: Path, row: tuple[object, ...]) -> None:
        with sqlite3.connect(runtime_dir / "ledger.db") as conn:
            conn.execute(
                """
                INSERT INTO ledger_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
            conn.commit()

    def _write_fixture_tree(self, root: Path) -> tuple[Path, Path, Path]:
        runtime_dir = root / "data" / "runtime"
        research_dir = root / "data" / "research"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        research_dir.mkdir(parents=True, exist_ok=True)

        status = {
            "run_id": "paper-test-run",
            "phase": "stopped",
            "mode": "paper",
            "heartbeat_ts": 123.45,
            "bankroll": 500.25,
            "fetched_markets": 8,
            "processed_markets": 3,
            "loop_count": 9,
            "open_position_count": 2,
            "resolved_trade_count": 1,
            "active_slots": [{"slot_id": "btc:5:1"}, {"slot_id": "eth:5:1"}],
            "pending_resolution_slots": [{"slot_id": "sol:5:1"}],
            "strategies": ["toxicity_mm", "opening_range"],
            "risk": {
                "open_order_count": 4,
                "marked_position_count": 2,
                "pending_settlement_count": 1,
                "pending_settlement_exposure": 6.4,
                "realized_pnl_total": 1.5,
                "unrealized_pnl_total": -0.2,
                "total_gross_exposure": 55.4,
                "max_drawdown": 0.09,
                "exposure_by_asset": {
                    "btc": {"total_exposure": 41.0},
                    "eth": {"total_exposure": 14.4},
                },
                "exposure_by_interval": {
                    "5": {"total_exposure": 39.0},
                    "15": {"total_exposure": 16.4},
                },
            },
        }
        (runtime_dir / "status.json").write_text(json.dumps(status), encoding="utf-8")

        metrics = {
            "opening_range": {
                "markets_seen": 4,
                "quotes_submitted": 1,
                "orders_filled": 0,
                "orders_resting": 0,
                "cancellations": 0,
                "toxic_book_skips": 2,
                "realized_pnl": 0.0,
            },
            "toxicity_mm": {
                "markets_seen": 8,
                "quotes_submitted": 5,
                "orders_filled": 2,
                "orders_resting": 1,
                "cancellations": 1,
                "toxic_book_skips": 3,
                "realized_pnl": 2.5,
            },
        }
        (runtime_dir / "strategy_metrics.json").write_text(json.dumps(metrics), encoding="utf-8")

        events = [
            {
                "ts": 111.0,
                "event_type": "runtime.started",
                "payload": {"run_id": "paper-test-run", "mode": "paper"},
            },
            {
                "ts": 112.0,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:300",
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
                "ts": 113.0,
                "event_type": "order.filled",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:300",
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
                "ts": 115.0,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:300",
                    "strategy_family": "toxicity_mm",
                    "bid_order_id": "order-3",
                    "ask_order_id": "order-4",
                    "book_quality": {
                        "outcome": "Up",
                        "mid_price": 0.58,
                        "spread_bps": 650.0,
                        "top_depth": 4.0,
                        "top_notional": 2.32,
                        "depth_ratio": 4.2,
                        "is_tradeable": True,
                    },
                },
            },
            {
                "ts": 116.0,
                "event_type": "order.filled",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:300",
                    "strategy_family": "toxicity_mm",
                    "order_id": "order-4",
                    "side": "SELL",
                    "outcome": "Up",
                    "fill_price": 0.60,
                    "size": 1.0,
                    "realized_pnl_delta": 0.3,
                    "time_to_expiry_seconds": 820.0,
                },
            },
            {
                "ts": 145.0,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:300",
                    "strategy_family": "toxicity_mm",
                    "bid_order_id": "order-5",
                    "ask_order_id": "order-6",
                    "book_quality": {
                        "outcome": "Up",
                        "mid_price": 0.55,
                        "spread_bps": 300.0,
                        "top_depth": 10.0,
                        "top_notional": 5.5,
                        "depth_ratio": 1.4,
                        "is_tradeable": True,
                    },
                },
            },
            {
                "ts": 180.0,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:300",
                    "strategy_family": "toxicity_mm",
                    "bid_order_id": "order-7",
                    "ask_order_id": "order-8",
                    "book_quality": {
                        "outcome": "Up",
                        "mid_price": 0.52,
                        "spread_bps": 280.0,
                        "top_depth": 11.0,
                        "top_notional": 5.72,
                        "depth_ratio": 1.3,
                        "is_tradeable": True,
                    },
                },
            },
            {
                "ts": 420.0,
                "event_type": "quote.submitted",
                "payload": {
                    "market_id": "mkt-1",
                    "market_slug": "btc-updown",
                    "slot_id": "btc:5:300",
                    "strategy_family": "toxicity_mm",
                    "bid_order_id": "order-9",
                    "ask_order_id": "order-10",
                    "book_quality": {
                        "outcome": "Up",
                        "mid_price": 0.49,
                        "spread_bps": 310.0,
                        "top_depth": 9.0,
                        "top_notional": 4.41,
                        "depth_ratio": 1.6,
                        "is_tradeable": True,
                    },
                },
            },
        ]
        (runtime_dir / "events.jsonl").write_text(
            "\n".join(json.dumps(row) for row in events) + "\n",
            encoding="utf-8",
        )

        samples = [
            {
                "ts": 211.0,
                "run_id": "paper-test-run",
                "market_id": "mkt-1",
                "market_slug": "btc-updown",
                "slot_id": "btc:5:1",
                "is_tradeable": True,
                "book_depth": 10.0,
                "book_notional": 5.0,
                "book_spread_bps": 210.5,
                "book_reasons": [],
                "volume": 123.0,
            },
            {
                "ts": 212.0,
                "run_id": "paper-test-run",
                "market_id": "mkt-2",
                "market_slug": "eth-updown",
                "slot_id": "eth:5:1",
                "is_tradeable": False,
                "book_depth": 8.0,
                "book_notional": 4.5,
                "book_spread_bps": 600.0,
                "book_reasons": ["wide_spread>500"],
                "volume": 20.0,
            },
        ]
        (runtime_dir / "market_samples.jsonl").write_text(
            "\n".join(json.dumps(row) for row in samples) + "\n",
            encoding="utf-8",
        )

        ledger_path = runtime_dir / "ledger.db"
        with sqlite3.connect(ledger_path) as conn:
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
                    idempotency_key TEXT NOT NULL UNIQUE,
                    causation_id TEXT,
                    correlation_id TEXT,
                    schema_version INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO ledger_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt-1",
                    "order",
                    "order-1",
                    1,
                    "order_created",
                    120.0,
                    120.0,
                    "paper-test-run",
                    "order_created:order-1",
                    None,
                    "corr-1",
                    1,
                    json.dumps(
                        {
                            "market_id": "mkt-1",
                            "market_slug": "btc-updown",
                            "slot_id": "btc:5:300",
                            "strategy_family": "toxicity_mm",
                            "side": "BUY",
                            "outcome": "Up",
                            "size": 3.5,
                            "price": 0.48,
                        }
                    ),
                ),
            )
            conn.execute(
                """
                INSERT INTO ledger_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "evt-2",
                    "slot",
                    "mkt-1",
                    2,
                    "slot_settled",
                    500.0,
                    500.0,
                    "paper-test-run",
                    "slot_settled:mkt-1",
                    None,
                    "corr-1",
                    1,
                    json.dumps(
                        {
                            "market_id": "mkt-1",
                            "winning_outcome": "Up",
                        }
                    ),
                ),
            )
            conn.commit()

        research = {
            "cycle_id": "cycle-runtime-1",
            "created_at": 300.0,
            "source": "live-runtime-artifacts",
            "summary": "Fixture summary",
            "top_recommendation": None,
            "next_actions": ["Observe only"],
            "context": {
                "runtime_dir": str(runtime_dir),
                "artifact_dir": str(research_dir),
            },
            "raw_context": {"gate_state": "YELLOW"},
        }
        research_path = research_dir / "latest.json"
        research_path.write_text(json.dumps(research), encoding="utf-8")

        duckdb_path = root / "data" / "analysis" / "base.duckdb"
        return runtime_dir, research_path, duckdb_path

    def test_build_duckdb_creates_expected_base_tables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))

            result = build_duckdb(
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
            )

            self.assertEqual(result["active_run_id"], "paper-test-run")
            self.assertEqual(result["runtime_events_rows"], 8)
            self.assertEqual(result["market_samples_rows"], 2)
            self.assertEqual(result["ledger_events_rows"], 2)
            self.assertTrue(duckdb_path.exists())

            with duckdb.connect(str(duckdb_path)) as conn:
                tables = {
                    name
                    for (name,) in conn.execute(
                        "show tables"
                    ).fetchall()
                }
                self.assertTrue(
                    {
                        "ingestion_runs",
                        "ingestion_sources",
                        "runtime_status_snapshot",
                        "strategy_metrics_snapshot",
                        "ledger_events",
                        "runtime_events",
                        "market_samples",
                        "research_latest_snapshot",
                    }.issubset(tables)
                )
                status_row = conn.execute(
                    "SELECT run_id, active_slot_count, pending_resolution_slot_count, strategy_count, risk_total_gross_exposure FROM runtime_status_snapshot"
                ).fetchone()
                self.assertEqual(status_row, ("paper-test-run", 2, 1, 2, 55.4))
                metric_rows = conn.execute(
                    "SELECT strategy_family, quotes_submitted, realized_pnl FROM strategy_metrics_snapshot ORDER BY strategy_family"
                ).fetchall()
                self.assertEqual(metric_rows, [("opening_range", 1, 0.0), ("toxicity_mm", 5, 2.5)])
                ledger_row = conn.execute(
                    "SELECT event_type, market_id, strategy_family, side, outcome, size, price FROM ledger_events ORDER BY event_id LIMIT 1"
                ).fetchone()
                self.assertEqual(ledger_row, ("order_created", "mkt-1", "toxicity_mm", "BUY", "Up", 3.5, 0.48))
                runtime_row = conn.execute(
                    "SELECT event_type, run_id, order_id FROM runtime_events ORDER BY line_number DESC LIMIT 1"
                ).fetchone()
                self.assertEqual(runtime_row, ("quote.submitted", None, "order-9"))
                sample_row = conn.execute(
                    "SELECT market_id, is_tradeable, book_reasons_json::VARCHAR FROM market_samples WHERE market_id = 'mkt-2'"
                ).fetchone()
                self.assertEqual(sample_row, ("mkt-2", False, '["wide_spread>500"]'))
                research_row = conn.execute(
                    "SELECT cycle_id, created_at, gate_state, source, runtime_dir, artifact_dir FROM research_latest_snapshot"
                ).fetchone()
                self.assertEqual(
                    research_row,
                    (
                        "cycle-runtime-1",
                        300.0,
                        "YELLOW",
                        "live-runtime-artifacts",
                        str(runtime_dir),
                        str(research_path.parent),
                    ),
                )
                research_columns = {
                    row[0]
                    for row in conn.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'main' AND table_name = 'research_latest_snapshot'"
                    ).fetchall()
                }
                self.assertNotIn("summary", research_columns)
                self.assertNotIn("top_recommendation", research_columns)
                self.assertNotIn("next_actions_json", research_columns)
                source_rows = conn.execute(
                    "SELECT source_name, exists_on_disk FROM ingestion_sources ORDER BY source_name"
                ).fetchall()
                self.assertEqual(
                    source_rows,
                    [
                        ("ledger_db", True),
                        ("market_samples", True),
                        ("research_latest", True),
                        ("runtime_events", True),
                        ("runtime_status", True),
                        ("strategy_metrics", True),
                    ],
                )

    def test_build_analysis_views_creates_expected_batch4_views(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))

            build_duckdb(runtime_dir=runtime_dir, research_path=research_path, duckdb_path=duckdb_path)
            result = build_analysis_views(duckdb_path=duckdb_path)

            self.assertEqual(result["validation_failed"], 0)
            self.assertEqual(
                result["views_built"],
                [
                    "analysis_slot_dimensions",
                    "analysis_quote_midpoints",
                    "analysis_fill_events",
                    "analysis_fill_markouts",
                    "analysis_pnl_by_asset_interval_side",
                    "analysis_time_to_expiry_buckets",
                    "analysis_fill_liquidity_context",
                    "analysis_spread_depth_imbalance_buckets",
                    "analysis_inventory_state_buckets",
                    "analysis_fill_cluster_features",
                    "analysis_win_loss_clusters",
                    "analysis_circuit_breaker_fill_context",
                    "analysis_circuit_breaker_precursors",
                    "analysis_validation_checks",
                ],
            )

            with duckdb.connect(str(duckdb_path)) as conn:
                tables = {
                    (table_name, table_type)
                    for table_name, table_type in conn.execute(
                        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main'"
                    ).fetchall()
                }
                for view_name in result["views_built"]:
                    self.assertIn((view_name, "VIEW"), tables)

                pnl_rows = conn.execute(
                    "SELECT asset, interval_minutes, side, fill_count, realized_pnl_delta_sum FROM analysis_pnl_by_asset_interval_side ORDER BY side"
                ).fetchall()
                self.assertEqual(pnl_rows, [("btc", 5, "BUY", 1, 0.0), ("btc", 5, "SELL", 1, 0.3)])
                markout_rows = conn.execute(
                    """
                    SELECT
                        side,
                        ROUND(markout_30s, 3),
                        ROUND(markout_60s, 3),
                        ROUND(markout_120s, 3),
                        ROUND(markout_300s, 3),
                        ROUND(markout_final, 3),
                        winning_outcome,
                        adverse_after_30s,
                        adverse_after_60s,
                        adverse_after_120s,
                        adverse_after_300s
                    FROM analysis_fill_markouts
                    ORDER BY fill_ts
                    """
                ).fetchall()
                self.assertEqual(
                    markout_rows,
                    [
                        ("BUY", 0.05, 0.02, -0.01, -0.01, 0.5, "Up", False, False, True, True),
                        ("SELL", 0.08, 0.08, 0.11, 0.11, -0.4, "Up", False, False, False, False),
                    ],
                )
                tte_rows = conn.execute(
                    """
                    SELECT
                        tte_bucket,
                        fill_count,
                        ROUND(avg_markout_30s, 3),
                        ROUND(avg_markout_60s, 3),
                        ROUND(avg_markout_120s, 3),
                        ROUND(avg_markout_300s, 3),
                        ROUND(avg_markout_final, 3)
                    FROM analysis_time_to_expiry_buckets
                    ORDER BY CASE tte_bucket
                        WHEN '<60s' THEN 1
                        WHEN '60-120s' THEN 2
                        WHEN '120-300s' THEN 3
                        WHEN '>300s' THEN 4
                        ELSE 5
                    END
                    """
                ).fetchall()
                self.assertEqual(tte_rows, [("120-300s", 1, 0.05, 0.02, -0.01, -0.01, 0.5), (">300s", 1, 0.08, 0.08, 0.11, 0.11, -0.4)])
                liquidity_rows = conn.execute(
                    "SELECT spread_bucket, depth_bucket, imbalance_bucket, fill_count FROM analysis_spread_depth_imbalance_buckets ORDER BY spread_bucket"
                ).fetchall()
                self.assertEqual(
                    liquidity_rows,
                    [
                        ("spread:500-1000bps", "depth:<5", "imbalance:skewed", 1),
                        ("spread:<250bps", "depth:5-20", "imbalance:balanced", 1),
                    ],
                )
                inventory_rows = conn.execute(
                    "SELECT inventory_state_bucket, fill_count FROM analysis_inventory_state_buckets ORDER BY inventory_state_bucket"
                ).fetchall()
                self.assertEqual(inventory_rows, [("flat", 1), ("long_light", 1)])
                win_loss_rows = conn.execute(
                    """
                    SELECT
                        side,
                        inventory_state_bucket,
                        final_outcome_bucket,
                        final_markout_sign_bucket,
                        markout_300s_sign_bucket,
                        fill_count,
                        ROUND(avg_markout_final, 3)
                    FROM analysis_win_loss_clusters
                    ORDER BY side
                    """
                ).fetchall()
                self.assertEqual(
                    win_loss_rows,
                    [
                        ("BUY", "flat", "win", "final_markout:positive", "markout_300s:negative", 1, 0.5),
                        ("SELL", "long_light", "win", "final_markout:negative", "markout_300s:positive", 1, -0.4),
                    ],
                )
                fill_context_rows = conn.execute(
                    """
                    SELECT
                        side,
                        proxy_context,
                        latest_drawdown_bucket,
                        latest_exposure_bucket,
                        latest_pending_settlement_bucket,
                        markout_deterioration_bucket
                    FROM analysis_circuit_breaker_fill_context
                    ORDER BY side
                    """
                ).fetchall()
                self.assertEqual(
                    fill_context_rows,
                    [
                        ("BUY", "latest_status_snapshot", "drawdown:8-15%", "exposure:50-100", "pending_settlement:moderate", "deterioration:3-10c"),
                        ("SELL", "latest_status_snapshot", "drawdown:8-15%", "exposure:50-100", "pending_settlement:moderate", "deterioration:<3c"),
                    ],
                )
                precursor_rows = conn.execute(
                    """
                    SELECT
                        side,
                        proxy_context,
                        latest_drawdown_bucket,
                        latest_exposure_bucket,
                        exposure_concentration_bucket,
                        latest_pending_settlement_bucket,
                        expiry_risk_bucket,
                        markout_path_bucket,
                        markout_deterioration_bucket,
                        fill_count,
                        ROUND(avg_max_drawdown, 3),
                        ROUND(avg_total_gross_exposure, 1),
                        ROUND(avg_pending_settlement_exposure, 1),
                        adverse_path_fill_count
                    FROM analysis_circuit_breaker_precursors
                    ORDER BY side
                    """
                ).fetchall()
                self.assertEqual(
                    precursor_rows,
                    [
                        ("BUY", "latest_status_snapshot", "drawdown:8-15%", "exposure:50-100", "concentration:flat", "pending_settlement:moderate", "expiry_risk:mid", "markout_path:reversal", "deterioration:3-10c", 1, 0.09, 55.4, 6.4, 1),
                        ("SELL", "latest_status_snapshot", "drawdown:8-15%", "exposure:50-100", "concentration:50-75%", "pending_settlement:moderate", "expiry_risk:early", "markout_path:stable_or_improving", "deterioration:<3c", 1, 0.09, 55.4, 6.4, 0),
                    ],
                )
                validation_rows = conn.execute(
                    "SELECT check_name, passed FROM analysis_validation_checks ORDER BY check_name"
                ).fetchall()
                self.assertTrue(validation_rows)
                self.assertTrue(all(passed for _, passed in validation_rows))

                recommendation_columns = {
                    row[0]
                    for row in conn.execute(
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'main' AND table_name LIKE 'analysis_%'"
                    ).fetchall()
                }
                self.assertNotIn("top_recommendation", recommendation_columns)
                self.assertNotIn("next_actions_json", recommendation_columns)
                self.assertNotIn("adverse_selection_score", recommendation_columns)
                self.assertTrue(
                    {"markout_30s", "markout_60s", "markout_120s", "markout_300s", "markout_final"}.issubset(
                        recommendation_columns
                    )
                )

    def test_build_analysis_views_uses_deterministic_quotes_and_deduped_settlements(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            self._append_runtime_events(
                runtime_dir,
                {
                    "ts": 143.0,
                    "event_type": "quote.submitted",
                    "payload": {
                        "market_id": "mkt-1",
                        "market_slug": "btc-updown",
                        "slot_id": "btc:5:300",
                        "strategy_family": "toxicity_mm",
                        "bid_order_id": "order-11",
                        "ask_order_id": "order-12",
                        "book_quality": {
                            "outcome": "Up",
                            "mid_price": 0.53,
                            "spread_bps": 240.0,
                            "top_depth": 8.0,
                            "top_notional": 4.24,
                            "depth_ratio": 1.1,
                            "is_tradeable": True,
                        },
                    },
                },
                {
                    "ts": 143.0,
                    "event_type": "quote.submitted",
                    "payload": {
                        "market_id": "mkt-1",
                        "market_slug": "btc-updown",
                        "slot_id": "btc:5:300",
                        "strategy_family": "toxicity_mm",
                        "bid_order_id": "order-13",
                        "ask_order_id": "order-14",
                        "book_quality": {
                            "outcome": "Up",
                            "mid_price": 0.57,
                            "spread_bps": 245.0,
                            "top_depth": 13.0,
                            "top_notional": 7.41,
                            "depth_ratio": 1.7,
                            "is_tradeable": True,
                        },
                    },
                },
            )
            self._insert_ledger_event(
                runtime_dir,
                (
                    "evt-3",
                    "slot",
                    "mkt-1",
                    3,
                    "slot_settled",
                    500.0,
                    500.0,
                    "paper-test-run",
                    "slot_settled:mkt-1:duplicate",
                    None,
                    "corr-1",
                    1,
                    json.dumps({"market_id": "mkt-1", "winning_outcome": "Up"}),
                ),
            )

            build_duckdb(runtime_dir=runtime_dir, research_path=research_path, duckdb_path=duckdb_path)
            build_analysis_views(duckdb_path=duckdb_path)

            with duckdb.connect(str(duckdb_path)) as conn:
                fill_count = conn.execute("SELECT COUNT(*) FROM analysis_fill_markouts").fetchone()[0]
                self.assertEqual(fill_count, 2)
                deterministic_markout = conn.execute(
                    """
                    SELECT future_mid_30s, ROUND(markout_30s, 3)
                    FROM analysis_fill_markouts
                    WHERE side = 'BUY'
                    """
                ).fetchone()
                self.assertEqual(deterministic_markout, (0.53, 0.03))
                deterministic_liquidity = conn.execute(
                    """
                    SELECT quote_ts, spread_bps, top_depth, depth_ratio
                    FROM analysis_fill_liquidity_context
                    WHERE side = 'SELL'
                    """
                ).fetchone()
                self.assertEqual(deterministic_liquidity, (115.0, 650.0, 4.0, 4.2))

    def test_build_analysis_views_requires_existing_batch3_tables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            duckdb_path = Path(tmpdir) / "orphan.duckdb"
            with duckdb.connect(str(duckdb_path)) as conn:
                conn.execute("CREATE TABLE unrelated_table (id INTEGER)")

            with self.assertRaisesRegex(ValueError, r"missing required Batch 3 base tables"):
                build_analysis_views(duckdb_path=duckdb_path)

    def test_build_duckdb_is_idempotent_full_refresh(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))

            build_duckdb(runtime_dir=runtime_dir, research_path=research_path, duckdb_path=duckdb_path)
            (runtime_dir / "events.jsonl").unlink()
            (runtime_dir / "market_samples.jsonl").write_text(
                json.dumps(
                    {
                        "ts": 999.0,
                        "run_id": "paper-test-run",
                        "market_id": "mkt-3",
                        "market_slug": "sol-updown",
                        "slot_id": "sol:5:1",
                        "is_tradeable": True,
                        "book_depth": 20.0,
                        "book_notional": 12.0,
                        "book_spread_bps": 150.0,
                        "book_reasons": [],
                        "volume": 10.0,
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            build_duckdb(runtime_dir=runtime_dir, research_path=research_path, duckdb_path=duckdb_path)

            with duckdb.connect(str(duckdb_path)) as conn:
                runtime_event_count = conn.execute("SELECT COUNT(*) FROM runtime_events").fetchone()[0]
                market_sample_ids = conn.execute(
                    "SELECT market_id FROM market_samples ORDER BY line_number"
                ).fetchall()
                self.assertEqual(runtime_event_count, 0)
                self.assertEqual(market_sample_ids, [("mkt-3",)])

    def test_missing_optional_events_file_still_creates_empty_runtime_events_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            (runtime_dir / "events.jsonl").unlink()

            result = build_duckdb(
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
            )

            self.assertEqual(result["runtime_events_rows"], 0)
            with duckdb.connect(str(duckdb_path)) as conn:
                runtime_event_count = conn.execute("SELECT COUNT(*) FROM runtime_events").fetchone()[0]
                optional_source = conn.execute(
                    "SELECT exists_on_disk FROM ingestion_sources WHERE source_name = 'runtime_events'"
                ).fetchone()[0]
                self.assertEqual(runtime_event_count, 0)
                self.assertFalse(optional_source)

    def test_missing_optional_market_samples_file_still_creates_empty_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            (runtime_dir / "market_samples.jsonl").unlink()

            result = build_duckdb(
                runtime_dir=runtime_dir,
                research_path=research_path,
                duckdb_path=duckdb_path,
            )

            self.assertEqual(result["market_samples_rows"], 0)
            with duckdb.connect(str(duckdb_path)) as conn:
                market_sample_count = conn.execute("SELECT COUNT(*) FROM market_samples").fetchone()[0]
                optional_source = conn.execute(
                    "SELECT exists_on_disk FROM ingestion_sources WHERE source_name = 'market_samples'"
                ).fetchone()[0]
                self.assertEqual(market_sample_count, 0)
                self.assertFalse(optional_source)

    def test_non_object_jsonl_row_raises_clear_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            (runtime_dir / "events.jsonl").write_text('[]\n', encoding="utf-8")

            with self.assertRaisesRegex(ValueError, r"events\.jsonl:1"):
                build_duckdb(
                    runtime_dir=runtime_dir,
                    research_path=research_path,
                    duckdb_path=duckdb_path,
                )

    def test_invalid_ledger_schema_raises_clear_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            ledger_path = runtime_dir / "ledger.db"
            ledger_path.unlink()
            with sqlite3.connect(ledger_path) as conn:
                conn.execute("CREATE TABLE wrong_table (id TEXT PRIMARY KEY)")
                conn.commit()

            with self.assertRaisesRegex(ValueError, r"missing ledger_events table"):
                build_duckdb(
                    runtime_dir=runtime_dir,
                    research_path=research_path,
                    duckdb_path=duckdb_path,
                )

    def test_non_object_ledger_payload_raises_clear_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            runtime_dir, research_path, duckdb_path = self._write_fixture_tree(Path(tmpdir))
            ledger_path = runtime_dir / "ledger.db"
            with sqlite3.connect(ledger_path) as conn:
                conn.execute("UPDATE ledger_events SET payload_json = ? WHERE event_id = ?", ('[]', 'evt-1'))
                conn.commit()

            with self.assertRaisesRegex(ValueError, r"ledger\.db:ledger_events:evt-1"):
                build_duckdb(
                    runtime_dir=runtime_dir,
                    research_path=research_path,
                    duckdb_path=duckdb_path,
                )


if __name__ == "__main__":
    unittest.main()
