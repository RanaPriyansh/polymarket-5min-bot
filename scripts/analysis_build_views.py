from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb


REQUIRED_BASE_TABLES = (
    "ledger_events",
    "market_samples",
    "runtime_events",
)


def _replace_view(conn: duckdb.DuckDBPyConnection, view_name: str, sql: str) -> None:
    conn.execute(f"DROP VIEW IF EXISTS {view_name}")
    conn.execute(f"CREATE VIEW {view_name} AS {sql}")


def _ensure_base_tables(conn: duckdb.DuckDBPyConnection) -> None:
    available = {name for (name,) in conn.execute("SHOW TABLES").fetchall()}
    missing = [name for name in REQUIRED_BASE_TABLES if name not in available]
    if missing:
        raise ValueError(
            "DuckDB is missing required Batch 3 base tables for Batch 4 views: " + ", ".join(missing)
        )


def build_analysis_views(*, duckdb_path: str | Path = "data/analysis/base.duckdb") -> dict[str, Any]:
    duckdb_path = Path(duckdb_path)
    if not duckdb_path.exists():
        raise FileNotFoundError(f"DuckDB not found: {duckdb_path}")

    with duckdb.connect(str(duckdb_path)) as conn:
        _ensure_base_tables(conn)
        _replace_view(
            conn,
            "analysis_slot_dimensions",
            """
            WITH slot_ids AS (
                SELECT DISTINCT slot_id FROM runtime_events WHERE slot_id IS NOT NULL
                UNION
                SELECT DISTINCT slot_id FROM ledger_events WHERE slot_id IS NOT NULL
                UNION
                SELECT DISTINCT slot_id FROM market_samples WHERE slot_id IS NOT NULL
            )
            SELECT
                slot_id,
                NULLIF(split_part(slot_id, ':', 1), '') AS asset,
                TRY_CAST(NULLIF(split_part(slot_id, ':', 2), '') AS BIGINT) AS interval_minutes,
                TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS BIGINT) AS expiry_ts
            FROM slot_ids
            WHERE slot_id IS NOT NULL AND slot_id <> ''
            """,
        )
        _replace_view(
            conn,
            "analysis_quote_midpoints",
            """
            SELECT
                line_number,
                ts,
                run_id,
                market_id,
                market_slug,
                slot_id,
                strategy_family,
                COALESCE(json_extract_string(raw_json, '$.payload.book_quality.outcome'), outcome, 'Up') AS outcome,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.mid_price') AS DOUBLE) AS mid_price,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.spread_bps') AS DOUBLE) AS spread_bps,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.top_depth') AS DOUBLE) AS top_depth,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.top_notional') AS DOUBLE) AS top_notional,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.depth_ratio') AS DOUBLE) AS depth_ratio,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.is_tradeable') AS BOOLEAN) AS is_tradeable,
                NULLIF(split_part(slot_id, ':', 1), '') AS asset,
                TRY_CAST(NULLIF(split_part(slot_id, ':', 2), '') AS BIGINT) AS interval_minutes,
                TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS BIGINT) AS expiry_ts
            FROM runtime_events
            WHERE event_type = 'quote.submitted'
              AND json_extract(raw_json, '$.payload.book_quality.mid_price') IS NOT NULL
            """,
        )
        _replace_view(
            conn,
            "analysis_fill_events",
            """
            WITH fills AS (
                SELECT
                    line_number,
                    ts AS fill_ts,
                    run_id,
                    market_id,
                    market_slug,
                    slot_id,
                    strategy_family,
                    COALESCE(side, json_extract_string(raw_json, '$.payload.side')) AS side,
                    COALESCE(outcome, json_extract_string(raw_json, '$.payload.outcome')) AS outcome,
                    order_id,
                    TRY_CAST(COALESCE(json_extract_string(raw_json, '$.payload.fill_price'), json_extract_string(raw_json, '$.payload.price')) AS DOUBLE) AS fill_price,
                    TRY_CAST(COALESCE(json_extract_string(raw_json, '$.payload.size'), json_extract_string(raw_json, '$.payload.fill_size')) AS DOUBLE) AS fill_size,
                    TRY_CAST(json_extract_string(raw_json, '$.payload.realized_pnl_delta') AS DOUBLE) AS realized_pnl_delta,
                    TRY_CAST(json_extract_string(raw_json, '$.payload.time_to_expiry_seconds') AS DOUBLE) AS payload_tte_seconds,
                    json_extract_string(raw_json, '$.payload.tte_bucket') AS payload_tte_bucket,
                    raw_json
                FROM runtime_events
                WHERE event_type = 'order.filled'
            )
            SELECT
                line_number,
                fill_ts,
                run_id,
                market_id,
                market_slug,
                slot_id,
                strategy_family,
                UPPER(COALESCE(side, 'UNKNOWN')) AS side,
                outcome,
                order_id,
                fill_price,
                fill_size,
                realized_pnl_delta,
                NULLIF(split_part(slot_id, ':', 1), '') AS asset,
                TRY_CAST(NULLIF(split_part(slot_id, ':', 2), '') AS BIGINT) AS interval_minutes,
                TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS BIGINT) AS expiry_ts,
                COALESCE(payload_tte_seconds, TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS DOUBLE) - fill_ts) AS time_to_expiry_seconds,
                COALESCE(
                    payload_tte_bucket,
                    CASE
                        WHEN COALESCE(payload_tte_seconds, TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS DOUBLE) - fill_ts) IS NULL THEN 'unknown'
                        WHEN COALESCE(payload_tte_seconds, TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS DOUBLE) - fill_ts) < 0 THEN 'expired'
                        WHEN COALESCE(payload_tte_seconds, TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS DOUBLE) - fill_ts) < 60 THEN '<60s'
                        WHEN COALESCE(payload_tte_seconds, TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS DOUBLE) - fill_ts) <= 120 THEN '60-120s'
                        WHEN COALESCE(payload_tte_seconds, TRY_CAST(NULLIF(split_part(slot_id, ':', 3), '') AS DOUBLE) - fill_ts) <= 300 THEN '120-300s'
                        ELSE '>300s'
                    END
                ) AS tte_bucket,
                raw_json
            FROM fills
            """,
        )
        _replace_view(
            conn,
            "analysis_fill_markouts",
            """
            WITH settlements AS (
                SELECT market_id, winning_outcome
                FROM (
                    SELECT
                        market_id,
                        json_extract_string(raw_json, '$.winning_outcome') AS winning_outcome,
                        ROW_NUMBER() OVER (
                            PARTITION BY market_id
                            ORDER BY event_ts DESC NULLS LAST, sequence_num DESC NULLS LAST, recorded_ts DESC NULLS LAST, event_id DESC
                        ) AS settlement_rank
                    FROM ledger_events
                    WHERE event_type = 'slot_settled'
                      AND json_extract(raw_json, '$.winning_outcome') IS NOT NULL
                ) ranked_settlements
                WHERE settlement_rank = 1
            ),
            base AS (
                SELECT
                    f.*,
                    (
                        SELECT q.mid_price FROM analysis_quote_midpoints q
                        WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts >= f.fill_ts + 30
                        ORDER BY q.ts ASC, q.line_number ASC LIMIT 1
                    ) AS future_mid_30s,
                    (
                        SELECT q.mid_price FROM analysis_quote_midpoints q
                        WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts >= f.fill_ts + 60
                        ORDER BY q.ts ASC, q.line_number ASC LIMIT 1
                    ) AS future_mid_60s,
                    (
                        SELECT q.mid_price FROM analysis_quote_midpoints q
                        WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts >= f.fill_ts + 120
                        ORDER BY q.ts ASC, q.line_number ASC LIMIT 1
                    ) AS future_mid_120s,
                    (
                        SELECT q.mid_price FROM analysis_quote_midpoints q
                        WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts >= f.fill_ts + 300
                        ORDER BY q.ts ASC, q.line_number ASC LIMIT 1
                    ) AS future_mid_300s,
                    s.winning_outcome,
                    CASE WHEN s.winning_outcome IS NULL THEN NULL ELSE CASE WHEN f.outcome = s.winning_outcome THEN 1.0 ELSE 0.0 END END AS settlement_price
                FROM analysis_fill_events f
                LEFT JOIN settlements s USING (market_id)
            )
            SELECT
                *,
                CASE
                    WHEN future_mid_30s IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN future_mid_30s - fill_price
                    ELSE fill_price - future_mid_30s
                END AS markout_30s,
                CASE
                    WHEN future_mid_60s IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN future_mid_60s - fill_price
                    ELSE fill_price - future_mid_60s
                END AS markout_60s,
                CASE
                    WHEN future_mid_120s IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN future_mid_120s - fill_price
                    ELSE fill_price - future_mid_120s
                END AS markout_120s,
                CASE
                    WHEN future_mid_300s IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN future_mid_300s - fill_price
                    ELSE fill_price - future_mid_300s
                END AS markout_300s,
                CASE
                    WHEN winning_outcome IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN settlement_price - fill_price
                    ELSE fill_price - settlement_price
                END AS markout_final,
                -- Batch 4 exposes deterministic horizon-by-horizon markouts instead of a
                -- single adverse_selection_score so downstream consumers can compute their
                -- preferred score without adding opinionated aggregation here.
                CASE
                    WHEN future_mid_30s IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN future_mid_30s < fill_price
                    ELSE future_mid_30s > fill_price
                END AS adverse_after_30s,
                CASE
                    WHEN future_mid_60s IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN future_mid_60s < fill_price
                    ELSE future_mid_60s > fill_price
                END AS adverse_after_60s,
                CASE
                    WHEN future_mid_120s IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN future_mid_120s < fill_price
                    ELSE future_mid_120s > fill_price
                END AS adverse_after_120s,
                CASE
                    WHEN future_mid_300s IS NULL OR fill_price IS NULL THEN NULL
                    WHEN side = 'BUY' THEN future_mid_300s < fill_price
                    ELSE future_mid_300s > fill_price
                END AS adverse_after_300s
            FROM base
            """,
        )
        _replace_view(
            conn,
            "analysis_pnl_by_asset_interval_side",
            """
            SELECT
                asset,
                interval_minutes,
                side,
                COUNT(*) AS fill_count,
                SUM(COALESCE(fill_size, 0.0)) AS gross_fill_size,
                SUM(COALESCE(realized_pnl_delta, 0.0)) AS realized_pnl_delta_sum,
                AVG(fill_price) AS avg_fill_price
            FROM analysis_fill_events
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """,
        )
        _replace_view(
            conn,
            "analysis_time_to_expiry_buckets",
            """
            SELECT
                tte_bucket,
                COUNT(*) AS fill_count,
                SUM(COALESCE(fill_size, 0.0)) AS gross_fill_size,
                SUM(COALESCE(realized_pnl_delta, 0.0)) AS realized_pnl_delta_sum,
                AVG(markout_30s) AS avg_markout_30s,
                AVG(markout_60s) AS avg_markout_60s,
                AVG(markout_120s) AS avg_markout_120s,
                AVG(markout_300s) AS avg_markout_300s,
                AVG(markout_final) AS avg_markout_final
            FROM analysis_fill_markouts
            GROUP BY 1
            ORDER BY CASE tte_bucket
                WHEN 'expired' THEN 0
                WHEN '<60s' THEN 1
                WHEN '60-120s' THEN 2
                WHEN '120-300s' THEN 3
                WHEN '>300s' THEN 4
                ELSE 5
            END, tte_bucket
            """,
        )
        _replace_view(
            conn,
            "analysis_fill_liquidity_context",
            """
            SELECT
                f.*,
                (
                    SELECT q.ts FROM analysis_quote_midpoints q
                    WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts <= f.fill_ts
                    ORDER BY q.ts DESC, q.line_number DESC LIMIT 1
                ) AS quote_ts,
                (
                    SELECT q.spread_bps FROM analysis_quote_midpoints q
                    WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts <= f.fill_ts
                    ORDER BY q.ts DESC, q.line_number DESC LIMIT 1
                ) AS spread_bps,
                (
                    SELECT q.top_depth FROM analysis_quote_midpoints q
                    WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts <= f.fill_ts
                    ORDER BY q.ts DESC, q.line_number DESC LIMIT 1
                ) AS top_depth,
                (
                    SELECT q.depth_ratio FROM analysis_quote_midpoints q
                    WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts <= f.fill_ts
                    ORDER BY q.ts DESC, q.line_number DESC LIMIT 1
                ) AS depth_ratio
            FROM analysis_fill_markouts f
            """,
        )
        _replace_view(
            conn,
            "analysis_spread_depth_imbalance_buckets",
            """
            WITH bucketed AS (
                SELECT
                    *,
                    CASE
                        WHEN spread_bps IS NULL THEN 'unknown'
                        WHEN spread_bps < 250 THEN 'spread:<250bps'
                        WHEN spread_bps < 500 THEN 'spread:250-500bps'
                        WHEN spread_bps < 1000 THEN 'spread:500-1000bps'
                        ELSE 'spread:1000bps+'
                    END AS spread_bucket,
                    CASE
                        WHEN top_depth IS NULL THEN 'depth:unknown'
                        WHEN top_depth < 5 THEN 'depth:<5'
                        WHEN top_depth < 20 THEN 'depth:5-20'
                        WHEN top_depth < 100 THEN 'depth:20-100'
                        ELSE 'depth:100+'
                    END AS depth_bucket,
                    CASE
                        WHEN depth_ratio IS NULL THEN 'imbalance:unknown'
                        WHEN depth_ratio <= 1.5 THEN 'imbalance:balanced'
                        WHEN depth_ratio <= 3.0 THEN 'imbalance:moderate'
                        ELSE 'imbalance:skewed'
                    END AS imbalance_bucket
                FROM analysis_fill_liquidity_context
            )
            SELECT
                spread_bucket,
                depth_bucket,
                imbalance_bucket,
                COUNT(*) AS fill_count,
                SUM(COALESCE(fill_size, 0.0)) AS gross_fill_size,
                SUM(COALESCE(realized_pnl_delta, 0.0)) AS realized_pnl_delta_sum,
                AVG(markout_30s) AS avg_markout_30s,
                AVG(markout_60s) AS avg_markout_60s,
                AVG(markout_120s) AS avg_markout_120s,
                AVG(markout_300s) AS avg_markout_300s,
                AVG(markout_final) AS avg_markout_final
            FROM bucketed
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """,
        )
        _replace_view(
            conn,
            "analysis_inventory_state_buckets",
            """
            WITH ordered_fills AS (
                SELECT
                    *,
                    COALESCE(
                        SUM(CASE WHEN side = 'BUY' THEN COALESCE(fill_size, 0.0) ELSE -COALESCE(fill_size, 0.0) END)
                            OVER (
                                PARTITION BY strategy_family, market_id, outcome
                                ORDER BY fill_ts ASC, line_number ASC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                            ),
                        0.0
                    ) AS inventory_before_fill
                FROM analysis_fill_markouts
            ),
            bucketed AS (
                SELECT
                    *,
                    CASE
                        WHEN ABS(inventory_before_fill) < 1e-9 THEN 'flat'
                        WHEN inventory_before_fill > 0 AND ABS(inventory_before_fill) <= 5 THEN 'long_light'
                        WHEN inventory_before_fill > 0 AND ABS(inventory_before_fill) <= 15 THEN 'long_medium'
                        WHEN inventory_before_fill > 0 THEN 'long_heavy'
                        WHEN inventory_before_fill < 0 AND ABS(inventory_before_fill) <= 5 THEN 'short_light'
                        WHEN inventory_before_fill < 0 AND ABS(inventory_before_fill) <= 15 THEN 'short_medium'
                        ELSE 'short_heavy'
                    END AS inventory_state_bucket
                FROM ordered_fills
            )
            SELECT
                inventory_state_bucket,
                COUNT(*) AS fill_count,
                AVG(inventory_before_fill) AS avg_inventory_before_fill,
                SUM(COALESCE(fill_size, 0.0)) AS gross_fill_size,
                SUM(COALESCE(realized_pnl_delta, 0.0)) AS realized_pnl_delta_sum,
                AVG(markout_30s) AS avg_markout_30s,
                AVG(markout_60s) AS avg_markout_60s,
                AVG(markout_120s) AS avg_markout_120s,
                AVG(markout_300s) AS avg_markout_300s,
                AVG(markout_final) AS avg_markout_final
            FROM bucketed
            GROUP BY 1
            ORDER BY 1
            """,
        )
        _replace_view(
            conn,
            "analysis_fill_cluster_features",
            """
            WITH ordered_fills AS (
                SELECT
                    f.*,
                    COALESCE(
                        SUM(CASE WHEN side = 'BUY' THEN COALESCE(fill_size, 0.0) ELSE -COALESCE(fill_size, 0.0) END)
                            OVER (
                                PARTITION BY strategy_family, market_id, outcome
                                ORDER BY fill_ts ASC, line_number ASC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                            ),
                        0.0
                    ) AS inventory_before_fill
                FROM analysis_fill_liquidity_context f
            )
            SELECT
                *,
                CASE
                    WHEN spread_bps IS NULL THEN 'spread:unknown'
                    WHEN spread_bps < 250 THEN 'spread:<250bps'
                    WHEN spread_bps < 500 THEN 'spread:250-500bps'
                    WHEN spread_bps < 1000 THEN 'spread:500-1000bps'
                    ELSE 'spread:1000bps+'
                END AS spread_bucket,
                CASE
                    WHEN top_depth IS NULL THEN 'depth:unknown'
                    WHEN top_depth < 5 THEN 'depth:<5'
                    WHEN top_depth < 20 THEN 'depth:5-20'
                    WHEN top_depth < 100 THEN 'depth:20-100'
                    ELSE 'depth:100+'
                END AS depth_bucket,
                CASE
                    WHEN depth_ratio IS NULL THEN 'imbalance:unknown'
                    WHEN depth_ratio <= 1.5 THEN 'imbalance:balanced'
                    WHEN depth_ratio <= 3.0 THEN 'imbalance:moderate'
                    ELSE 'imbalance:skewed'
                END AS imbalance_bucket,
                CASE
                    WHEN ABS(inventory_before_fill) < 1e-9 THEN 'flat'
                    WHEN inventory_before_fill > 0 AND ABS(inventory_before_fill) <= 5 THEN 'long_light'
                    WHEN inventory_before_fill > 0 AND ABS(inventory_before_fill) <= 15 THEN 'long_medium'
                    WHEN inventory_before_fill > 0 THEN 'long_heavy'
                    WHEN inventory_before_fill < 0 AND ABS(inventory_before_fill) <= 5 THEN 'short_light'
                    WHEN inventory_before_fill < 0 AND ABS(inventory_before_fill) <= 15 THEN 'short_medium'
                    ELSE 'short_heavy'
                END AS inventory_state_bucket,
                CASE
                    WHEN winning_outcome IS NULL THEN 'pending'
                    WHEN outcome = winning_outcome THEN 'win'
                    ELSE 'loss'
                END AS final_outcome_bucket,
                CASE
                    WHEN markout_final IS NULL THEN 'final_markout:unknown'
                    WHEN markout_final > 0 THEN 'final_markout:positive'
                    WHEN markout_final < 0 THEN 'final_markout:negative'
                    ELSE 'final_markout:flat'
                END AS final_markout_sign_bucket,
                CASE
                    WHEN markout_300s IS NULL THEN 'markout_300s:unknown'
                    WHEN markout_300s > 0 THEN 'markout_300s:positive'
                    WHEN markout_300s < 0 THEN 'markout_300s:negative'
                    ELSE 'markout_300s:flat'
                END AS markout_300s_sign_bucket
            FROM ordered_fills
            """,
        )
        _replace_view(
            conn,
            "analysis_win_loss_clusters",
            """
            SELECT
                asset,
                interval_minutes,
                side,
                spread_bucket,
                depth_bucket,
                imbalance_bucket,
                inventory_state_bucket,
                tte_bucket,
                final_outcome_bucket,
                final_markout_sign_bucket,
                markout_300s_sign_bucket,
                COUNT(*) AS fill_count,
                SUM(COALESCE(fill_size, 0.0)) AS gross_fill_size,
                SUM(COALESCE(realized_pnl_delta, 0.0)) AS realized_pnl_delta_sum,
                AVG(fill_price) AS avg_fill_price,
                AVG(markout_30s) AS avg_markout_30s,
                AVG(markout_60s) AS avg_markout_60s,
                AVG(markout_120s) AS avg_markout_120s,
                AVG(markout_300s) AS avg_markout_300s,
                AVG(markout_final) AS avg_markout_final,
                SUM(CASE WHEN COALESCE(markout_final, 0.0) > 0 THEN 1 ELSE 0 END) AS positive_final_markout_count,
                SUM(CASE WHEN COALESCE(markout_final, 0.0) < 0 THEN 1 ELSE 0 END) AS negative_final_markout_count
            FROM analysis_fill_cluster_features
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
            ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
            """,
        )
        _replace_view(
            conn,
            "analysis_circuit_breaker_fill_context",
            """
            WITH latest_status AS (
                SELECT *
                FROM runtime_status_snapshot
                ORDER BY heartbeat_ts DESC NULLS LAST, ingest_ts DESC NULLS LAST, source_mtime DESC NULLS LAST
                LIMIT 1
            ),
            status_metrics AS (
                SELECT
                    run_id AS status_run_id,
                    COALESCE(TRY_CAST(json_extract_string(raw_json, '$.risk.max_drawdown') AS DOUBLE), 0.0) AS max_drawdown,
                    COALESCE(risk_total_gross_exposure, 0.0) AS total_gross_exposure,
                    COALESCE(risk_pending_settlement_count, 0) AS pending_settlement_count,
                    COALESCE(TRY_CAST(json_extract_string(raw_json, '$.risk.pending_settlement_exposure') AS DOUBLE), 0.0) AS pending_settlement_exposure
                FROM latest_status
            )
            SELECT
                f.*,
                'latest_status_snapshot' AS proxy_context,
                s.status_run_id,
                s.max_drawdown,
                s.total_gross_exposure,
                s.pending_settlement_count,
                s.pending_settlement_exposure,
                COALESCE(
                    ABS(f.inventory_before_fill) / NULLIF(
                        SUM(COALESCE(f.fill_size, 0.0)) OVER (PARTITION BY f.strategy_family, f.market_id, f.outcome),
                        0.0
                    ),
                    0.0
                ) AS inventory_concentration_ratio,
                CASE
                    WHEN s.max_drawdown >= 0.15 THEN 'drawdown:15%+'
                    WHEN s.max_drawdown >= 0.08 THEN 'drawdown:8-15%'
                    WHEN s.max_drawdown >= 0.03 THEN 'drawdown:3-8%'
                    ELSE 'drawdown:<3%'
                END AS latest_drawdown_bucket,
                CASE
                    WHEN s.total_gross_exposure >= 100 THEN 'exposure:100+'
                    WHEN s.total_gross_exposure >= 50 THEN 'exposure:50-100'
                    WHEN s.total_gross_exposure >= 10 THEN 'exposure:10-50'
                    ELSE 'exposure:<10'
                END AS latest_exposure_bucket,
                CASE
                    WHEN COALESCE(
                        ABS(f.inventory_before_fill) / NULLIF(
                            SUM(COALESCE(f.fill_size, 0.0)) OVER (PARTITION BY f.strategy_family, f.market_id, f.outcome),
                            0.0
                        ),
                        0.0
                    ) >= 0.75 THEN 'concentration:75%+'
                    WHEN COALESCE(
                        ABS(f.inventory_before_fill) / NULLIF(
                            SUM(COALESCE(f.fill_size, 0.0)) OVER (PARTITION BY f.strategy_family, f.market_id, f.outcome),
                            0.0
                        ),
                        0.0
                    ) >= 0.50 THEN 'concentration:50-75%'
                    WHEN COALESCE(
                        ABS(f.inventory_before_fill) / NULLIF(
                            SUM(COALESCE(f.fill_size, 0.0)) OVER (PARTITION BY f.strategy_family, f.market_id, f.outcome),
                            0.0
                        ),
                        0.0
                    ) > 0 THEN 'concentration:<50%'
                    ELSE 'concentration:flat'
                END AS exposure_concentration_bucket,
                CASE
                    WHEN s.pending_settlement_count >= 5 OR s.pending_settlement_exposure >= 25 THEN 'pending_settlement:high'
                    WHEN s.pending_settlement_count >= 2 OR s.pending_settlement_exposure >= 5 THEN 'pending_settlement:moderate'
                    ELSE 'pending_settlement:low'
                END AS latest_pending_settlement_bucket,
                CASE
                    WHEN markout_30s IS NULL OR markout_300s IS NULL THEN 'markout_path:unknown'
                    WHEN markout_30s > 0 AND markout_300s < 0 THEN 'markout_path:reversal'
                    WHEN markout_30s <= 0 AND markout_300s < markout_30s THEN 'markout_path:deteriorating'
                    WHEN markout_30s <= 0 THEN 'markout_path:adverse_early'
                    ELSE 'markout_path:stable_or_improving'
                END AS markout_path_bucket,
                CASE
                    WHEN tte_bucket IN ('expired', '<60s', '60-120s') THEN 'expiry_risk:late'
                    WHEN tte_bucket = '120-300s' THEN 'expiry_risk:mid'
                    WHEN tte_bucket = '>300s' THEN 'expiry_risk:early'
                    ELSE 'expiry_risk:unknown'
                END AS expiry_risk_bucket,
                CASE
                    WHEN markout_30s IS NULL OR markout_300s IS NULL THEN 'deterioration:unknown'
                    WHEN (markout_30s - markout_300s) >= 0.10 THEN 'deterioration:>=10c'
                    WHEN (markout_30s - markout_300s) >= 0.03 THEN 'deterioration:3-10c'
                    ELSE 'deterioration:<3c'
                END AS markout_deterioration_bucket
            FROM analysis_fill_cluster_features f
            CROSS JOIN status_metrics s
            """,
        )
        _replace_view(
            conn,
            "analysis_circuit_breaker_precursors",
            """
            SELECT
                asset,
                interval_minutes,
                side,
                proxy_context,
                latest_drawdown_bucket,
                latest_exposure_bucket,
                exposure_concentration_bucket,
                latest_pending_settlement_bucket,
                inventory_state_bucket,
                expiry_risk_bucket,
                markout_path_bucket,
                markout_deterioration_bucket,
                COUNT(*) AS fill_count,
                SUM(COALESCE(fill_size, 0.0)) AS gross_fill_size,
                SUM(COALESCE(realized_pnl_delta, 0.0)) AS realized_pnl_delta_sum,
                AVG(markout_30s) AS avg_markout_30s,
                AVG(markout_60s) AS avg_markout_60s,
                AVG(markout_120s) AS avg_markout_120s,
                AVG(markout_300s) AS avg_markout_300s,
                AVG(markout_final) AS avg_markout_final,
                AVG(max_drawdown) AS avg_max_drawdown,
                AVG(total_gross_exposure) AS avg_total_gross_exposure,
                AVG(pending_settlement_exposure) AS avg_pending_settlement_exposure,
                SUM(CASE WHEN markout_path_bucket IN ('markout_path:reversal', 'markout_path:deteriorating', 'markout_path:adverse_early') THEN 1 ELSE 0 END) AS adverse_path_fill_count
            FROM analysis_circuit_breaker_fill_context
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
            ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
            """,
        )
        _replace_view(
            conn,
            "analysis_validation_checks",
            """
            SELECT * FROM (
                SELECT
                    'fill_event_count_matches_runtime_events' AS check_name,
                    (SELECT COUNT(*) FROM analysis_fill_events) AS actual_value,
                    (SELECT COUNT(*) FROM runtime_events WHERE event_type = 'order.filled') AS expected_value,
                    (SELECT COUNT(*) FROM analysis_fill_events) = (SELECT COUNT(*) FROM runtime_events WHERE event_type = 'order.filled') AS passed,
                    'analysis_fill_events should be a 1:1 projection of runtime order.filled events' AS notes
                UNION ALL
                SELECT
                    'inventory_bucket_fill_count_matches_fill_events',
                    COALESCE((SELECT SUM(fill_count) FROM analysis_inventory_state_buckets), 0),
                    (SELECT COUNT(*) FROM analysis_fill_events),
                    COALESCE((SELECT SUM(fill_count) FROM analysis_inventory_state_buckets), 0) = (SELECT COUNT(*) FROM analysis_fill_events),
                    'inventory-state buckets should partition all fill events'
                UNION ALL
                SELECT
                    'pnl_rollup_matches_fill_sum',
                    COALESCE((SELECT SUM(realized_pnl_delta_sum) FROM analysis_pnl_by_asset_interval_side), 0.0),
                    COALESCE((SELECT SUM(realized_pnl_delta) FROM analysis_fill_events), 0.0),
                    COALESCE((SELECT SUM(realized_pnl_delta_sum) FROM analysis_pnl_by_asset_interval_side), 0.0) = COALESCE((SELECT SUM(realized_pnl_delta) FROM analysis_fill_events), 0.0),
                    'aggregated pnl rollup should equal the fill-level realized pnl sum'
                UNION ALL
                SELECT
                    'quote_midpoint_count_matches_runtime_events',
                    (SELECT COUNT(*) FROM analysis_quote_midpoints),
                    (SELECT COUNT(*) FROM runtime_events WHERE event_type = 'quote.submitted' AND json_extract(raw_json, '$.payload.book_quality.mid_price') IS NOT NULL),
                    (SELECT COUNT(*) FROM analysis_quote_midpoints) = (SELECT COUNT(*) FROM runtime_events WHERE event_type = 'quote.submitted' AND json_extract(raw_json, '$.payload.book_quality.mid_price') IS NOT NULL),
                    'analysis_quote_midpoints should cover all quote.submitted events with a usable mid_price'
                UNION ALL
                SELECT
                    'spread_depth_bucket_fill_count_matches_fill_events',
                    COALESCE((SELECT SUM(fill_count) FROM analysis_spread_depth_imbalance_buckets), 0),
                    (SELECT COUNT(*) FROM analysis_fill_events),
                    COALESCE((SELECT SUM(fill_count) FROM analysis_spread_depth_imbalance_buckets), 0) = (SELECT COUNT(*) FROM analysis_fill_events),
                    'spread/depth/imbalance buckets should partition all fill events'
                UNION ALL
                SELECT
                    'tte_bucket_fill_count_matches_fill_events',
                    COALESCE((SELECT SUM(fill_count) FROM analysis_time_to_expiry_buckets), 0),
                    (SELECT COUNT(*) FROM analysis_fill_events),
                    COALESCE((SELECT SUM(fill_count) FROM analysis_time_to_expiry_buckets), 0) = (SELECT COUNT(*) FROM analysis_fill_events),
                    'time-to-expiry buckets should partition all fill events'
                UNION ALL
                SELECT
                    'win_loss_clusters_fill_count_matches_fill_events',
                    COALESCE((SELECT SUM(fill_count) FROM analysis_win_loss_clusters), 0),
                    (SELECT COUNT(*) FROM analysis_fill_events),
                    COALESCE((SELECT SUM(fill_count) FROM analysis_win_loss_clusters), 0) = (SELECT COUNT(*) FROM analysis_fill_events),
                    'win/loss clusters should partition all fill events'
                UNION ALL
                SELECT
                    'circuit_breaker_precursors_fill_count_matches_fill_events',
                    COALESCE((SELECT SUM(fill_count) FROM analysis_circuit_breaker_precursors), 0),
                    (SELECT COUNT(*) FROM analysis_fill_events),
                    COALESCE((SELECT SUM(fill_count) FROM analysis_circuit_breaker_precursors), 0) = (SELECT COUNT(*) FROM analysis_fill_events),
                    'circuit-breaker precursor clusters should partition all fill events'
            )
            ORDER BY check_name
            """,
        )

        return {
            "duckdb_path": str(duckdb_path),
            "views_built": [
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
            "validation_failed": conn.execute(
                "SELECT COUNT(*) FROM analysis_validation_checks WHERE NOT passed"
            ).fetchone()[0],
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Batch 4/5 analytical DuckDB views on top of Batch 3 base tables.")
    parser.add_argument("--duckdb-path", default="data/analysis/base.duckdb")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = build_analysis_views(duckdb_path=args.duckdb_path)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())