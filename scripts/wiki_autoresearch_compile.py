from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from bisect import bisect_left

import duckdb

from research.gate import check_settlement_pnl_computable, count_run_lineage_fragmentation
from scripts.analysis_build_views import build_analysis_views
from scripts.analysis_ingest_duckdb import build_duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUNTIME_DIR = REPO_ROOT / "data" / "runtime"
DEFAULT_RESEARCH_PATH = REPO_ROOT / "data" / "research" / "latest.json"
DEFAULT_DUCKDB_PATH = REPO_ROOT / "data" / "analysis" / "base.duckdb"
DEFAULT_WIKI_ROOT = Path("/root/wiki/polymarket-5min-bot")

CURRENT_STATE_PATH = Path("00_overview/current_state.md")
CONTRADICTION_LOG_PATH = Path("08_decisions/contradiction_log.md")
EXPERIMENT_REGISTRY_PATH = Path("07_experiments/experiment_registry.md")


@dataclass(frozen=True)
class ContradictionEntry:
    key: str
    title: str
    status: str
    evidence_a: str
    evidence_b: str
    why_it_matters: str


@dataclass(frozen=True)
class ExperimentEntry:
    key: str
    title: str
    status: str
    objective: str
    evidence: tuple[str, ...]
    close_criteria: tuple[str, ...]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            payload = json.loads(raw_line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _read_existing_created(path: Path, fallback: str) -> str:
    if not path.exists():
        return fallback
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("created:"):
                return line.split(":", 1)[1].strip()
    except Exception:
        return fallback
    return fallback


def _iso_utc(ts: float | None) -> str:
    if ts is None:
        return "n/a"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _date_utc(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _fmt_float(value: Any, digits: int = 4) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def _fmt_int(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return str(int(value))
    except Exception:
        return "n/a"


def _safe_len(value: Any) -> int:
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    return 0


def _ensure_analysis(
    *,
    runtime_dir: Path,
    research_path: Path,
    duckdb_path: Path,
    refresh_analysis: bool,
) -> dict[str, Any]:
    if refresh_analysis:
        ingest_result = build_duckdb(
            runtime_dir=runtime_dir,
            research_path=research_path,
            duckdb_path=duckdb_path,
        )
        views_result = build_analysis_views(duckdb_path=duckdb_path)
        return {
            "duckdb_path": str(duckdb_path),
            "ingest": ingest_result,
            "views": views_result,
            "analysis_mode": "batch3_to_batch5_refresh",
        }
    if not _has_batch3_tables(duckdb_path):
        return {
            "duckdb_path": str(duckdb_path),
            "ingest": None,
            "views": None,
            "analysis_mode": "runtime_fallback_no_duckdb",
        }
    return {
        "duckdb_path": str(duckdb_path),
        "ingest": None,
        "views": None,
        "analysis_mode": "existing_duckdb",
    }


def _has_analysis_views(conn: duckdb.DuckDBPyConnection) -> bool:
    tables = {name for (name,) in conn.execute("SHOW TABLES").fetchall()}
    required = {
        "analysis_fill_markouts",
        "analysis_spread_depth_imbalance_buckets",
        "analysis_time_to_expiry_buckets",
        "analysis_circuit_breaker_precursors",
        "analysis_validation_checks",
    }
    return required.issubset(tables)


def _has_batch3_tables(duckdb_path: Path) -> bool:
    if not duckdb_path.exists():
        return False
    try:
        with duckdb.connect(str(duckdb_path)) as conn:
            tables = {name for (name,) in conn.execute("SHOW TABLES").fetchall()}
        return {"runtime_events", "market_samples", "ledger_events"}.issubset(tables)
    except Exception:
        return False


def _duckdb_row(conn: duckdb.DuckDBPyConnection, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    cursor = conn.execute(sql, params)
    row = cursor.fetchone()
    if row is None:
        return {}
    columns = [item[0] for item in cursor.description]
    return dict(zip(columns, row))


def _duckdb_rows(conn: duckdb.DuckDBPyConnection, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, params)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _collect_analysis_summary(duckdb_path: Path, run_id: str | None) -> dict[str, Any]:
    with duckdb.connect(str(duckdb_path)) as conn:
        if not _has_analysis_views(conn):
            return _collect_analysis_summary_fallback(conn, run_id)
        validation = {"failed_checks": None}
        validation_scope = "omitted_for_current_run_scope" if run_id is not None else "all_runs"
        if run_id is None:
            validation = _duckdb_row(
                conn,
                "SELECT COUNT(*) AS failed_checks FROM analysis_validation_checks WHERE NOT passed",
            )
        fill_summary = _duckdb_row(
            conn,
            """
            SELECT
                COUNT(*) AS fill_count,
                SUM(CASE WHEN markout_60s IS NOT NULL THEN 1 ELSE 0 END) AS matured_60s_count,
                SUM(CASE WHEN markout_300s IS NOT NULL THEN 1 ELSE 0 END) AS matured_300s_count,
                AVG(markout_60s) AS avg_markout_60s,
                AVG(markout_300s) AS avg_markout_300s,
                AVG(markout_final) AS avg_markout_final
            FROM analysis_fill_markouts
            WHERE (? IS NULL OR run_id = ?)
            """,
            (run_id, run_id),
        )
        top_liquidity_bucket = _duckdb_row(
            conn,
            """
            SELECT
                spread_bucket,
                depth_bucket,
                imbalance_bucket,
                fill_count,
                avg_markout_60s,
                avg_markout_300s,
                realized_pnl_delta_sum
            FROM (
                SELECT
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
                    COUNT(*) AS fill_count,
                    AVG(markout_60s) AS avg_markout_60s,
                    AVG(markout_300s) AS avg_markout_300s,
                    SUM(COALESCE(realized_pnl_delta, 0.0)) AS realized_pnl_delta_sum
                FROM analysis_fill_liquidity_context
                WHERE (? IS NULL OR run_id = ?)
                GROUP BY 1, 2, 3
            ) scoped_buckets
            ORDER BY fill_count DESC, realized_pnl_delta_sum DESC, spread_bucket, depth_bucket, imbalance_bucket
            LIMIT 1
            """,
            (run_id, run_id),
        )
        tte_buckets = _duckdb_rows(
            conn,
            """
            SELECT
                tte_bucket,
                fill_count,
                avg_markout_60s,
                avg_markout_300s,
                avg_markout_final
            FROM (
                SELECT
                    tte_bucket,
                    COUNT(*) AS fill_count,
                    AVG(markout_60s) AS avg_markout_60s,
                    AVG(markout_300s) AS avg_markout_300s,
                    AVG(markout_final) AS avg_markout_final
                FROM analysis_fill_markouts
                WHERE (? IS NULL OR run_id = ?)
                GROUP BY 1
            ) scoped_tte_buckets
            ORDER BY fill_count DESC, tte_bucket
            """,
            (run_id, run_id),
        )
        adverse_paths = _duckdb_row(
            conn,
            """
            SELECT
                COALESCE(SUM(adverse_path_fill_count), 0) AS adverse_path_fill_count,
                COALESCE(SUM(fill_count), 0) AS clustered_fill_count
            FROM (
                SELECT
                    COUNT(*) AS fill_count,
                    SUM(CASE WHEN markout_path_bucket IN ('markout_path:reversal', 'markout_path:deteriorating', 'markout_path:adverse_early') THEN 1 ELSE 0 END) AS adverse_path_fill_count
                FROM analysis_circuit_breaker_fill_context
                WHERE (? IS NULL OR run_id = ?)
            ) scoped_precursors
            """,
            (run_id, run_id),
        )
        pnl_rows = _duckdb_rows(
            conn,
            """
            SELECT asset, interval_minutes, side, fill_count, realized_pnl_delta_sum
            FROM (
                SELECT
                    asset,
                    interval_minutes,
                    side,
                    COUNT(*) AS fill_count,
                    SUM(COALESCE(realized_pnl_delta, 0.0)) AS realized_pnl_delta_sum
                FROM analysis_fill_events
                WHERE (? IS NULL OR run_id = ?)
                GROUP BY 1, 2, 3
            ) scoped_pnl
            ORDER BY asset, interval_minutes, side
            """,
            (run_id, run_id),
        )
    return {
        "validation": validation,
        "validation_scope": validation_scope,
        "fill_summary": fill_summary,
        "top_liquidity_bucket": top_liquidity_bucket,
        "tte_buckets": tte_buckets,
        "adverse_paths": adverse_paths,
        "pnl_rows": pnl_rows,
        "analysis_source": "batch4_batch5_views",
        "analysis_scope": "current_run_only" if run_id is not None else "all_runs",
    }


def _bucket_tte(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    if seconds < 0:
        return "expired"
    if seconds < 60:
        return "<60s"
    if seconds <= 120:
        return "60-120s"
    if seconds <= 300:
        return "120-300s"
    return ">300s"


def _spread_bucket(spread_bps: float | None) -> str:
    if spread_bps is None:
        return "spread:unknown"
    if spread_bps < 250:
        return "spread:<250bps"
    if spread_bps < 500:
        return "spread:250-500bps"
    if spread_bps < 1000:
        return "spread:500-1000bps"
    return "spread:1000bps+"


def _depth_bucket(top_depth: float | None) -> str:
    if top_depth is None:
        return "depth:unknown"
    if top_depth < 5:
        return "depth:<5"
    if top_depth < 20:
        return "depth:5-20"
    if top_depth < 100:
        return "depth:20-100"
    return "depth:100+"


def _imbalance_bucket(depth_ratio: float | None) -> str:
    if depth_ratio is None:
        return "imbalance:unknown"
    if depth_ratio <= 1.5:
        return "imbalance:balanced"
    if depth_ratio <= 3.0:
        return "imbalance:moderate"
    return "imbalance:skewed"


def _matches_run_scope(row_run_id: Any, run_id: str | None) -> bool:
    return run_id is None or row_run_id in (None, "", run_id)


def _collect_runtime_analysis_fallback(runtime_dir: Path, run_id: str | None) -> dict[str, Any]:
    events = _read_jsonl(runtime_dir / "events.jsonl")
    quotes_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    fills: list[dict[str, Any]] = []
    for row in events:
        row_run_id = row.get("run_id") or ((row.get("payload") or {}).get("run_id"))
        event_type = row.get("event_type")
        payload = row.get("payload") or {}
        if event_type == "quote.submitted" and _matches_run_scope(row_run_id, run_id):
            key = (
                str(payload.get("market_id") or row.get("market_id") or ""),
                str((payload.get("book_quality") or {}).get("outcome") or payload.get("outcome") or row.get("outcome") or "Up"),
            )
            quotes_by_key.setdefault(key, []).append(
                {
                    "ts": float(row.get("ts", 0.0) or 0.0),
                    "mid_price": (payload.get("book_quality") or {}).get("mid_price"),
                    "spread_bps": (payload.get("book_quality") or {}).get("spread_bps"),
                    "top_depth": (payload.get("book_quality") or {}).get("top_depth"),
                    "depth_ratio": (payload.get("book_quality") or {}).get("depth_ratio"),
                }
            )
        elif event_type == "order.filled" and _matches_run_scope(row_run_id, run_id):
            fills.append(
                {
                    "ts": float(row.get("ts", 0.0) or 0.0),
                    "run_id": row_run_id,
                    "market_id": str(payload.get("market_id") or row.get("market_id") or ""),
                    "slot_id": str(payload.get("slot_id") or row.get("slot_id") or ""),
                    "outcome": str(payload.get("outcome") or row.get("outcome") or ""),
                    "side": str(payload.get("side") or row.get("side") or "UNKNOWN").upper(),
                    "fill_price": payload.get("fill_price", payload.get("price")),
                    "realized_pnl_delta": payload.get("realized_pnl_delta"),
                    "time_to_expiry_seconds": payload.get("time_to_expiry_seconds"),
                }
            )

    for quote_rows in quotes_by_key.values():
        quote_rows.sort(key=lambda item: item["ts"])

    settlement_by_market: dict[str, str] = {}
    with sqlite3.connect(runtime_dir / "ledger.db") as conn:
        conn.row_factory = sqlite3.Row
        if run_id is None:
            rows = conn.execute(
                "SELECT payload_json, event_ts, run_id FROM ledger_events WHERE event_type='slot_settled' ORDER BY event_ts DESC"
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT payload_json, event_ts, run_id
                FROM ledger_events
                WHERE event_type='slot_settled'
                  AND (run_id IS NULL OR run_id = ?)
                ORDER BY event_ts DESC
                """,
                (run_id,),
            ).fetchall()
    for row in rows:
        payload = json.loads(row["payload_json"])
        market_id = str(payload.get("market_id") or "")
        if market_id and market_id not in settlement_by_market:
            settlement_by_market[market_id] = str(payload.get("winning_outcome") or "")

    markout60_values: list[float] = []
    markout300_values: list[float] = []
    markout_final_values: list[float] = []
    pnl_rows: dict[tuple[str | None, int | None, str], dict[str, Any]] = {}
    tte_rows: dict[str, dict[str, Any]] = {}
    bucket_rows: dict[tuple[str, str, str], dict[str, Any]] = {}

    def next_quote(key: tuple[str, str], cutoff_ts: float) -> dict[str, Any] | None:
        options = quotes_by_key.get(key) or []
        timestamps = [item["ts"] for item in options]
        idx = bisect_left(timestamps, cutoff_ts)
        return options[idx] if idx < len(options) else None

    def prev_quote(key: tuple[str, str], cutoff_ts: float) -> dict[str, Any] | None:
        options = quotes_by_key.get(key) or []
        timestamps = [item["ts"] for item in options]
        idx = bisect_left(timestamps, cutoff_ts) - 1
        return options[idx] if idx >= 0 else None

    for fill in fills:
        slot_id = fill["slot_id"]
        parts = slot_id.split(":") if slot_id else []
        asset = parts[0] if len(parts) >= 1 and parts[0] else None
        interval_minutes = int(parts[1]) if len(parts) >= 2 and parts[1].isdigit() else None
        tte_bucket = _bucket_tte(float(fill["time_to_expiry_seconds"]) if fill.get("time_to_expiry_seconds") is not None else None)
        key = (fill["market_id"], fill["outcome"])
        q60 = next_quote(key, fill["ts"] + 60)
        q300 = next_quote(key, fill["ts"] + 300)
        qprev = prev_quote(key, fill["ts"])
        fill_price = float(fill["fill_price"]) if fill.get("fill_price") is not None else None
        markout_60 = None
        if q60 and fill_price is not None and q60.get("mid_price") is not None:
            mid = float(q60["mid_price"])
            markout_60 = mid - fill_price if fill["side"] == "BUY" else fill_price - mid
            markout60_values.append(markout_60)
        if q300 and fill_price is not None and q300.get("mid_price") is not None:
            mid = float(q300["mid_price"])
            markout_300 = mid - fill_price if fill["side"] == "BUY" else fill_price - mid
            markout300_values.append(markout_300)
        winning_outcome = settlement_by_market.get(fill["market_id"])
        if winning_outcome and fill_price is not None:
            settlement_price = 1.0 if fill["outcome"] == winning_outcome else 0.0
            final = settlement_price - fill_price if fill["side"] == "BUY" else fill_price - settlement_price
            markout_final_values.append(final)

        pnl_key = (asset, interval_minutes, fill["side"])
        pnl_row = pnl_rows.setdefault(
            pnl_key,
            {
                "asset": asset,
                "interval_minutes": interval_minutes,
                "side": fill["side"],
                "fill_count": 0,
                "realized_pnl_delta_sum": 0.0,
            },
        )
        pnl_row["fill_count"] += 1
        pnl_row["realized_pnl_delta_sum"] += float(fill.get("realized_pnl_delta") or 0.0)

        tte_row = tte_rows.setdefault(
            tte_bucket,
            {
                "tte_bucket": tte_bucket,
                "fill_count": 0,
                "markout_60_values": [],
                "avg_markout_300s": None,
                "avg_markout_final": None,
            },
        )
        tte_row["fill_count"] += 1
        if markout_60 is not None:
            tte_row["markout_60_values"].append(markout_60)

        spread_bucket = _spread_bucket(float(qprev["spread_bps"]) if qprev and qprev.get("spread_bps") is not None else None)
        depth_bucket = _depth_bucket(float(qprev["top_depth"]) if qprev and qprev.get("top_depth") is not None else None)
        imbalance_bucket = _imbalance_bucket(float(qprev["depth_ratio"]) if qprev and qprev.get("depth_ratio") is not None else None)
        bucket_key = (spread_bucket, depth_bucket, imbalance_bucket)
        bucket_row = bucket_rows.setdefault(
            bucket_key,
            {
                "spread_bucket": spread_bucket,
                "depth_bucket": depth_bucket,
                "imbalance_bucket": imbalance_bucket,
                "fill_count": 0,
                "markout_60_values": [],
                "realized_pnl_delta_sum": 0.0,
            },
        )
        bucket_row["fill_count"] += 1
        if markout_60 is not None:
            bucket_row["markout_60_values"].append(markout_60)
        bucket_row["realized_pnl_delta_sum"] += float(fill.get("realized_pnl_delta") or 0.0)

    for row in tte_rows.values():
        values = row.pop("markout_60_values")
        row["avg_markout_60s"] = sum(values) / len(values) if values else None
    for row in bucket_rows.values():
        values = row.pop("markout_60_values")
        row["avg_markout_60s"] = sum(values) / len(values) if values else None
        row["avg_markout_300s"] = None

    top_bucket = {}
    if bucket_rows:
        top_bucket = sorted(
            bucket_rows.values(),
            key=lambda item: (-item["fill_count"], -item["realized_pnl_delta_sum"], item["spread_bucket"], item["depth_bucket"], item["imbalance_bucket"]),
        )[0]

    return {
        "validation": {"failed_checks": 0},
        "validation_scope": "runtime_fallback",
        "fill_summary": {
            "fill_count": len(fills),
            "matured_60s_count": len(markout60_values),
            "matured_300s_count": len(markout300_values),
            "avg_markout_60s": (sum(markout60_values) / len(markout60_values)) if markout60_values else None,
            "avg_markout_300s": (sum(markout300_values) / len(markout300_values)) if markout300_values else None,
            "avg_markout_final": (sum(markout_final_values) / len(markout_final_values)) if markout_final_values else None,
        },
        "top_liquidity_bucket": top_bucket,
        "tte_buckets": sorted(tte_rows.values(), key=lambda item: (-item["fill_count"], item["tte_bucket"])),
        "adverse_paths": {"adverse_path_fill_count": 0, "clustered_fill_count": len(fills)},
        "pnl_rows": sorted(pnl_rows.values(), key=lambda item: (str(item["asset"]), item["interval_minutes"] or 0, item["side"])),
        "analysis_source": "runtime_jsonl_fallback",
        "analysis_scope": "current_run_only" if run_id is not None else "all_runs",
    }


def _collect_analysis_summary_fallback(conn: duckdb.DuckDBPyConnection, run_id: str | None) -> dict[str, Any]:
    fill_summary = _duckdb_row(
        conn,
        """
        WITH fills AS (
            SELECT
                ts AS fill_ts,
                run_id,
                market_id,
                COALESCE(outcome, json_extract_string(raw_json, '$.payload.outcome')) AS outcome,
                UPPER(COALESCE(side, json_extract_string(raw_json, '$.payload.side'), 'UNKNOWN')) AS side,
                TRY_CAST(COALESCE(json_extract_string(raw_json, '$.payload.fill_price'), json_extract_string(raw_json, '$.payload.price')) AS DOUBLE) AS fill_price,
                TRY_CAST(COALESCE(json_extract_string(raw_json, '$.payload.size'), json_extract_string(raw_json, '$.payload.fill_size')) AS DOUBLE) AS fill_size
            FROM runtime_events
            WHERE event_type = 'order.filled'
              AND (? IS NULL OR run_id = ?)
        ),
        quotes AS (
            SELECT
                ts,
                market_id,
                COALESCE(json_extract_string(raw_json, '$.payload.book_quality.outcome'), outcome, 'Up') AS outcome,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.mid_price') AS DOUBLE) AS mid_price
            FROM runtime_events
            WHERE event_type = 'quote.submitted'
              AND json_extract(raw_json, '$.payload.book_quality.mid_price') IS NOT NULL
              AND (? IS NULL OR run_id IS NULL OR run_id = ?)
        )
        SELECT
            COUNT(*) AS fill_count,
            SUM(CASE WHEN q60.mid_price IS NOT NULL THEN 1 ELSE 0 END) AS matured_60s_count,
            SUM(CASE WHEN q300.mid_price IS NOT NULL THEN 1 ELSE 0 END) AS matured_300s_count,
            AVG(CASE WHEN q60.mid_price IS NULL OR f.fill_price IS NULL THEN NULL WHEN f.side = 'BUY' THEN q60.mid_price - f.fill_price ELSE f.fill_price - q60.mid_price END) AS avg_markout_60s,
            AVG(CASE WHEN q300.mid_price IS NULL OR f.fill_price IS NULL THEN NULL WHEN f.side = 'BUY' THEN q300.mid_price - f.fill_price ELSE f.fill_price - q300.mid_price END) AS avg_markout_300s,
            AVG(CASE WHEN s.winning_outcome IS NULL OR f.fill_price IS NULL THEN NULL WHEN f.side = 'BUY' THEN CASE WHEN f.outcome = s.winning_outcome THEN 1.0 ELSE 0.0 END - f.fill_price ELSE f.fill_price - CASE WHEN f.outcome = s.winning_outcome THEN 1.0 ELSE 0.0 END END) AS avg_markout_final
        FROM fills f
        LEFT JOIN LATERAL (
            SELECT q.mid_price
            FROM quotes q
            WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts >= f.fill_ts + 60
            ORDER BY q.ts ASC
            LIMIT 1
        ) q60 ON TRUE
        LEFT JOIN LATERAL (
            SELECT q.mid_price
            FROM quotes q
            WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts >= f.fill_ts + 300
            ORDER BY q.ts ASC
            LIMIT 1
        ) q300 ON TRUE
        LEFT JOIN LATERAL (
            SELECT json_extract_string(raw_json, '$.winning_outcome') AS winning_outcome
            FROM ledger_events le
            WHERE le.event_type = 'slot_settled'
              AND json_extract_string(raw_json, '$.market_id') = f.market_id
              AND (? IS NULL OR le.run_id IS NULL OR le.run_id = ?)
            ORDER BY event_ts DESC NULLS LAST, sequence_num DESC NULLS LAST
            LIMIT 1
        ) s ON TRUE
        """,
        (run_id, run_id, run_id, run_id, run_id, run_id),
    )
    top_liquidity_bucket = _duckdb_row(
        conn,
        """
        WITH fills AS (
            SELECT
                line_number,
                ts AS fill_ts,
                run_id,
                market_id,
                COALESCE(outcome, json_extract_string(raw_json, '$.payload.outcome')) AS outcome,
                UPPER(COALESCE(side, json_extract_string(raw_json, '$.payload.side'), 'UNKNOWN')) AS side,
                TRY_CAST(COALESCE(json_extract_string(raw_json, '$.payload.fill_price'), json_extract_string(raw_json, '$.payload.price')) AS DOUBLE) AS fill_price,
                TRY_CAST(json_extract_string(raw_json, '$.payload.realized_pnl_delta') AS DOUBLE) AS realized_pnl_delta
            FROM runtime_events
            WHERE event_type = 'order.filled'
              AND (? IS NULL OR run_id = ?)
        ),
        quotes AS (
            SELECT
                line_number,
                ts,
                market_id,
                COALESCE(json_extract_string(raw_json, '$.payload.book_quality.outcome'), outcome, 'Up') AS outcome,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.mid_price') AS DOUBLE) AS mid_price,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.spread_bps') AS DOUBLE) AS spread_bps,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.top_depth') AS DOUBLE) AS top_depth,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.depth_ratio') AS DOUBLE) AS depth_ratio
            FROM runtime_events
            WHERE event_type = 'quote.submitted'
              AND json_extract(raw_json, '$.payload.book_quality.mid_price') IS NOT NULL
              AND (? IS NULL OR run_id IS NULL OR run_id = ?)
        ),
        enriched AS (
            SELECT
                CASE
                    WHEN q.spread_bps IS NULL THEN 'spread:unknown'
                    WHEN q.spread_bps < 250 THEN 'spread:<250bps'
                    WHEN q.spread_bps < 500 THEN 'spread:250-500bps'
                    WHEN q.spread_bps < 1000 THEN 'spread:500-1000bps'
                    ELSE 'spread:1000bps+'
                END AS spread_bucket,
                CASE
                    WHEN q.top_depth IS NULL THEN 'depth:unknown'
                    WHEN q.top_depth < 5 THEN 'depth:<5'
                    WHEN q.top_depth < 20 THEN 'depth:5-20'
                    WHEN q.top_depth < 100 THEN 'depth:20-100'
                    ELSE 'depth:100+'
                END AS depth_bucket,
                CASE
                    WHEN q.depth_ratio IS NULL THEN 'imbalance:unknown'
                    WHEN q.depth_ratio <= 1.5 THEN 'imbalance:balanced'
                    WHEN q.depth_ratio <= 3.0 THEN 'imbalance:moderate'
                    ELSE 'imbalance:skewed'
                END AS imbalance_bucket,
                CASE WHEN q60.mid_price IS NULL OR f.fill_price IS NULL THEN NULL WHEN f.side = 'BUY' THEN q60.mid_price - f.fill_price ELSE f.fill_price - q60.mid_price END AS markout_60s,
                COALESCE(f.realized_pnl_delta, 0.0) AS realized_pnl_delta
            FROM fills f
            LEFT JOIN LATERAL (
                SELECT * FROM quotes q
                WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts <= f.fill_ts
                ORDER BY q.ts DESC, q.line_number DESC
                LIMIT 1
            ) q ON TRUE
            LEFT JOIN LATERAL (
                SELECT q.mid_price
                FROM quotes q
                WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts >= f.fill_ts + 60
                ORDER BY q.ts ASC, q.line_number ASC
                LIMIT 1
            ) q60 ON TRUE
        )
        SELECT
            spread_bucket,
            depth_bucket,
            imbalance_bucket,
            COUNT(*) AS fill_count,
            AVG(markout_60s) AS avg_markout_60s,
            CAST(NULL AS DOUBLE) AS avg_markout_300s,
            SUM(realized_pnl_delta) AS realized_pnl_delta_sum
        FROM enriched
        GROUP BY 1, 2, 3
        ORDER BY fill_count DESC, realized_pnl_delta_sum DESC, spread_bucket, depth_bucket, imbalance_bucket
        LIMIT 1
        """,
        (run_id, run_id, run_id, run_id),
    )
    tte_buckets = _duckdb_rows(
        conn,
        """
        WITH fills AS (
            SELECT
                COALESCE(json_extract_string(raw_json, '$.payload.tte_bucket'),
                    CASE
                        WHEN TRY_CAST(json_extract_string(raw_json, '$.payload.time_to_expiry_seconds') AS DOUBLE) IS NULL THEN 'unknown'
                        WHEN TRY_CAST(json_extract_string(raw_json, '$.payload.time_to_expiry_seconds') AS DOUBLE) < 0 THEN 'expired'
                        WHEN TRY_CAST(json_extract_string(raw_json, '$.payload.time_to_expiry_seconds') AS DOUBLE) < 60 THEN '<60s'
                        WHEN TRY_CAST(json_extract_string(raw_json, '$.payload.time_to_expiry_seconds') AS DOUBLE) <= 120 THEN '60-120s'
                        WHEN TRY_CAST(json_extract_string(raw_json, '$.payload.time_to_expiry_seconds') AS DOUBLE) <= 300 THEN '120-300s'
                        ELSE '>300s'
                    END
                ) AS tte_bucket,
                UPPER(COALESCE(side, json_extract_string(raw_json, '$.payload.side'), 'UNKNOWN')) AS side,
                COALESCE(outcome, json_extract_string(raw_json, '$.payload.outcome')) AS outcome,
                market_id,
                ts AS fill_ts,
                TRY_CAST(COALESCE(json_extract_string(raw_json, '$.payload.fill_price'), json_extract_string(raw_json, '$.payload.price')) AS DOUBLE) AS fill_price,
                run_id
            FROM runtime_events
            WHERE event_type = 'order.filled'
              AND (? IS NULL OR run_id = ?)
        ),
        quotes AS (
            SELECT
                ts,
                market_id,
                COALESCE(json_extract_string(raw_json, '$.payload.book_quality.outcome'), outcome, 'Up') AS outcome,
                TRY_CAST(json_extract_string(raw_json, '$.payload.book_quality.mid_price') AS DOUBLE) AS mid_price
            FROM runtime_events
            WHERE event_type = 'quote.submitted'
              AND json_extract(raw_json, '$.payload.book_quality.mid_price') IS NOT NULL
              AND (? IS NULL OR run_id IS NULL OR run_id = ?)
        )
        SELECT
            f.tte_bucket,
            COUNT(*) AS fill_count,
            AVG(CASE WHEN q60.mid_price IS NULL OR f.fill_price IS NULL THEN NULL WHEN f.side = 'BUY' THEN q60.mid_price - f.fill_price ELSE f.fill_price - q60.mid_price END) AS avg_markout_60s,
            CAST(NULL AS DOUBLE) AS avg_markout_300s,
            CAST(NULL AS DOUBLE) AS avg_markout_final
        FROM fills f
        LEFT JOIN LATERAL (
            SELECT q.mid_price
            FROM quotes q
            WHERE q.market_id = f.market_id AND COALESCE(q.outcome, '') = COALESCE(f.outcome, '') AND q.ts >= f.fill_ts + 60
            ORDER BY q.ts ASC
            LIMIT 1
        ) q60 ON TRUE
        GROUP BY 1
        ORDER BY fill_count DESC, tte_bucket
        """,
        (run_id, run_id, run_id, run_id),
    )
    pnl_rows = _duckdb_rows(
        conn,
        """
        SELECT
            asset,
            interval_minutes,
            UPPER(COALESCE(side, json_extract_string(raw_json, '$.payload.side'), 'UNKNOWN')) AS side,
            COUNT(*) AS fill_count,
            SUM(COALESCE(TRY_CAST(json_extract_string(raw_json, '$.payload.realized_pnl_delta') AS DOUBLE), 0.0)) AS realized_pnl_delta_sum
        FROM runtime_events
        WHERE event_type = 'order.filled'
          AND (? IS NULL OR run_id = ?)
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
        """,
        (run_id, run_id),
    )
    return {
        "validation": {"failed_checks": 0},
        "validation_scope": "batch3_duckdb_fallback",
        "fill_summary": fill_summary,
        "top_liquidity_bucket": top_liquidity_bucket,
        "tte_buckets": tte_buckets,
        "adverse_paths": {"adverse_path_fill_count": 0, "clustered_fill_count": int(fill_summary.get('fill_count', 0) or 0)},
        "pnl_rows": pnl_rows,
        "analysis_source": "batch3_duckdb_fallback",
        "analysis_scope": "current_run_only" if run_id is not None else "all_runs",
    }


def _analysis_provenance_lines(analysis: dict[str, Any]) -> list[str]:
    return [
        f"- analysis_mode: `{analysis.get('analysis_mode', 'n/a')}`",
        f"- analysis_source: `{analysis.get('analysis_source', 'n/a')}`",
        f"- analysis_scope: `{analysis.get('analysis_scope', 'n/a')}`",
        f"- validation_scope: `{analysis.get('validation_scope', 'n/a')}`",
    ]


def _ledger_summary(runtime_dir: Path, run_id: str | None) -> dict[str, Any]:
    ledger_path = runtime_dir / "ledger.db"
    with sqlite3.connect(ledger_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        current_run_counts = {
            row["event_type"]: row["n"]
            for row in cur.execute(
                "SELECT event_type, COUNT(*) AS n FROM ledger_events WHERE run_id = ? GROUP BY event_type ORDER BY event_type",
                (run_id,),
            ).fetchall()
        }
        latest_run_rows = [
            dict(row)
            for row in cur.execute(
                """
                SELECT run_id, COUNT(*) AS n, MIN(event_ts) AS min_ts, MAX(event_ts) AS max_ts
                FROM ledger_events
                GROUP BY run_id
                ORDER BY max_ts DESC, run_id DESC
                LIMIT 8
                """
            ).fetchall()
        ]
    return {
        "current_run_counts": current_run_counts,
        "latest_run_rows": latest_run_rows,
        "run_lineage_fragmentation": count_run_lineage_fragmentation(str(runtime_dir)),
        "settlement_pnl_computable": check_settlement_pnl_computable(str(runtime_dir)),
    }


def _collect_contradictions(
    *,
    status: dict[str, Any],
    strategy_metrics: dict[str, Any],
    research_latest: dict[str, Any],
    analysis: dict[str, Any],
    ledger: dict[str, Any],
) -> list[ContradictionEntry]:
    contradictions: list[ContradictionEntry] = []
    run_id = status.get("run_id")
    risk = status.get("risk") or {}
    baseline_strategy = status.get("baseline_strategy") or "unknown"
    baseline_metrics = strategy_metrics.get(baseline_strategy) or {}
    risk_realized = float(risk.get("realized_pnl_total", 0.0) or 0.0)
    baseline_realized = float(baseline_metrics.get("realized_pnl", 0.0) or 0.0)
    positions_len = len(status.get("positions") or {})
    open_position_count = int(status.get("open_position_count", 0) or 0)
    resolved_trade_count = int(status.get("resolved_trade_count", 0) or 0)
    slot_settled_count = int(ledger["current_run_counts"].get("slot_settled", 0) or 0)
    fill_applied_count = int(ledger["current_run_counts"].get("fill_applied", 0) or 0)
    fill_summary = analysis.get("fill_summary") or {}
    validation_failed = int((analysis.get("validation") or {}).get("failed_checks", 0) or 0)
    research_gate_reasons = tuple((research_latest.get("raw_context") or {}).get("gate_reasons") or ())

    if abs(risk_realized - baseline_realized) > 1e-9:
        contradictions.append(
            ContradictionEntry(
                key="C-001",
                title="Runtime realized PnL and strategy snapshot disagree",
                status="active",
                evidence_a=(
                    f"status.json risk.realized_pnl_total={_fmt_float(risk_realized)} for run_id={run_id}"
                ),
                evidence_b=(
                    f"strategy_metrics.json {baseline_strategy}.realized_pnl={_fmt_float(baseline_realized)}"
                ),
                why_it_matters="The compiler cannot treat realized economics as settled truth until head-status and family snapshot semantics line up.",
            )
        )

    if open_position_count != positions_len:
        contradictions.append(
            ContradictionEntry(
                key="C-002",
                title="Open-position count and position map disagree",
                status="active",
                evidence_a=f"status.json open_position_count={open_position_count}",
                evidence_b=f"status.json positions map contains {positions_len} entries",
                why_it_matters="Operator headcount metrics stop being trustworthy when summary counts and enumerated positions diverge.",
            )
        )

    if resolved_trade_count != slot_settled_count:
        contradictions.append(
            ContradictionEntry(
                key="C-003",
                title="Resolved-trade summary and slot-settled ledger counts disagree",
                status="active",
                evidence_a=f"status.json resolved_trade_count={resolved_trade_count}",
                evidence_b=f"ledger current-run slot_settled count={slot_settled_count}",
                why_it_matters="This exposes unresolved semantics between trade-level resolution and slot-level settlement, which must be explicit before later evidence packets can be trusted.",
            )
        )

    if status.get("phase") == "running" and status.get("stop_reason"):
        contradictions.append(
            ContradictionEntry(
                key="C-004",
                title="Runtime is marked running while a stop_reason is still present",
                status="active",
                evidence_a=f"status.json phase={status.get('phase')}",
                evidence_b=f"status.json stop_reason={status.get('stop_reason')}",
                why_it_matters="A sticky stop reason can make a recovered runtime look simultaneously alive and halted, which is exactly the kind of operator story drift this bridge should surface early.",
            )
        )

    if ledger.get("run_lineage_fragmentation", 0) >= 4:
        contradictions.append(
            ContradictionEntry(
                key="C-005",
                title="Recent run lineage is fragmented",
                status="active",
                evidence_a=(
                    f"ledger distinct run_id count in the last 2h={ledger.get('run_lineage_fragmentation', 0)}"
                ),
                evidence_b="research gate treats fragmentation >= 4 as a contradiction-first block condition",
                why_it_matters="Restart fragmentation can manufacture false cleanliness by repeatedly resetting runtime context before problems mature.",
            )
        )

    if research_gate_reasons and ledger.get("settlement_pnl_computable"):
        stale_reason = next(
            (reason for reason in research_gate_reasons if "settlement_pnl_computable=False" in reason),
            None,
        )
        if stale_reason:
            contradictions.append(
                ContradictionEntry(
                    key="C-006",
                    title="Research snapshot says settlement PnL is not computable, but live ledger says it is",
                    status="active",
                    evidence_a=f"research latest gate reason={stale_reason}",
                    evidence_b="ledger slot_settled payloads now include realized_pnl",
                    why_it_matters="The bridge should prevent an old research block condition from being mistaken for current runtime truth.",
                )
            )

    matured_300s_count = int(fill_summary.get("matured_300s_count", 0) or 0)
    if fill_applied_count > 0 and matured_300s_count == 0:
        contradictions.append(
            ContradictionEntry(
                key="C-007",
                title="Execution is proven before 300s markout quality is proven",
                status="active",
                evidence_a=f"ledger current-run fill_applied count={fill_applied_count}",
                evidence_b="analysis_fill_markouts matured_300s_count=0",
                why_it_matters="A live fill stream proves contact with the market, not edge quality.",
            )
        )

    if validation_failed > 0:
        contradictions.append(
            ContradictionEntry(
                key="C-008",
                title="DuckDB analysis validation checks are failing",
                status="active",
                evidence_a=f"analysis_validation_checks failed_checks={validation_failed}",
                evidence_b="Batch 3-5 projections are expected to fully partition and reconcile their fill-level rollups",
                why_it_matters="If the analytical bridge is internally inconsistent, wiki outputs should not present analytical summaries as stable truth.",
            )
        )

    return contradictions


def _collect_experiments(
    *,
    status: dict[str, Any],
    strategy_metrics: dict[str, Any],
    analysis: dict[str, Any],
    ledger: dict[str, Any],
    contradictions: list[ContradictionEntry],
) -> list[ExperimentEntry]:
    baseline_strategy = status.get("baseline_strategy") or "unknown"
    baseline_metrics = strategy_metrics.get(baseline_strategy) or {}
    fill_summary = analysis.get("fill_summary") or {}
    top_bucket = analysis.get("top_liquidity_bucket") or {}
    contradiction_ids = tuple(item.key for item in contradictions)

    entries = [
        ExperimentEntry(
            key="EXP-001",
            title="Runtime truth reconciliation bridge",
            status="active" if contradiction_ids else "observe",
            objective="Keep the compiler grounded in canonical runtime files until head-status, strategy snapshot, and ledger semantics stop disagreeing.",
            evidence=(
                f"active contradictions={', '.join(contradiction_ids) if contradiction_ids else 'none'}",
                f"status realized_pnl_total={_fmt_float((status.get('risk') or {}).get('realized_pnl_total'))}",
                f"{baseline_strategy} strategy realized_pnl={_fmt_float(baseline_metrics.get('realized_pnl'))}",
            ),
            close_criteria=(
                "realized PnL precedence is explicit and contradiction-free",
                "open-position summary matches enumerated positions",
                "resolved-trade and slot-settled semantics are operator-legible",
            ),
        ),
        ExperimentEntry(
            key="EXP-002",
            title="Markout maturity bridge",
            status="active" if int(fill_summary.get("fill_count", 0) or 0) > 0 else "blocked",
            objective="Track when fill activity matures into usable 60s and 300s markout evidence without turning it into strategy advice.",
            evidence=(
                f"analysis fill_count={_fmt_int(fill_summary.get('fill_count'))}",
                f"matured_60s_count={_fmt_int(fill_summary.get('matured_60s_count'))}",
                f"matured_300s_count={_fmt_int(fill_summary.get('matured_300s_count'))}",
            ),
            close_criteria=(
                "300s markout coverage exists for a non-trivial fill sample",
                "markout averages can be cited without caveating every row as immature",
            ),
        ),
        ExperimentEntry(
            key="EXP-003",
            title="Circuit-breaker precursor baseline",
            status="active" if status.get("stop_reason") == "circuit_breaker" else "observe",
            objective="Use Batch 5 precursor views as context only, so runtime stress can be described before any intervention layer is added.",
            evidence=(
                f"status stop_reason={status.get('stop_reason') or 'none'}",
                f"adverse_path_fill_count={_fmt_int((analysis.get('adverse_paths') or {}).get('adverse_path_fill_count'))}",
                f"latest_drawdown={_fmt_float((status.get('risk') or {}).get('max_drawdown'))}",
            ),
            close_criteria=(
                "circuit-breaker history is attributable to observable precursor buckets",
                "runtime stress can be described without mixing stale and current narratives",
            ),
        ),
        ExperimentEntry(
            key="EXP-004",
            title="Settlement semantics bridge",
            status="active",
            objective="Describe slot-level settlement flow and current pending capital lock without adding a recommendation layer.",
            evidence=(
                f"current-run slot_settled={_fmt_int(ledger.get('current_run_counts', {}).get('slot_settled'))}",
                f"status resolved_trade_count={_fmt_int(status.get('resolved_trade_count'))}",
                f"status pending_settlement_count={_fmt_int((status.get('risk') or {}).get('pending_settlement_count'))}",
            ),
            close_criteria=(
                "slot-level and trade-level resolution counts are explicitly scoped",
                "pending-settlement head metrics can be read without semantic ambiguity",
            ),
        ),
        ExperimentEntry(
            key="EXP-005",
            title="Liquidity bucket baseline",
            status="active" if top_bucket else "blocked",
            objective="Keep a contradiction-first pointer to the dominant spread/depth/imbalance bucket observed in Batch 4 outputs.",
            evidence=(
                f"top bucket={top_bucket.get('spread_bucket', 'n/a')} | {top_bucket.get('depth_bucket', 'n/a')} | {top_bucket.get('imbalance_bucket', 'n/a')}",
                f"bucket fill_count={_fmt_int(top_bucket.get('fill_count'))}",
                f"bucket avg_markout_60s={_fmt_float(top_bucket.get('avg_markout_60s'))}",
            ),
            close_criteria=(
                "dominant liquidity bucket remains stable enough to compare across compiler runs",
                "bucket summaries are used as observation scaffolding only, not promotion logic",
            ),
        ),
    ]
    return entries


def _render_frontmatter(
    *,
    title: str,
    created: str,
    updated: str,
    doc_type: str,
    tags: list[str],
    sources: list[str],
) -> list[str]:
    return [
        "---",
        f"title: {title}",
        f"created: {created}",
        f"updated: {updated}",
        f"type: {doc_type}",
        f"tags: [{', '.join(tags)}]",
        f"sources: [{', '.join(sources)}]",
        "compiler: scripts/wiki_autoresearch_compile.py",
        "---",
        "",
    ]


def _render_current_state(
    *,
    output_path: Path,
    generated_ts: float,
    status: dict[str, Any],
    strategy_metrics: dict[str, Any],
    research_latest: dict[str, Any],
    analysis: dict[str, Any],
    ledger: dict[str, Any],
    contradictions: list[ContradictionEntry],
) -> str:
    created = _read_existing_created(output_path, _date_utc(generated_ts))
    updated = _date_utc(generated_ts)
    baseline_strategy = status.get("baseline_strategy") or "unknown"
    baseline_metrics = strategy_metrics.get(baseline_strategy) or {}
    risk = status.get("risk") or {}
    fill_summary = analysis.get("fill_summary") or {}
    top_bucket = analysis.get("top_liquidity_bucket") or {}
    lines = _render_frontmatter(
        title="Current State",
        created=created,
        updated=updated,
        doc_type="overview",
        tags=["overview", "runtime-truth", "compiler", "contradiction-first"],
        sources=[
            "runtime:data/runtime/status.json",
            "runtime:data/runtime/strategy_metrics.json",
            "runtime:data/runtime/ledger.db",
            "runtime:data/runtime/events.jsonl",
            "runtime:data/runtime/market_samples.jsonl",
            "research:data/research/latest.json",
            "analysis:data/analysis/base.duckdb",
        ],
    )
    lines.extend(
        [
            "# Current State",
            "",
            "## Snapshot",
            f"- compiled_at: `{_iso_utc(generated_ts)}`",
            f"- status_heartbeat: `{_iso_utc(status.get('heartbeat_ts'))}`",
            f"- run_id: `{status.get('run_id', 'n/a')}`",
            f"- phase: `{status.get('phase', 'n/a')}`",
            f"- mode: `{status.get('mode', 'n/a')}`",
            f"- baseline_strategy: `{baseline_strategy}`",
            "",
            "## Analysis provenance",
        ]
    )
    lines.extend(_analysis_provenance_lines(analysis))
    lines.extend(
        [
            "",
            "## Runtime head",
            f"- bankroll: `{_fmt_float(status.get('bankroll'))}`",
            f"- open_position_count: `{_fmt_int(status.get('open_position_count'))}`",
            f"- positions_map_entries: `{len(status.get('positions') or {})}`",
            f"- resolved_trade_count: `{_fmt_int(status.get('resolved_trade_count'))}`",
            f"- pending_resolution_slots: `{_safe_len(status.get('pending_resolution_slots'))}`",
            f"- open_order_count: `{_fmt_int(risk.get('open_order_count'))}`",
            f"- pending_settlement_count: `{_fmt_int(risk.get('pending_settlement_count'))}`",
            f"- pending_settlement_exposure: `{_fmt_float(risk.get('pending_settlement_exposure'))}`",
            f"- total_gross_exposure: `{_fmt_float(risk.get('total_gross_exposure'))}`",
            f"- realized_pnl_total: `{_fmt_float(risk.get('realized_pnl_total'))}`",
            f"- unrealized_pnl_total: `{_fmt_float(risk.get('unrealized_pnl_total'))}`",
            f"- stop_reason: `{status.get('stop_reason', 'none')}`",
            "",
            "## Strategy snapshot",
            f"- {baseline_strategy}.markets_seen: `{_fmt_int(baseline_metrics.get('markets_seen'))}`",
            f"- {baseline_strategy}.quotes_submitted: `{_fmt_int(baseline_metrics.get('quotes_submitted'))}`",
            f"- {baseline_strategy}.orders_filled: `{_fmt_int(baseline_metrics.get('orders_filled'))}`",
            f"- {baseline_strategy}.toxic_book_skips: `{_fmt_int(baseline_metrics.get('toxic_book_skips'))}`",
            f"- {baseline_strategy}.realized_pnl: `{_fmt_float(baseline_metrics.get('realized_pnl'))}`",
            "",
            "## Analysis bridge",
            f"- analysis_failed_checks: `{_fmt_int((analysis.get('validation') or {}).get('failed_checks'))}`",
            f"- fill_count: `{_fmt_int(fill_summary.get('fill_count'))}`",
            f"- matured_60s_count: `{_fmt_int(fill_summary.get('matured_60s_count'))}`",
            f"- matured_300s_count: `{_fmt_int(fill_summary.get('matured_300s_count'))}`",
            f"- avg_markout_60s: `{_fmt_float(fill_summary.get('avg_markout_60s'))}`",
            f"- avg_markout_300s: `{_fmt_float(fill_summary.get('avg_markout_300s'))}`",
            f"- avg_markout_final: `{_fmt_float(fill_summary.get('avg_markout_final'))}`",
            f"- top_liquidity_bucket: `{top_bucket.get('spread_bucket', 'n/a')} | {top_bucket.get('depth_bucket', 'n/a')} | {top_bucket.get('imbalance_bucket', 'n/a')}`",
            "",
            "## Contradiction-first summary",
            f"- active_contradictions: `{len(contradictions)}`",
            f"- recent_run_lineage_fragmentation: `{ledger.get('run_lineage_fragmentation', 0)}`",
            f"- settlement_pnl_computable_live: `{str(bool(ledger.get('settlement_pnl_computable'))).lower()}`",
            f"- research_latest_cycle_id: `{research_latest.get('cycle_id', 'n/a')}`",
        ]
    )
    if contradictions:
        lines.append("- contradiction_ids: `" + ", ".join(item.key for item in contradictions) + "`")
    lines.extend(["", "## Notes", "- `status.json` remains the exact-now head artifact.", "- This compiler layer records contradictions and base observations only."])
    lines.append("")
    return "\n".join(lines) + "\n"


def _render_contradiction_log(
    *,
    output_path: Path,
    generated_ts: float,
    contradictions: list[ContradictionEntry],
    ledger: dict[str, Any],
    analysis: dict[str, Any],
) -> str:
    created = _read_existing_created(output_path, _date_utc(generated_ts))
    updated = _date_utc(generated_ts)
    lines = _render_frontmatter(
        title="Contradiction Log",
        created=created,
        updated=updated,
        doc_type="contradiction",
        tags=["contradiction", "runtime-truth", "compiler", "analysis-bridge"],
        sources=[
            "runtime:data/runtime/status.json",
            "runtime:data/runtime/strategy_metrics.json",
            "runtime:data/runtime/ledger.db",
            "research:data/research/latest.json",
            "analysis:data/analysis/base.duckdb",
        ],
    )
    lines.extend(
        [
            "# Contradiction Log",
            "",
            "## Summary",
            f"- compiled_at: `{_iso_utc(generated_ts)}`",
            f"- active_contradictions: `{len(contradictions)}`",
            f"- recent_run_lineage_fragmentation: `{ledger.get('run_lineage_fragmentation', 0)}`",
            f"- analysis_failed_checks: `{_fmt_int((analysis.get('validation') or {}).get('failed_checks'))}`",
            "",
            "## Analysis provenance",
        ]
    )
    lines.extend(_analysis_provenance_lines(analysis))
    lines.extend(
        [
            "",
            "## Active entries",
            "",
        ]
    )
    if not contradictions:
        lines.append("No active contradictions detected by the base compiler bridge.")
        lines.append("")
        return "\n".join(lines) + "\n"

    for item in contradictions:
        lines.extend(
            [
                f"### {item.key} — {item.title}",
                f"- status: `{item.status}`",
                f"- evidence_a: {item.evidence_a}",
                f"- evidence_b: {item.evidence_b}",
                f"- why_it_matters: {item.why_it_matters}",
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def _render_experiment_registry(
    *,
    output_path: Path,
    generated_ts: float,
    experiments: list[ExperimentEntry],
    analysis: dict[str, Any],
) -> str:
    created = _read_existing_created(output_path, _date_utc(generated_ts))
    updated = _date_utc(generated_ts)
    lines = _render_frontmatter(
        title="Experiment Registry",
        created=created,
        updated=updated,
        doc_type="experiment",
        tags=["experiment", "compiler", "runtime-truth", "analysis-bridge"],
        sources=[
            "runtime:data/runtime/status.json",
            "runtime:data/runtime/strategy_metrics.json",
            "runtime:data/runtime/ledger.db",
            "research:data/research/latest.json",
            "analysis:data/analysis/base.duckdb",
        ],
    )
    lines.extend(
        [
            "# Experiment Registry",
            "",
            "Base-layer registry only. This compiler bridge records active measurement tracks and closure conditions without adding a recommendation layer.",
            "",
            "## Analysis provenance",
        ]
    )
    lines.extend(_analysis_provenance_lines(analysis))
    lines.append("")
    for item in experiments:
        lines.extend(
            [
                f"## {item.key} — {item.title}",
                f"- status: `{item.status}`",
                f"- objective: {item.objective}",
                "- evidence:",
            ]
        )
        lines.extend(f"  - {entry}" for entry in item.evidence)
        lines.append("- close_criteria:")
        lines.extend(f"  - {entry}" for entry in item.close_criteria)
        lines.append("")
    return "\n".join(lines) + "\n"


def _write_if_changed(path: Path, content: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") == content:
        return False
    path.write_text(content, encoding="utf-8")
    return True


def compile_wiki(
    *,
    repo_root: str | Path = REPO_ROOT,
    runtime_dir: str | Path = DEFAULT_RUNTIME_DIR,
    research_path: str | Path = DEFAULT_RESEARCH_PATH,
    duckdb_path: str | Path = DEFAULT_DUCKDB_PATH,
    wiki_root: str | Path = DEFAULT_WIKI_ROOT,
    generated_ts: float | None = None,
    refresh_analysis: bool = True,
) -> dict[str, Any]:
    repo_root = Path(repo_root)
    runtime_dir = Path(runtime_dir)
    research_path = Path(research_path)
    duckdb_path = Path(duckdb_path)
    wiki_root = Path(wiki_root)
    generated_ts = float(generated_ts if generated_ts is not None else time.time())

    status_path = runtime_dir / "status.json"
    strategy_metrics_path = runtime_dir / "strategy_metrics.json"
    ledger_path = runtime_dir / "ledger.db"
    missing = [
        str(path)
        for path in (status_path, strategy_metrics_path, ledger_path, research_path)
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("Missing required wiki compiler inputs: " + ", ".join(missing))

    status = _read_json(status_path)
    strategy_metrics = _read_json(strategy_metrics_path)
    research_latest = _read_json(research_path)

    analysis_refresh = _ensure_analysis(
        runtime_dir=runtime_dir,
        research_path=research_path,
        duckdb_path=duckdb_path,
        refresh_analysis=refresh_analysis,
    )
    if analysis_refresh.get("analysis_mode") == "runtime_fallback_no_duckdb":
        analysis = _collect_runtime_analysis_fallback(runtime_dir, status.get("run_id"))
    else:
        analysis = _collect_analysis_summary(duckdb_path, status.get("run_id"))
    analysis["analysis_mode"] = analysis_refresh.get("analysis_mode")
    ledger = _ledger_summary(runtime_dir, status.get("run_id"))
    contradictions = _collect_contradictions(
        status=status,
        strategy_metrics=strategy_metrics,
        research_latest=research_latest,
        analysis=analysis,
        ledger=ledger,
    )
    experiments = _collect_experiments(
        status=status,
        strategy_metrics=strategy_metrics,
        analysis=analysis,
        ledger=ledger,
        contradictions=contradictions,
    )

    outputs = {
        CURRENT_STATE_PATH: _render_current_state(
            output_path=wiki_root / CURRENT_STATE_PATH,
            generated_ts=generated_ts,
            status=status,
            strategy_metrics=strategy_metrics,
            research_latest=research_latest,
            analysis=analysis,
            ledger=ledger,
            contradictions=contradictions,
        ),
        CONTRADICTION_LOG_PATH: _render_contradiction_log(
            output_path=wiki_root / CONTRADICTION_LOG_PATH,
            generated_ts=generated_ts,
            contradictions=contradictions,
            ledger=ledger,
            analysis=analysis,
        ),
        EXPERIMENT_REGISTRY_PATH: _render_experiment_registry(
            output_path=wiki_root / EXPERIMENT_REGISTRY_PATH,
            generated_ts=generated_ts,
            experiments=experiments,
            analysis=analysis,
        ),
    }

    changed_files: list[str] = []
    for relative_path, content in outputs.items():
        full_path = wiki_root / relative_path
        if _write_if_changed(full_path, content):
            changed_files.append(str(full_path))

    return {
        "repo_root": str(repo_root),
        "runtime_dir": str(runtime_dir),
        "research_path": str(research_path),
        "duckdb_path": str(duckdb_path),
        "wiki_root": str(wiki_root),
        "compiled_at": _iso_utc(generated_ts),
        "changed_files": changed_files,
        "contradiction_count": len(contradictions),
        "experiment_count": len(experiments),
        "analysis_refresh": analysis_refresh,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compile contradiction-first wiki base outputs from runtime and Batch 3-5 analysis artifacts.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--runtime-dir", default=str(DEFAULT_RUNTIME_DIR))
    parser.add_argument("--research-path", default=str(DEFAULT_RESEARCH_PATH))
    parser.add_argument("--duckdb-path", default=str(DEFAULT_DUCKDB_PATH))
    parser.add_argument("--wiki-root", default=str(DEFAULT_WIKI_ROOT))
    parser.add_argument("--generated-ts", type=float, default=None)
    parser.add_argument(
        "--skip-analysis-refresh",
        action="store_true",
        help="Use existing DuckDB outputs if present instead of rebuilding Batch 3-5 bridge tables/views.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = compile_wiki(
        repo_root=args.repo_root,
        runtime_dir=args.runtime_dir,
        research_path=args.research_path,
        duckdb_path=args.duckdb_path,
        wiki_root=args.wiki_root,
        generated_ts=args.generated_ts,
        refresh_analysis=not args.skip_analysis_refresh,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
