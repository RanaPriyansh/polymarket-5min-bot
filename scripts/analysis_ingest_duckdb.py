from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import duckdb


@dataclass(frozen=True)
class SourceArtifact:
    source_name: str
    path: Path
    required: bool = True


def _json_text(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _coerce_dict(payload: Any, *, source: str) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    raise ValueError(f"Expected JSON object in {source}, found {type(payload).__name__}")


def _read_json(path: Path) -> dict[str, Any]:
    return _coerce_dict(json.loads(path.read_text(encoding="utf-8")), source=str(path))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as fh:
        for line_number, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            rows.append(_coerce_dict(json.loads(line), source=f"{path}:{line_number}"))
    return rows


def _source_metadata_rows(sources: Iterable[SourceArtifact], active_run_id: str | None) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for source in sources:
        exists = source.path.exists()
        stat = source.path.stat() if exists else None
        rows.append(
            (
                source.source_name,
                str(source.path),
                source.required,
                exists,
                stat.st_mtime if stat else None,
                stat.st_size if stat else None,
                active_run_id,
            )
        )
    return rows


def _runtime_status_row(status: dict[str, Any], ingest_ts: float, source_mtime: float | None) -> tuple[Any, ...]:
    risk = status.get("risk") or {}
    return (
        ingest_ts,
        source_mtime,
        status.get("run_id"),
        status.get("phase"),
        status.get("mode"),
        status.get("heartbeat_ts"),
        status.get("bankroll"),
        status.get("fetched_markets"),
        status.get("processed_markets"),
        status.get("loop_count"),
        status.get("open_position_count"),
        status.get("resolved_trade_count"),
        len(status.get("active_slots") or []),
        len(status.get("pending_resolution_slots") or []),
        len(status.get("strategies") or []),
        risk.get("open_order_count"),
        risk.get("marked_position_count"),
        risk.get("pending_settlement_count"),
        risk.get("realized_pnl_total"),
        risk.get("unrealized_pnl_total"),
        risk.get("total_gross_exposure"),
        _json_text(status),
    )


def _strategy_metrics_rows(
    metrics: dict[str, Any],
    ingest_ts: float,
    source_mtime: float | None,
    active_run_id: str | None,
) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for strategy_family in sorted(metrics):
        payload = metrics[strategy_family] or {}
        rows.append(
            (
                ingest_ts,
                source_mtime,
                active_run_id,
                strategy_family,
                payload.get("markets_seen"),
                payload.get("quotes_submitted"),
                payload.get("orders_filled"),
                payload.get("orders_resting"),
                payload.get("cancellations"),
                payload.get("toxic_book_skips"),
                payload.get("realized_pnl"),
                _json_text(payload),
            )
        )
    return rows


def _payload_field(payload: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in payload:
            return payload.get(name)
    return None


def _runtime_event_rows(events: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for line_number, event in enumerate(events, start=1):
        payload = event.get("payload") or {}
        rows.append(
            (
                line_number,
                event.get("ts"),
                _payload_field(payload, "run_id") or event.get("run_id"),
                event.get("event_type"),
                _payload_field(payload, "market_id", "id"),
                _payload_field(payload, "market_slug", "slug"),
                payload.get("slot_id"),
                payload.get("strategy_family"),
                payload.get("asset"),
                payload.get("interval_minutes"),
                _payload_field(payload, "order_id", "bid_order_id", "ask_order_id"),
                payload.get("side"),
                payload.get("outcome"),
                _json_text(event),
            )
        )
    return rows


def _market_sample_rows(samples: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for line_number, sample in enumerate(samples, start=1):
        rows.append(
            (
                line_number,
                sample.get("ts"),
                sample.get("run_id"),
                sample.get("market_id"),
                sample.get("market_slug"),
                sample.get("slot_id"),
                sample.get("is_tradeable"),
                sample.get("book_depth"),
                sample.get("book_notional"),
                sample.get("book_spread_bps"),
                sample.get("volume"),
                _json_text(sample.get("book_reasons") or []),
                _json_text(sample),
            )
        )
    return rows


def _ledger_event_rows(ledger_db_path: Path) -> list[tuple[Any, ...]]:
    if not ledger_db_path.exists():
        return []
    with sqlite3.connect(ledger_db_path) as conn:
        conn.row_factory = sqlite3.Row
        table_row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'ledger_events'"
        ).fetchone()
        if table_row is None:
            raise ValueError(f"Invalid ledger.db schema at {ledger_db_path}: missing ledger_events table")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(ledger_events)").fetchall()}
        required_columns = {
            "event_id",
            "stream",
            "aggregate_id",
            "sequence_num",
            "event_type",
            "event_ts",
            "recorded_ts",
            "run_id",
            "idempotency_key",
            "causation_id",
            "correlation_id",
            "schema_version",
            "payload_json",
        }
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            raise ValueError(
                f"Invalid ledger.db schema at {ledger_db_path}: ledger_events missing columns: {', '.join(missing_columns)}"
            )
        rows = conn.execute(
            """
            SELECT event_id, stream, aggregate_id, sequence_num, event_type, event_ts,
                   recorded_ts, run_id, idempotency_key, causation_id, correlation_id,
                   schema_version, payload_json
            FROM ledger_events
            ORDER BY recorded_ts ASC, aggregate_id ASC, sequence_num ASC, event_id ASC
            """
        ).fetchall()
    result: list[tuple[Any, ...]] = []
    for row in rows:
        payload_json = row["payload_json"]
        payload = _coerce_dict(json.loads(payload_json), source=f"{ledger_db_path}:ledger_events:{row['event_id']}")
        result.append(
            (
                row["event_id"],
                row["stream"],
                row["aggregate_id"],
                row["sequence_num"],
                row["event_type"],
                row["event_ts"],
                row["recorded_ts"],
                row["run_id"],
                row["idempotency_key"],
                row["causation_id"],
                row["correlation_id"],
                row["schema_version"],
                payload.get("market_id"),
                _payload_field(payload, "market_slug", "slug"),
                payload.get("slot_id"),
                payload.get("strategy_family"),
                payload.get("side"),
                payload.get("outcome"),
                _payload_field(payload, "size", "filled_qty", "remaining_qty"),
                _payload_field(payload, "price", "average_fill_price"),
                _payload_field(payload, "status", "fill_status"),
                payload.get("realized_pnl"),
                payload_json,
            )
        )
    return result


def _research_snapshot_row(research_payload: dict[str, Any], ingest_ts: float, source_mtime: float | None) -> tuple[Any, ...]:
    context = research_payload.get("context") or {}
    return (
        ingest_ts,
        source_mtime,
        research_payload.get("cycle_id"),
        research_payload.get("created_at"),
        (research_payload.get("raw_context") or {}).get("gate_state"),
        research_payload.get("source"),
        context.get("runtime_dir"),
        context.get("artifact_dir"),
        _json_text(research_payload),
    )


def _replace_table(conn: duckdb.DuckDBPyConnection, ddl: str, table_name: str, rows: list[tuple[Any, ...]]) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(ddl)
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(rows[0]))
    conn.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)


def build_duckdb(
    *,
    runtime_dir: str | Path = "data/runtime",
    research_path: str | Path = "data/research/latest.json",
    duckdb_path: str | Path = "data/analysis/base.duckdb",
) -> dict[str, Any]:
    runtime_dir = Path(runtime_dir)
    research_path = Path(research_path)
    duckdb_path = Path(duckdb_path)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)

    status_path = runtime_dir / "status.json"
    strategy_metrics_path = runtime_dir / "strategy_metrics.json"
    ledger_db_path = runtime_dir / "ledger.db"
    events_path = runtime_dir / "events.jsonl"
    market_samples_path = runtime_dir / "market_samples.jsonl"

    required_sources = [
        SourceArtifact("runtime_status", status_path, True),
        SourceArtifact("strategy_metrics", strategy_metrics_path, True),
        SourceArtifact("ledger_db", ledger_db_path, True),
        SourceArtifact("runtime_events", events_path, False),
        SourceArtifact("market_samples", market_samples_path, False),
        SourceArtifact("research_latest", research_path, True),
    ]
    missing = [source.source_name for source in required_sources if source.required and not source.path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing required ingestion sources: {', '.join(missing)}")

    ingest_ts = time.time()
    status = _read_json(status_path)
    metrics = _read_json(strategy_metrics_path)
    research_payload = _read_json(research_path)
    runtime_events = _read_jsonl(events_path)
    market_samples = _read_jsonl(market_samples_path)
    ledger_events = _ledger_event_rows(ledger_db_path)
    active_run_id = status.get("run_id")

    with duckdb.connect(str(duckdb_path)) as conn:
        _replace_table(
            conn,
            """
            CREATE TABLE ingestion_runs (
                ingest_ts DOUBLE,
                active_run_id VARCHAR,
                runtime_dir VARCHAR,
                research_path VARCHAR,
                duckdb_path VARCHAR
            )
            """,
            "ingestion_runs",
            [(ingest_ts, active_run_id, str(runtime_dir), str(research_path), str(duckdb_path))],
        )
        _replace_table(
            conn,
            """
            CREATE TABLE ingestion_sources (
                source_name VARCHAR,
                source_path VARCHAR,
                required BOOLEAN,
                exists_on_disk BOOLEAN,
                source_mtime DOUBLE,
                source_size_bytes BIGINT,
                active_run_id VARCHAR
            )
            """,
            "ingestion_sources",
            _source_metadata_rows(required_sources, active_run_id),
        )
        _replace_table(
            conn,
            """
            CREATE TABLE runtime_status_snapshot (
                ingest_ts DOUBLE,
                source_mtime DOUBLE,
                run_id VARCHAR,
                phase VARCHAR,
                mode VARCHAR,
                heartbeat_ts DOUBLE,
                bankroll DOUBLE,
                fetched_markets BIGINT,
                processed_markets BIGINT,
                loop_count BIGINT,
                open_position_count BIGINT,
                resolved_trade_count BIGINT,
                active_slot_count BIGINT,
                pending_resolution_slot_count BIGINT,
                strategy_count BIGINT,
                risk_open_order_count BIGINT,
                risk_marked_position_count BIGINT,
                risk_pending_settlement_count BIGINT,
                risk_realized_pnl_total DOUBLE,
                risk_unrealized_pnl_total DOUBLE,
                risk_total_gross_exposure DOUBLE,
                raw_json JSON
            )
            """,
            "runtime_status_snapshot",
            [
                _runtime_status_row(
                    status,
                    ingest_ts,
                    status_path.stat().st_mtime if status_path.exists() else None,
                )
            ],
        )
        _replace_table(
            conn,
            """
            CREATE TABLE strategy_metrics_snapshot (
                ingest_ts DOUBLE,
                source_mtime DOUBLE,
                run_id VARCHAR,
                strategy_family VARCHAR,
                markets_seen BIGINT,
                quotes_submitted BIGINT,
                orders_filled BIGINT,
                orders_resting BIGINT,
                cancellations BIGINT,
                toxic_book_skips BIGINT,
                realized_pnl DOUBLE,
                raw_json JSON
            )
            """,
            "strategy_metrics_snapshot",
            _strategy_metrics_rows(
                metrics,
                ingest_ts,
                strategy_metrics_path.stat().st_mtime if strategy_metrics_path.exists() else None,
                active_run_id,
            ),
        )
        _replace_table(
            conn,
            """
            CREATE TABLE ledger_events (
                event_id VARCHAR,
                stream VARCHAR,
                aggregate_id VARCHAR,
                sequence_num BIGINT,
                event_type VARCHAR,
                event_ts DOUBLE,
                recorded_ts DOUBLE,
                run_id VARCHAR,
                idempotency_key VARCHAR,
                causation_id VARCHAR,
                correlation_id VARCHAR,
                schema_version BIGINT,
                market_id VARCHAR,
                market_slug VARCHAR,
                slot_id VARCHAR,
                strategy_family VARCHAR,
                side VARCHAR,
                outcome VARCHAR,
                size DOUBLE,
                price DOUBLE,
                status VARCHAR,
                realized_pnl DOUBLE,
                raw_json JSON
            )
            """,
            "ledger_events",
            ledger_events,
        )
        _replace_table(
            conn,
            """
            CREATE TABLE runtime_events (
                line_number BIGINT,
                ts DOUBLE,
                run_id VARCHAR,
                event_type VARCHAR,
                market_id VARCHAR,
                market_slug VARCHAR,
                slot_id VARCHAR,
                strategy_family VARCHAR,
                asset VARCHAR,
                interval_minutes BIGINT,
                order_id VARCHAR,
                side VARCHAR,
                outcome VARCHAR,
                raw_json JSON
            )
            """,
            "runtime_events",
            _runtime_event_rows(runtime_events),
        )
        _replace_table(
            conn,
            """
            CREATE TABLE market_samples (
                line_number BIGINT,
                ts DOUBLE,
                run_id VARCHAR,
                market_id VARCHAR,
                market_slug VARCHAR,
                slot_id VARCHAR,
                is_tradeable BOOLEAN,
                book_depth DOUBLE,
                book_notional DOUBLE,
                book_spread_bps DOUBLE,
                volume DOUBLE,
                book_reasons_json JSON,
                raw_json JSON
            )
            """,
            "market_samples",
            _market_sample_rows(market_samples),
        )
        _replace_table(
            conn,
            """
            CREATE TABLE research_latest_snapshot (
                ingest_ts DOUBLE,
                source_mtime DOUBLE,
                cycle_id VARCHAR,
                created_at DOUBLE,
                gate_state VARCHAR,
                source VARCHAR,
                runtime_dir VARCHAR,
                artifact_dir VARCHAR,
                raw_json JSON
            )
            """,
            "research_latest_snapshot",
            [
                _research_snapshot_row(
                    research_payload,
                    ingest_ts,
                    research_path.stat().st_mtime if research_path.exists() else None,
                )
            ],
        )
    return {
        "duckdb_path": str(duckdb_path),
        "active_run_id": active_run_id,
        "runtime_events_rows": len(runtime_events),
        "market_samples_rows": len(market_samples),
        "ledger_events_rows": len(ledger_events),
        "strategy_metrics_rows": len(metrics),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build DuckDB base tables from runtime and research artifacts.")
    parser.add_argument("--runtime-dir", default="data/runtime")
    parser.add_argument("--research-path", default="data/research/latest.json")
    parser.add_argument("--duckdb-path", default="data/analysis/base.duckdb")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = build_duckdb(
        runtime_dir=args.runtime_dir,
        research_path=args.research_path,
        duckdb_path=args.duckdb_path,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
