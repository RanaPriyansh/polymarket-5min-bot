from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional


@dataclass(frozen=True)
class LedgerEvent:
    event_id: str
    stream: str
    aggregate_id: str
    sequence_num: int
    event_type: str
    event_ts: float
    recorded_ts: float
    run_id: str
    idempotency_key: str
    causation_id: Optional[str]
    correlation_id: Optional[str]
    schema_version: int
    payload: dict[str, Any]


class SQLiteLedger:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ledger_events (
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
                "CREATE INDEX IF NOT EXISTS idx_ledger_stream_aggregate_seq ON ledger_events(stream, aggregate_id, sequence_num)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ledger_run_recorded_ts ON ledger_events(run_id, recorded_ts)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ledger_event_type_recorded_ts ON ledger_events(event_type, recorded_ts)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS replay_checkpoints (
                    checkpoint_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    created_ts REAL NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def append_event(self, event: LedgerEvent) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO ledger_events (
                    event_id, stream, aggregate_id, sequence_num, event_type,
                    event_ts, recorded_ts, run_id, idempotency_key,
                    causation_id, correlation_id, schema_version, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.stream,
                    event.aggregate_id,
                    event.sequence_num,
                    event.event_type,
                    event.event_ts,
                    event.recorded_ts,
                    event.run_id,
                    event.idempotency_key,
                    event.causation_id,
                    event.correlation_id,
                    event.schema_version,
                    json.dumps(event.payload, separators=(",", ":"), sort_keys=True),
                ),
            )
            conn.commit()

    def list_events(self, *, run_id: str | None = None) -> list[LedgerEvent]:
        query = (
            "SELECT event_id, stream, aggregate_id, sequence_num, event_type, event_ts, "
            "recorded_ts, run_id, idempotency_key, causation_id, correlation_id, schema_version, payload_json "
            "FROM ledger_events"
        )
        params: tuple[Any, ...] = ()
        if run_id is not None:
            query += " WHERE run_id = ?"
            params = (run_id,)
        query += " ORDER BY recorded_ts ASC, aggregate_id ASC, sequence_num ASC, event_id ASC"

        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_event(row) for row in rows]

    def reset_projections(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM replay_checkpoints")
            conn.commit()

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> LedgerEvent:
        return LedgerEvent(
            event_id=row["event_id"],
            stream=row["stream"],
            aggregate_id=row["aggregate_id"],
            sequence_num=row["sequence_num"],
            event_type=row["event_type"],
            event_ts=row["event_ts"],
            recorded_ts=row["recorded_ts"],
            run_id=row["run_id"],
            idempotency_key=row["idempotency_key"],
            causation_id=row["causation_id"],
            correlation_id=row["correlation_id"],
            schema_version=row["schema_version"],
            payload=json.loads(row["payload_json"]),
        )
