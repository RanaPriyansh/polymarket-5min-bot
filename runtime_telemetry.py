from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from research.loop import read_jsonl_tail


class RuntimeTelemetry:
    def __init__(self, runtime_dir: str | Path):
        self.runtime_dir = Path(runtime_dir)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.runtime_dir / "status.json"
        self.events_path = self.runtime_dir / "events.jsonl"
        self.strategy_metrics_path = self.runtime_dir / "strategy_metrics.json"
        self.market_samples_path = self.runtime_dir / "market_samples.jsonl"
        self.latest_status_text_path = self.runtime_dir / "latest-status.txt"

    @staticmethod
    def make_run_id(prefix: str = "paper") -> str:
        return f"{prefix}-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    def append_event(self, event_type: str, payload: Dict, *, run_id: str | None = None) -> None:
        resolved_run_id = run_id or payload.get("run_id") or self.current_run_id()
        event = {
            "ts": time.time(),
            "event_type": event_type,
            "run_id": resolved_run_id,
            "payload": payload,
        }
        with self.events_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, sort_keys=True, default=str) + "\n")

    def append_market_sample(self, payload: Dict, *, run_id: str | None = None) -> None:
        resolved_run_id = run_id or payload.get("run_id") or self.current_run_id()
        sample = {
            "ts": time.time(),
            "run_id": resolved_run_id,
            **payload,
        }
        with self.market_samples_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(sample, sort_keys=True, default=str) + "\n")

    def update_status(self, **fields) -> Dict:
        current = self.read_json(self.status_path) or {}
        current.update(fields)
        current["heartbeat_ts"] = time.time()
        self.status_path.write_text(json.dumps(current, indent=2, sort_keys=True, default=str), encoding="utf-8")
        self.latest_status_text_path.write_text(self._render_latest_status_text(current), encoding="utf-8")
        return current

    def write_strategy_metrics(self, metrics: Dict) -> None:
        self.strategy_metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True, default=str), encoding="utf-8")

    def write_runtime_snapshot(self, **snapshot) -> Dict:
        return self.update_status(**snapshot)

    def read_json(self, path: Path) -> Optional[Dict]:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def read_status(self) -> Dict:
        return self.read_json(self.status_path) or {}

    def current_run_id(self) -> str | None:
        status = self.read_status()
        run_id = status.get("run_id")
        return str(run_id) if run_id else None

    def read_strategy_metrics(self) -> Dict:
        return self.read_json(self.strategy_metrics_path) or {}

    @staticmethod
    def _row_run_id(row: Dict[str, Any]) -> str | None:
        run_id = row.get("run_id")
        if run_id:
            return str(run_id)
        payload = row.get("payload") or {}
        if isinstance(payload, dict) and payload.get("run_id"):
            return str(payload.get("run_id"))
        return None

    def read_jsonl(self, path: Path, limit: Optional[int] = None, run_id: str | None = None) -> Iterable[Dict]:
        rows = list(read_jsonl_tail(path, limit=limit))
        if run_id is None:
            return rows
        return [row for row in rows if self._row_run_id(row) == run_id]

    def read_events(self, limit: Optional[int] = None, run_id: str | None = None):
        return list(self.read_jsonl(self.events_path, limit=limit, run_id=run_id))

    def read_market_samples(self, limit: Optional[int] = None, run_id: str | None = None):
        return list(self.read_jsonl(self.market_samples_path, limit=limit, run_id=run_id))

    @staticmethod
    def _render_latest_status_text(status: Dict) -> str:
        risk = status.get("risk", {}) or {}
        return (
            f"run_id={status.get('run_id', 'unknown')}\n"
            f"phase={status.get('phase', 'unknown')} mode={status.get('mode', 'unknown')} loop_count={status.get('loop_count', 0)}\n"
            f"bankroll={float(risk.get('capital', status.get('bankroll', 0.0))):.2f}\n"
            f"open_position_count={status.get('open_position_count', 0)} resolved_trade_count={status.get('resolved_trade_count', 0)}\n"
            f"heartbeat_ts={status.get('heartbeat_ts', 0)}\n"
        )
