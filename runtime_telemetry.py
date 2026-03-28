from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional


class RuntimeTelemetryStore:
    def __init__(self, runtime_dir: str | Path = "data/runtime"):
        self.runtime_dir = Path(runtime_dir)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.runtime_dir / "status.json"
        self.events_path = self.runtime_dir / "events.jsonl"
        self.paper_state_path = self.runtime_dir / "paper_broker_state.json"

    @staticmethod
    def make_run_id(prefix: str = "paper") -> str:
        return f"{prefix}-{uuid.uuid4().hex[:8]}"

    def append_event(self, event_type: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        event = {
            "event_type": event_type,
            "timestamp": time.time(),
            "payload": payload or {},
        }
        with self.events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event) + "\n")
        return event

    def update_status(self, **fields: Any) -> Dict[str, Any]:
        status = self.read_status()
        status.update(fields)
        status["heartbeat_ts"] = float(fields.get("heartbeat_ts", time.time()))
        self.status_path.write_text(json.dumps(status, indent=2), encoding="utf-8")
        return status

    def read_status(self) -> Dict[str, Any]:
        if not self.status_path.exists():
            return {}
        try:
            return json.loads(self.status_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def save_paper_state(self, state: Dict[str, Any]) -> Path:
        self.paper_state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")
        return self.paper_state_path

    def load_paper_state(self) -> Dict[str, Any]:
        if not self.paper_state_path.exists():
            return {}
        try:
            return json.loads(self.paper_state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
