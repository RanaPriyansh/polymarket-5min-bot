from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Dict, Iterable, Optional


class RuntimeTelemetry:
    def __init__(self, runtime_dir: str | Path):
        self.runtime_dir = Path(runtime_dir)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.runtime_dir / "status.json"
        self.events_path = self.runtime_dir / "events.jsonl"
        self.strategy_metrics_path = self.runtime_dir / "strategy_metrics.json"
        self.market_samples_path = self.runtime_dir / "market_samples.jsonl"

    @staticmethod
    def make_run_id(prefix: str = "paper") -> str:
        return f"{prefix}-{int(time.time())}-{uuid.uuid4().hex[:8]}"

    def append_event(self, event_type: str, payload: Dict) -> None:
        event = {
            "ts": time.time(),
            "event_type": event_type,
            "payload": payload,
        }
        with self.events_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, sort_keys=True, default=str) + "\n")

    def append_market_sample(self, payload: Dict) -> None:
        sample = {
            "ts": time.time(),
            **payload,
        }
        with self.market_samples_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(sample, sort_keys=True, default=str) + "\n")

    def update_status(self, **fields) -> Dict:
        current = self.read_json(self.status_path) or {}
        current.update(fields)
        current["heartbeat_ts"] = time.time()
        self.status_path.write_text(json.dumps(current, indent=2, sort_keys=True, default=str), encoding="utf-8")
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

    def read_strategy_metrics(self) -> Dict:
        return self.read_json(self.strategy_metrics_path) or {}

    def read_jsonl(self, path: Path, limit: Optional[int] = None) -> Iterable[Dict]:
        if not path.exists():
            return []
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if limit is not None:
            rows = rows[-limit:]
        return rows

    def read_events(self, limit: Optional[int] = None):
        return list(self.read_jsonl(self.events_path, limit=limit))

    def read_market_samples(self, limit: Optional[int] = None):
        return list(self.read_jsonl(self.market_samples_path, limit=limit))
