from __future__ import annotations

import json
import shutil
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
    def _snapshot_label(snapshot_ts: float) -> str:
        return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(snapshot_ts))

    def _artifact_metadata(self, source: Path, destination: Path) -> Dict[str, Any]:
        stat = destination.stat()
        return {
            "source": str(source),
            "dest": str(destination),
            "mtime": stat.st_mtime,
            "size_bytes": stat.st_size,
        }

    def _copy_artifact_if_present(self, source: Path, destination: Path, artifacts: list[Dict[str, Any]]) -> None:
        if not source.exists() or not source.is_file():
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        artifacts.append(self._artifact_metadata(source, destination))

    def preserve_run_evidence(
        self,
        *,
        trigger: str,
        run_id: str | None = None,
        snapshot_ts: float | None = None,
    ) -> Dict[str, Any]:
        snapshot_epoch = snapshot_ts if snapshot_ts is not None else time.time()
        snapshot_label = self._snapshot_label(snapshot_epoch)
        data_dir = self.runtime_dir.parent
        snapshot_root = data_dir / "forensic-snapshots"
        snapshot_dir = snapshot_root / snapshot_label
        suffix = 1
        while snapshot_dir.exists():
            snapshot_dir = snapshot_root / f"{snapshot_label}-{suffix}"
            suffix += 1
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        status = self.read_status()
        resolved_run_id = run_id or status.get("run_id") or self.current_run_id()
        risk = status.get("risk", {}) or {}
        artifacts: list[Dict[str, Any]] = []

        self._copy_artifact_if_present(self.status_path, snapshot_dir / "status.json", artifacts)
        self._copy_artifact_if_present(self.events_path, snapshot_dir / "events.jsonl", artifacts)
        self._copy_artifact_if_present(self.market_samples_path, snapshot_dir / "market_samples.jsonl", artifacts)
        self._copy_artifact_if_present(self.strategy_metrics_path, snapshot_dir / "strategy_metrics.json", artifacts)
        self._copy_artifact_if_present(self.runtime_dir / "ledger.db", snapshot_dir / "ledger.db", artifacts)
        self._copy_artifact_if_present(self.latest_status_text_path, snapshot_dir / "latest-status.txt", artifacts)

        research_dir = data_dir / "research"
        self._copy_artifact_if_present(research_dir / "latest.json", snapshot_dir / "research-latest.json", artifacts)
        self._copy_artifact_if_present(research_dir / "latest.md", snapshot_dir / "research-latest.md", artifacts)

        for ops_file in sorted(path for path in self.runtime_dir.glob("ops*") if path.is_file()):
            self._copy_artifact_if_present(ops_file, snapshot_dir / ops_file.name, artifacts)
        for runtime_artifact in (
            self.runtime_dir / "fill_markout_audit_latest.md",
            self.runtime_dir / "settlement_latency_audit_latest.md",
            self.runtime_dir / "reconcile_metrics_latest.txt",
        ):
            self._copy_artifact_if_present(runtime_artifact, snapshot_dir / runtime_artifact.name, artifacts)

        manifest = {
            "snapshot_ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(snapshot_epoch)),
            "snapshot_dir": str(snapshot_dir),
            "trigger": trigger,
            "run_id": resolved_run_id,
            "bankroll": risk.get("capital", status.get("bankroll")),
            "realized_pnl": risk.get("realized_pnl_total", risk.get("daily_pnl")),
            "win_rate": status.get("win_rate"),
            "resolved_trade_count": status.get("resolved_trade_count"),
            "artifacts": artifacts,
        }
        manifest_path = snapshot_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True, default=str), encoding="utf-8")
        return manifest

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
