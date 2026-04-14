from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

HEARTBEAT_STALE_SECONDS = 180


def load_json(path: Path, default=None):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {} if default is None else default


def fmt_ts(ts: float | None) -> str:
    if not ts:
        return "n/a"
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def artifact_truth_context(
    runtime_dir: Path,
    *,
    artifact_run_id: str | None,
    generated_at_ts: float | None = None,
    stale_after_seconds: int = HEARTBEAT_STALE_SECONDS,
) -> dict:
    status = load_json(runtime_dir / "status.json", {})
    generated_at_ts = float(generated_at_ts if generated_at_ts is not None else time.time())
    current_run_id = str(status.get("run_id", "") or "")
    artifact_run_id = str(artifact_run_id or "")
    heartbeat_ts = float(status.get("heartbeat_ts", 0.0) or 0.0)
    heartbeat_age_seconds = generated_at_ts - heartbeat_ts if heartbeat_ts > 0 else None
    run_match = bool(artifact_run_id) and artifact_run_id == current_run_id
    stale_reasons: list[str] = []
    if not current_run_id:
        stale_reasons.append("missing_status_run_id")
    if not artifact_run_id:
        stale_reasons.append("missing_artifact_run_id")
    elif artifact_run_id != current_run_id:
        stale_reasons.append("run_id_mismatch")
    if heartbeat_ts <= 0:
        stale_reasons.append("missing_heartbeat")
    elif heartbeat_age_seconds is not None and heartbeat_age_seconds > stale_after_seconds:
        stale_reasons.append(f"heartbeat_age>{stale_after_seconds}s")
    freshness = "CURRENT" if not stale_reasons else "STALE"
    return {
        "generated_at_ts": generated_at_ts,
        "artifact_run_id": artifact_run_id,
        "status_run_id": current_run_id,
        "run_match": run_match,
        "heartbeat_ts": heartbeat_ts,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "freshness": freshness,
        "stale_reasons": stale_reasons,
    }


def artifact_truth_lines(
    runtime_dir: Path,
    *,
    artifact_run_id: str | None,
    generated_at_ts: float | None = None,
    markdown: bool = False,
    stale_after_seconds: int = HEARTBEAT_STALE_SECONDS,
) -> list[str]:
    ctx = artifact_truth_context(
        runtime_dir,
        artifact_run_id=artifact_run_id,
        generated_at_ts=generated_at_ts,
        stale_after_seconds=stale_after_seconds,
    )
    reasons = ", ".join(ctx["stale_reasons"]) if ctx["stale_reasons"] else "none"
    heartbeat_age = ctx["heartbeat_age_seconds"]
    heartbeat_age_text = f"{heartbeat_age:.1f}s" if heartbeat_age is not None else "n/a"
    fields = [
        ("generated_at", fmt_ts(ctx["generated_at_ts"])),
        ("artifact_run_id", ctx["artifact_run_id"] or "unknown"),
        ("status_run_id", ctx["status_run_id"] or "unknown"),
        ("run_match", "yes" if ctx["run_match"] else "no"),
        ("heartbeat_ts", fmt_ts(ctx["heartbeat_ts"])),
        ("heartbeat_age", heartbeat_age_text),
        ("freshness", ctx["freshness"]),
        ("stale_reasons", reasons),
    ]
    if markdown:
        return [f"- {key}: `{value}`" for key, value in fields]
    return [f"{key}: {value}" for key, value in fields]


def status_truth_context(
    runtime_dir: Path,
    *,
    generated_at_ts: float | None = None,
    stale_after_seconds: int = HEARTBEAT_STALE_SECONDS,
) -> dict:
    status = load_json(runtime_dir / "status.json", {})
    generated_at_ts = float(generated_at_ts if generated_at_ts is not None else time.time())
    status_run_id = str(status.get("run_id", "") or "")
    heartbeat_ts = float(status.get("heartbeat_ts", 0.0) or 0.0)
    heartbeat_age_seconds = generated_at_ts - heartbeat_ts if heartbeat_ts > 0 else None
    stale_reasons: list[str] = []
    if not status_run_id:
        stale_reasons.append("missing_status_run_id")
    if heartbeat_ts <= 0:
        stale_reasons.append("missing_heartbeat")
    elif heartbeat_age_seconds is not None and heartbeat_age_seconds > stale_after_seconds:
        stale_reasons.append(f"heartbeat_age>{stale_after_seconds}s")
    freshness = "CURRENT" if not stale_reasons else "STALE"
    return {
        "generated_at_ts": generated_at_ts,
        "status_run_id": status_run_id,
        "heartbeat_ts": heartbeat_ts,
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "freshness": freshness,
        "stale_reasons": stale_reasons,
    }


def status_truth_lines(
    runtime_dir: Path,
    *,
    generated_at_ts: float | None = None,
    markdown: bool = False,
    stale_after_seconds: int = HEARTBEAT_STALE_SECONDS,
) -> list[str]:
    ctx = status_truth_context(
        runtime_dir,
        generated_at_ts=generated_at_ts,
        stale_after_seconds=stale_after_seconds,
    )
    reasons = ", ".join(ctx["stale_reasons"]) if ctx["stale_reasons"] else "none"
    heartbeat_age = ctx["heartbeat_age_seconds"]
    heartbeat_age_text = f"{heartbeat_age:.1f}s" if heartbeat_age is not None else "n/a"
    fields = [
        ("generated_at", fmt_ts(ctx["generated_at_ts"])),
        ("status_run_id", ctx["status_run_id"] or "unknown"),
        ("heartbeat_ts", fmt_ts(ctx["heartbeat_ts"])),
        ("heartbeat_age", heartbeat_age_text),
        ("freshness", ctx["freshness"]),
        ("stale_reasons", reasons),
    ]
    if markdown:
        return [f"- {key}: `{value}`" for key, value in fields]
    return [f"{key}: {value}" for key, value in fields]
