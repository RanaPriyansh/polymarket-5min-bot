from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LATCH_FILENAME = "risk_stop_latch.json"
ARCHIVE_DIRNAME = "risk_latch_archive"


def latch_path(runtime_dir: str | Path) -> Path:
    return Path(runtime_dir) / LATCH_FILENAME


def read_risk_stop_latch(runtime_dir: str | Path) -> dict[str, Any] | None:
    path = latch_path(runtime_dir)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {
            "stop_reason": "unreadable_risk_stop_latch",
            "new_orders_paused": True,
            "latch_path": str(path),
        }
    return payload if isinstance(payload, dict) else {
        "stop_reason": "invalid_risk_stop_latch",
        "new_orders_paused": True,
        "latch_path": str(path),
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    tmp_path.replace(path)


def _risk_value(risk_report: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in risk_report and risk_report.get(key) is not None:
            return risk_report.get(key)
    return None


def write_risk_stop_latch(
    runtime_dir: str | Path,
    *,
    run_id: str,
    stop_reason: str,
    gate_state: str | None = None,
    gate_reasons: list[str] | None = None,
    risk_report: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist a fail-closed risk stop latch that survives process restarts."""
    now = time.time()
    risk = risk_report or {}
    payload: dict[str, Any] = {
        "timestamp": now,
        "timestamp_iso": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "run_id": run_id,
        "stop_reason": str(stop_reason or "risk_stop"),
        "gate_state": gate_state,
        "gate_reasons": list(gate_reasons or []),
        "pid": os.getpid(),
        "new_orders_paused": True,
        "equity": _risk_value(risk, "capital", "mark_to_market_capital", "equity"),
        "drawdown": _risk_value(risk, "max_drawdown", "drawdown"),
        "daily_pnl": _risk_value(risk, "daily_pnl"),
        "risk": risk,
    }
    if extra:
        payload.update(extra)
    _write_json_atomic(latch_path(runtime_dir), payload)
    return payload


def persist_runtime_risk_stop(
    runtime: Any,
    runtime_dir: str | Path,
    *,
    run_id: str,
    mode: str,
    loop_count: int,
    stop_reason: str,
    risk_report: dict[str, Any],
    gate_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist and surface a runtime risk stop through the shared latch/status path."""
    gate_snapshot = gate_snapshot or {}
    latch = write_risk_stop_latch(
        runtime_dir,
        run_id=run_id,
        stop_reason=stop_reason,
        gate_state=str(gate_snapshot.get("gate_state") or ""),
        gate_reasons=list(gate_snapshot.get("gate_reasons") or []),
        risk_report=risk_report,
    )
    runtime.append_event("runtime.risk_stop", {"run_id": run_id, "stop_reason": stop_reason, "risk": risk_report})
    runtime.update_status(
        run_id=run_id,
        phase="stopping",
        mode=mode,
        loop_count=loop_count,
        stop_reason=stop_reason,
        risk=risk_report,
        new_order_pause=True,
        new_orders_paused=True,
        pause_policy="persistent_risk_stop_latch",
        pause_scope="all_new_orders",
        pause_reason=stop_reason,
        risk_latch_present=True,
        risk_latch_reason=stop_reason,
        revived_after_risk_stop=False,
    )
    return latch


def risk_latch_summary(runtime_dir: str | Path, *, current_run_id: str | None = None) -> dict[str, Any]:
    latch = read_risk_stop_latch(runtime_dir)
    if not latch:
        return {
            "risk_latch_present": False,
            "risk_latch_reason": None,
            "revived_after_risk_stop": False,
            "risk_latch": None,
        }
    latch_run_id = str(latch.get("run_id") or "")
    current = str(current_run_id or "")
    return {
        "risk_latch_present": True,
        "risk_latch_reason": latch.get("stop_reason") or latch.get("reason") or "unknown",
        "revived_after_risk_stop": bool(current and latch_run_id and current != latch_run_id),
        "risk_latch": latch,
    }


def apply_startup_risk_latch(runtime: Any, *, run_id: str, mode: str, strategies: list[str] | None = None) -> bool:
    """If a latch exists, mark this run paused/fail-closed and prevent new orders."""
    summary = risk_latch_summary(runtime.runtime_dir, current_run_id=run_id)
    if not summary["risk_latch_present"]:
        return False
    reason = summary["risk_latch_reason"]
    runtime.append_event(
        "runtime.risk_latch_present",
        {
            "run_id": run_id,
            "mode": mode,
            "risk_latch_reason": reason,
            "revived_after_risk_stop": summary["revived_after_risk_stop"],
        },
        run_id=run_id,
    )
    runtime.update_status(
        run_id=run_id,
        phase="paused",
        mode=mode,
        strategies=list(strategies or []),
        loop_count=0,
        new_order_pause=True,
        new_orders_paused=True,
        pause_policy="persistent_risk_stop_latch",
        pause_scope="all_new_orders",
        pause_reason=f"risk_stop_latch_present:{reason}",
        gate_state="RED",
        gate_reasons=[f"risk_stop_latch:{reason}"],
        risk_latch_present=True,
        risk_latch_reason=reason,
        revived_after_risk_stop=summary["revived_after_risk_stop"],
        risk_latch=summary["risk_latch"],
        stop_reason="risk_stop_latch_present",
    )
    return True


def manual_reset_risk_latch(runtime_dir: str | Path, *, reason: str, operator: str | None = None) -> Path:
    clean_reason = str(reason or "").strip()
    if not clean_reason:
        raise ValueError("reset reason is required")
    runtime_path = Path(runtime_dir)
    source = latch_path(runtime_path)
    if not source.exists():
        raise FileNotFoundError(str(source))
    original_bytes = source.read_bytes()
    latch = read_risk_stop_latch(runtime_path) or {}
    now = time.time()
    archived_at_iso = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
    archive_dir = runtime_path / ARCHIVE_DIRNAME
    archive_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(latch.get("run_id") or "unknown").replace("/", "_")
    archive_path = archive_dir / f"risk_stop_latch-{int(now)}-{run_id}.json"
    suffix = 1
    while archive_path.exists():
        archive_path = archive_dir / f"risk_stop_latch-{int(now)}-{run_id}-{suffix}.json"
        suffix += 1
    tmp_path = archive_path.with_name(f".{archive_path.name}.tmp")
    tmp_path.write_bytes(original_bytes)
    tmp_path.replace(archive_path)
    _write_json_atomic(
        archive_path.with_name(f"reset-{archive_path.name}.json"),
        {
            "archived_at": now,
            "archived_at_iso": archived_at_iso,
            "archived_from": str(source),
            "archive_path": str(archive_path),
            "reset_reason": clean_reason,
            "reset_operator": operator,
            "original_stop_reason": latch.get("stop_reason"),
            "original_run_id": latch.get("run_id"),
        },
    )
    source.unlink()
    return archive_path
