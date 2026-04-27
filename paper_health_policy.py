from __future__ import annotations

from typing import Any, Mapping


_PROTECTIVE_STOP_REASONS = {
    "circuit_breaker",
    "completed",
    "keyboard_interrupt",
    "max_loops",
}
_PROTECTIVE_STOP_PREFIXES = ("signal_",)
_PROTECTIVE_STOP_SUBSTRINGS = (
    "daily_loss",
    "daily loss",
    "drawdown",
    "manual_stop",
    "operator_stop",
    "bounded_stop",
)
_PROTECTIVE_GATE_MARKERS = (
    "circuit_breaker",
    "contradiction_log_open",
    "run_lineage_fragmentation",
    "daily_loss",
    "daily loss",
    "drawdown",
)
_PROTECTIVE_PAUSE_REASONS = {"hard_stop_red_gate"}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _lower(value: Any) -> str:
    return _text(value).lower()


def _as_reason_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    return [text] if text else []


def is_protective_stop(status: Mapping[str, Any] | None) -> tuple[bool, list[str]]:
    snapshot = status or {}
    reasons: list[str] = []

    stop_reason = _lower(snapshot.get("stop_reason"))
    if stop_reason:
        if stop_reason in _PROTECTIVE_STOP_REASONS:
            reasons.append(f"stop_reason={stop_reason}")
        elif any(stop_reason.startswith(prefix) for prefix in _PROTECTIVE_STOP_PREFIXES):
            reasons.append(f"stop_reason={stop_reason}")
        elif any(marker in stop_reason for marker in _PROTECTIVE_STOP_SUBSTRINGS):
            reasons.append(f"stop_reason={stop_reason}")

    gate_state = _lower(snapshot.get("gate_state"))
    gate_reasons = _as_reason_list(snapshot.get("gate_reasons"))
    for gate_reason in gate_reasons:
        normalized = gate_reason.lower()
        if gate_state == "red" and any(marker in normalized for marker in _PROTECTIVE_GATE_MARKERS):
            reasons.append(f"gate_reason={gate_reason}")

    pause_reason = _lower(snapshot.get("pause_reason"))
    if pause_reason in _PROTECTIVE_PAUSE_REASONS:
        reasons.append(f"pause_reason={pause_reason}")

    deduped = sorted(dict.fromkeys(reasons))
    return bool(deduped), deduped


def evaluate_healthcheck_restart_policy(
    health_payload: Mapping[str, Any],
    *,
    paper_process_count: int,
    service_main_pid: int | None = None,
    managed_paper_process_count: int | None = None,
) -> dict[str, Any]:
    healthy = bool(health_payload.get("healthy"))
    status = health_payload.get("status") or {}
    protective_stop, protective_stop_reasons = is_protective_stop(status)

    decision = {
        "healthy": healthy,
        "paper_process_count": int(paper_process_count),
        "service_main_pid": int(service_main_pid or 0),
        "managed_paper_process_count": int(managed_paper_process_count or 0),
        "protective_stop": protective_stop,
        "protective_stop_reasons": protective_stop_reasons,
        "should_restart": False,
        "reason": "healthy" if healthy else "unhealthy",
    }
    if healthy:
        return decision
    if paper_process_count > 1:
        decision["reason"] = "duplicate_paper_processes"
        return decision
    if paper_process_count == 1 and decision["managed_paper_process_count"] == 0:
        decision["reason"] = "orphan_paper_process"
        return decision
    if protective_stop:
        decision["reason"] = "protective_stop"
        return decision
    decision["should_restart"] = True
    decision["reason"] = "genuine_unhealthy"
    return decision
