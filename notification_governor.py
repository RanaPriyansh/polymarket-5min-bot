from __future__ import annotations

import fcntl
import json
import os
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1
from pathlib import Path
from typing import Any

from runtime_telemetry import RuntimeTelemetry
from status_utils import runtime_status_payload

DEFAULT_STATE_PATH = Path("/root/.hermes/state/polymarket-5min-bot/notification_governor.json")
DEFAULT_SERVICE_NAME = "polymarket-paper-bot.service"
DEFAULT_RESEARCH_TIMER = "polymarket-paper-research.timer"
DEFAULT_RESEARCH_SERVICE = "polymarket-paper-research.service"
DEFAULT_OPS_TIMER = "polymarket-paper-ops-hourly.timer"
DEFAULT_HEARTBEAT_STALE_SECONDS = 180


@dataclass(frozen=True)
class NotificationPolicy:
    digest_interval_seconds: int = 21600
    warning_min_interval_seconds: int = 21600
    critical_dedup_seconds: int = 21600
    critical_bypass: bool = True
    paper_only: bool = True
    supervision_window_hours: int = 72

    @classmethod
    def from_env(cls) -> "NotificationPolicy":
        return cls(
            digest_interval_seconds=_env_int("TELEGRAM_DIGEST_INTERVAL_SECONDS", 21600),
            warning_min_interval_seconds=_env_int("TELEGRAM_WARNING_MIN_INTERVAL_SECONDS", 21600),
            critical_dedup_seconds=_env_int("TELEGRAM_CRITICAL_DEDUP_SECONDS", 21600),
            critical_bypass=_env_bool("TELEGRAM_CRITICAL_BYPASS", True),
            paper_only=_env_bool("PAPER_ONLY", True),
            supervision_window_hours=_env_int("SUPERVISION_WINDOW_HOURS", 72),
        )


@dataclass(frozen=True)
class NotificationDecision:
    should_send: bool
    reason: str
    fingerprint: str | None = None
    next_allowed_at: float | None = None


class NotificationGovernor:
    def __init__(self, state_path: str | Path | None = None, policy: NotificationPolicy | None = None):
        env_state_path = os.getenv("HERMES_NOTIFICATION_STATE_PATH")
        env_state_dir = os.getenv("HERMES_NOTIFICATION_STATE_DIR")
        if state_path is None and env_state_path:
            resolved_state_path = Path(env_state_path)
        elif state_path is None and env_state_dir:
            resolved_state_path = Path(env_state_dir) / "notification_governor.json"
        elif state_path is None:
            resolved_state_path = DEFAULT_STATE_PATH
        else:
            resolved_state_path = Path(state_path)
        self.state_path = resolved_state_path
        self.lock_path = self.state_path.with_suffix(self.state_path.suffix + ".lock")
        self.policy = policy or NotificationPolicy.from_env()

    @contextmanager
    def _locked_state(self) -> Any:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock_fh:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
            try:
                yield self._load_state_unlocked()
            finally:
                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    def _load_state_unlocked(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return self._default_state()
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return self._default_state()
        return self._migrate_state(raw if isinstance(raw, dict) else {})

    def load_state(self) -> dict[str, Any]:
        with self._locked_state() as state:
            return json.loads(json.dumps(state))

    def save_state(self, state: dict[str, Any]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(self.state_path.parent), delete=False) as tmp:
            tmp.write(json.dumps(state, indent=2, sort_keys=True))
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_path = Path(tmp.name)
        tmp_path.replace(self.state_path)

    def digest_decision(self, *, now_ts: float | None = None, force: bool = False) -> NotificationDecision:
        now_ts = float(time.time() if now_ts is None else now_ts)
        if force:
            return NotificationDecision(True, "forced_digest")
        state = self.load_state()
        return self._digest_decision_from_state(state, now_ts=now_ts)

    def claim_digest_send(self, *, now_ts: float | None = None, force: bool = False, claim_ttl_seconds: int = 300) -> NotificationDecision:
        now_ts = float(time.time() if now_ts is None else now_ts)
        with self._locked_state() as state:
            claim_expires_at = state.get("digest_claim_expires_at")
            if claim_expires_at not in (None, ""):
                try:
                    if now_ts < float(claim_expires_at):
                        return NotificationDecision(False, "digest_claim_active", next_allowed_at=float(claim_expires_at))
                except (TypeError, ValueError):
                    pass
            state["digest_claimed_at"] = None
            state["digest_claim_expires_at"] = None
            if force:
                state["digest_claimed_at"] = now_ts
                state["digest_claim_expires_at"] = now_ts + float(claim_ttl_seconds)
                self.save_state(state)
                return NotificationDecision(True, "forced_digest")
            decision = self._digest_decision_from_state(state, now_ts=now_ts)
            if decision.should_send:
                state["digest_claimed_at"] = now_ts
                state["digest_claim_expires_at"] = now_ts + float(claim_ttl_seconds)
                self.save_state(state)
            return decision

    def confirm_digest_sent(self, *, now_ts: float | None = None) -> dict[str, Any]:
        now_ts = float(time.time() if now_ts is None else now_ts)
        with self._locked_state() as state:
            state["last_digest_sent_at"] = now_ts
            state["digest_claimed_at"] = None
            state["digest_claim_expires_at"] = None
            self.save_state(state)
            return json.loads(json.dumps(state))

    def release_digest_claim(self) -> dict[str, Any]:
        with self._locked_state() as state:
            state["digest_claimed_at"] = None
            state["digest_claim_expires_at"] = None
            self.save_state(state)
            return json.loads(json.dumps(state))

    def _digest_decision_from_state(self, state: dict[str, Any], *, now_ts: float) -> NotificationDecision:
        last_digest_sent_at = state.get("last_digest_sent_at")
        if last_digest_sent_at in (None, ""):
            return NotificationDecision(True, "initial_digest")
        next_allowed_at = float(last_digest_sent_at) + self.policy.digest_interval_seconds
        if now_ts >= next_allowed_at:
            return NotificationDecision(True, "digest_interval_elapsed", next_allowed_at=next_allowed_at)
        return NotificationDecision(False, "digest_interval_not_elapsed", next_allowed_at=next_allowed_at)

    def record_digest_sent(self, *, now_ts: float | None = None) -> dict[str, Any]:
        return self.confirm_digest_sent(now_ts=now_ts)

    def incident_fingerprint(self, incident: dict[str, Any]) -> str:
        stable_payload = {
            "category": str(incident.get("category") or incident.get("kind") or "unknown").strip().lower(),
            "root_cause": str(
                incident.get("root_cause")
                or incident.get("rootcause")
                or incident.get("reason")
                or incident.get("code")
                or incident.get("summary")
                or "unknown"
            )
            .strip()
            .lower(),
            "scope": str(incident.get("scope") or incident.get("service") or incident.get("component") or "global")
            .strip()
            .lower(),
        }
        return sha1(json.dumps(stable_payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]

    def incident_decision(self, severity: str, incident: dict[str, Any], *, now_ts: float | None = None) -> NotificationDecision:
        now_ts = float(time.time() if now_ts is None else now_ts)
        severity = str(severity or "info").strip().lower()
        fingerprint = self.incident_fingerprint(incident)
        state = self.load_state()
        return self._incident_decision_from_state(state, severity, incident, fingerprint=fingerprint, now_ts=now_ts)

    def claim_incident_notification(self, severity: str, incident: dict[str, Any], *, now_ts: float | None = None) -> NotificationDecision:
        now_ts = float(time.time() if now_ts is None else now_ts)
        severity = str(severity or "info").strip().lower()
        fingerprint = self.incident_fingerprint(incident)
        with self._locked_state() as state:
            decision = self._incident_decision_from_state(state, severity, incident, fingerprint=fingerprint, now_ts=now_ts)
            if decision.should_send:
                active_incidents = state.setdefault("active_incidents", {})
                current = active_incidents.get(fingerprint)
                if not isinstance(current, dict):
                    current = {"fingerprint": fingerprint, "first_seen": now_ts}
                current.update(
                    {
                        "severity": severity,
                        "category": str(incident.get("category") or incident.get("kind") or "unknown"),
                        "root_cause": str(
                            incident.get("root_cause")
                            or incident.get("rootcause")
                            or incident.get("reason")
                            or incident.get("code")
                            or incident.get("summary")
                            or "unknown"
                        ),
                        "scope": str(incident.get("scope") or incident.get("service") or incident.get("component") or "global"),
                        "last_seen": now_ts,
                        "last_sent": now_ts,
                        "resolved_at": None,
                    }
                )
                current.setdefault("first_seen", now_ts)
                active_incidents[fingerprint] = current
                self.save_state(state)
            return decision

    def _incident_decision_from_state(
        self,
        state: dict[str, Any],
        severity: str,
        incident: dict[str, Any],
        *,
        fingerprint: str,
        now_ts: float,
    ) -> NotificationDecision:
        active_incidents = state.get("active_incidents", {}) or {}
        current = active_incidents.get(fingerprint)
        if not isinstance(current, dict):
            return NotificationDecision(True, "new_incident", fingerprint=fingerprint)
        resolved_at = current.get("resolved_at")
        if resolved_at:
            return NotificationDecision(True, "incident_reappeared", fingerprint=fingerprint)
        last_sent = current.get("last_sent")
        if severity == "critical":
            if self.policy.critical_bypass and not last_sent:
                return NotificationDecision(True, "critical_bypass", fingerprint=fingerprint)
            next_allowed_at = (float(last_sent) if last_sent else now_ts) + self.policy.critical_dedup_seconds
            if not last_sent or now_ts >= next_allowed_at:
                return NotificationDecision(True, "critical_dedup_elapsed", fingerprint=fingerprint, next_allowed_at=next_allowed_at)
            return NotificationDecision(False, "critical_dedup_active", fingerprint=fingerprint, next_allowed_at=next_allowed_at)
        if severity == "warning":
            next_allowed_at = (float(last_sent) if last_sent else now_ts) + self.policy.warning_min_interval_seconds
            if not last_sent or now_ts >= next_allowed_at:
                return NotificationDecision(True, "warning_interval_elapsed", fingerprint=fingerprint, next_allowed_at=next_allowed_at)
            return NotificationDecision(False, "warning_interval_active", fingerprint=fingerprint, next_allowed_at=next_allowed_at)
        return NotificationDecision(not bool(last_sent), "noncritical_first_only" if not last_sent else "noncritical_already_sent", fingerprint=fingerprint)

    def record_incident_notification(
        self,
        fingerprint: str,
        severity: str,
        incident: dict[str, Any],
        *,
        now_ts: float | None = None,
    ) -> dict[str, Any]:
        now_ts = float(time.time() if now_ts is None else now_ts)
        state = self.load_state()
        active_incidents = state.setdefault("active_incidents", {})
        current = active_incidents.get(fingerprint)
        if not isinstance(current, dict):
            current = {
                "fingerprint": fingerprint,
                "first_seen": now_ts,
            }
        current.update(
            {
                "severity": str(severity or "info").lower(),
                "category": str(incident.get("category") or incident.get("kind") or "unknown"),
                "root_cause": str(
                    incident.get("root_cause")
                    or incident.get("rootcause")
                    or incident.get("reason")
                    or incident.get("code")
                    or incident.get("summary")
                    or "unknown"
                ),
                "scope": str(incident.get("scope") or incident.get("service") or incident.get("component") or "global"),
                "last_seen": now_ts,
                "last_sent": now_ts,
                "resolved_at": None,
            }
        )
        current.setdefault("first_seen", now_ts)
        active_incidents[fingerprint] = current
        self.save_state(state)
        return current

    def observe_incident(self, severity: str, incident: dict[str, Any], *, now_ts: float | None = None) -> dict[str, Any]:
        now_ts = float(time.time() if now_ts is None else now_ts)
        fingerprint = self.incident_fingerprint(incident)
        state = self.load_state()
        active_incidents = state.setdefault("active_incidents", {})
        current = active_incidents.get(fingerprint)
        if not isinstance(current, dict):
            current = {
                "fingerprint": fingerprint,
                "first_seen": now_ts,
                "last_sent": None,
            }
        current.update(
            {
                "severity": str(severity or "info").lower(),
                "category": str(incident.get("category") or incident.get("kind") or "unknown"),
                "root_cause": str(
                    incident.get("root_cause")
                    or incident.get("rootcause")
                    or incident.get("reason")
                    or incident.get("code")
                    or incident.get("summary")
                    or "unknown"
                ),
                "scope": str(incident.get("scope") or incident.get("service") or incident.get("component") or "global"),
                "last_seen": now_ts,
                "resolved_at": None,
            }
        )
        current.setdefault("first_seen", now_ts)
        active_incidents[fingerprint] = current
        self.save_state(state)
        return current

    def resolve_incident(self, fingerprint: str, *, now_ts: float | None = None) -> dict[str, Any] | None:
        now_ts = float(time.time() if now_ts is None else now_ts)
        state = self.load_state()
        active_incidents = state.setdefault("active_incidents", {})
        current = active_incidents.get(fingerprint)
        if not isinstance(current, dict):
            return None
        current["last_seen"] = now_ts
        current["resolved_at"] = now_ts
        active_incidents[fingerprint] = current
        self.save_state(state)
        return current

    @staticmethod
    def _default_state() -> dict[str, Any]:
        return {
            "last_digest_sent_at": None,
            "digest_claimed_at": None,
            "digest_claim_expires_at": None,
            "active_incidents": {},
        }

    def _migrate_state(self, raw: dict[str, Any]) -> dict[str, Any]:
        state = self._default_state()
        state["last_digest_sent_at"] = raw.get("last_digest_sent_at")
        state["digest_claimed_at"] = raw.get("digest_claimed_at")
        state["digest_claim_expires_at"] = raw.get("digest_claim_expires_at")
        legacy_incidents = raw.get("active_incidents")
        if not isinstance(legacy_incidents, dict):
            legacy_incidents = raw.get("incidents") if isinstance(raw.get("incidents"), dict) else {}
        normalized: dict[str, Any] = {}
        for fingerprint, row in legacy_incidents.items():
            if not isinstance(row, dict):
                continue
            normalized[str(fingerprint)] = {
                "fingerprint": str(row.get("fingerprint") or fingerprint),
                "severity": row.get("severity", "info"),
                "category": row.get("category", "unknown"),
                "root_cause": row.get("root_cause") or row.get("reason") or "unknown",
                "scope": row.get("scope") or row.get("service") or row.get("component") or "global",
                "first_seen": row.get("first_seen"),
                "last_seen": row.get("last_seen"),
                "last_sent": row.get("last_sent"),
                "resolved_at": row.get("resolved_at"),
            }
        state["active_incidents"] = normalized
        return state


def build_readable_digest(
    runtime_dir: str | Path,
    *,
    now_ts: float | None = None,
    governor: NotificationGovernor | None = None,
) -> str:
    now_ts = float(time.time() if now_ts is None else now_ts)
    runtime_dir = Path(runtime_dir)
    governor = governor or NotificationGovernor()
    payload = runtime_status_payload(runtime_dir)
    status = payload.get("status") or {}
    risk = status.get("risk") or {}
    service = _systemd_unit_status(DEFAULT_SERVICE_NAME)
    research_timer = _systemd_unit_status(DEFAULT_RESEARCH_TIMER)
    research_service = _systemd_unit_status(DEFAULT_RESEARCH_SERVICE)
    ops_timer = _systemd_unit_status(DEFAULT_OPS_TIMER)
    process_count = _paper_process_count()
    heartbeat_age_seconds = payload.get("heartbeat_age_seconds")
    overall_status = _overall_status(status, service, heartbeat_age_seconds)
    runtime_duration_seconds = _infer_runtime_duration_seconds(runtime_dir, status, now_ts)
    state = governor.load_state()
    next_digest_at = _next_digest_at(state.get("last_digest_sent_at"), governor.policy.digest_interval_seconds, now_ts)

    positions = status.get("positions") or {}
    open_positions_line = "none"
    if isinstance(positions, dict) and positions:
        rendered = []
        for key, row in list(sorted(positions.items()))[:5]:
            if not isinstance(row, dict):
                rendered.append(str(key))
                continue
            rendered.append(
                "{key} {qty:+.2f}@${price:.3f}".format(
                    key=key,
                    qty=float(row.get("quantity", 0.0) or 0.0),
                    price=float(row.get("average_price", 0.0) or 0.0),
                )
            )
        if len(positions) > 5:
            rendered.append(f"+{len(positions) - 5} more")
        open_positions_line = "; ".join(rendered)

    stop_reason = status.get("stop_reason") or status.get("last_error") or status.get("last_stop_reason") or "none"
    runtime_mode = str(status.get("mode", "unknown") or "unknown").upper()
    policy_mode = "PAPER ONLY" if governor.policy.paper_only else runtime_mode
    capital_value = float(risk.get("capital", risk.get("mark_to_market_capital", status.get("bankroll", 0.0))) or 0.0)
    realized_value = float(risk.get("realized_pnl_total", 0.0) or 0.0)
    unrealized_value = float(risk.get("unrealized_pnl_total", 0.0) or 0.0)
    daily_value = float(risk.get("daily_pnl", 0.0) or 0.0)
    drawdown_value = float(risk.get("max_drawdown", 0.0) or 0.0)

    lines = [
        "POLYMARKET PAPER TRADING STATUS — 6H DIGEST",
        f"Time: {_fmt_timestamp(now_ts)}",
        f"Status: {overall_status}",
        f"Mode: {runtime_mode}",
        f"Policy: {policy_mode}",
        "",
        "Runtime:",
        f"- Service: {_render_unit(service)}",
        f"- Paper process count: {process_count}",
        f"- Run ID: {status.get('run_id', 'unknown')}",
        f"- Runtime duration: {_fmt_duration(runtime_duration_seconds)}",
        f"- Loop count: {status.get('loop_count', 0)}",
        f"- Heartbeat age: {_fmt_age(heartbeat_age_seconds)}",
        "",
        "Trading:",
        f"- Gate: {status.get('gate_state', 'unknown')}",
        f"- New orders paused: {bool(status.get('new_order_pause', False))}",
        f"- Settled count: {status.get('resolved_trade_count', 0)}",
        f"- Win rate: {float(status.get('win_rate', 0.0) or 0.0):.1%}",
        f"- Open positions: {open_positions_line}",
        f"- Last error/stop reason: {stop_reason}",
        "",
        "PnL:",
        f"- Capital: ${capital_value:,.2f}",
        f"- Realized: ${realized_value:+,.2f}",
        f"- Unrealized: ${unrealized_value:+,.2f}",
        f"- Daily: ${daily_value:+,.2f}",
        f"- Drawdown: {drawdown_value:.2%}",
        "",
        "Autoresearch:",
        f"- Timer: {_render_unit(research_timer)}",
        f"- Service: {_render_unit(research_service)}",
        f"- Ops timer: {_render_unit(ops_timer)}",
        "",
        "Notes:",
        "- Overall status derived from runtime/service/heartbeat truth.",
        f"- PAPER_ONLY policy {'enabled' if governor.policy.paper_only else 'disabled'}.",
        f"- Next expected digest time: {_fmt_timestamp(next_digest_at)}",
    ]
    return "\n".join(lines)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _systemd_unit_status(unit_name: str) -> dict[str, Any]:
    proc = subprocess.run(
        ["systemctl", "show", unit_name, "-p", "ActiveState", "-p", "SubState", "-p", "UnitFileState", "-p", "MainPID", "--no-pager"],
        check=False,
        capture_output=True,
        text=True,
    )
    result: dict[str, Any] = {
        "unit": unit_name,
        "active_state": "unknown",
        "sub_state": "unknown",
        "unit_file_state": "unknown",
        "main_pid": 0,
        "ok": False,
    }
    if proc.returncode != 0:
        return result
    for line in proc.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key == "ActiveState":
            result["active_state"] = value or "unknown"
        elif key == "SubState":
            result["sub_state"] = value or "unknown"
        elif key == "UnitFileState":
            result["unit_file_state"] = value or "unknown"
        elif key == "MainPID":
            try:
                result["main_pid"] = int(value or 0)
            except ValueError:
                result["main_pid"] = 0
    result["ok"] = result["active_state"] == "active"
    return result


def _paper_process_count() -> int:
    proc = subprocess.run(["ps", "-eo", "args="], check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        return 0
    count = 0
    for line in proc.stdout.splitlines():
        line = line.strip()
        if "cli.py run" in line and "--mode paper" in line:
            count += 1
    return count


def _infer_runtime_duration_seconds(runtime_dir: Path, status: dict[str, Any], now_ts: float) -> float | None:
    run_id = str(status.get("run_id") or "")
    parts = run_id.split("-")
    if len(parts) >= 3:
        try:
            started_ts = float(parts[1])
            if started_ts > 0 and now_ts >= started_ts:
                return now_ts - started_ts
        except ValueError:
            pass
    heartbeat_ts = float(status.get("heartbeat_ts", 0.0) or 0.0)
    loop_count = float(status.get("loop_count", 0) or 0)
    if heartbeat_ts > 0 and loop_count > 0:
        estimate = heartbeat_ts - (loop_count * 5.0)
        if now_ts >= estimate > 0:
            return now_ts - estimate
    events = RuntimeTelemetry(runtime_dir).read_events(run_id=run_id or None, limit=5000)
    if events:
        timestamps = [float(row.get("ts", 0.0) or 0.0) for row in events if float(row.get("ts", 0.0) or 0.0) > 0]
        if timestamps:
            first_ts = min(timestamps)
            if now_ts >= first_ts:
                return now_ts - first_ts
    return None


def _overall_status(status: dict[str, Any], service: dict[str, Any], heartbeat_age_seconds: float | None) -> str:
    phase = str(status.get("phase") or "unknown").lower()
    gate_state = str(status.get("gate_state") or "unknown").upper()
    if phase in {"failed", "stopped"} or status.get("stop_reason"):
        return "FAILED"
    if service.get("active_state") not in {"active", "unknown"}:
        return "FAILED"
    if heartbeat_age_seconds is None or heartbeat_age_seconds > DEFAULT_HEARTBEAT_STALE_SECONDS:
        return "DEGRADED"
    if gate_state in {"RED", "YELLOW"} or bool(status.get("new_order_pause", False)):
        return "DEGRADED"
    return "OK"


def _render_unit(unit_status: dict[str, Any]) -> str:
    return "{unit}:{active}/{sub} pid={pid}".format(
        unit=unit_status.get("unit", "unit"),
        active=unit_status.get("active_state", "unknown"),
        sub=unit_status.get("sub_state", "unknown"),
        pid=unit_status.get("main_pid", 0),
    )


def _fmt_duration(seconds: float | None) -> str:
    if seconds is None:
        return "best-effort unavailable"
    seconds = max(0.0, float(seconds))
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60.0
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60.0
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24.0:.1f}d"


def _fmt_age(seconds: float | None) -> str:
    return "n/a" if seconds is None else _fmt_duration(seconds)


def _fmt_timestamp(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _next_digest_at(last_digest_sent_at: Any, digest_interval_seconds: int, now_ts: float) -> float:
    if last_digest_sent_at in (None, ""):
        return now_ts
    try:
        return float(last_digest_sent_at) + float(digest_interval_seconds)
    except (TypeError, ValueError):
        return now_ts
