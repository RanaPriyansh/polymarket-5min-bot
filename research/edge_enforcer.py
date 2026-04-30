from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable

import yaml

from research.experiment_registry import KILL_IF, PROMOTE_IF
from research.gate import build_gate_inputs, compute_gate_state


@dataclass(frozen=True)
class EdgeDecision:
    family: str
    action: str
    reason: str
    evidence: dict[str, Any] = field(default_factory=dict)
    applied: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


DEFAULT_POLICY = {
    "enabled": True,
    "operator_approved_paper_auto_activation": True,
    "require_gate_green": True,
    "max_active_strategies": 1,
    "min_settled_trades": PROMOTE_IF["settled_trades_>="],
    "min_win_rate": PROMOTE_IF["win_rate_>="],
    "min_pnl_per_trade": PROMOTE_IF["pnl_per_trade_>="],
    "demote_settled_trades": KILL_IF["settled_trades_>="],
    "demote_pnl_per_trade_below": KILL_IF["pnl_per_trade_<"],
}


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def edge_policy(config: dict[str, Any]) -> dict[str, Any]:
    research_cfg = config.get("research", {}) or {}
    configured = research_cfg.get("edge_enforcement", {}) or {}
    policy = dict(DEFAULT_POLICY)
    policy.update(configured)
    return policy


def _state(config: dict[str, Any], family: str) -> str:
    return str(((config.get("strategies", {}) or {}).get("states", {}) or {}).get(family, "disabled"))


def _candidate_families(config: dict[str, Any]) -> set[str]:
    return {str(x) for x in ((config.get("strategies", {}) or {}).get("candidates", []) or [])}


def _active_families(config: dict[str, Any]) -> list[str]:
    return [str(x) for x in ((config.get("strategies", {}) or {}).get("active", []) or [])]


def _promotable(row: dict[str, Any], policy: dict[str, Any]) -> bool:
    return (
        _as_int(row.get("settled_trades")) >= _as_int(policy.get("min_settled_trades"))
        and _as_float(row.get("win_rate")) >= _as_float(policy.get("min_win_rate"))
        and _as_float(row.get("pnl_per_trade")) >= _as_float(policy.get("min_pnl_per_trade"))
    )


def _demotable(row: dict[str, Any], policy: dict[str, Any]) -> bool:
    return (
        _as_int(row.get("settled_trades")) >= _as_int(policy.get("demote_settled_trades"))
        and _as_float(row.get("pnl_per_trade")) < _as_float(policy.get("demote_pnl_per_trade_below"))
    )


def _risk_fingerprint(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "risk": config.get("risk", {}),
        "execution_caps": {
            k: (config.get("execution", {}) or {}).get(k)
            for k in [
                "paper_starting_bankroll",
                "mm_paper_max_notional_usd",
                "min_seconds_to_expiry_for_new_orders",
                "live_order_approval",
                "directional_order_ttl_enabled",
                "directional_order_ttl_seconds",
                "fill_policy",
            ]
        },
        "clob_environment": (config.get("polymarket", {}) or {}).get("clob_environment"),
    }


def build_edge_decisions(
    *,
    config: dict[str, Any],
    scoreboard_rows: Iterable[dict[str, Any]],
    gate_state: str,
    gate_reasons: list[str] | None = None,
) -> list[EdgeDecision]:
    policy = edge_policy(config)
    rows = list(scoreboard_rows)
    active = set(_active_families(config))
    candidates = _candidate_families(config)
    decisions: list[EdgeDecision] = []

    if not bool(policy.get("enabled", True)):
        return [EdgeDecision("__policy__", "noop", "edge_enforcement_disabled", {"policy": policy})]
    if bool(policy.get("require_gate_green", True)) and gate_state != "GREEN":
        return [
            EdgeDecision(
                "__gate__",
                "block",
                f"gate_not_green:{gate_state}",
                {"gate_state": gate_state, "gate_reasons": gate_reasons or []},
            )
        ]

    # Demotion dominates promotion. Bad settled evidence is a stop sign, not a debate.
    for row in rows:
        family = str(row.get("family") or "")
        if not family:
            continue
        if _demotable(row, policy) and (family in active or _state(config, family) == "paper_active"):
            decisions.append(
                EdgeDecision(
                    family,
                    "demote",
                    "settled_scoreboard_breached_kill_threshold",
                    dict(row),
                )
            )

    existing_survivors = [f for f in _active_families(config) if not any(d.family == f and d.action == "demote" for d in decisions)]
    max_active = max(0, _as_int(policy.get("max_active_strategies"), 1))
    open_slots = max(0, max_active - len(existing_survivors))
    if open_slots <= 0:
        return decisions

    promotable = [
        row
        for row in rows
        if str(row.get("family") or "") in candidates | active
        and str(row.get("family") or "") not in existing_survivors
        and _promotable(row, policy)
    ]
    promotable.sort(
        key=lambda r: (
            _as_float(r.get("pnl_per_trade")),
            _as_float(r.get("win_rate")),
            _as_int(r.get("settled_trades")),
        ),
        reverse=True,
    )
    for row in promotable[:open_slots]:
        family = str(row.get("family"))
        decisions.append(
            EdgeDecision(
                family,
                "promote_to_paper_active",
                "settled_scoreboard_crossed_promote_threshold",
                dict(row),
            )
        )
    return decisions


def apply_edge_decisions(config: dict[str, Any], decisions: Iterable[EdgeDecision]) -> tuple[dict[str, Any], list[EdgeDecision]]:
    before_risk = _risk_fingerprint(config)
    updated = json.loads(json.dumps(config))
    strategies = updated.setdefault("strategies", {})
    states = strategies.setdefault("states", {})
    active = [str(x) for x in strategies.get("active", []) or []]
    applied: list[EdgeDecision] = []

    for decision in decisions:
        if decision.action == "demote":
            active = [family for family in active if family != decision.family]
            states[decision.family] = "disabled"
            applied.append(EdgeDecision(decision.family, decision.action, decision.reason, decision.evidence, True))
        elif decision.action == "promote_to_paper_active":
            states[decision.family] = "paper_active"
            if decision.family not in active:
                active.append(decision.family)
            applied.append(EdgeDecision(decision.family, decision.action, decision.reason, decision.evidence, True))
        else:
            applied.append(decision)

    strategies["active"] = active
    after_risk = _risk_fingerprint(updated)
    if after_risk != before_risk:
        raise ValueError("edge enforcement attempted to change risk/execution safety constraints")
    if bool((updated.get("execution", {}) or {}).get("live_order_approval", False)):
        raise ValueError("edge enforcement refuses live_order_approval=true")
    return updated, applied


def enforce_research_edges(
    *,
    config_path: str | Path,
    runtime_dir: str | Path,
    artifact_dir: str | Path,
    apply: bool = True,
) -> dict[str, Any]:
    config_path = Path(config_path)
    artifact_dir = Path(artifact_dir)
    runtime_dir = Path(runtime_dir)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    policy = edge_policy(config)
    gate_state, gate_reasons = compute_gate_state(build_gate_inputs(runtime_dir))
    scoreboard = _read_json(artifact_dir / "family_scoreboard.json", [])
    decisions = build_edge_decisions(
        config=config,
        scoreboard_rows=scoreboard,
        gate_state=gate_state,
        gate_reasons=gate_reasons,
    )
    applied_decisions = decisions
    updated_config = config
    apply_blocked_reason = None
    if apply and bool(policy.get("enabled", True)):
        if not bool(policy.get("operator_approved_paper_auto_activation", False)):
            apply_blocked_reason = "operator_approval_missing"
        elif any(decision.action in {"demote", "promote_to_paper_active"} for decision in decisions):
            updated_config, applied_decisions = apply_edge_decisions(config, decisions)
            config_path.write_text(yaml.safe_dump(updated_config, sort_keys=False), encoding="utf-8")

    candidates_without_evidence = [
        str(row.get("family"))
        for row in scoreboard
        if str(row.get("family") or "") in _candidate_families(config)
        and _as_int(row.get("settled_trades")) < _as_int(policy.get("min_settled_trades"))
    ]
    payload = {
        "created_at": time.time(),
        "config_path": str(config_path),
        "runtime_dir": str(runtime_dir),
        "artifact_dir": str(artifact_dir),
        "gate_state": gate_state,
        "gate_reasons": gate_reasons,
        "policy": policy,
        "decisions": [decision.to_dict() for decision in decisions],
        "applied_decisions": [decision.to_dict() for decision in applied_decisions],
        "apply_requested": bool(apply),
        "apply_blocked_reason": apply_blocked_reason,
        "active_after": _active_families(updated_config),
        "candidate_evidence_gap": candidates_without_evidence,
        "next_experiment": "run_bakeoff" if candidates_without_evidence else None,
        "risk_constraints_unchanged": _risk_fingerprint(config) == _risk_fingerprint(updated_config),
    }
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "edge_enforcement_latest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload
