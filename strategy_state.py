from __future__ import annotations

from typing import Iterable


VALID_STRATEGY_STATES = {
    "disabled",
    "scanner_only",
    "candidate_only",
    "paper_active",
    "future_live_eligible_but_blocked",
}

ACTIVE_ALLOWED_STATES = {"paper_active"}


def strategy_state(config: dict, strategy_family: str) -> str:
    states = config.get("strategies", {}).get("states", {}) or {}
    state = str(states.get(strategy_family, "paper_active")).strip()
    if state not in VALID_STRATEGY_STATES:
        return "disabled"
    return state


def validate_active_strategy_states(config: dict, active_strategies: Iterable[str]) -> list[str]:
    strategies_cfg = config.get("strategies", {}) or {}
    if bool(strategies_cfg.get("allow_unapproved_activation", False)):
        return []
    violations = []
    for family in active_strategies:
        state = strategy_state(config, family)
        if state not in ACTIVE_ALLOWED_STATES:
            violations.append(f"{family}:{state}")
    return violations
