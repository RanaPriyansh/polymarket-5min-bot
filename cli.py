#!/usr/bin/env python3
"""
Polymarket 5/15-minute bot CLI.
Commands: run, backtest, paper, live, collect, research
"""

from __future__ import annotations

import asyncio
import json
import logging
import shlex
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List

import click
import yaml

from baseline_evidence import build_baseline_evidence, render_baseline_evidence_text
from execution import resolve_directional_signal_entry_style
from research.gate import build_gate_inputs, compute_gate_state
from research.loop import ResearchLoop
from research.polymarket import PolymarketRuntimeResearchAdapter
from runtime_telemetry import RuntimeTelemetry
from status_utils import render_status_text, runtime_health_payload
from strategy_bakeoff import (
    build_trial_command,
    build_trial_runtime_dir,
    collect_trial_outcome,
    load_bakeoff_spec,
    rank_trials,
    write_bakeoff_artifacts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent
CLI_SCRIPT_PATH = PROJECT_ROOT / "cli.py"


@click.group()
def cli():
    """Polymarket 5/15-minute trading bot."""
    pass


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:
        return
    load_dotenv()


def _ensure_runtime_dirs(cfg: dict) -> None:
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    runtime_dir = Path(cfg.get("runtime", {}).get("dir", "data/runtime"))
    runtime_dir.mkdir(parents=True, exist_ok=True)
    log_file = Path(cfg.get("logging", {}).get("file", "logs/bot.log"))
    log_file.parent.mkdir(parents=True, exist_ok=True)


def _apply_env_overrides(cfg: dict) -> dict:
    import os

    env_mapping = {
        ("polymarket", "wallet_address"): "POLYMARKET_WALLET_ADDRESS",
        ("polymarket", "private_key"): "POLYMARKET_PRIVATE_KEY",
        ("telegram", "bot_token"): "TELEGRAM_BOT_TOKEN",
        ("telegram", "chat_id"): "TELEGRAM_CHAT_ID",
    }
    for path, env_name in env_mapping.items():
        value = os.getenv(env_name)
        if value:
            cursor = cfg
            for key in path[:-1]:
                cursor = cursor.setdefault(key, {})
            cursor[path[-1]] = value
    return cfg


def load_cfg() -> dict:
    _load_dotenv()
    with open("config.yaml", "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    cfg = _apply_env_overrides(cfg)
    _ensure_runtime_dirs(cfg)
    return cfg


def _emit_events(runtime: RuntimeTelemetry, events: Iterable[Dict], *, run_id: str | None = None) -> None:
    for event in events:
        payload = dict(event)
        event_type = payload.pop("event_type", "runtime.event")
        runtime.append_event(event_type, payload, run_id=run_id)


def _resolve_active_strategies(cfg: dict, strategies: str | None) -> List[str]:
    if strategies and strategies.strip():
        return [item.strip() for item in strategies.split(",") if item.strip()]
    configured = cfg.get("strategies", {}).get("active", [])
    return [str(item).strip() for item in configured if str(item).strip()]


def _active_slot_summary(markets: List[Dict]) -> List[Dict]:
    return [
        {
            "slot_id": market["slot_id"],
            "market_id": market["id"],
            "market_slug": market["slug"],
            "asset": market.get("asset"),
            "interval_minutes": market.get("interval_minutes"),
            "end_ts": market["end_ts"],
        }
        for market in markets
    ]


def _strategy_governance(cfg: dict, active_strategies: List[str]) -> Dict[str, object]:
    active_set = {str(item).strip() for item in active_strategies if str(item).strip()}
    candidates = [
        str(item).strip()
        for item in cfg.get("strategies", {}).get("candidates", [])
        if str(item).strip() and str(item).strip() not in active_set
    ]
    if "toxicity_mm" in active_set:
        baseline = "toxicity_mm"
    else:
        baseline = active_strategies[0] if len(active_strategies) == 1 else None
    return {
        "baseline_strategy": baseline,
        "active_strategy_families": list(active_strategies),
        "research_candidates": candidates,
    }


def _write_runtime_snapshot(
    runtime: RuntimeTelemetry,
    *,
    cfg: dict,
    run_id: str,
    phase: str,
    mode: str,
    loop_count: int,
    fetched_markets: int,
    processed_markets: int,
    toxic_skips: int,
    bankroll: float,
    all_markets: List[Dict],
    executor_snapshot: Dict,
    positions,
    risk_report: Dict,
    gate_snapshot: Dict[str, object],
    strategy_governance: Dict[str, object],
    bucket_pause_decisions: Dict[tuple[str, str, str, str], Dict[str, object]] | None = None,
) -> Dict:
    active_families = [
        str(item).strip()
        for item in (strategy_governance.get("active_strategy_families") or [])
        if str(item).strip()
    ]
    if not active_families:
        baseline_strategy = str(strategy_governance.get("baseline_strategy") or "").strip()
        if baseline_strategy:
            active_families = [baseline_strategy]
    gate_pause = _aggregate_gate_pause_decision(
        str(gate_snapshot["gate_state"]),
        list(gate_snapshot["gate_reasons"]),
        active_families=active_families,
        settlement_truth_blocking=_settlement_truth_pause_enabled(cfg),
    )
    market_eligibility = runtime.summarize_market_eligibility(run_id=run_id)
    return runtime.write_runtime_snapshot(
        run_id=run_id,
        phase=phase,
        mode=mode,
        loop_count=loop_count,
        fetched_markets=fetched_markets,
        processed_markets=processed_markets,
        toxic_skips=toxic_skips,
        bankroll=round(bankroll, 6),
        open_position_count=executor_snapshot["open_position_count"],
        resolved_trade_count=executor_snapshot["resolved_trade_count"],
        win_rate=round(executor_snapshot["win_rate"], 6),
        active_slots=_active_slot_summary(all_markets),
        pending_resolution_slots=executor_snapshot["pending_resolution_slots"],
        latest_slot_resolution=executor_snapshot.get("latest_slot_resolution"),
        latest_position_settlement=executor_snapshot.get("latest_position_settlement"),
        latest_settlement=executor_snapshot.get("latest_settlement"),
        positions=positions,
        risk=risk_report,
        gate_state=gate_snapshot["gate_state"],
        gate_reasons=gate_snapshot["gate_reasons"],
        gate_inputs=gate_snapshot["gate_inputs"],
        new_order_pause=bool(gate_pause["pause"]),
        pause_policy=str(gate_pause["policy"]),
        pause_reason=str(gate_pause["reason"]),
        pause_scope=str(gate_pause["scope"]),
        pause_blocking_reasons=list(gate_pause["blocking_gate_reasons"]),
        pause_family_decisions=dict(gate_pause["family_pause_decisions"]),
        bucket_pause=_bucket_pause_status(bucket_pause_decisions),
        market_eligibility=market_eligibility,
        **strategy_governance,
    )


def _write_runtime_status_snapshot(
    runtime: RuntimeTelemetry,
    *,
    cfg: dict | None = None,
    run_id: str,
    phase: str,
    mode: str,
    loop_count: int,
    fetched_markets: int,
    processed_markets: int,
    toxic_skips: int,
    bankroll: float,
    all_markets: List[Dict],
    executor_snapshot: Dict,
    positions,
    risk_report: Dict,
    gate_snapshot: Dict[str, object],
    strategy_governance: Dict[str, object],
    bucket_pause_decisions: Dict[tuple[str, str, str, str], Dict[str, object]] | None = None,
) -> Dict:
    return _write_runtime_snapshot(
        runtime,
        cfg=cfg or {},
        run_id=run_id,
        phase=phase,
        mode=mode,
        loop_count=loop_count,
        fetched_markets=fetched_markets,
        processed_markets=processed_markets,
        toxic_skips=toxic_skips,
        bankroll=bankroll,
        all_markets=all_markets,
        executor_snapshot=executor_snapshot,
        positions=positions,
        risk_report=risk_report,
        gate_snapshot=gate_snapshot,
        strategy_governance=strategy_governance,
        bucket_pause_decisions=bucket_pause_decisions,
    )


def _runtime_gate_snapshot(runtime_dir: str | Path) -> Dict[str, object]:
    gate_inputs = build_gate_inputs(str(runtime_dir))
    gate_state, gate_reasons = compute_gate_state(gate_inputs)
    return {
        "gate_state": gate_state,
        "gate_reasons": gate_reasons,
        "gate_inputs": gate_inputs,
    }


_MM_EXEMPT_FAMILIES = {"toxicity_mm", "market_making"}
_HARD_STOP_REASON_MARKERS = (
    "circuit_breaker",
    "contradiction_log_open",
    "run_lineage_fragmentation",
)
_LOW_WIN_RATE_REASON_MARKERS = ("win_rate=",)
_SETTLEMENT_TRUTH_REASON_MARKERS = ("settlement_pnl_computable=",)
_NON_BLOCKING_RED_REASON_MARKERS = _SETTLEMENT_TRUTH_REASON_MARKERS


def _is_market_making_family(strategy_family: str) -> bool:
    normalized = str(strategy_family or "").strip().lower()
    return normalized in _MM_EXEMPT_FAMILIES


def _matching_gate_reasons(gate_reasons: List[str], markers: tuple[str, ...]) -> List[str]:
    matched: List[str] = []
    for reason in gate_reasons:
        normalized_reason = str(reason).lower()
        if any(marker in normalized_reason for marker in markers):
            matched.append(reason)
    return matched


def _settlement_truth_pause_enabled(cfg: dict | None) -> bool:
    config = cfg or {}
    execution_cfg = config.get("execution", {}) or {}
    runtime_gate_cfg = config.get("runtime_gate", {}) or {}
    return bool(
        runtime_gate_cfg.get(
            "block_on_uncomputable_settlement_truth",
            execution_cfg.get("block_on_uncomputable_settlement_truth", False),
        )
    )


def _aggregate_gate_pause_decision(
    gate_state: str,
    gate_reasons: List[str],
    *,
    active_families: List[str],
    settlement_truth_blocking: bool = False,
) -> Dict[str, object]:
    families = [str(item).strip() for item in active_families if str(item).strip()]
    if not families:
        families = [""]
    decisions = {
        family or "unknown": _gate_pause_decision(
            gate_state,
            gate_reasons,
            strategy_family=family,
            settlement_truth_blocking=settlement_truth_blocking,
        )
        for family in families
    }
    paused_families = [family for family, decision in decisions.items() if bool(decision["pause"])]
    unpaused_families = [family for family, decision in decisions.items() if not bool(decision["pause"])]
    decision_reasons = {str(decision["reason"]) for decision in decisions.values()}
    if paused_families and unpaused_families:
        aggregate_reason = "mixed_by_family"
        aggregate_pause = False
        aggregate_scope = "mixed_by_family"
    elif paused_families:
        aggregate_reason = next(iter(decision_reasons)) if len(decision_reasons) == 1 else "all_paused_mixed_reasons"
        aggregate_pause = True
        aggregate_scope = "all_active_families"
    else:
        aggregate_reason = next(iter(decision_reasons)) if len(decision_reasons) == 1 else "all_unpaused_mixed_reasons"
        aggregate_pause = False
        aggregate_scope = "no_active_families_paused"
    return {
        "pause": aggregate_pause,
        "policy": "family-aware",
        "reason": aggregate_reason,
        "scope": aggregate_scope,
        "blocking_gate_reasons": sorted(
            {reason for decision in decisions.values() for reason in decision["blocking_gate_reasons"]}
        ),
        "family_pause_decisions": decisions,
        "active_families": [family for family in decisions.keys() if family != "unknown"],
    }


def _gate_pause_decision(
    gate_state: str,
    gate_reasons: List[str],
    *,
    strategy_family: str,
    settlement_truth_blocking: bool = False,
) -> Dict[str, object]:
    normalized_family = str(strategy_family or "").strip().lower()
    if gate_state != "RED":
        return {
            "pause": False,
            "policy": "family-aware",
            "reason": "gate_not_red",
            "blocking_gate_reasons": [],
        }

    hard_stop_reasons = _matching_gate_reasons(gate_reasons, _HARD_STOP_REASON_MARKERS)
    if hard_stop_reasons:
        return {
            "pause": True,
            "policy": "family-aware",
            "reason": "hard_stop_red_gate",
            "blocking_gate_reasons": hard_stop_reasons,
        }

    settlement_truth_reasons = _matching_gate_reasons(gate_reasons, _SETTLEMENT_TRUTH_REASON_MARKERS)
    if settlement_truth_blocking and settlement_truth_reasons:
        return {
            "pause": True,
            "policy": "family-aware",
            "reason": "settlement_truth_blocking_red_gate",
            "blocking_gate_reasons": settlement_truth_reasons,
        }

    low_win_rate_reasons = _matching_gate_reasons(gate_reasons, _LOW_WIN_RATE_REASON_MARKERS)
    if low_win_rate_reasons:
        non_low_win_rate_reasons = [reason for reason in gate_reasons if reason not in low_win_rate_reasons]
        if _is_market_making_family(normalized_family):
            non_blocking_reasons = _matching_gate_reasons(non_low_win_rate_reasons, _NON_BLOCKING_RED_REASON_MARKERS)
            if len(non_blocking_reasons) == len(non_low_win_rate_reasons):
                return {
                    "pause": False,
                    "policy": "family-aware",
                    "reason": (
                        "mm_exempt_low_win_rate_only"
                        if not non_low_win_rate_reasons
                        else "mm_exempt_low_win_rate_non_blocking_red_gate"
                    ),
                    "blocking_gate_reasons": [],
                }
            return {
                "pause": True,
                "policy": "family-aware",
                "reason": "mm_low_win_rate_mixed_blocking_red_gate",
                "blocking_gate_reasons": low_win_rate_reasons + non_low_win_rate_reasons,
            }
        return {
            "pause": True,
            "policy": "family-aware",
            "reason": "directional_low_win_rate_red_gate",
            "blocking_gate_reasons": low_win_rate_reasons,
        }

    return {
        "pause": False,
        "policy": "family-aware",
        "reason": "non_blocking_red_gate",
        "blocking_gate_reasons": [],
    }


def _should_pause_new_orders(gate_state: str, gate_reasons: List[str], *, strategy_family: str) -> bool:
    decision = _gate_pause_decision(gate_state, gate_reasons, strategy_family=strategy_family)
    return bool(decision["pause"])


def _bucket_pause_cfg(cfg: dict) -> dict:
    research_cfg = (cfg.get("research", {}) or {})
    return {
        "enabled": bool(research_cfg.get("bucket_pause_enabled", True)),
        "warn_only": bool(research_cfg.get("bucket_pause_warn_only", False)),
        "min_settled_trades": int(research_cfg.get("bucket_pause_min_settled_trades", 20) or 20),
    }


def _bucket_for_market(strategy_family: str, market: Dict, tte_bucket: str | None = None) -> tuple[str, str, str, str]:
    asset = str(market.get("asset") or "unknown").lower()
    interval = str(market.get("interval_minutes") or "unknown")
    if not tte_bucket:
        tte_bucket = "unknown"
    return (str(strategy_family), asset, interval, str(tte_bucket))


def _load_bucket_pause_decisions(runtime_dir: str | Path, cfg: dict) -> Dict[tuple[str, str, str, str], Dict[str, object]]:
    policy = _bucket_pause_cfg(cfg)
    if not policy["enabled"]:
        return {}
    runtime_path = Path(runtime_dir)
    artifact_dir = runtime_path.parent / "research" if runtime_path.name == "runtime" else runtime_path / "research"
    path = artifact_dir / "bucket_scoreboard.json"
    if not path.exists():
        return {}
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    decisions: Dict[tuple[str, str, str, str], Dict[str, object]] = {}
    if not isinstance(rows, list):
        return decisions
    for row in rows:
        if not isinstance(row, dict) or not row.get("pause"):
            continue
        try:
            settled = int(row.get("settled_trades", 0) or 0)
        except (TypeError, ValueError):
            settled = 0
        if settled < int(policy["min_settled_trades"]):
            continue
        key = (
            str(row.get("family") or ""),
            str(row.get("asset") or "unknown").lower(),
            str(row.get("interval") or "unknown"),
            str(row.get("tte_bucket") or "unknown"),
        )
        decisions[key] = row
    return decisions


def _bucket_pause_decision_for_market(
    bucket_pause_decisions: Dict[tuple[str, str, str, str], Dict[str, object]] | None,
    strategy_family: str,
    market: Dict,
    tte_bucket: str | None,
) -> Dict[str, object] | None:
    if not bucket_pause_decisions:
        return None
    exact_key = _bucket_for_market(strategy_family, market, tte_bucket)
    if exact_key in bucket_pause_decisions:
        return bucket_pause_decisions[exact_key]
    unknown_tte_key = _bucket_for_market(strategy_family, market, "unknown")
    return bucket_pause_decisions.get(unknown_tte_key)


def _bucket_pause_status(bucket_pause_decisions: Dict[tuple[str, str, str, str], Dict[str, object]] | None) -> dict:
    rows = list((bucket_pause_decisions or {}).values())
    return {
        "enabled": True,
        "paused_bucket_count": len(rows),
        "paused_buckets": rows[:25],
    }


def _strategy_directional_signal_entry_style(cfg: dict, strategy_family: str, signal=None) -> str:
    return resolve_directional_signal_entry_style(cfg, strategy_family, signal)


def _bounded_directional_signal_size(risk_mgr, strategy_family: str, signal, reference_price: float) -> float:
    price = max(float(reference_price or 0.0), 0.01)
    limit_price = max(float(getattr(signal, "price", price) or price), 0.01)
    confidence = max(0.0, min(float(getattr(signal, "confidence", 0.0) or 0.0), 1.0))
    stop_loss_distance = max(abs(limit_price - price), 0.02)
    edge = max(stop_loss_distance * confidence, 0.0)
    bounded = risk_mgr.calculate_bounded_size(
        strategy_family,
        edge=edge,
        price=price,
        stop_loss_distance=stop_loss_distance,
        confidence=confidence,
    )
    requested_size = float(getattr(signal, "size", 0.0) or 0.0)
    return min(requested_size, bounded.size) if bounded.size > 0 else 0.0


def _mark_directional_fired_on_fill(fill: Dict, *, time_decay=None, spot_momentum=None, opening_range=None) -> Dict | None:
    if fill.get("order_kind") != "signal":
        return None
    family = str(fill.get("strategy_family") or "")
    if family not in {"mean_reversion_5min", "opening_range", "time_decay", "spot_momentum"}:
        return None
    slot_id = fill.get("slot_id")
    outcome = fill.get("outcome")
    if not slot_id or not outcome:
        return None
    if family == "time_decay" and time_decay is not None:
        key = (str(slot_id), str(outcome))
        if key in getattr(time_decay, "_fired_slots", set()):
            return None
        time_decay.mark_fired(str(slot_id), str(outcome))
    elif family == "spot_momentum" and spot_momentum is not None:
        if str(slot_id) in getattr(spot_momentum, "_fired_slots", set()):
            return None
        spot_momentum.mark_fired(str(slot_id))
    elif family == "opening_range" and opening_range is not None:
        opening_key = str(fill.get("market_id") or slot_id)
        if opening_range.is_fired(opening_key):
            return None
        opening_range.mark_fired(opening_key, str(outcome))
    return {
        "event_type": "signal.fired_on_fill",
        "slot_id": slot_id,
        "market_id": fill.get("market_id"),
        "market_slug": fill.get("market_slug"),
        "strategy_family": family,
        "outcome": outcome,
        "order_id": fill.get("order_id"),
        "reason": "filled_signal_order_consumed_slot",
    }


def _filled_event_from_execution_result(result: Dict) -> Dict | None:
    for event in result.get("events", []) or []:
        if event.get("event_type") == "order.filled" and event.get("order_kind") == "signal":
            return event
    return None


def _strategy_min_seconds_to_expiry(cfg: dict, strategy_family: str) -> float:
    strategy_cfg = (cfg.get("strategies", {}) or {}).get(strategy_family, {}) or {}
    if "min_seconds_to_expiry_for_new_orders" in strategy_cfg:
        return float(strategy_cfg.get("min_seconds_to_expiry_for_new_orders", 0))
    execution_cfg = cfg.get("execution", {}) or {}
    return float(execution_cfg.get("min_seconds_to_expiry_for_new_orders", 120))


def _quote_submission_post_only(reason: str | None) -> bool:
    """Flattening is a risk-reduction intent; do not let it rest as a maker quote."""
    return "endgame_flatten" not in str(reason or "")


def _toxicity_mm_runtime_entry_allowed(
    *,
    entry_allowed: bool,
    entry_reasons: List[str],
    seconds_to_expiry: float,
    has_existing_exposure: bool,
) -> bool:
    if entry_allowed:
        return True
    tte_only_block = bool(entry_reasons) and all(str(reason).startswith("tte_lt_") for reason in entry_reasons)
    if not tte_only_block:
        return False
    if has_existing_exposure:
        return True
    return float(seconds_to_expiry) < 30.0


def _toxicity_mm_has_family_market_state(executor, market_id: str) -> bool:
    """Return true when toxicity_mm has exposure or cancellable family orders in a market."""
    if executor.has_strategy_market_exposure("toxicity_mm", market_id):
        return True
    open_statuses = {"open", "partially_filled", "acknowledged"}
    return any(
        order.get("market_id") == market_id
        and order.get("strategy_family") == "toxicity_mm"
        and order.get("status") in open_statuses
        for order in getattr(executor, "orders", {}).values()
    )


def _risk_reduce_toxicity_quote(quote, *, inventory: float):
    if quote is None:
        return None
    inventory = float(inventory or 0.0)
    if inventory == 0 or "endgame_flatten" in str(getattr(quote, "reason", "")):
        return quote
    bid_size = float(getattr(quote, "bid_size", 0.0) or 0.0)
    ask_size = float(getattr(quote, "ask_size", 0.0) or 0.0)
    if inventory > 0:
        bid_size = 0.0
        ask_size = min(ask_size, abs(inventory))
    else:
        bid_size = min(bid_size, abs(inventory))
        ask_size = 0.0
    if bid_size <= 0 and ask_size <= 0:
        return None
    reason = str(getattr(quote, "reason", ""))
    if "risk_reduce_only" not in reason:
        reason = f"{reason}|risk_reduce_only" if reason else "risk_reduce_only"
    return quote.__class__(
        market_id=quote.market_id,
        outcome=quote.outcome,
        bid_price=quote.bid_price,
        ask_price=quote.ask_price,
        bid_size=round(bid_size, 2),
        ask_size=round(ask_size, 2),
        reason=reason,
        book_quality=quote.book_quality,
    )


def _toxicity_mm_existing_quote_orders(executor, market_id: str, outcome: str) -> Dict[str, Dict]:
    open_statuses = {"open", "partially_filled", "acknowledged"}
    result: Dict[str, Dict] = {}
    for order in getattr(executor, "orders", {}).values():
        if order.get("market_id") != market_id:
            continue
        if order.get("strategy_family") != "toxicity_mm" or order.get("order_kind") != "quote":
            continue
        if order.get("outcome") != outcome or order.get("status") not in open_statuses:
            continue
        side_key = "bid" if str(order.get("side") or "").upper() == "BUY" else "ask"
        result[side_key] = order
    return result


def _toxicity_mm_quote_refresh_decision(
    quote,
    *,
    existing_orders: Dict[str, Dict] | None,
    now_ts: float,
    price_threshold: float,
    ttl_seconds: float,
    size_reduction_threshold: float = 0.10,
) -> Dict[str, object]:
    if quote is None:
        return {"refresh": False, "reason": "no_quote"}
    if "endgame_flatten" in str(getattr(quote, "reason", "")):
        return {"refresh": True, "reason": "flatten_refresh"}
    existing_orders = existing_orders or {}
    desired = []
    if float(getattr(quote, "bid_size", 0.0) or 0.0) > 0:
        desired.append(("bid", float(getattr(quote, "bid_price", 0.0) or 0.0), float(getattr(quote, "bid_size", 0.0) or 0.0)))
    if float(getattr(quote, "ask_size", 0.0) or 0.0) > 0:
        desired.append(("ask", float(getattr(quote, "ask_price", 0.0) or 0.0), float(getattr(quote, "ask_size", 0.0) or 0.0)))
    if not desired:
        return {"refresh": False, "reason": "zero_sized_quote"}
    for side, _, _ in desired:
        if side not in existing_orders:
            return {"refresh": True, "reason": "missing_quote_side"}
    desired_sides = {side for side, _, _ in desired}
    extra_sides = sorted(set(existing_orders) - desired_sides)
    if extra_sides:
        return {"refresh": True, "reason": "extra_quote_side", "extra_sides": extra_sides}
    max_age = 0.0
    max_price_delta = 0.0
    max_size_reduction = 0.0
    for side, desired_price, desired_size in desired:
        order = existing_orders[side]
        created_ts = float(order.get("created_ts", order.get("timestamp", now_ts)) or now_ts)
        max_age = max(max_age, float(now_ts) - created_ts)
        max_price_delta = max(max_price_delta, abs(float(order.get("price", 0.0) or 0.0) - desired_price))
        existing_size = float(order.get("remaining_size", order.get("remaining_qty", order.get("size", 0.0))) or 0.0)
        if existing_size > 0 and desired_size < existing_size:
            max_size_reduction = max(max_size_reduction, (existing_size - desired_size) / existing_size)
    if max_age >= float(ttl_seconds):
        return {"refresh": True, "reason": "quote_ttl_expired", "age_seconds": round(max_age, 3)}
    if max_price_delta >= float(price_threshold):
        return {"refresh": True, "reason": "quote_reprice_threshold", "price_delta": round(max_price_delta, 6)}
    if max_size_reduction >= float(size_reduction_threshold):
        return {"refresh": True, "reason": "quote_size_reduced", "size_reduction": round(max_size_reduction, 6)}
    return {
        "refresh": False,
        "reason": "quote_reuse_within_threshold",
        "age_seconds": round(max_age, 3),
        "price_delta": round(max_price_delta, 6),
        "size_reduction": round(max_size_reduction, 6),
    }


def _entry_gate_for_market(
    cfg: dict,
    market: Dict,
    *,
    now_ts: float,
    strategy_family: str,
    gate_state: str,
    gate_reasons: List[str],
    bucket_pause_decisions: Dict[tuple[str, str, str, str], Dict[str, object]] | None = None,
    tte_bucket: str | None = None,
    allow_bucket_pause_for_risk_reducing: bool = False,
) -> tuple[bool, List[str]]:
    reasons: List[str] = []
    gate_pause = _gate_pause_decision(
        gate_state,
        gate_reasons,
        strategy_family=strategy_family,
        settlement_truth_blocking=_settlement_truth_pause_enabled(cfg),
    )
    if bool(gate_pause["pause"]):
        reasons.append("runtime_gate_red")
        reasons.extend([f"gate:{reason}" for reason in gate_pause["blocking_gate_reasons"] or gate_reasons])
        return False, reasons

    bucket_pause = _bucket_pause_decision_for_market(bucket_pause_decisions, strategy_family, market, tte_bucket)
    if bucket_pause:
        reason = str(bucket_pause.get("pause_reason") or "bucket_paused")
        reasons.append("bucket_paused")
        reasons.append(f"bucket:{reason}")
        if not allow_bucket_pause_for_risk_reducing and not _bucket_pause_cfg(cfg)["warn_only"]:
            return False, reasons

    min_seconds = _strategy_min_seconds_to_expiry(cfg, strategy_family)
    end_ts = market.get("end_ts")
    if end_ts is not None:
        seconds_left = float(end_ts) - float(now_ts)
        if seconds_left < min_seconds:
            reasons.append(f"tte_lt_{int(min_seconds)}s")
            return False, reasons

    return True, reasons


def _strategy_entry_allowed(
    runtime: RuntimeTelemetry,
    cfg: dict,
    market: Dict,
    *,
    now_ts: float,
    strategy_family: str,
    gate_snapshot: Dict[str, object],
    bucket_pause_decisions: Dict[tuple[str, str, str, str], Dict[str, object]] | None = None,
    tte_bucket: str | None = None,
) -> bool:
    entry_allowed, entry_reasons = _entry_gate_for_market(
        cfg,
        market,
        now_ts=now_ts,
        strategy_family=strategy_family,
        gate_state=str(gate_snapshot["gate_state"]),
        gate_reasons=list(gate_snapshot["gate_reasons"]),
        bucket_pause_decisions=bucket_pause_decisions,
        tte_bucket=tte_bucket,
    )
    if not entry_allowed:
        runtime.append_event("market.entry_blocked", {
            "market_id": market["id"],
            "market_slug": market["slug"],
            "strategy_family": strategy_family,
            "reasons": entry_reasons,
            "bucket_pause": _bucket_pause_decision_for_market(bucket_pause_decisions, strategy_family, market, tte_bucket),
        })
    return entry_allowed


def _enforce_clock_drift(drift_seconds: float) -> None:
    if drift_seconds > 5:
        logger.warning("Clock drift is %.2fs vs Polymarket API time", drift_seconds)
    if drift_seconds > 30:
        logger.warning("Clock drift exceeds 30s; paper trading may query stale windows")
    if drift_seconds > 60:
        raise click.ClickException(f"Clock drift too large for interval markets: {drift_seconds:.2f}s")


def _install_signal_shutdown(loop: asyncio.AbstractEventLoop, main_task: asyncio.Task, stop_context: dict) -> List[int]:
    registered_signals: List[int] = []

    def _request_shutdown(sig: int) -> None:
        if stop_context.get("reason") == "completed":
            stop_context["reason"] = f"signal_{signal.Signals(sig).name.lower()}"
        click.echo(f"Received {signal.Signals(sig).name}; shutting down...")
        main_task.cancel()

    for sig in (signal.SIGTERM,):
        try:
            loop.add_signal_handler(sig, _request_shutdown, sig)
            registered_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            logger.warning("Signal handlers unavailable for %s", signal.Signals(sig).name)
    return registered_signals


def _remove_signal_shutdown(loop: asyncio.AbstractEventLoop, registered_signals: Iterable[int]) -> None:
    for sig in registered_signals:
        try:
            loop.remove_signal_handler(sig)
        except (NotImplementedError, RuntimeError):
            logger.debug("Unable to remove signal handler for %s", signal.Signals(sig).name)


def _read_prior_bankroll(ledger_db_path: str) -> "float | None":
    """Read the most recent bankroll from risk_snapshot_recorded events in the ledger.

    Returns the bankroll float if found, or None if the ledger does not exist or has no
    risk_snapshot_recorded events yet.
    """
    import json
    import sqlite3

    from pathlib import Path as _Path

    p = _Path(ledger_db_path)
    if not p.exists():
        return None
    try:
        conn = sqlite3.connect(str(p))
        cur = conn.cursor()
        cur.execute(
            "SELECT payload_json FROM ledger_events"
            " WHERE event_type='risk_snapshot_recorded'"
            " ORDER BY event_ts DESC LIMIT 1"
        )
        row = cur.fetchone()
        conn.close()
        if row is None:
            return None
        payload = json.loads(row[0])
        bankroll = payload.get("capital")
        if bankroll is not None:
            return float(bankroll)
        return None
    except Exception:
        return None


def _detect_prior_run_id(ledger_db_path: str) -> "tuple[str | None, float | None]":
    """Return (prior_run_id, prior_bankroll) from the most recent risk_snapshot_recorded event.

    Returns (None, None) when the ledger does not exist or contains no such events.
    """
    import json
    import sqlite3

    from pathlib import Path as _Path

    p = _Path(ledger_db_path)
    if not p.exists():
        return None, None
    try:
        conn = sqlite3.connect(str(p))
        cur = conn.cursor()
        cur.execute(
            "SELECT run_id, payload_json FROM ledger_events"
            " WHERE event_type='risk_snapshot_recorded'"
            " ORDER BY event_ts DESC LIMIT 1"
        )
        row = cur.fetchone()
        conn.close()
        if row is None:
            return None, None
        prior_run_id = row[0]
        payload = json.loads(row[1])
        prior_bankroll = float(payload.get("capital", 0.0))
        return prior_run_id, prior_bankroll
    except Exception:
        return None, None


@cli.command()
@click.option("--mode", type=click.Choice(["backtest", "paper", "live"]), default="paper", help="Execution mode")
@click.option("--strategies", default=None, help="Comma-separated active strategies (defaults to config.yaml strategies.active)")
@click.option("--max-loops", type=int, default=0, help="Bounded loop count for smoke tests")
@click.option("--runtime-dir", default="data/runtime", help="Directory for durable runtime telemetry")
@click.option("--sleep-seconds", type=int, default=15, help="Loop sleep duration")
def run(mode, strategies, max_loops, runtime_dir, sleep_seconds):
    """Run the trading bot in specified mode."""
    from execution import PolymarketExecutor
    from tradeability_policy import assess_tradeability
    from market_context import build_market_context
    from market_data import PolymarketData
    from risk import RiskManager
    from spot_provider import SpotProvider
    from strategies.mean_reversion_5min import MeanReversion5Min
    from strategies.opening_range import OpeningRangeBreakout
    from strategies.spot_momentum import SpotMomentum
    from strategies.time_decay import TimeDecay
    from strategies.toxicity_mm import ToxicityMM

    if mode == "live":
        raise click.ClickException("Live mode is deferred until the restored paper workflow is proven.")

    cfg = load_cfg()
    active_strategies = _resolve_active_strategies(cfg, strategies)
    cfg.setdefault("strategies", {})["active"] = active_strategies
    initial_capital = float(cfg.get("execution", {}).get("paper_starting_bankroll", 500.0))
    ledger_db_path = str(Path(runtime_dir) / "ledger.db")
    prior_bankroll = _read_prior_bankroll(ledger_db_path)
    if prior_bankroll is not None:
        click.echo(f"Bankroll continuity: resuming from prior bankroll ${prior_bankroll:.2f} (not resetting to config default)")
        initial_capital = prior_bankroll
    else:
        click.echo(f"No prior ledger found — starting with config bankroll ${initial_capital:.2f}")
    strategy_governance = _strategy_governance(cfg, active_strategies)

    click.echo(f"Starting bot in {mode} mode with strategies: {','.join(active_strategies)}")
    click.echo("Press Ctrl+C to stop.")

    async def main_loop():
        loop = asyncio.get_running_loop()
        main_task = asyncio.current_task()
        if main_task is None:
            raise RuntimeError("main_loop must run inside an asyncio task")

        runtime = RuntimeTelemetry(runtime_dir)
        run_id = runtime.make_run_id(mode)
        stop_reason = "completed"
        stop_snapshot_dir: str | None = None
        registered_signals = _install_signal_shutdown(loop, main_task, _stop_context)

        # AC-8: emit service_restart lineage event when a prior run exists in the ledger
        ledger_path = str(Path(runtime_dir) / "ledger.db")
        prior_run_id, prior_bankroll_at_stop = _detect_prior_run_id(ledger_path)
        if prior_run_id and prior_run_id != run_id:
            runtime.append_event("service_restart", {
                "prior_run_id": prior_run_id,
                "new_run_id": run_id,
                "restart_ts": time.time(),
                "trigger": "unknown",
                "prior_bankroll": prior_bankroll_at_stop,
                "new_bankroll": initial_capital,
                "bankroll_delta": round(initial_capital - (prior_bankroll_at_stop or 0.0), 6),
            })

        runtime.append_event("runtime.started", {"run_id": run_id, "mode": mode, "strategies": active_strategies, **strategy_governance})
        runtime.update_status(run_id=run_id, phase="starting", mode=mode, strategies=active_strategies, loop_count=0, **strategy_governance)

        async with PolymarketData(cfg) as md:
            startup = await md.smoke_check()
            _enforce_clock_drift(startup["clock_drift_seconds"])
            runtime.append_event("runtime.startup_check", startup)

            cfg.setdefault("execution", {})
            cfg["execution"].setdefault("ledger_db_path", str(Path(runtime_dir) / "ledger.db"))

            mean_rev = MeanReversion5Min(cfg)
            opening_range = OpeningRangeBreakout(cfg)
            time_decay = TimeDecay(cfg)
            spot_momentum = SpotMomentum(cfg)
            mm = ToxicityMM(cfg)
            spot_provider = SpotProvider()
            spot_provider.start()
            mid_history_store = {}
            slot_spot_anchor_store = {}
            risk_mgr = RiskManager(cfg, initial_capital=initial_capital)
            executor = PolymarketExecutor(cfg, md, mode=mode, run_id=run_id)
            await executor.__aenter__()
            loop_count = 0

            try:
                while True:
                    loop_count += 1
                    loop_now = time.time()
                    all_markets = await md.get_markets_by_duration(minutes=max(md.intervals))
                    click.echo(f"Discovered {len(all_markets)} strict interval markets")

                    for market in all_markets:
                        executor.register_market(market)
                        runtime.append_event("market.discovered", {
                            "slot_id": market["slot_id"],
                            "market_id": market["id"],
                            "market_slug": market["slug"],
                            "asset": market.get("asset"),
                            "interval_minutes": market.get("interval_minutes"),
                            "end_ts": market["end_ts"],
                        })

                    order_books = await asyncio.gather(
                        *[md.get_orderbook(market) for market in all_markets],
                        return_exceptions=True,
                    )
                    gate_snapshot = _runtime_gate_snapshot(runtime_dir)
                    bucket_pause_decisions = _load_bucket_pause_decisions(runtime_dir, cfg)

                    processed_markets = 0
                    toxic_skips = 0
                    fill_events: List[Dict] = []
                    orderbooks_by_market: Dict[str, object] = {}

                    for idx, market in enumerate(all_markets):
                        market_id = market["id"]
                        primary_outcome = market["outcomes"][0]
                        ob_or_exc = order_books[idx]
                        if isinstance(ob_or_exc, Exception):
                            click.echo(f"Error fetching order book for {market_id}: {ob_or_exc}")
                            runtime.append_event("market.fetch_error", {"market_id": market_id, "error": str(ob_or_exc)})
                            continue

                        orderbook = ob_or_exc
                        orderbooks_by_market[market_id] = orderbook
                        market_context = build_market_context(
                            market,
                            orderbook,
                            loop_now,
                            spot_provider.get,
                            mid_history_store,
                            slot_spot_anchor_store,
                        )
                        book_quality = assess_tradeability(
                            cfg,
                            "runtime_baseline",
                            orderbook,
                            primary_outcome,
                        )
                        runtime.append_market_sample({
                            "run_id": run_id,
                            "market_id": market_id,
                            "market_slug": market["slug"],
                            "slot_id": market["slot_id"],
                            "volume": float(market.get("volume", 0)),
                            "book_spread_bps": round(book_quality.spread_bps, 3),
                            "book_depth": round(book_quality.top_depth, 3),
                            "book_notional": round(book_quality.top_notional, 3),
                            "book_reasons": list(book_quality.reasons),
                            "is_tradeable": book_quality.is_tradeable,
                        })

                        new_fills = executor.evaluate_market_orders(market_id, orderbook)
                        for fill in new_fills:
                            fired_event = _mark_directional_fired_on_fill(fill, time_decay=time_decay, spot_momentum=spot_momentum, opening_range=opening_range)
                            if fired_event:
                                runtime.append_event(fired_event.pop("event_type"), fired_event, run_id=run_id)
                        fill_events.extend(new_fills)
                        expired_signal_events = await executor.expire_directional_signal_orders(market_id=market_id, now_ts=loop_now)
                        if expired_signal_events:
                            _emit_events(runtime, expired_signal_events, run_id=run_id)
                        mid = md.mid_price(orderbook, primary_outcome)
                        mean_rev.update_price(
                            market_id,
                            mid,
                            loop_now,
                            float(market.get("volume", 0)),
                            interval_minutes=market.get("interval_minutes"),
                        )

                        for strategy_family in active_strategies:
                            executor.note_market_seen(strategy_family)

                        if not book_quality.is_tradeable:
                            toxic_skips += 1
                            runtime.append_event("market.runtime_baseline_untradeable", {
                                "market_id": market_id,
                                "market_slug": market["slug"],
                                "reasons": list(book_quality.reasons),
                                "spread_bps": book_quality.spread_bps,
                                "top_depth": book_quality.top_depth,
                            })

                        processed_markets += 1
                        volume = float(market.get("volume", 0))

                        if "mean_reversion_5min" in active_strategies and _strategy_entry_allowed(
                            runtime,
                            cfg,
                            market,
                            now_ts=loop_now,
                            strategy_family="mean_reversion_5min",
                            gate_snapshot=gate_snapshot,
                            bucket_pause_decisions=bucket_pause_decisions,
                            tte_bucket=market_context.tte_bucket,
                        ):
                            signal = mean_rev.generate_signal(
                                market_id,
                                primary_outcome,
                                mid,
                                orderbook,
                                volume,
                                interval_minutes=market.get("interval_minutes"),
                            )
                            if signal:
                                signal.size = _bounded_directional_signal_size(
                                    risk_mgr,
                                    "mean_reversion_5min",
                                    signal,
                                    reference_price=max(mid, 0.01),
                                )
                                if signal.size > 0:
                                    click.echo(
                                        f"SIGNAL: {market['slug']} {signal.outcome} {signal.action} {signal.size}@{mid:.4f} ({signal.reason})"
                                    )
                                    result = await executor.execute_signal_trade(market, orderbook, signal)
                                    _emit_events(runtime, result.get("events", []), run_id=run_id)
                                    if result.get("filled"):
                                        fired_fill = _filled_event_from_execution_result(result)
                                        fired_event = _mark_directional_fired_on_fill(fired_fill or result, time_decay=time_decay, spot_momentum=spot_momentum, opening_range=opening_range)
                                        if fired_event:
                                            runtime.append_event(fired_event.pop("event_type"), fired_event, run_id=run_id)

                        if "opening_range" in active_strategies and _strategy_entry_allowed(
                            runtime,
                            cfg,
                            market,
                            now_ts=loop_now,
                            strategy_family="opening_range",
                            gate_snapshot=gate_snapshot,
                            bucket_pause_decisions=bucket_pause_decisions,
                            tte_bucket=market_context.tte_bucket,
                        ):
                            opening_range.update_price(market_id, mid, volume)
                            signal = opening_range.generate_signal(
                                market_id,
                                primary_outcome,
                                mid,
                                orderbook,
                                volume,
                                slot_id=market.get("slot_id", ""),
                            )
                            if signal:
                                signal.size = _bounded_directional_signal_size(
                                    risk_mgr,
                                    "opening_range",
                                    signal,
                                    reference_price=max(signal.price, 0.01),
                                )
                                if signal.size > 0 and not executor.has_strategy_market_exposure("opening_range", market_id):
                                    click.echo(
                                        f"OPENING RANGE: {market['slug']} {signal.outcome} {signal.action} {signal.size}@{signal.price:.4f} ({signal.reason})"
                                    )
                                    result = await executor.execute_signal_trade(market, orderbook, signal, strategy_family="opening_range")
                                    _emit_events(runtime, result.get("events", []), run_id=run_id)
                                    if result.get("filled"):
                                        fired_fill = _filled_event_from_execution_result(result)
                                        fired_event = _mark_directional_fired_on_fill(fired_fill or result, time_decay=time_decay, spot_momentum=spot_momentum, opening_range=opening_range)
                                        if fired_event:
                                            runtime.append_event(fired_event.pop("event_type"), fired_event, run_id=run_id)

                        if "time_decay" in active_strategies and _strategy_entry_allowed(
                            runtime,
                            cfg,
                            market,
                            now_ts=loop_now,
                            strategy_family="time_decay",
                            gate_snapshot=gate_snapshot,
                            bucket_pause_decisions=bucket_pause_decisions,
                            tte_bucket=market_context.tte_bucket,
                        ):
                            signal = time_decay.generate_signal(market_id, market, orderbook, current_time=loop_now)
                            if signal:
                                signal.size = _bounded_directional_signal_size(
                                    risk_mgr,
                                    "time_decay",
                                    signal,
                                    reference_price=max(signal.price, 0.01),
                                )
                                if signal.size > 0 and not executor.has_strategy_market_exposure("time_decay", market_id):
                                    click.echo(
                                        f"TIME DECAY: {market['slug']} {signal.outcome} {signal.action} {signal.size}@{signal.price:.4f} ({signal.reason})"
                                    )
                                    result = await executor.execute_signal_trade(market, orderbook, signal, strategy_family="time_decay")
                                    _emit_events(runtime, result.get("events", []), run_id=run_id)
                                    if result.get("filled"):
                                        fired_fill = _filled_event_from_execution_result(result)
                                        fired_event = _mark_directional_fired_on_fill(fired_fill or result, time_decay=time_decay, spot_momentum=spot_momentum, opening_range=opening_range)
                                        if fired_event:
                                            runtime.append_event(fired_event.pop("event_type"), fired_event, run_id=run_id)

                        if "spot_momentum" in active_strategies and _strategy_entry_allowed(
                            runtime,
                            cfg,
                            market,
                            now_ts=loop_now,
                            strategy_family="spot_momentum",
                            gate_snapshot=gate_snapshot,
                            bucket_pause_decisions=bucket_pause_decisions,
                            tte_bucket=market_context.tte_bucket,
                        ):
                            signal = spot_momentum.generate_signal(market_context)
                            if signal:
                                signal.size = _bounded_directional_signal_size(
                                    risk_mgr,
                                    "spot_momentum",
                                    signal,
                                    reference_price=max(signal.price, 0.01),
                                )
                                if signal.size > 0 and not executor.has_strategy_market_exposure("spot_momentum", market_id):
                                    click.echo(
                                        f"SPOT MOMENTUM: {market['slug']} {signal.outcome} {signal.action} {signal.size}@{signal.price:.4f} ({signal.reason})"
                                    )
                                    result = await executor.execute_signal_trade(market, orderbook, signal, strategy_family="spot_momentum")
                                    _emit_events(runtime, result.get("events", []), run_id=run_id)
                                    if result.get("filled"):
                                        fired_fill = _filled_event_from_execution_result(result)
                                        fired_event = _mark_directional_fired_on_fill(fired_fill or result, time_decay=time_decay, spot_momentum=spot_momentum, opening_range=opening_range)
                                        if fired_event:
                                            runtime.append_event(fired_event.pop("event_type"), fired_event, run_id=run_id)

                        toxicity_mm_entry_allowed = False
                        toxicity_mm_has_exposure = False
                        toxicity_mm_has_market_state = False
                        if "toxicity_mm" in active_strategies:
                            toxicity_mm_has_exposure = executor.has_strategy_market_exposure("toxicity_mm", market_id)
                            toxicity_mm_has_market_state = _toxicity_mm_has_family_market_state(executor, market_id)
                            mm_entry_allowed, mm_entry_reasons = _entry_gate_for_market(
                                cfg,
                                market,
                                now_ts=loop_now,
                                strategy_family="toxicity_mm",
                                gate_state=str(gate_snapshot["gate_state"]),
                                gate_reasons=list(gate_snapshot["gate_reasons"]),
                                bucket_pause_decisions=bucket_pause_decisions,
                                tte_bucket=market_context.tte_bucket,
                                allow_bucket_pause_for_risk_reducing=toxicity_mm_has_exposure,
                            )
                            if _toxicity_mm_runtime_entry_allowed(
                                entry_allowed=mm_entry_allowed,
                                entry_reasons=mm_entry_reasons,
                                seconds_to_expiry=market_context.seconds_to_expiry,
                                has_existing_exposure=toxicity_mm_has_exposure,
                            ):
                                toxicity_mm_entry_allowed = True
                            else:
                                cancelled_count = 0
                                if toxicity_mm_has_market_state:
                                    cancelled_count = await executor.cancel_family_market(market_id, "toxicity_mm")
                                runtime.append_event("market.entry_blocked", {
                                    "market_id": market["id"],
                                    "market_slug": market["slug"],
                                    "strategy_family": "toxicity_mm",
                                    "reasons": mm_entry_reasons,
                                    "bucket_pause": _bucket_pause_decision_for_market(
                                        bucket_pause_decisions, "toxicity_mm", market, market_context.tte_bucket
                                    ),
                                    "cancelled_orders": cancelled_count,
                                })

                        if toxicity_mm_entry_allowed:
                            # Choose quote outcome: prefer whichever side has less executor exposure.
                            # If no position exists, alternate between Up/Down per loop to avoid pure directionality.
                            primary_outcome = market["outcomes"][0]  # "Up"
                            secondary_outcome = market["outcomes"][1] if len(market["outcomes"]) > 1 else primary_outcome  # "Down"

                            qty_up = executor.get_position_quantity("toxicity_mm", market_id, primary_outcome)
                            qty_dn = executor.get_position_quantity("toxicity_mm", market_id, secondary_outcome)
                            mm.positions[market_id] = {
                                primary_outcome: {"size": qty_up, "avg": 0.0},
                                secondary_outcome: {"size": qty_dn, "avg": 0.0},
                            }

                            # With existing exposure, quote the inventory-carrying side so
                            # replacements can reduce risk.  When flat, quote the side with
                            # less absolute inventory and tie-break by loop parity.
                            if toxicity_mm_has_exposure:
                                if abs(qty_up) > abs(qty_dn):
                                    preferred_outcome = primary_outcome
                                elif abs(qty_dn) > abs(qty_up):
                                    preferred_outcome = secondary_outcome
                                else:
                                    preferred_outcome = primary_outcome if loop_count % 2 == 0 else secondary_outcome
                            elif abs(qty_up) < abs(qty_dn):
                                preferred_outcome = primary_outcome
                            elif abs(qty_dn) < abs(qty_up):
                                preferred_outcome = secondary_outcome
                            else:
                                preferred_outcome = primary_outcome if loop_count % 2 == 0 else secondary_outcome

                            quote_yes, _, quality = mm.generate_quotes(
                                market_id,
                                orderbook,
                                preferred_outcome=preferred_outcome,
                                context=market_context,
                            )
                            preferred_inventory = qty_up if preferred_outcome == primary_outcome else qty_dn
                            is_flatten_quote = bool(quote_yes and "endgame_flatten" in quote_yes.reason)
                            if quote_yes and toxicity_mm_has_exposure and not is_flatten_quote:
                                quote_yes = _risk_reduce_toxicity_quote(quote_yes, inventory=preferred_inventory)
                            if not quote_yes:
                                executor.note_toxic_book_skip("toxicity_mm")
                                cancelled_count = 0
                                if toxicity_mm_has_market_state:
                                    cancelled_count = await executor.cancel_family_market(market_id, "toxicity_mm")
                                runtime.append_event("quote.skipped", {
                                    "market_id": market_id,
                                    "market_slug": market["slug"],
                                    "strategy_family": "toxicity_mm",
                                    "reasons": list(quality.reasons),
                                    "cancelled_orders": cancelled_count,
                                })
                            else:
                                mm_cfg = (cfg.get("strategies", {}) or {}).get("toxicity_mm", {}) or {}
                                existing_quote_orders = _toxicity_mm_existing_quote_orders(executor, market_id, quote_yes.outcome)
                                refresh_decision = _toxicity_mm_quote_refresh_decision(
                                    quote_yes,
                                    existing_orders=existing_quote_orders,
                                    now_ts=loop_now,
                                    price_threshold=float(mm_cfg.get("quote_reprice_threshold", 0.005)),
                                    ttl_seconds=float(mm_cfg.get("quote_ttl_seconds", 15.0)),
                                    size_reduction_threshold=float(mm_cfg.get("quote_size_reduction_refresh_pct", 0.10)),
                                )
                                if not bool(refresh_decision.get("refresh")):
                                    runtime.append_event("quote.reused", {
                                        "market_id": market_id,
                                        "market_slug": market["slug"],
                                        "strategy_family": "toxicity_mm",
                                        "outcome": quote_yes.outcome,
                                        "reason": refresh_decision.get("reason"),
                                        "decision": refresh_decision,
                                    })
                                else:
                                    await executor.cancel_family_market(market_id, "toxicity_mm")
                                    bid_id = None
                                    ask_id = None
                                    quote_post_only = _quote_submission_post_only(quote_yes.reason)
                                    if quote_yes.bid_size > 0:
                                        bid_id = await executor.place_order(
                                            market_id,
                                            quote_yes.outcome,
                                            "BUY",
                                            quote_yes.bid_size,
                                            quote_yes.bid_price,
                                            post_only=quote_post_only,
                                            strategy_family="toxicity_mm",
                                            order_kind="quote",
                                            market=market,
                                        )
                                    if quote_yes.ask_size > 0:
                                        ask_id = await executor.place_order(
                                            market_id,
                                            quote_yes.outcome,
                                            "SELL",
                                            quote_yes.ask_size,
                                            quote_yes.ask_price,
                                            post_only=quote_post_only,
                                            strategy_family="toxicity_mm",
                                            order_kind="quote",
                                            market=market,
                                        )
                                    runtime.append_event("quote.submitted", {
                                        "market_id": market_id,
                                        "market_slug": market["slug"],
                                        "strategy_family": "toxicity_mm",
                                        "bid_order_id": bid_id,
                                        "ask_order_id": ask_id,
                                        "reason": quote_yes.reason,
                                        "refresh_reason": refresh_decision.get("reason"),
                                        "refresh_decision": refresh_decision,
                                        "book_quality": quote_yes.book_quality,
                                    })

                    _emit_events(runtime, [{**fill, "event_type": "order.filled"} for fill in fill_events], run_id=run_id)
                    settlement_events = await executor.process_pending_resolutions(now_ts=time.time())
                    _emit_events(runtime, settlement_events, run_id=run_id)

                    positions = await executor.refresh_positions()
                    strategy_metrics = executor.get_family_metrics()
                    runtime.write_strategy_metrics(strategy_metrics)

                    snapshot_ts = time.time()
                    executor_snapshot = executor.get_runtime_snapshot(now_ts=snapshot_ts, orderbooks_by_market=orderbooks_by_market)
                    risk_report = risk_mgr.get_risk_report(
                        executor_snapshot=executor_snapshot,
                        ledger_events=executor.get_ledger_events(),
                        now_ts=snapshot_ts,
                    )
                    executor.record_risk_snapshot(risk_report, snapshot_ts=snapshot_ts)
                    _write_runtime_snapshot(
                        runtime,
                        cfg=cfg,
                        run_id=run_id,
                        phase="running",
                        mode=mode,
                        loop_count=loop_count,
                        fetched_markets=len(all_markets),
                        processed_markets=processed_markets,
                        toxic_skips=toxic_skips,
                        bankroll=risk_report["capital"],
                        all_markets=all_markets,
                        executor_snapshot=executor_snapshot,
                        positions=positions,
                        risk_report=risk_report,
                        gate_snapshot=gate_snapshot,
                        strategy_governance=strategy_governance,
                        bucket_pause_decisions=bucket_pause_decisions,
                    )
                    click.echo(
                        "Paper bankroll ${capital:.2f} | open={open} | resolved={resolved} | pending={pending}".format(
                            capital=risk_report["capital"],
                            open=executor_snapshot["open_position_count"],
                            resolved=executor_snapshot["resolved_trade_count"],
                            pending=len(executor_snapshot["pending_resolution_slots"]),
                        )
                    )

                    if risk_mgr.check_circuit_breakers(risk_report):
                        stop_reason = "circuit_breaker"
                        runtime.append_event("runtime.circuit_breaker", {"run_id": run_id, "risk": risk_report})
                        runtime.update_status(
                            run_id=run_id,
                            phase="stopping",
                            mode=mode,
                            loop_count=loop_count,
                            stop_reason=stop_reason,
                            risk=risk_report,
                        )
                        evidence_manifest = runtime.preserve_run_evidence(trigger=stop_reason, run_id=run_id)
                        stop_snapshot_dir = str(evidence_manifest["snapshot_dir"])
                        runtime.append_event(
                            "runtime.evidence_preserved",
                            {"run_id": run_id, "trigger": stop_reason, "snapshot_dir": stop_snapshot_dir},
                        )
                        click.echo(f"Preserved stop evidence at {stop_snapshot_dir}")
                        click.echo("CIRCUIT BREAKER TRIGGERED — stopping trading")
                        break

                    if max_loops and loop_count >= max_loops:
                        stop_reason = "max_loops"
                        runtime.append_event("runtime.max_loops_reached", {"run_id": run_id, "max_loops": max_loops})
                        break

                    await asyncio.sleep(sleep_seconds)
            except KeyboardInterrupt:
                stop_reason = "keyboard_interrupt"
                click.echo("Shutting down...")
                runtime.append_event("runtime.interrupted", {"run_id": run_id})
            except asyncio.CancelledError:
                stop_reason = _stop_context.get("reason", stop_reason)
                runtime.append_event("runtime.interrupted", {"run_id": run_id, "signal": stop_reason})
            finally:
                _remove_signal_shutdown(loop, registered_signals)
                spot_provider.stop()
                runtime.update_status(run_id=run_id, phase="stopped", mode=mode, loop_count=loop_count, stop_reason=stop_reason)
                runtime.append_event(
                    "runtime.stopped",
                    {"run_id": run_id, "loop_count": loop_count, "stop_reason": stop_reason, "snapshot_dir": stop_snapshot_dir},
                )
                await executor.__aexit__(None, None, None)
                _stop_context["reason"] = stop_reason  # AC-2: surface stop_reason to outer scope

    _stop_context: dict = {"reason": "completed"}
    asyncio.run(main_loop())

    # AC-2: circuit breaker must exit with code 2 so systemd RestartPreventExitStatus=2 suppresses restart
    if _stop_context["reason"] == "circuit_breaker":
        click.echo("Exiting with code 2 (circuit_breaker) — service will not auto-restart")
        sys.exit(2)


@cli.command()
@click.option("--data", default="data/sample_backtest.csv", help="Path to historical order book data CSV")
def backtest(data):
    """Run backtest on historical data."""
    from backtest_engine import Backtester

    cfg = load_cfg()
    bt = Backtester(cfg, initial_capital=float(cfg.get("execution", {}).get("paper_starting_bankroll", 500.0)))
    click.echo(f"Loading data from {data}")
    df = bt.load_historical_orderbooks(data)
    if df.empty:
        click.echo("No data. Exiting.")
        return

    market_ids = df["market_id"].unique()[:5]
    results = []
    for mid in market_ids:
        result = bt.simulate_mean_reversion(df, mid)
        if result.total_trades > 0:
            results.append(result)
            click.echo(f"{mid}: {result.total_trades} trades, WR {result.win_rate:.1%}, PnL ${result.total_pnl:.2f}")

    if results:
        total_pnl = sum(r.total_pnl for r in results)
        total_trades = sum(r.total_trades for r in results)
        wins = sum(int(r.win_rate * r.total_trades) for r in results)
        overall_wr = wins / total_trades if total_trades > 0 else 0
        click.echo(f"AGGREGATE: {total_trades} trades, WR {overall_wr:.1%}, Total PnL ${total_pnl:.2f}")
    else:
        click.echo("No trades generated. Adjust parameters or data?")


@cli.command()
def paper():
    """Run paper trading simulation (alias for run --mode=paper)."""
    ctx = click.get_current_context()
    ctx.invoke(run, mode="paper")


@cli.command()
def live():
    """Run live trading (currently disabled pending paper validation)."""
    ctx = click.get_current_context()
    ctx.invoke(run, mode="live")


@cli.command()
def collect():
    """Collect real-time order book data for later backtesting."""
    import pandas as pd

    from market_data import PolymarketData

    cfg = load_cfg()
    click.echo("Starting data collection. Press Ctrl+C to stop and save.")

    async def collect_loop():
        async with PolymarketData(cfg) as md:
            records = []
            try:
                while True:
                    markets = await md.get_markets_by_duration(minutes=max(md.intervals))
                    orderbooks = await asyncio.gather(*[md.get_orderbook(market) for market in markets], return_exceptions=True)
                    for market, orderbook in zip(markets, orderbooks):
                        if isinstance(orderbook, Exception):
                            continue
                        for outcome in market["outcomes"]:
                            records.append({
                                "timestamp": time.time(),
                                "market_id": market["id"],
                                "market_slug": market["slug"],
                                "slot_id": market["slot_id"],
                                "asset": market.get("asset"),
                                "interval_minutes": market.get("interval_minutes"),
                                "outcome": outcome,
                                "best_bid": md.best_bid(orderbook, outcome),
                                "best_ask": md.best_ask(orderbook, outcome),
                                "mid_price": md.mid_price(orderbook, outcome),
                                "imbalance": md.calculate_imbalance(orderbook, outcome),
                                "volume": float(market.get("volume", 0)),
                            })
                    click.echo(f"Collected {len(records)} snapshots")
                    await asyncio.sleep(30)
            except KeyboardInterrupt:
                pass
            finally:
                fname = Path("data") / f"collection_{int(time.time())}.csv"
                pd.DataFrame(records).to_csv(fname, index=False)
                click.echo(f"Saved {len(records)} records to {fname}")

    asyncio.run(collect_loop())


@cli.command()
@click.option("--runtime-dir", default="data/runtime", help="Directory containing live runtime artifacts")
def status(runtime_dir):
    """Render the current replay-backed runtime status."""
    click.echo(render_status_text(runtime_dir))


@cli.command(name="health")
@click.option("--runtime-dir", default="data/runtime", help="Directory containing live runtime artifacts")
@click.option("--max-heartbeat-age", default=180, help="Max acceptable heartbeat age in seconds")
def health_cmd(runtime_dir, max_heartbeat_age):
    """Check runtime health from durable heartbeat artifacts."""
    payload = runtime_health_payload(runtime_dir, max_heartbeat_age=max_heartbeat_age)
    click.echo(render_status_text(runtime_dir))
    click.echo(f"Healthy: {payload['healthy']} (threshold={max_heartbeat_age}s)")
    if not payload["healthy"]:
        raise SystemExit(1)


@cli.command(name="evidence")
@click.option("--runtime-dir", default="data/runtime", help="Directory containing runtime artifacts")
@click.option("--strategy-family", default="toxicity_mm", help="Strategy family to summarize")
@click.option("--event-limit", default=5000, help="Max events to inspect for current run")
@click.option("--sample-limit", default=5000, help="Max market samples to inspect for current run")
def evidence_cmd(runtime_dir, strategy_family, event_limit, sample_limit):
    """Render a baseline evidence packet from runtime artifacts and ledger history."""
    payload = build_baseline_evidence(
        runtime_dir,
        strategy_family=strategy_family,
        event_limit=event_limit,
        sample_limit=sample_limit,
    )
    click.echo(render_baseline_evidence_text(payload))


@cli.command(name="bakeoff")
@click.option("--spec-path", default="configs/strategy-bakeoff.yaml", help="Bakeoff YAML spec path")
@click.option("--python-bin", default=".venv/bin/python", help="Python interpreter used for trial subprocesses")
@click.option("--dry-run/--no-dry-run", default=False, help="Print commands without executing bounded runs")
def bakeoff(spec_path, python_bin, dry_run):
    """Run bounded isolated strategy family trials and emit one summary packet."""
    spec = load_bakeoff_spec(spec_path)
    experiment_dir = Path(spec.runtime_root)
    trials_dir = experiment_dir / "trials"

    outcomes = []
    click.echo(f"Bakeoff: {spec.experiment_id} ({len(spec.trials)} trials)")
    for trial in spec.trials:
        runtime_dir = Path(build_trial_runtime_dir(spec, trial))
        command = build_trial_command(
            python_bin=python_bin,
            trial=trial,
            runtime_dir=runtime_dir,
            max_loops=spec.max_loops,
            sleep_seconds=spec.sleep_seconds,
            cli_path=str(CLI_SCRIPT_PATH),
        )
        click.echo(f"- {trial.family}: {shlex.join(command)}")
        if dry_run:
            continue

        experiment_dir.mkdir(parents=True, exist_ok=True)
        trials_dir.mkdir(parents=True, exist_ok=True)
        if runtime_dir.exists():
            shutil.rmtree(runtime_dir)
        runtime_dir.mkdir(parents=True, exist_ok=True)

        try:
            completed = subprocess.run(
                command,
                check=False,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
            )
        except OSError as exc:
            raise click.ClickException(
                f"Unable to launch trial subprocess for {trial.family}: command={shlex.join(command)} | error={exc}"
            ) from exc
        if completed.returncode != 0:
            stdout_snippet = (completed.stdout or "").strip()
            stderr_snippet = (completed.stderr or "").strip()
            details = [
                f"Trial failed for {trial.family}",
                f"returncode={completed.returncode}",
                f"command={shlex.join(command)}",
            ]
            if stdout_snippet:
                details.append(f"stdout={stdout_snippet[-500:]}")
            if stderr_snippet:
                details.append(f"stderr={stderr_snippet[-500:]}")
            raise click.ClickException(" | ".join(details))

        outcome = collect_trial_outcome(runtime_dir, trial.family, label=trial.label)
        outcomes.append(outcome)

    if dry_run:
        return

    ranked = rank_trials(outcomes)
    artifacts = write_bakeoff_artifacts(experiment_dir, spec, ranked)
    winner = ranked[0] if ranked else None
    if winner is None:
        click.echo("Winner: none")
    else:
        click.echo(
            f"Winner: {winner.family} settled={winner.settled_count} resolved={winner.resolved_count} "
            f"pnl={winner.realized_pnl:.4f} fills={winner.fill_count} toxic_skips={winner.toxic_skip_count}"
        )
    click.echo(f"Summary: {artifacts['summary_json']}")
    click.echo(f"Report: {artifacts['summary_md']}")


@cli.command()
@click.option("--runtime-dir", default="data/runtime", help="Directory containing live runtime artifacts")
@click.option("--artifact-dir", default="data/research", help="Directory to write research outputs")
@click.option("--sample-limit", default=200, help="How many market samples to analyze")
def research(runtime_dir, artifact_dir, sample_limit):
    """Run autoresearch on live runtime artifacts, scoped to the current run by default."""
    telemetry = RuntimeTelemetry(runtime_dir)
    adapter = PolymarketRuntimeResearchAdapter(
        runtime_dir,
        sample_limit=sample_limit,
        run_id=telemetry.current_run_id(),
    )
    loop = ResearchLoop(artifact_dir)
    result = loop.run_cycle(adapter, runtime_dir=runtime_dir)
    click.echo(result.summary)
    for insight in result.insights:
        click.echo(f"- {insight.title}: {insight.recommendation} [{insight.confidence:.0%}]")
    click.echo(f"Artifacts written to {artifact_dir}/{result.cycle_id}.json and .md")


if __name__ == "__main__":
    cli()
