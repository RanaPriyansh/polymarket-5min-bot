from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from runtime_telemetry import RuntimeTelemetry


def _render_slot_resolution_line(latest_slot_resolution: dict[str, Any] | None) -> str:
    if not latest_slot_resolution:
        return "Last slot resolved: none"

    slot_id = latest_slot_resolution.get("slot_id", "?")
    event_type = latest_slot_resolution.get("event_type")
    winning_outcome = latest_slot_resolution.get("winning_outcome", "?")
    position_count = int(latest_slot_resolution.get("position_count", 0) or 0)
    if event_type == "slot_closed":
        return f"Last slot resolved: {slot_id}  closed flat at expiry (winner={winning_outcome})"
    if position_count > 1:
        realized = float(latest_slot_resolution.get("realized_pnl", 0.0) or 0.0)
        return (
            f"Last slot resolved: {slot_id}  winner={winning_outcome}  "
            f"multi-leg settlement legs={position_count} realized=${realized:+.2f}"
        )
    if (
        latest_slot_resolution.get("position_outcome") is None
        and latest_slot_resolution.get("realized_pnl") is None
    ):
        return f"Last slot resolved: {slot_id}  resolved with no held position (winner={winning_outcome})"
    return f"Last slot resolved: {slot_id}  winner={winning_outcome}"



def _render_position_settlement_line(
    latest_position_settlement: dict[str, Any] | None,
    latest_settlement: dict[str, Any] | None,
) -> str:
    if latest_position_settlement:
        return (
            "Last position settlement: {slot_id}  outcome={outcome} size={size:.2f} "
            "entry=${entry:.3f} realized=${realized:+.2f}"
        ).format(
            slot_id=latest_position_settlement.get("slot_id", "?"),
            outcome=latest_position_settlement.get("position_outcome")
            or latest_position_settlement.get("outcome", "?"),
            size=float(latest_position_settlement.get("position_size", latest_position_settlement.get("quantity", 0.0)) or 0.0),
            entry=float(latest_position_settlement.get("entry_price", latest_position_settlement.get("average_price", 0.0)) or 0.0),
            realized=float(
                latest_position_settlement.get("realized_pnl", latest_position_settlement.get("realized_pnl_delta", 0.0)) or 0.0
            ),
        )
    if latest_settlement:
        return f"Last position settlement: {latest_settlement}"
    return "Last position settlement: none"



def runtime_status_payload(runtime_dir: str | Path) -> dict[str, Any]:
    telemetry = RuntimeTelemetry(runtime_dir)
    status = telemetry.read_status()
    strategy_metrics = telemetry.read_strategy_metrics()
    heartbeat_ts = float(status.get("heartbeat_ts", 0.0) or 0.0)
    now_ts = time.time()
    heartbeat_age = max(0.0, now_ts - heartbeat_ts) if heartbeat_ts else None
    market_eligibility = status.get("market_eligibility")
    if not isinstance(market_eligibility, dict):
        market_eligibility = telemetry.summarize_market_eligibility(
            run_id=str(status.get("run_id")) if status.get("run_id") else None,
        )
    return {
        "runtime_dir": str(Path(runtime_dir)),
        "status": status,
        "strategy_metrics": strategy_metrics,
        "heartbeat_age_seconds": heartbeat_age,
        "recent_events": telemetry.read_events(limit=10),
        "market_eligibility": market_eligibility,
    }


def runtime_health_payload(runtime_dir: str | Path, max_heartbeat_age: int = 180) -> dict[str, Any]:
    payload = runtime_status_payload(runtime_dir)
    heartbeat_age = payload.get("heartbeat_age_seconds")
    healthy = heartbeat_age is not None and heartbeat_age <= max_heartbeat_age
    return {
        **payload,
        "healthy": healthy,
        "max_heartbeat_age_seconds": max_heartbeat_age,
    }


def format_reason_counts(items: Any) -> str:
    if not items:
        return "none recently"

    formatted_items: list[str] = []
    for item in items:
        reason: Any = item
        count: Any = None

        if isinstance(item, dict):
            reason = item.get("reason", "?")
            count = item.get("count", 0)
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            reason, count = item[0], item[1]

        formatted_items.append(f"{reason} ({count})")

    return "; ".join(formatted_items)


def market_eligibility_scope_label(eligibility: dict[str, Any] | None) -> str:
    eligibility = eligibility or {}
    summary_scope = str(eligibility.get("summary_scope") or "run")
    reason_scope = str(eligibility.get("reason_counts_scope") or "")
    if summary_scope in {"run", "bounded_recent_run"} and reason_scope == "recent_run_events":
        return "bounded recent-run"
    return summary_scope


def pause_surface_lines(status: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    pause_policy = status.get("pause_policy")
    pause_reason = status.get("pause_reason")
    pause_scope = status.get("pause_scope")
    if pause_policy or pause_reason or pause_scope:
        lines.append(
            f"Pause policy: {pause_policy or 'n/a'} | Scope: {pause_scope or 'n/a'} | Reason: {pause_reason or 'n/a'}"
        )
    pause_family_decisions = status.get("pause_family_decisions", {}) or {}
    if pause_family_decisions:
        lines.append(
            "Family-aware pause detail: "
            + "; ".join(
                f"{family}={'paused' if bool(decision.get('pause')) else 'active'} ({decision.get('reason', 'n/a')})"
                for family, decision in sorted(pause_family_decisions.items())
            )
        )
    bucket_pause = status.get("bucket_pause", {}) or {}
    paused_buckets = bucket_pause.get("paused_buckets", []) or []
    if bucket_pause:
        lines.append(
            f"Bucket pause: paused={int(bucket_pause.get('paused_bucket_count', 0) or 0)}"
        )
    if paused_buckets:
        rendered = []
        for row in paused_buckets[:8]:
            rendered.append(
                "{family}/{asset}/{interval}m/{tte} ({reason})".format(
                    family=row.get("family", "?"),
                    asset=row.get("asset", "?"),
                    interval=row.get("interval", "?"),
                    tte=row.get("tte_bucket", "?"),
                    reason=row.get("pause_reason", "paused"),
                )
            )
        lines.append("Paused buckets: " + "; ".join(rendered))
    return lines


def render_status_text(runtime_dir: str | Path) -> str:
    payload = runtime_status_payload(runtime_dir)
    status = payload["status"]
    risk = status.get("risk", {}) or {}
    strategy_metrics = payload["strategy_metrics"] or {}
    heartbeat_age = payload.get("heartbeat_age_seconds")
    heartbeat_text = "n/a" if heartbeat_age is None else f"{heartbeat_age:.1f}s"
    eligibility = payload.get("market_eligibility") or {}

    baseline = status.get("baseline_strategy")
    research_candidates = status.get("research_candidates", []) or []

    eligibility_scope = market_eligibility_scope_label(eligibility)
    reason_scope_label = "recent events" if eligibility.get("reason_counts_scope") == "recent_run_events" else "events"

    lines = [
        f"Runtime dir: {payload['runtime_dir']}",
        f"Run id: {status.get('run_id', 'unknown')}",
        f"Phase: {status.get('phase', 'unknown')} | Mode: {status.get('mode', 'unknown')} | Loop: {status.get('loop_count', 0)}",
        f"Heartbeat age: {heartbeat_text}",
        f"Markets: fetched={status.get('fetched_markets', 0)} processed={status.get('processed_markets', 0)} toxic_skips={status.get('toxic_skips', 0)}",
        (
            "Market eligibility ({scope}-scoped): discovered={discovered} structural={structural} governance={governance} quoted/entered={quoted}"
        ).format(
            scope=eligibility_scope,
            discovered=int(eligibility.get("discovered_markets", 0) or 0),
            structural=int(eligibility.get("structurally_untradeable_markets", 0) or 0),
            governance=int(eligibility.get("governance_blocked_markets", 0) or 0),
            quoted=int(eligibility.get("quoted_or_entered_markets", 0) or 0),
        ),
        f"Structural reasons ({reason_scope_label}): {format_reason_counts(eligibility.get('top_structural_reasons', []))}",
        f"Governance reasons ({reason_scope_label}): {format_reason_counts(eligibility.get('top_governance_reasons', []))}",
        f"Capital: ${float(risk.get('capital', status.get('bankroll', 0.0))):.2f} | Realized: {float(risk.get('realized_pnl_total', 0.0)):.4f} | Unrealized: {float(risk.get('unrealized_pnl_total', 0.0)):.4f} | Drawdown: {float(risk.get('max_drawdown', 0.0)):.2%}",
        f"Open positions: {status.get('open_position_count', 0)} | Open orders: {int(risk.get('open_order_count', status.get('open_order_count', 0)))} | Pending settlements: {len(status.get('pending_resolution_slots', []))}",
        f"Marks: marked={int(risk.get('marked_position_count', 0))} unmarked={int(risk.get('unmarked_position_count', 0))}",
        f"Exposure: gross={float(risk.get('total_gross_exposure', 0.0)):.4f} position={float(risk.get('gross_position_exposure', 0.0)):.4f} open_orders={float(risk.get('gross_open_order_exposure', 0.0)):.4f}",
        f"Resolved: {status.get('resolved_trade_count', 0)} | Win rate: {float(status.get('win_rate', 0.0)):.2%}",
    ]
    quote_skip_reasons = eligibility.get("top_quote_skip_reasons", [])
    if quote_skip_reasons:
        lines.append(f"Quote skip reasons ({reason_scope_label}): {format_reason_counts(quote_skip_reasons)}")

    if status.get("gate_state"):
        lines.append(
            f"Runtime gate: {status.get('gate_state')} | New orders paused: {bool(status.get('new_order_pause', False))}"
        )
        gate_reasons = status.get("gate_reasons", []) or []
        if gate_reasons:
            lines.append(f"Gate reasons: {'; '.join(str(item) for item in gate_reasons)}")
        lines.extend(pause_surface_lines(status))

    if baseline:
        lines.append(f"Baseline strategy: {baseline}")
    if research_candidates:
        lines.append(f"Research candidates: {', '.join(str(item) for item in research_candidates)}")

    if strategy_metrics:
        lines.append("Strategy metrics:")
        for family, metrics in sorted(strategy_metrics.items()):
            lines.append(
                "  - {family}: quotes={quotes} resting={resting} filled={filled} cancels={cancels} pnl={pnl:.4f} seen={seen} toxic_skips={toxic}".format(
                    family=family,
                    quotes=metrics.get("quotes_submitted", 0),
                    resting=metrics.get("orders_resting", 0),
                    filled=metrics.get("orders_filled", 0),
                    cancels=metrics.get("cancellations", 0),
                    pnl=float(metrics.get("realized_pnl", 0.0)),
                    seen=metrics.get("markets_seen", 0),
                    toxic=metrics.get("toxic_book_skips", 0),
                )
            )

    latest_slot_resolution = status.get("latest_slot_resolution") or status.get("latest_resolution")
    latest_position_settlement = status.get("latest_position_settlement")
    latest_settlement = status.get("latest_settlement") or latest_position_settlement
    lines.append(_render_slot_resolution_line(latest_slot_resolution))
    lines.append(_render_position_settlement_line(latest_position_settlement, latest_settlement))

    return "\n".join(lines)
