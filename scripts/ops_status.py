#!/usr/bin/env python3
"""
Canonical STATUS surface.

Reads runtime artifacts and produces a human-readable, run-scoped status report.
Backed by data, not opinion.

Usage:
    python scripts/ops_status.py              -- stdout
    python scripts/ops_status.py --write      -- also writes data/runtime/ops_status.txt
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.operator_truth import artifact_truth_lines
from runtime_telemetry import RuntimeTelemetry
from status_utils import format_reason_counts, market_eligibility_scope_label, pause_surface_lines


def load_json(path: Path, default=None):
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return default or {}


def fmt_ts(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "?"


def fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours/24:.1f}d"


def render_status(runtime_dir: Path, now_ts: float | None = None) -> str:
    runtime_dir = Path(runtime_dir)
    now_ts = now_ts or time.time()
    status_json = load_json(runtime_dir / "status.json")
    strategy_json = load_json(runtime_dir / "strategy_metrics.json")
    telemetry = RuntimeTelemetry(runtime_dir)
    metrics_path = runtime_dir.parent.parent / "data" / "runtime" / "strategy_metrics.json"
    if not strategy_json and metrics_path.exists():
        strategy_json = load_json(metrics_path)

    run_id = status_json.get("run_id", "unknown")
    mode = status_json.get("mode", "?")
    phase = status_json.get("phase", "?")
    strategy = status_json.get("baseline_strategy", "?")
    bankroll = status_json.get("bankroll", 0.0)
    heartbeat = status_json.get("heartbeat_ts", 0)
    loop_count = status_json.get("loop_count", 0)
    active_slots = status_json.get("active_slots", [])
    open_pos = status_json.get("open_position_count", 0)
    pos_detail = status_json.get("positions", {})
    risk = status_json.get("risk", {})
    pending = status_json.get("pending_resolution_slots", [])
    win_rate = status_json.get("win_rate", 0.0)
    resolved_count = status_json.get("resolved_trade_count", 0)
    strategies = status_json.get("strategies", [])
    fetched = status_json.get("fetched_markets", 0)
    toxic_skips = status_json.get("toxic_skips", 0)
    latest_slot_resolution = status_json.get("latest_slot_resolution") or status_json.get("latest_resolution")
    latest_position_settlement = status_json.get("latest_position_settlement")
    latest_settlement = status_json.get("latest_settlement") or latest_position_settlement

    heartbeat_ago = now_ts - heartbeat if heartbeat > 0 else None
    run_age = 0  # best guess from status.json timestamps
    # Try to infer run age from loop count * sleep (5s default)
    # More reliable: check for the oldest event with this run_id
    run_age_hint = f"~{loop_count * 5}s (from loop count * 5s)"
    research = status_json.get("research_candidates", [])
    market_eligibility = status_json.get("market_eligibility")
    if not isinstance(market_eligibility, dict):
        market_eligibility = telemetry.summarize_market_eligibility(
            run_id=str(run_id) if run_id and run_id != "unknown" else None,
        )

    eligibility_scope = market_eligibility_scope_label(market_eligibility)
    reason_scope_label = "recent events" if market_eligibility.get("reason_counts_scope") == "recent_run_events" else "events"

    lines = []
    lines.append("=" * 72)
    lines.append("  POLYMARKET 5/15m BOT  —  OPERATOR STATUS")
    lines.append("=" * 72)
    lines.append("")
    lines.extend(artifact_truth_lines(runtime_dir, artifact_run_id=run_id, generated_at_ts=now_ts))
    lines.append("")

    # Identity
    lines.append(f"Run:        {run_id}")
    lines.append(f"Mode:       {mode}")
    lines.append(f"Phase:      {phase}")
    lines.append(f"Strategy:   {strategy}")
    lines.append(f"As-of:      {fmt_ts(now_ts)}")
    lines.append(f"Heartbeat:  {fmt_ts(heartbeat)} ({fmt_duration(heartbeat_ago) if heartbeat_ago else '?'} ago)")
    lines.append(f"Loops:      {loop_count}")
    lines.append(f"Runtime:    {run_age_hint}")
    lines.append("")

    # PnL
    cap = risk.get("mark_to_market_capital", bankroll)
    daily_pnl = risk.get("daily_pnl", 0.0)
    unrealized = risk.get("unrealized_pnl_total", 0.0)
    realized_total = risk.get("realized_pnl_total", 0.0)
    exposure = risk.get("total_gross_exposure", 0.0)
    peak = risk.get("peak", bankroll)
    dd = risk.get("max_drawdown", 0.0)

    lines.append("--- PNL & RISK ---")
    lines.append(f"Bankroll:       ${bankroll:,.2f}")
    lines.append(f"Mark-to-market: ${cap:,.2f}")
    lines.append(f"Peak:           ${peak:,.2f}")
    lines.append(f"Max drawdown:   {dd * 100:.2f}%")
    lines.append(f"Daily PnL:      ${daily_pnl:+,.2f}")
    lines.append(f"Realized total: ${realized_total:+,.2f}")
    lines.append(f"Unrealized:     ${unrealized:+,.2f}")
    lines.append(f"Exposure:       ${exposure:,.2f}")
    lines.append("")

    # Positions
    lines.append("--- OPEN POSITIONS ({}) ---".format(open_pos))
    if pos_detail:
        for key, pos in sorted(pos_detail.items()):
            q = pos.get("quantity", 0)
            p = pos.get("average_price", 0)
            slug = pos.get("market_slug", "?")
            lines.append(f"  {key}: {q:+.2f} @ ${p:.3f} ({slug})")
    else:
        lines.append("  (none)")
    lines.append("")

    # Slots
    lines.append(f"--- ACTIVE SLOTS ({len(active_slots)}) ---")
    for slot in sorted(active_slots, key=lambda s: (s.get("interval_minutes", 0), s.get("asset", ""))):
        asset = slot.get("asset", "?")
        interval = slot.get("interval_minutes", "?")
        end_ts = slot.get("end_ts", 0)
        market_id = slot.get("market_id", "?")
        remaining = end_ts - now_ts if end_ts > now_ts else 0
        lines.append(f"  {asset.upper():>4} {interval}m  market={market_id:>10}  resolves in {fmt_duration(remaining) if remaining > 0 else 'EXPIRED'}")
    lines.append("")

    # Pending resolution
    if pending:
        lines.append(f"--- PENDING RESOLUTION ({len(pending)}) ---")
        for p_slot in pending:
            sid = p_slot.get("slot_id", "?")
            slug = p_slot.get("market_slug", "?")
            next_poll = p_slot.get("next_poll_ts", 0)
            deferred = p_slot.get("deferred", False)
            poll_at = fmt_ts(next_poll) if next_poll > 0 else "?"
            lines.append(f"  {sid}: {slug}  next_poll={poll_at}  deferred={deferred}")
        lines.append("")
    elif resolved_count == 0:
        lines.append("--- PENDING RESOLUTION: 0 (no markets have reached pending yet) ---")
        lines.append("")

    # Settlement
    lines.append("--- SETTLEMENT ---")
    lines.append(f"Resolved trades: {resolved_count}")
    lines.append(f"Win rate:        {win_rate * 100:.1f}%")
    if latest_slot_resolution:
        slot_id = latest_slot_resolution.get("slot_id", "?")
        event_type = latest_slot_resolution.get("event_type")
        winning_outcome = latest_slot_resolution.get("winning_outcome", "?")
        position_count = int(latest_slot_resolution.get("position_count", 0) or 0)
        if event_type == "slot_closed":
            lines.append(f"Last slot resolved: {slot_id}  closed flat at expiry (winner={winning_outcome})")
        elif position_count > 1:
            realized = float(latest_slot_resolution.get("realized_pnl", 0.0) or 0.0)
            lines.append(
                f"Last slot resolved: {slot_id}  winner={winning_outcome}  multi-leg settlement legs={position_count} realized=${realized:+.2f}"
            )
        elif latest_slot_resolution.get("position_outcome") is None and latest_slot_resolution.get("realized_pnl") is None:
            lines.append(f"Last slot resolved: {slot_id}  resolved with no held position (winner={winning_outcome})")
        else:
            lines.append(f"Last slot resolved: {slot_id}  winner={winning_outcome}")
    else:
        lines.append("Last slot resolved: none")

    if latest_position_settlement:
        lines.append(
            "Last position settlement: {slot_id}  outcome={outcome} size={size:.2f} entry=${entry:.3f} realized=${realized:+.2f}".format(
                slot_id=latest_position_settlement.get("slot_id", "?"),
                outcome=latest_position_settlement.get("position_outcome") or latest_position_settlement.get("outcome", "?"),
                size=float(latest_position_settlement.get("position_size", latest_position_settlement.get("quantity", 0.0)) or 0.0),
                entry=float(latest_position_settlement.get("entry_price", latest_position_settlement.get("average_price", 0.0)) or 0.0),
                realized=float(latest_position_settlement.get("realized_pnl", latest_position_settlement.get("realized_pnl_delta", 0.0)) or 0.0),
            )
        )
    elif latest_settlement:
        lines.append(f"Last position settlement: {latest_settlement}")
    else:
        lines.append("Last position settlement: none")
    lines.append("")

    gate_state = status_json.get("gate_state")
    if gate_state:
        lines.append("--- RUNTIME GATE ---")
        lines.append(f"Runtime gate: {gate_state}  New orders paused: {bool(status_json.get('new_order_pause', False))}")
        gate_reasons = status_json.get("gate_reasons", []) or []
        if gate_reasons:
            lines.append(f"Gate reasons: {'; '.join(str(item) for item in gate_reasons)}")
        lines.extend(pause_surface_lines(status_json))
        lines.append("")

    # Family metrics
    lines.append("--- FAMILY METRICS ---")
    family_metrics = strategy_json.get(strategy, {})
    if family_metrics:
        for k, v in sorted(family_metrics.items()):
            lines.append(f"  {k}: {v}")
    else:
        # inline status metrics
        st_metrics = status_json.get("strategy_metrics", {})
        if st_metrics:
            for fam, vals in sorted(st_metrics.items()):
                lines.append(f"  [{fam}]")
                for k, v in sorted(vals.items()):
                    lines.append(f"    {k}: {v}")
        else:
            lines.append("  (no metrics yet)")
    lines.append("")

    # Markets
    lines.append(f"Fetched markets: {fetched}  Toxic skips: {toxic_skips}")
    lines.append(
        "Market eligibility ({scope}-scoped): discovered={discovered} structural={structural} governance={governance} quoted/entered={quoted}".format(
            scope=eligibility_scope,
            discovered=int(market_eligibility.get("discovered_markets", 0) or 0),
            structural=int(market_eligibility.get("structurally_untradeable_markets", 0) or 0),
            governance=int(market_eligibility.get("governance_blocked_markets", 0) or 0),
            quoted=int(market_eligibility.get("quoted_or_entered_markets", 0) or 0),
        )
    )
    lines.append(f"Structural reasons ({reason_scope_label}): {format_reason_counts(market_eligibility.get('top_structural_reasons', []))}")
    lines.append(f"Governance reasons ({reason_scope_label}): {format_reason_counts(market_eligibility.get('top_governance_reasons', []))}")
    quote_skip_reasons = market_eligibility.get("top_quote_skip_reasons", [])
    if quote_skip_reasons:
        lines.append(f"Quote skip reasons ({reason_scope_label}): {format_reason_counts(quote_skip_reasons)}")
    lines.append("")

    # Research
    if research:
        lines.append(f"Research candidates: {', '.join(research)}")

    lines.append("=" * 72)

    return "\n".join(lines)


def main():
    write_mode = "--write" in sys.argv
    runtime_dir = REPO / "data" / "runtime"

    if not (runtime_dir / "status.json").exists():
        print("ERROR: status.json not found in", runtime_dir)
        sys.exit(1)

    report = render_status(runtime_dir)
    print(report)

    if write_mode:
        output = runtime_dir / "ops_status.txt"
        output.write_text(report + "\n")
        print(f"\n  [written] {output}")


if __name__ == "__main__":
    main()
