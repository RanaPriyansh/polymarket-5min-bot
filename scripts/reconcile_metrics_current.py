#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from ledger import SQLiteLedger
from replay import replay_ledger

from operator_truth import artifact_truth_lines, fmt_ts, load_json

RUNTIME = REPO / "data" / "runtime"
DB = RUNTIME / "ledger.db"
STATUS = RUNTIME / "status.json"
METRICS = RUNTIME / "strategy_metrics.json"
OUTPUT = RUNTIME / "reconcile_metrics_latest.txt"


def build_report(now_ts: float | None = None) -> str:
    now_ts = float(now_ts if now_ts is not None else datetime.now(tz=timezone.utc).timestamp())
    status = load_json(STATUS, {})
    metrics = load_json(METRICS, {})
    run_id = str(status.get("run_id", "") or "")
    heartbeat_ts = float(status.get("heartbeat_ts", 0.0) or 0.0)
    lines = [
        "=" * 72,
        "CURRENT-RUN METRIC RECONCILIATION",
        "=" * 72,
        *artifact_truth_lines(RUNTIME, artifact_run_id=run_id, generated_at_ts=now_ts),
        "",
    ]

    if not DB.exists() or not run_id:
        lines.append("No ledger.db or run_id available.")
        return "\n".join(lines) + "\n"

    ledger = SQLiteLedger(DB)
    all_events = ledger.list_events(run_id=run_id)
    if heartbeat_ts > 0:
        replay_events = [event for event in all_events if float(event.event_ts) <= heartbeat_ts]
    else:
        replay_events = list(all_events)
    projection = replay_ledger(replay_events)
    full_projection = replay_ledger(all_events)

    counts: dict[str, int] = {}
    full_counts: dict[str, int] = {}
    for event in replay_events:
        counts[event.event_type] = counts.get(event.event_type, 0) + 1
    for event in all_events:
        full_counts[event.event_type] = full_counts.get(event.event_type, 0) + 1

    open_positions_replay = len(projection.positions)
    open_positions_full = len(full_projection.positions)
    open_positions_status = int(status.get("open_position_count", 0) or 0)
    resolved_status = int(status.get("resolved_trade_count", 0) or 0)
    pending_status = len(status.get("pending_resolution_slots", []) or [])
    fills_applied = int(counts.get("fill_applied", 0) or 0)
    fills_observed = int(counts.get("fill_observed", 0) or 0)
    slot_settled = int(counts.get("slot_settled", 0) or 0)
    events_after_heartbeat = len(all_events) - len(replay_events)
    family = status.get("baseline_strategy", "toxicity_mm")
    fam_metrics = metrics.get(family, {}) if isinstance(metrics, dict) else {}
    realized_status = float((status.get("risk") or {}).get("realized_pnl_total", 0.0) or 0.0)

    contradictions = []
    if open_positions_replay != open_positions_status:
        contradictions.append(
            f"open positions mismatch: replay_asof_heartbeat={open_positions_replay} status={open_positions_status}"
        )
    if resolved_status != projection.resolved_trade_count:
        contradictions.append(
            f"resolved_trade_count mismatch: replay_asof_heartbeat={projection.resolved_trade_count} status={resolved_status}"
        )
    if abs(realized_status - float(projection.realized_pnl_total)) > 1e-6:
        contradictions.append(
            f"realized pnl mismatch: replay_asof_heartbeat={projection.realized_pnl_total:.4f} status={realized_status:.4f}"
        )
    if fills_applied != fills_observed:
        contradictions.append(f"fill counts mismatch: observed={fills_observed} applied={fills_applied}")

    lines += [
        f"comparison_as_of_heartbeat: {fmt_ts(heartbeat_ts)}",
        f"ledger_events_current_run={len(all_events)} asof_heartbeat={len(replay_events)} after_heartbeat={events_after_heartbeat}",
        f"status.bankroll={float(status.get('bankroll', 0.0) or 0.0):.4f}",
        f"status.realized_pnl_total={realized_status:.4f}",
        f"status.unrealized_pnl_total={float((status.get('risk') or {}).get('unrealized_pnl_total', 0.0) or 0.0):.4f}",
        f"status.open_position_count={open_positions_status}",
        f"status.pending_resolution_slots={pending_status}",
        f"status.resolved_trade_count={resolved_status}",
        f"replay_asof_heartbeat.fill_observed={fills_observed}",
        f"replay_asof_heartbeat.fill_applied={fills_applied}",
        f"replay_asof_heartbeat.slot_settled={slot_settled}",
        f"replay_asof_heartbeat.open_positions={open_positions_replay}",
        f"replay_asof_heartbeat.resolved_trade_count={projection.resolved_trade_count}",
        f"replay_asof_heartbeat.realized_pnl_total={projection.realized_pnl_total:.4f}",
        f"replay_full_current_run.open_positions={open_positions_full}",
        f"replay_full_current_run.resolved_trade_count={full_projection.resolved_trade_count}",
        f"replay_full_current_run.slot_settled={int(full_counts.get('slot_settled', 0) or 0)}",
        f"strategy_metrics[{family}].orders_filled={fam_metrics.get('orders_filled', 0)}",
        f"strategy_metrics[{family}].quotes_submitted={fam_metrics.get('quotes_submitted', 0)}",
        f"strategy_metrics[{family}].realized_pnl={fam_metrics.get('realized_pnl', 0)}",
        "",
        "Contradictions:",
    ]

    if contradictions:
        lines.extend(f"- {item}" for item in contradictions)
        verdict = "BROKEN"
    else:
        lines.append("- none detected against replay-backed current-run projection")
        verdict = "PASS"

    lines += ["", f"Verdict: {verdict}"]
    return "\n".join(lines) + "\n"


def main() -> None:
    report = build_report()
    OUTPUT.write_text(report)
    print(OUTPUT)


if __name__ == "__main__":
    main()
