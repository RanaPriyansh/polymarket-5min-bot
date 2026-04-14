#!/usr/bin/env python3
"""
SETTLEMENT DIAGNOSTICS surface.

Proves the settlement lifecycle (or lack thereof) by examining runtime artifacts.

Usage:
    python scripts/ops_settlement_diagnostics.py              -- stdout
    python scripts/ops_settlement_diagnostics.py --write      -- also writes data/runtime/ops_settlement_diagnostics.txt
"""
from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.operator_truth import status_truth_lines


def fmt_ts(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "?"


def fmt_duration(seconds: float) -> str:
    if seconds < 0:
        return f"-{fmt_duration(-seconds)}"
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours/24:.1f}d"


def load_events_chunked(runtime_dir: Path, event_types: set[str]) -> list[dict]:
    """Load only events matching the given types from events.jsonl."""
    events = []
    path = runtime_dir / "events.jsonl"
    if not path.exists():
        return events
    with open(path) as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                evt = json.loads(stripped)
                if evt.get("event_type", "") in event_types:
                    events.append(evt)
            except json.JSONDecodeError:
                continue
    # Also check for settled events in events.jsonl
    return events


def count_event_types_in_ledger(runtime_dir: Path) -> dict[str, int]:
    """Get event type counts from ledger.db without loading all events."""
    try:
        import sqlite3
        db_path = runtime_dir / "ledger.db"
        if not db_path.exists():
            return {}
        conn = sqlite3.connect(str(db_path))
        cur = conn.cursor()
        cur.execute("SELECT event_type, COUNT(*) as cnt FROM ledger_events GROUP BY event_type ORDER BY cnt DESC")
        result = dict(cur.fetchall())
        conn.close()
        return result
    except Exception:
        return {}


def render_settlement_diagnostics(runtime_dir: Path, now_ts: float | None = None) -> str:
    now_ts = now_ts or time.time()

    ledger_counts = count_event_types_in_ledger(runtime_dir)
    settled_count = ledger_counts.get("slot_settled", 0)
    pending_count = ledger_counts.get("slot_resolution_pending", 0)
    total_events = sum(ledger_counts.values()) if ledger_counts else 0

    lifecycle_events = load_events_chunked(runtime_dir, {
        "market.pending_resolution",
        "market.settled",
        "slot_settled",
    })
    jsonl_counts = defaultdict(int)

    slot_lifecycles = defaultdict(list)
    for evt in lifecycle_events:
        event_type = evt.get("event_type", "")
        jsonl_counts[event_type] += 1
        payload = evt.get("payload", {})
        slot_id = payload.get("slot_id", evt.get("aggregate_id", "unknown"))
        ts = evt.get("ts", 0)
        slot_lifecycles[slot_id].append({
            "event_type": event_type,
            "ts": ts,
            "next_poll_ts": payload.get("next_poll_ts", 0),
            "delay_seconds": payload.get("delay_seconds", 0),
            "deferred": payload.get("deferred", False),
        })

    unique_slots = len(slot_lifecycles)
    deferred_slots = sum(
        1 for events in slot_lifecycles.values()
        if any(e["deferred"] for e in events)
    )

    max_delays = defaultdict(float)
    for events in slot_lifecycles.values():
        for e in events:
            max_delays[e["event_type"]] = max(max_delays.get(e["event_type"], 0), e["delay_seconds"])

    run_ids = {evt.get("run_id") for evt in lifecycle_events if evt.get("run_id")}

    status_path = runtime_dir / "status.json"
    status = {}
    if status_path.exists():
        try:
            with open(status_path) as f:
                status = json.load(f)
        except Exception:
            pass

    pending_from_status = status.get("pending_resolution_slots", [])

    lines = []
    lines.append("=" * 72)
    lines.append("  SETTLEMENT DIAGNOSTICS SURFACE")
    lines.append("=" * 72)
    lines.append("")
    lines.append("Report scope: all-run settlement lifecycle evidence + current pending-status snapshot")
    lines.extend(status_truth_lines(runtime_dir, generated_at_ts=now_ts))
    lines.append("")
    lines.append(f"Active status run: {status.get('run_id', 'unknown')}")
    lines.append(f"Report time:  {fmt_ts(now_ts)}")
    lines.append(f"Total events: {total_events:,}")
    lines.append("")

    lines.append("--- SETTLEMENT EVENT COUNTS (all runs in ledger.db/events.jsonl) ---")
    lines.append(f"  slot_resolution_pending (ledger.db):   {pending_count:>10,}")
    lines.append(f"  slot_settled (ledger.db):              {settled_count:>10,}")
    lines.append(f"  market.pending_resolution (events):    {jsonl_counts['market.pending_resolution']:>10,}")
    lines.append(f"  market.settled (events):               {jsonl_counts['market.settled']:>10,}")
    lines.append(f"  slot_settled (events):                 {jsonl_counts['slot_settled']:>10,}")
    lines.append("")

    lines.append("--- ARTIFACT FINDINGS ---")
    if settled_count == 0 and jsonl_counts["market.settled"] == 0 and jsonl_counts["slot_settled"] == 0:
        lines.append("  No settlement events were found in the checked runtime artifacts.")
    else:
        lines.append("  Settlement events are present in the checked runtime artifacts.")
    lines.append(f"  Unique slots represented in loaded settlement events: {unique_slots}")
    lines.append(f"  Slots with any deferred=true event payload:          {deferred_slots}")
    lines.append(f"  Distinct run_ids represented in loaded events:       {len(run_ids)}")
    lines.append("  pending_resolution_slots in status.json is a current snapshot,")
    lines.append("  not a proof that every listed slot will settle later.")
    if len(run_ids) > 10:
        lines.append(f"  Note: loaded settlement events span {len(run_ids)} run_ids.")
    lines.append("")

    lines.append("--- CURRENT PENDING RESOLUTION SLOTS ---")
    if pending_from_status:
        for ps in pending_from_status:
            sid = ps.get("slot_id", "?")
            slug = ps.get("market_slug", "?")
            next_poll = ps.get("next_poll_ts", 0)
            deferred = ps.get("deferred", False)
            lines.append(f"  {sid}: {slug}")
            lines.append(f"    next_poll={fmt_ts(next_poll)}  deferred={deferred}")
    else:
        lines.append("  (none in current status.json)")
    lines.append("")

    if max_delays:
        lines.append("--- POLL DELAY STATISTICS ---")
        for et, delay in sorted(max_delays.items()):
            lines.append(f"  {et}: max poll delay = {delay:.1f}s")
        lines.append("")
        lines.append("  Config: resolution_initial_poll_seconds: 10")
        lines.append("          resolution_poll_cap_seconds: 300")

    lines.append("")
    lines.append("--- INTERPRETATION NOTES ---")
    lines.append("  - ledger.db counts are all-run event totals currently present in ledger.db.")
    lines.append("  - events.jsonl counts come only from events.jsonl lines currently present on disk.")
    lines.append("  - status.json pending slots describe the latest snapshot only.")
    lines.append("  - This report does not perform live API or curl verification.")

    lines.append("")
    lines.append("=" * 72)
    lines.append("  Note: This surface is generated from runtime artifacts only.")
    lines.append("=" * 72)

    return "\n".join(lines)


def main():
    write_mode = "--write" in sys.argv
    runtime_dir = REPO / "data" / "runtime"

    report = render_settlement_diagnostics(runtime_dir)
    print(report)

    if write_mode:
        output = runtime_dir / "ops_settlement_diagnostics.txt"
        output.write_text(report + "\n")
        print(f"\n  [written] {output}")


if __name__ == "__main__":
    main()
