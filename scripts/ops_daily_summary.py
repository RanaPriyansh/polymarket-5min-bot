#!/usr/bin/env python3
"""
DAILY OPERATOR SUMMARY surface.

Compresses one day of runtime activity into a single operator-readable report.
Backed by ledger artifacts, not opinion.

Usage:
    python scripts/ops_daily_summary.py              -- stdout (current day)
    python scripts/ops_daily_summary.py --date 2026-04-05  -- specific date
    python scripts/ops_daily_summary.py --write      -- also writes data/runtime/ops_daily_summary.txt
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.operator_truth import load_json, status_truth_lines


def fmt_ts(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%H:%M:%S UTC")
    except Exception:
        return "?"


def fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.1f}m"
    hours = minutes / 60
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours/24:.1f}d"


def get_events_for_day(runtime_dir: Path, target_date: str) -> list[dict]:
    """Get events filtered by day from events.jsonl."""
    if not isinstance(target_date, str):
        target_date = target_date.isoformat()

    target_start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    target_end = target_start + 86400

    events = []
    path = runtime_dir / "events.jsonl"
    if not path.exists():
        return events

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
                ts = evt.get("ts", 0)
                if target_start <= ts < target_end:
                    events.append(evt)
            except (json.JSONDecodeError, ValueError):
                continue

    return events


def render_daily_summary(runtime_dir: Path, target_date: str | None = None) -> str:
    if target_date is None:
        now = datetime.now(tz=timezone.utc)
        target_date = now.strftime("%Y-%m-%d")
    else:
        # Validate date format
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            return f"ERROR: Invalid date format '{target_date}'. Use YYYY-MM-DD."

    events = get_events_for_day(runtime_dir, target_date)

    if not events:
        status = load_json(runtime_dir / "status.json", {})
        generated_at_ts = datetime.now(tz=timezone.utc).timestamp()
        lines = [
            "=" * 72,
            "  DAILY OPERATOR SUMMARY",
            "=" * 72,
            "",
            f"Report scope: UTC day {target_date} from events.jsonl/ledger.db + current status snapshot",
            *status_truth_lines(runtime_dir, generated_at_ts=generated_at_ts),
            "",
            f"No events found for {target_date}",
            "  (events.jsonl may have been rotated or the day had no activity)",
            "",
            f"Checking alternative sources...",
            ""
        ]

        # Check if ledger has events for this day
        db_path = runtime_dir / "ledger.db"
        if db_path.exists():
            target_start = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
            target_end = target_start + 86400
            try:
                conn = sqlite3.connect(str(db_path))
                cur = conn.cursor()
                cur.execute(
                    "SELECT event_type, COUNT(*) FROM ledger_events "
                    "WHERE event_ts >= ? AND event_ts < ? GROUP BY event_type ORDER BY COUNT(*) DESC",
                    (target_start, target_end)
                )
                day_events = cur.fetchall()
                conn.close()

                if day_events:
                    total = sum(count for _, count in day_events)
                    lines.append("")
                    lines.append(f"Ledger has events for {target_date}:")
                    lines.append(f"  Total events: {total:,}")
                    for et, count in day_events[:15]:
                        lines.append(f"  {et:40s} {count:>10,}")
                    if len(day_events) > 15:
                        lines.append(f"  ... and {len(day_events) - 15} more types")
            except Exception as e:
                lines.append(f"  Ledger query failed: {e}")

        lines.append("")
        lines.append("=" * 72)
        return "\n".join(lines)

    # Aggregate events
    total_events = len(events)
    event_counts = defaultdict(int)
    family_metrics = defaultdict(lambda: {
        "orders": 0, "fills": 0, "cancels": 0, "pnl": 0.0
    })
    unique_run_ids = set()

    for evt in events:
        et = evt.get("event_type", "unknown")
        event_counts[et] += 1
        payload = evt.get("payload", {})
        family = payload.get("strategy_family", "unknown")
        run_id = evt.get("run_id")

        if run_id:
            unique_run_ids.add(run_id)

        if et in {"quote.submitted", "order_created", "order_acknowledged", "order.accepted"}:
            family_metrics[family]["orders"] += 1
        elif et in {"order.filled", "fill_applied", "fill_observed"}:
            family_metrics[family]["fills"] += 1
        elif et in {"order.cancelled", "order_cancelled"}:
            family_metrics[family]["cancels"] += 1

    first_ts = min(e.get("ts", 0) for e in events)
    last_ts = max(e.get("ts", 0) for e in events)
    span = last_ts - first_ts
    status = load_json(runtime_dir / "status.json", {})
    generated_at_ts = datetime.now(tz=timezone.utc).timestamp()

    lines = []
    lines.append("=" * 72)
    lines.append(f"  DAILY OPERATOR SUMMARY: {target_date}")
    lines.append("=" * 72)
    lines.append("")
    lines.append(f"Report scope: UTC day {target_date} from events.jsonl + current status snapshot")
    lines.extend(status_truth_lines(runtime_dir, generated_at_ts=generated_at_ts))
    lines.append("")
    lines.append(f"Period:       {fmt_ts(first_ts)} to {fmt_ts(last_ts)} UTC")
    lines.append(f"Total events: {total_events:,}")
    lines.append(f"Active span:  {fmt_duration(span)}")
    lines.append(f"Unique run_ids: {len(unique_run_ids)}")
    if len(unique_run_ids) > 1:
        lines.append(f"  WARNING: {len(unique_run_ids)} restarts detected")

    # Event breakdown
    lines.append("")
    lines.append("--- EVENT COUNTS ---")
    for et, cnt in sorted(event_counts.items(), key=lambda x: -x[1]):
        lines.append(f"  {et:40s} {cnt:>10,}")
    lines.append("")

    # Family performance
    lines.append("--- FAMILY METRICS ---")
    for fam, m in sorted(family_metrics.items()):
        if m["orders"] > 0:
            fill_rate = m["fills"] / m["orders"] * 100
            lines.append(f"[{fam}]")
            lines.append(f"  Orders:    {m['orders']:,}")
            lines.append(f"  Fills:     {m['fills']:,}")
            lines.append(f"  Cancels:   {m['cancels']:,}")
            lines.append(f"  Fill rate: {fill_rate:.1f}%")
            lines.append("")

    # Status check
    status_path = runtime_dir / "status.json"
    if status_path.exists():
        try:
            with open(status_path) as f:
                status = json.load(f)
                bankroll = status.get("bankroll", 0.0)
                resolved = status.get("resolved_trade_count", 0)
                win_rate = status.get("win_rate", 0.0)

                lines.append("--- CURRENT STATUS SNAPSHOT ---")
                lines.append(f"Run:          {status.get('run_id', '?')}")
                lines.append(f"Bankroll:     ${bankroll:,.2f}")
                lines.append(f"Resolved:     {resolved}")
                lines.append(f"Win rate:     {win_rate*100:.1f}%")
                lines.append("")
        except Exception:
            pass

    lines.append("=" * 72)
    lines.append("  Note: This report is generated from runtime artifacts only.")
    lines.append("=" * 72)

    return "\n".join(lines)


def main():
    write_mode = "--write" in sys.argv
    runtime_dir = REPO / "data" / "runtime"

    target_date = None
    for i, arg in enumerate(sys.argv):
        if arg == "--date" and i + 1 < len(sys.argv):
            target_date = sys.argv[i + 1]
            break

    report = render_daily_summary(runtime_dir, target_date)
    print(report)

    if write_mode:
        date_label = target_date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        output = runtime_dir / f"ops_daily_summary_{date_label}.txt"
        output.write_text(report + "\n")
        print(f"\n  [written] {output}")


if __name__ == "__main__":
    main()
