#!/usr/bin/env python3
"""
EVIDENCE surface.

Replays the ledger and produces a strategy-family performance report backed
exclusively by runtime artifacts (ledger.db + strategy_metrics.json).

Two data sources:
  1. strategy_metrics.json -- authoritative, updated every cycle
  2. ops_status.json (status.json) -- supplemental run-scoped metrics

Usage:
    python scripts/ops_evidence.py              -- stdout
    python scripts/ops_evidence.py --write      -- also writes data/runtime/ops_evidence.txt
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))


def fmt_ts(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "?"


def load_json_safe(path: Path):
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def get_ledger_event_counts(runtime_dir: Path) -> dict[str, int]:
    try:
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


def render_evidence(runtime_dir: Path) -> str:
    now_ts = time.time()

    metrics = load_json_safe(runtime_dir / "strategy_metrics.json")
    status = load_json_safe(runtime_dir / "status.json")
    ledger_counts = get_ledger_event_counts(runtime_dir)

    run_id = status.get("run_id", "unknown")
    mode = status.get("mode", "?")
    bankroll = status.get("bankroll", 0.0)
    family = status.get("baseline_strategy", "?")

    total_events = sum(ledger_counts.values()) if ledger_counts else 0

    lines = []
    lines.append("=" * 72)
    lines.append("  EVIDENCE SURFACE -- Strategy Performance from Runtime Artifacts")
    lines.append("=" * 72)
    lines.append("")
    lines.append(f"Run:          {run_id}")
    lines.append(f"Mode:         {mode}")
    lines.append(f"Bankroll:     ${bankroll:,.2f}")
    lines.append(f"Report time:  {fmt_ts(now_ts)}")
    lines.append(f"Ledger events: {total_events:,}")
    lines.append("")

    # Ledger event breakdown
    if ledger_counts:
        lines.append("--- LEDGER EVENT COUNTS (from ledger.db) ---")
        for et, cnt in sorted(ledger_counts.items(), key=lambda x: -x[1])[:15]:
            lines.append(f"  {et:40s} {cnt:>10,}")
        if len(ledger_counts) > 15:
            lines.append(f"  ... and {len(ledger_counts) - 15} more types")
        lines.append("")

    # Family metrics
    lines.append("--- FAMILY METRICS (from strategy_metrics.json) ---")
    if metrics:
        for fam, m in sorted(metrics.items()):
            if isinstance(m, dict):
                lines.append("")
                lines.append(f"[{fam}]")
                for k, v in sorted(m.items()):
                    if isinstance(v, float):
                        lines.append(f"  {k:30s} ${v:+,.6f}")
                    else:
                        lines.append(f"  {k:30s} {v:,}")

                # Derived metrics
                orders = m.get("orders_filled", 0)
                quotes = m.get("quotes_submitted", 0)
                if quotes > 0:
                    fill_rate = orders / quotes * 100
                    lines.append(f"  {'fill_rate':30s} {fill_rate:.1f}%")
    else:
        lines.append("  (no metrics yet)")

    lines.append("")

    # Run-scoped snapshot
    run_metrics = status.get("strategy_metrics", {})
    if run_metrics:
        lines.append("--- RUN-SCOPED METRICS (from status.json) ---")
        for fam, m in sorted(run_metrics.items()):
            if isinstance(m, dict):
                lines.append(f"[{fam}]")
                for k, v in sorted(m.items()):
                    if isinstance(v, float):
                        lines.append(f"  {k:30s} ${v:+,.6f}")
                    else:
                        lines.append(f"  {k:30s} {v:,}")
                lines.append("")

    lines.append("=" * 72)
    lines.append("  Note: Fill PnL differs from settlement PnL.")
    lines.append("  Fill PnL = realized through market-making fills")
    lines.append("  Settlement PnL = realized through Gamma outcome resolution")
    lines.append("=" * 72)

    return "\n".join(lines)


def main():
    write_mode = "--write" in sys.argv
    runtime_dir = REPO / "data" / "runtime"

    report = render_evidence(runtime_dir)
    print(report)

    if write_mode:
        output = runtime_dir / "ops_evidence.txt"
        output.write_text(report + "\n")
        print(f"\n  [written] {output}")


if __name__ == "__main__":
    main()
