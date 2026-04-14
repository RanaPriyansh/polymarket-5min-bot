#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.operator_truth import artifact_truth_lines

RUNTIME = REPO / "data" / "runtime"
DB = RUNTIME / "ledger.db"
STATUS = RUNTIME / "status.json"
OUTPUT = RUNTIME / "settlement_latency_audit_latest.md"


def fmt_ts(ts: float | None) -> str:
    if not ts:
        return "n/a"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = min(len(sorted_vals) - 1, max(0, round((len(sorted_vals) - 1) * p)))
    return sorted_vals[idx]


def slot_end_ts(slot_id: str | None = None, market_slug: str | None = None) -> tuple[float | None, str | None]:
    if slot_id:
        parts = slot_id.split(":")
        if len(parts) == 3:
            try:
                interval = int(parts[1])
                start_ts = int(parts[2])
                return float(start_ts + interval * 60), slot_id
            except ValueError:
                pass
    if market_slug:
        parts = market_slug.split("-")
        if len(parts) >= 4 and parts[-2].endswith("m"):
            try:
                interval = int(parts[-2][:-1])
                start_ts = int(parts[-1])
                asset = parts[0]
                inferred_slot = f"{asset}:{interval}:{start_ts}"
                return float(start_ts + interval * 60), inferred_slot
            except ValueError:
                pass
    return None, slot_id or market_slug


def main() -> None:
    status = json.loads(STATUS.read_text()) if STATUS.exists() else {}
    run_id = status.get("run_id", "")
    generated_at_ts = datetime.now(tz=timezone.utc).timestamp()
    lines = [
        "# Settlement Latency Audit",
        "",
        *artifact_truth_lines(RUNTIME, artifact_run_id=run_id, generated_at_ts=generated_at_ts, markdown=True),
        "",
    ]

    if not DB.exists():
        lines.append("ledger.db not found")
        OUTPUT.write_text("\n".join(lines) + "\n")
        print(OUTPUT)
        return

    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    cur.execute(
        """
        SELECT run_id, event_ts, payload_json
        FROM ledger_events
        WHERE event_type='slot_settled'
        ORDER BY event_ts DESC
        LIMIT 500
        """
    )
    rows = cur.fetchall()
    conn.close()

    recent = []
    current = []
    for row_run_id, event_ts, payload_json in rows:
        payload = json.loads(payload_json)
        end_ts, slot_id = slot_end_ts(payload.get("slot_id"), payload.get("market_slug"))
        if end_ts is None:
            continue
        latency = float(event_ts) - end_ts
        entry = {
            "run_id": row_run_id,
            "slot_id": slot_id,
            "settled_ts": float(event_ts),
            "end_ts": end_ts,
            "latency_sec": latency,
            "winning_outcome": payload.get("winning_outcome", "?"),
        }
        recent.append(entry)
        if row_run_id == run_id:
            current.append(entry)

    def add_section(title: str, entries: list[dict]) -> None:
        lines.extend([f"## {title}", ""])
        if not entries:
            lines.append("No slot_settled events available for this scope yet.")
            lines.append("")
            return
        vals = sorted(e["latency_sec"] for e in entries)
        mean = sum(vals) / len(vals)
        verdict = "healthy" if percentile(vals, 0.5) <= 300 else "late"
        lines.append(f"- count: `{len(vals)}`")
        lines.append(f"- min: `{min(vals):.1f}s`")
        lines.append(f"- p50: `{percentile(vals, 0.5):.1f}s`")
        lines.append(f"- p90: `{percentile(vals, 0.9):.1f}s`")
        lines.append(f"- max: `{max(vals):.1f}s`")
        lines.append(f"- mean: `{mean:.1f}s`")
        lines.append(f"- verdict: `{verdict}`")
        lines.append("")
        lines.append("Latest settlements:")
        for entry in sorted(entries, key=lambda item: item["settled_ts"], reverse=True)[:5]:
            lines.append(
                f"- `{entry['slot_id']}` latency=`{entry['latency_sec']:.1f}s` settled_at=`{fmt_ts(entry['settled_ts'])}` winner=`{entry['winning_outcome']}`"
            )
        lines.append("")

    add_section("Current run", current)
    add_section("Recent all-run sample", recent)

    lines += [
        "## Interpretation",
        "",
        "- Healthy means median settlement is within 5 minutes after scheduled expiry.",
        "- Late means median settlement is still happening but too slowly for trustworthy operator accounting.",
    ]

    OUTPUT.write_text("\n".join(lines) + "\n")
    print(OUTPUT)


if __name__ == "__main__":
    main()
