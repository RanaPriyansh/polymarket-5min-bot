#!/usr/bin/env python3
from __future__ import annotations

import bisect
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.operator_truth import artifact_truth_lines

RUNTIME = REPO / "data" / "runtime"
DB = RUNTIME / "ledger.db"
STATUS = RUNTIME / "status.json"
EVENTS = RUNTIME / "events.jsonl"
SAMPLES = RUNTIME / "market_samples.jsonl"
OUTPUT = RUNTIME / "fill_markout_audit_latest.md"

HORIZONS = [60, 300]


def fmt_ts(ts: float | None) -> str:
    if not ts:
        return "n/a"
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def load_status() -> dict:
    return json.loads(STATUS.read_text()) if STATUS.exists() else {}


def load_fills(run_id: str) -> list[dict]:
    if not DB.exists() or not run_id:
        return []
    conn = sqlite3.connect(str(DB))
    cur = conn.cursor()
    cur.execute(
        """
        SELECT event_ts, payload_json
        FROM ledger_events
        WHERE event_type='fill_applied' AND run_id=?
        ORDER BY event_ts
        """,
        (run_id,),
    )
    rows = cur.fetchall()
    conn.close()
    fills = []
    for event_ts, payload_json in rows:
        payload = json.loads(payload_json)
        fills.append(
            {
                "ts": float(event_ts),
                "market_id": str(payload.get("market_id", "")),
                "outcome": payload.get("outcome") or "Up",
                "side": (payload.get("side") or "BUY").upper(),
                "fill_price": float(payload.get("fill_price", 0.0) or 0.0),
                "fill_size": float(payload.get("fill_size", 0.0) or 0.0),
                "slot_id": payload.get("slot_id", "?"),
                "order_id": payload.get("order_id", "?"),
            }
        )
    return fills


def load_samples(run_id: str) -> dict[tuple[str, str], list[tuple[float, float]]]:
    samples: dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)
    seen: dict[tuple[str, str], set[tuple[float, float]]] = defaultdict(set)
    if not run_id:
        return samples

    def add_sample(market_id: str, outcome: str, ts: float, mid_price: float) -> None:
        key = (market_id, outcome)
        point = (float(ts), float(mid_price))
        if point in seen[key]:
            return
        seen[key].add(point)
        samples[key].append(point)

    if EVENTS.exists():
        with EVENTS.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if evt.get("run_id") != run_id or evt.get("event_type") != "quote.submitted":
                    continue
                payload = evt.get("payload", {})
                book_quality = payload.get("book_quality", {})
                market_id = str(payload.get("market_id", ""))
                outcome = book_quality.get("outcome") or payload.get("outcome") or "Up"
                mid_price = book_quality.get("mid_price")
                ts = evt.get("ts")
                if market_id and ts is not None and mid_price is not None:
                    add_sample(market_id, outcome, float(ts), float(mid_price))

    if SAMPLES.exists():
        with SAMPLES.open() as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("run_id") != run_id:
                    continue
                market_id = str(payload.get("market_id", ""))
                outcome = payload.get("outcome") or "Up"
                mid_price = payload.get("mid_price")
                ts = payload.get("ts")
                if market_id and ts is not None and mid_price is not None:
                    add_sample(market_id, outcome, float(ts), float(mid_price))

    for key in samples:
        samples[key].sort()
    return samples


def future_mid(sample_series: list[tuple[float, float]], target_ts: float) -> tuple[float | None, float | None]:
    timestamps = [ts for ts, _ in sample_series]
    idx = bisect.bisect_left(timestamps, target_ts)
    if idx >= len(sample_series):
        return None, None
    ts, mid = sample_series[idx]
    return ts, mid


def main() -> None:
    status = load_status()
    run_id = status.get("run_id", "")
    fills = load_fills(run_id)
    samples = load_samples(run_id)
    generated_at_ts = datetime.now(tz=timezone.utc).timestamp()

    lines = [
        f"# Fill Markout Audit — current run",
        "",
        *artifact_truth_lines(RUNTIME, artifact_run_id=run_id, generated_at_ts=generated_at_ts, markdown=True),
        f"- fill_count: `{len(fills)}`",
        f"- sample_series: `{sum(len(v) for v in samples.values())}`",
        "",
    ]

    if not fills:
        lines += ["No current-run fills yet. Audit loop is armed but waiting for executions."]
        OUTPUT.write_text("\n".join(lines) + "\n")
        print(OUTPUT)
        return

    summary: dict[int, list[float]] = {h: [] for h in HORIZONS}
    examples = []
    for fill in fills:
        series = samples.get((fill["market_id"], fill["outcome"]), [])
        signed = 1.0 if fill["side"] == "BUY" else -1.0
        for horizon in HORIZONS:
            obs_ts, obs_mid = future_mid(series, fill["ts"] + horizon)
            if obs_ts is None:
                continue
            markout = signed * (obs_mid - fill["fill_price"])
            summary[horizon].append(markout)
            if horizon == 60:
                examples.append(
                    {
                        "slot_id": fill["slot_id"],
                        "side": fill["side"],
                        "fill_price": fill["fill_price"],
                        "future_mid": obs_mid,
                        "markout_60s": markout,
                        "obs_ts": obs_ts,
                    }
                )

    lines += ["## Aggregate markout", ""]
    any_data = False
    for horizon in HORIZONS:
        vals = summary[horizon]
        if not vals:
            lines.append(f"- {horizon}s: no forward mids available yet")
            continue
        any_data = True
        avg = sum(vals) / len(vals)
        win = sum(1 for v in vals if v > 0)
        lines.append(
            f"- {horizon}s: n=`{len(vals)}` avg_markout=`{avg:+.5f}` positive_share=`{win / len(vals):.1%}`"
        )
    if not any_data:
        lines += ["Forward market samples are not available yet for current-run fills."]

    examples.sort(key=lambda row: row["markout_60s"])
    lines += ["", "## Most adverse 60s markouts", ""]
    for row in examples[:5]:
        lines.append(
            "- `{slot}` {side} fill=`{fill:.4f}` future_mid=`{mid:.4f}` markout_60s=`{mark:+.5f}` observed_at=`{obs}`".format(
                slot=row["slot_id"],
                side=row["side"],
                fill=row["fill_price"],
                mid=row["future_mid"],
                mark=row["markout_60s"],
                obs=fmt_ts(row["obs_ts"]),
            )
        )

    lines += [
        "",
        "## Verdict",
        "",
        "- This is an execution-to-forward-mid sanity check, not a full alpha proof.",
        "- Negative average markout means the bot is getting run over after fills.",
        "- Positive average markout means the fill side is surviving immediate adverse selection.",
    ]
    OUTPUT.write_text("\n".join(lines) + "\n")
    print(OUTPUT)


if __name__ == "__main__":
    main()
