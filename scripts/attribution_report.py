#!/usr/bin/env python3
"""
ATTRIBUTION REPORT

Queries ledger.db for slot_settled events and fill_applied events and produces
three analysis reports:

  3A  PnL by Asset / Interval / Outcome
  3C  TTE Bucket Analysis
  3F/3G  Loss / Win Cluster Detection

Sources:
  data/runtime/ledger.db    -- slot_settled + fill_applied events
  data/runtime/status.json  -- for current run_id

Output:
  data/runtime/attribution_report_latest.txt  (human-readable)
  data/runtime/attribution_report_latest.json (machine-readable)

Usage:
  python scripts/attribution_report.py            # stdout + write files
  python scripts/attribution_report.py --no-write # stdout only
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

RUNTIME_DIR = REPO / "data" / "runtime"
DB = RUNTIME_DIR / "ledger.db"
STATUS = RUNTIME_DIR / "status.json"
OUT_TXT = RUNTIME_DIR / "attribution_report_latest.txt"
OUT_JSON = RUNTIME_DIR / "attribution_report_latest.json"

# Minimum settlements needed before results are considered reliable
RELIABLE_THRESHOLD = 20
# Minimum for showing data at all
DATA_THRESHOLD = 5
# Cluster: minimum streak length to be reported
CLUSTER_MIN_LEN = 3
# Cluster: max gap in seconds for consecutive events to belong to same cluster
CLUSTER_MAX_WINDOW = 3600  # 60 minutes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fmt_ts(ts: float | None) -> str:
    if ts is None:
        return "n/a"
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return "?"


def fmt_ts_full(ts: float | None) -> str:
    if ts is None:
        return "n/a"
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return "?"


def load_status() -> dict:
    if STATUS.exists():
        try:
            return json.loads(STATUS.read_text())
        except Exception:
            pass
    return {}


def parse_slug(market_slug: str) -> tuple[str, str]:
    """
    Extract (asset, interval_minutes) from a market_slug.
    e.g. 'btc-updown-5m-1775884500' -> ('btc', '5')
         'eth-updown-15m-1775884500' -> ('eth', '15')
    """
    if not market_slug:
        return "unknown", "?"
    parts = market_slug.split("-")
    asset = parts[0] if parts else "unknown"
    interval = "?"
    # Find the part that ends with 'm' and is numeric before the 'm'
    for part in parts:
        if part.endswith("m") and part[:-1].isdigit():
            interval = part[:-1]
            break
    return asset, interval


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_settlements() -> list[dict]:
    """Load all slot_settled events that have realized_pnl from ledger.db."""
    if not DB.exists():
        return []
    rows = []
    try:
        conn = sqlite3.connect(str(DB))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                event_ts,
                aggregate_id,
                json_extract(payload_json, '$.market_slug')        AS market_slug,
                json_extract(payload_json, '$.position_outcome')   AS position_outcome,
                json_extract(payload_json, '$.winning_outcome')    AS winning_outcome,
                json_extract(payload_json, '$.realized_pnl')       AS realized_pnl,
                json_extract(payload_json, '$.is_win')             AS is_win,
                json_extract(payload_json, '$.settled_ts')         AS settled_ts,
                json_extract(payload_json, '$.entry_price')        AS entry_price,
                json_extract(payload_json, '$.position_size')      AS position_size
            FROM ledger_events
            WHERE event_type = 'slot_settled'
              AND json_extract(payload_json, '$.realized_pnl') IS NOT NULL
            ORDER BY json_extract(payload_json, '$.settled_ts') ASC
            """
        )
        for row in cur.fetchall():
            (event_ts, aggregate_id, market_slug, position_outcome,
             winning_outcome, realized_pnl, is_win, settled_ts,
             entry_price, position_size) = row

            asset, interval = parse_slug(market_slug or "")
            rows.append({
                "event_ts": float(event_ts) if event_ts else 0.0,
                "aggregate_id": aggregate_id or "",
                "market_slug": market_slug or "",
                "asset": asset,
                "interval": interval,
                "position_outcome": position_outcome or "?",
                "winning_outcome": winning_outcome or "?",
                "realized_pnl": float(realized_pnl) if realized_pnl is not None else 0.0,
                "is_win": int(is_win) if is_win is not None else 0,
                "settled_ts": float(settled_ts) if settled_ts else float(event_ts) if event_ts else 0.0,
                "entry_price": float(entry_price) if entry_price is not None else None,
                "position_size": float(position_size) if position_size is not None else None,
            })
        conn.close()
    except Exception as exc:
        sys.stderr.write(f"[attribution] load_settlements error: {exc}\n")
    return rows


def load_total_settlement_count() -> int:
    """Count all slot_settled events regardless of pnl field."""
    if not DB.exists():
        return 0
    try:
        conn = sqlite3.connect(str(DB))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM ledger_events WHERE event_type='slot_settled'")
        n = cur.fetchone()[0]
        conn.close()
        return n
    except Exception:
        return 0


def load_fills() -> list[dict]:
    """Load fill_applied events that have tte_bucket from ledger.db."""
    if not DB.exists():
        return []
    rows = []
    try:
        conn = sqlite3.connect(str(DB))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                event_ts,
                json_extract(payload_json, '$.tte_bucket')             AS tte_bucket,
                json_extract(payload_json, '$.fill_price')             AS fill_price,
                json_extract(payload_json, '$.fill_size')              AS fill_size,
                json_extract(payload_json, '$.side')                   AS side,
                json_extract(payload_json, '$.outcome')                AS outcome,
                json_extract(payload_json, '$.time_to_expiry_seconds') AS tte_seconds,
                json_extract(payload_json, '$.strategy_family')        AS strategy_family,
                json_extract(payload_json, '$.slot_id')                AS slot_id
            FROM ledger_events
            WHERE event_type = 'fill_applied'
              AND json_extract(payload_json, '$.tte_bucket') IS NOT NULL
            ORDER BY event_ts ASC
            """
        )
        for row in cur.fetchall():
            (event_ts, tte_bucket, fill_price, fill_size, side,
             outcome, tte_seconds, strategy_family, slot_id) = row
            rows.append({
                "event_ts": float(event_ts) if event_ts else 0.0,
                "tte_bucket": tte_bucket or "?",
                "fill_price": float(fill_price) if fill_price is not None else 0.0,
                "fill_size": float(fill_size) if fill_size is not None else 0.0,
                "side": (side or "?").upper(),
                "outcome": outcome or "?",
                "tte_seconds": float(tte_seconds) if tte_seconds is not None else None,
                "strategy_family": strategy_family or "?",
                "slot_id": slot_id or "?",
            })
        conn.close()
    except Exception as exc:
        sys.stderr.write(f"[attribution] load_fills error: {exc}\n")
    return rows


# ---------------------------------------------------------------------------
# Report 3A: PnL by Asset / Interval / Outcome
# ---------------------------------------------------------------------------

def build_3a(settlements: list[dict]) -> dict:
    """Aggregate PnL by (asset, interval, position_outcome)."""
    # group_key -> {trade_count, win_count, loss_count, total_pnl, pnl_list}
    groups: dict[tuple, dict] = defaultdict(lambda: {
        "trade_count": 0,
        "win_count": 0,
        "loss_count": 0,
        "total_pnl": 0.0,
        "pnl_list": [],
    })

    for s in settlements:
        key = (s["asset"], s["interval"], s["position_outcome"])
        g = groups[key]
        g["trade_count"] += 1
        g["total_pnl"] += s["realized_pnl"]
        g["pnl_list"].append(s["realized_pnl"])
        if s["is_win"]:
            g["win_count"] += 1
        else:
            g["loss_count"] += 1

    rows = []
    for key in sorted(groups.keys()):
        asset, interval, outcome = key
        g = groups[key]
        tc = g["trade_count"]
        wc = g["win_count"]
        lc = g["loss_count"]
        wr = wc / tc if tc > 0 else 0.0
        tpnl = g["total_pnl"]
        avgpnl = tpnl / tc if tc > 0 else 0.0
        rows.append({
            "asset": asset,
            "interval": interval,
            "outcome": outcome,
            "trade_count": tc,
            "win_count": wc,
            "loss_count": lc,
            "win_rate": round(wr, 4),
            "total_pnl": round(tpnl, 4),
            "avg_pnl": round(avgpnl, 4),
        })

    # Grand totals
    grand = {
        "trade_count": len(settlements),
        "win_count": sum(1 for s in settlements if s["is_win"]),
        "loss_count": sum(1 for s in settlements if not s["is_win"]),
        "total_pnl": round(sum(s["realized_pnl"] for s in settlements), 4),
    }
    grand["win_rate"] = round(
        grand["win_count"] / grand["trade_count"] if grand["trade_count"] > 0 else 0.0, 4
    )
    grand["avg_pnl"] = round(
        grand["total_pnl"] / grand["trade_count"] if grand["trade_count"] > 0 else 0.0, 4
    )

    return {"rows": rows, "grand": grand}


def render_3a_txt(data: dict, n_pnl: int) -> list[str]:
    lines = []
    lines.append("3A. PnL BY ASSET / INTERVAL / OUTCOME")
    lines.append("-" * 72)

    if n_pnl < DATA_THRESHOLD:
        lines.append(f"  INSUFFICIENT DATA -- only {n_pnl} settlement(s) with PnL found.")
        lines.append(f"  Need at least {DATA_THRESHOLD} to show breakdown, {RELIABLE_THRESHOLD} for reliable stats.")
        lines.append("")
        return lines

    if n_pnl < RELIABLE_THRESHOLD:
        lines.append(
            f"  WARNING: Only {n_pnl} settlements with PnL. "
            f"Need {RELIABLE_THRESHOLD} for reliable report. Showing preliminary data."
        )

    # Header
    hdr = (
        f"  {'asset':<6} {'intv':>5} {'outcome':<8} "
        f"{'trades':>6} {'wins':>5} {'losses':>6} "
        f"{'win_rate':>9} {'total_pnl':>10} {'avg_pnl':>10}"
    )
    lines.append(hdr)
    lines.append("  " + "-" * 68)

    for r in data["rows"]:
        wr_str = f"{r['win_rate']*100:.1f}%"
        lines.append(
            f"  {r['asset']:<6} {r['interval']:>5} {r['outcome']:<8} "
            f"{r['trade_count']:>6} {r['win_count']:>5} {r['loss_count']:>6} "
            f"{wr_str:>9} {r['total_pnl']:>+10.4f} {r['avg_pnl']:>+10.4f}"
        )

    # Grand total separator
    lines.append("  " + "-" * 68)
    g = data["grand"]
    wr_str = f"{g['win_rate']*100:.1f}%"
    lines.append(
        f"  {'TOTAL':<6} {'--':>5} {'--':<8} "
        f"{g['trade_count']:>6} {g['win_count']:>5} {g['loss_count']:>6} "
        f"{wr_str:>9} {g['total_pnl']:>+10.4f} {g['avg_pnl']:>+10.4f}"
    )
    lines.append("")
    return lines


# ---------------------------------------------------------------------------
# Report 3C: TTE Bucket Analysis
# ---------------------------------------------------------------------------

# Canonical bucket ordering
TTE_BUCKET_ORDER = ["<60s", "60-120s", "120-300s", ">300s"]


def build_3c(fills: list[dict]) -> dict:
    """Aggregate fill statistics by TTE bucket."""
    groups: dict[str, dict] = defaultdict(lambda: {
        "fill_count": 0,
        "price_sum": 0.0,
        "size_sum": 0.0,
        "buy_count": 0,
    })

    for f in fills:
        bucket = f["tte_bucket"]
        g = groups[bucket]
        g["fill_count"] += 1
        g["price_sum"] += f["fill_price"]
        g["size_sum"] += f["fill_size"]
        if f["side"] == "BUY":
            g["buy_count"] += 1

    rows = []
    # Show in canonical order; unknown buckets go at end
    known = [b for b in TTE_BUCKET_ORDER if b in groups]
    unknown = [b for b in sorted(groups.keys()) if b not in TTE_BUCKET_ORDER]
    for bucket in known + unknown:
        g = groups[bucket]
        fc = g["fill_count"]
        avg_price = g["price_sum"] / fc if fc > 0 else 0.0
        avg_size = g["size_sum"] / fc if fc > 0 else 0.0
        buy_pct = g["buy_count"] / fc if fc > 0 else 0.0
        rows.append({
            "bucket": bucket,
            "fill_count": fc,
            "avg_fill_price": round(avg_price, 4),
            "avg_fill_size": round(avg_size, 4),
            "buy_pct": round(buy_pct, 4),
        })

    total_fills = sum(g["fill_count"] for g in groups.values())
    return {"rows": rows, "total_fills": total_fills}


def render_3c_txt(data: dict) -> list[str]:
    lines = []
    lines.append("3C. TTE BUCKET ANALYSIS")
    lines.append("-" * 72)

    total = data["total_fills"]
    if total == 0:
        lines.append("  No fill_applied events with tte_bucket found.")
        lines.append("")
        return lines

    lines.append(f"  Total fills with TTE data: {total:,}")
    lines.append("")
    hdr = (
        f"  {'bucket':<12} {'fills':>7} {'avg_price':>10} "
        f"{'avg_size':>10} {'buy_pct':>9}"
    )
    lines.append(hdr)
    lines.append("  " + "-" * 52)

    for r in data["rows"]:
        bp_str = f"{r['buy_pct']*100:.0f}%"
        lines.append(
            f"  {r['bucket']:<12} {r['fill_count']:>7} {r['avg_fill_price']:>10.4f} "
            f"{r['avg_fill_size']:>10.4f} {bp_str:>9}"
        )

    lines.append("")
    return lines


# ---------------------------------------------------------------------------
# Report 3F/3G: Loss / Win Cluster Detection
# ---------------------------------------------------------------------------

def build_clusters(settlements: list[dict]) -> dict:
    """
    Detect loss and win clusters from ordered settlement events.

    A cluster is 3+ consecutive settled events (same win/loss outcome)
    where consecutive events are within 60 minutes of each other.
    """
    if not settlements:
        return {"loss_clusters": [], "win_clusters": []}

    loss_clusters: list[dict] = []
    win_clusters: list[dict] = []

    # settlements are already ordered by settled_ts ASC
    # We scan with a running streak tracker
    streak_is_win: int | None = None
    streak_events: list[dict] = []

    def finalize_streak(events: list[dict], is_win_val: int) -> None:
        if len(events) < CLUSTER_MIN_LEN:
            return
        assets = sorted(set(e["asset"] for e in events))
        avg_pnl = sum(e["realized_pnl"] for e in events) / len(events)
        cluster = {
            "type": "win" if is_win_val else "loss",
            "length": len(events),
            "start_ts": events[0]["settled_ts"],
            "end_ts": events[-1]["settled_ts"],
            "assets": assets,
            "avg_pnl": round(avg_pnl, 4),
            "total_pnl": round(sum(e["realized_pnl"] for e in events), 4),
        }
        if is_win_val:
            win_clusters.append(cluster)
        else:
            loss_clusters.append(cluster)

    for event in settlements:
        curr_is_win = event["is_win"]
        curr_ts = event["settled_ts"]

        if streak_is_win is None:
            # Start first streak
            streak_is_win = curr_is_win
            streak_events = [event]
            continue

        # Check if this event continues the current streak
        prev_ts = streak_events[-1]["settled_ts"]
        time_gap = curr_ts - prev_ts
        same_outcome = (curr_is_win == streak_is_win)
        within_window = (time_gap <= CLUSTER_MAX_WINDOW)

        if same_outcome and within_window:
            streak_events.append(event)
        else:
            # Streak broken — finalize and start new
            finalize_streak(streak_events, streak_is_win)
            streak_is_win = curr_is_win
            streak_events = [event]

    # Finalize last streak
    if streak_events:
        finalize_streak(streak_events, streak_is_win)

    return {"loss_clusters": loss_clusters, "win_clusters": win_clusters}


def render_cluster_txt(data: dict) -> list[str]:
    lines = []
    lines.append("3F/3G. LOSS / WIN CLUSTERS")
    lines.append("-" * 72)
    lines.append(
        f"  (cluster = {CLUSTER_MIN_LEN}+ consecutive same-outcome settlements "
        f"within {CLUSTER_MAX_WINDOW//60}min)"
    )
    lines.append("")

    loss_clusters = data["loss_clusters"]
    win_clusters = data["win_clusters"]

    lines.append(f"  Loss clusters found: {len(loss_clusters)}")
    if loss_clusters:
        for i, c in enumerate(loss_clusters, 1):
            assets_str = ",".join(c["assets"])
            lines.append(
                f"    [{i}] {c['length']} consecutive losses "
                f"from {fmt_ts(c['start_ts'])} to {fmt_ts(c['end_ts'])} UTC "
                f"| assets: {assets_str} "
                f"| avg_pnl: {c['avg_pnl']:+.4f} "
                f"| total_pnl: {c['total_pnl']:+.4f}"
            )
    else:
        lines.append("    (none)")

    lines.append("")
    lines.append(f"  Win clusters found: {len(win_clusters)}")
    if win_clusters:
        for i, c in enumerate(win_clusters, 1):
            assets_str = ",".join(c["assets"])
            lines.append(
                f"    [{i}] {c['length']} consecutive wins "
                f"from {fmt_ts(c['start_ts'])} to {fmt_ts(c['end_ts'])} UTC "
                f"| assets: {assets_str} "
                f"| avg_pnl: {c['avg_pnl']:+.4f} "
                f"| total_pnl: {c['total_pnl']:+.4f}"
            )
    else:
        lines.append("    (none)")

    lines.append("")
    return lines


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_report() -> tuple[str, dict]:
    """Build the full attribution report. Returns (txt_report, json_data)."""
    now_ts = time.time()
    generated_at = fmt_ts_full(now_ts)

    status = load_status()
    run_id = status.get("run_id", "unknown")

    settlements = load_settlements()
    fills = load_fills()
    total_settlements = load_total_settlement_count()
    n_pnl = len(settlements)

    # Build sub-reports
    data_3a = build_3a(settlements)
    data_3c = build_3c(fills)
    data_clusters = build_clusters(settlements)

    # -----------------------------------------------------------------------
    # Text report
    # -----------------------------------------------------------------------
    SEP = "=" * 72
    lines = []
    lines.append(SEP)
    lines.append("  ATTRIBUTION REPORT")
    lines.append(f"  Generated:           {generated_at}")
    lines.append(f"  Run ID:              {run_id}")
    lines.append(f"  Settlements with PnL: {n_pnl} / {total_settlements} total")
    lines.append(f"  Fill events (TTE):    {data_3c['total_fills']:,} / {len(fills):,} total fills")
    if n_pnl < RELIABLE_THRESHOLD:
        lines.append(
            f"  Data maturity:        PRELIMINARY "
            f"({n_pnl}/{RELIABLE_THRESHOLD} min for reliable stats)"
        )
    else:
        lines.append(f"  Data maturity:        SUFFICIENT ({n_pnl} settlements)")
    lines.append(SEP)
    lines.append("")

    lines.extend(render_3a_txt(data_3a, n_pnl))
    lines.extend(render_3c_txt(data_3c))
    lines.extend(render_cluster_txt(data_clusters))

    lines.append(SEP)
    lines.append("")

    txt = "\n".join(lines)

    # -----------------------------------------------------------------------
    # JSON data
    # -----------------------------------------------------------------------
    json_data = {
        "generated_at": generated_at,
        "generated_ts": round(now_ts, 3),
        "run_id": run_id,
        "settlements_with_pnl": n_pnl,
        "total_settlements": total_settlements,
        "total_fills_with_tte": data_3c["total_fills"],
        "data_maturity": "sufficient" if n_pnl >= RELIABLE_THRESHOLD else "preliminary",
        "report_3a": data_3a,
        "report_3c": data_3c,
        "report_3fg": data_clusters,
    }

    return txt, json_data


def main() -> None:
    no_write = "--no-write" in sys.argv

    txt, json_data = build_report()

    print(txt)

    if not no_write:
        try:
            RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
            OUT_TXT.write_text(txt)
            OUT_JSON.write_text(json.dumps(json_data, indent=2) + "\n")
            print(f"[written] {OUT_TXT}")
            print(f"[written] {OUT_JSON}")
        except Exception as exc:
            sys.stderr.write(f"[attribution] write error: {exc}\n")
            sys.exit(1)


if __name__ == "__main__":
    main()
