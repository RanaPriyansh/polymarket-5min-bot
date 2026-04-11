#!/usr/bin/env python3
"""
FILL MARKOUT REPORT  (spec section 3B)

Computes fill markouts by horizon (30s, 60s, 120s, 300s, final settlement).

A markout answers: after we get filled, does the price move in our favor
or against us?  Negative markout = adverse selection signal.

Sources:
  data/runtime/ledger.db          -- fill_applied + slot_settled events
  data/runtime/events.jsonl       -- quote.submitted events with mid_price
                                     per (market_id, outcome, ts)

Output:
  data/runtime/fill_markout_report_latest.txt  (human-readable)
  data/runtime/fill_markout_report_latest.json (machine-readable)

Usage:
  python scripts/fill_markout_report.py            # write + stdout
  python scripts/fill_markout_report.py --no-write # stdout only
"""
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

RUNTIME_DIR = REPO / "data" / "runtime"
DB = RUNTIME_DIR / "ledger.db"
EVENTS_JSONL = RUNTIME_DIR / "events.jsonl"
OUT_TXT = RUNTIME_DIR / "fill_markout_report_latest.txt"
OUT_JSON = RUNTIME_DIR / "fill_markout_report_latest.json"

# Time horizons in seconds
HORIZONS = [30, 60, 120, 300]

# Canonical TTE bucket ordering
TTE_BUCKET_ORDER = ["<60s", "60-120s", "120-300s", ">300s"]

# Minimum fills for reliable stats  
RELIABLE_MIN = 10
DATA_MIN = 3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fmt_ts(ts: float | None) -> str:
    if ts is None:
        return "n/a"
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        )
    except Exception:
        return "?"


def safe_avg(vals: list[float]) -> float | None:
    return sum(vals) / len(vals) if vals else None


def pct_str(numerator: int, denominator: int, default: str = "n/a") -> str:
    if denominator == 0:
        return default
    return f"{numerator / denominator * 100:.0f}%"


def fmt_float(v: float | None, fmt: str = ".4f", default: str = "n/a") -> str:
    if v is None:
        return default
    return format(v, fmt)


# ---------------------------------------------------------------------------
# Step 1 — Load fills from ledger.db
# ---------------------------------------------------------------------------

def load_fills() -> list[dict]:
    """
    Load all fill_applied events from ledger.db.
    Returns list of dicts with fill metadata.
    """
    if not DB.exists():
        return []
    fills = []
    try:
        conn = sqlite3.connect(str(DB))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                event_ts,
                json_extract(payload_json, '$.fill_price')             AS fill_price,
                json_extract(payload_json, '$.fill_size')              AS fill_size,
                json_extract(payload_json, '$.market_id')              AS market_id,
                json_extract(payload_json, '$.slot_id')                AS slot_id,
                json_extract(payload_json, '$.outcome')                AS outcome,
                json_extract(payload_json, '$.side')                   AS side,
                json_extract(payload_json, '$.tte_bucket')             AS tte_bucket,
                json_extract(payload_json, '$.time_to_expiry_seconds') AS tte_seconds,
                json_extract(payload_json, '$.strategy_family')        AS strategy_family
            FROM ledger_events
            WHERE event_type = 'fill_applied'
            ORDER BY event_ts ASC
            """
        )
        for row in cur.fetchall():
            (event_ts, fill_price, fill_size, market_id, slot_id,
             outcome, side, tte_bucket, tte_seconds, strategy_family) = row
            fills.append({
                "ts": float(event_ts) if event_ts else 0.0,
                "fill_price": float(fill_price) if fill_price is not None else 0.0,
                "fill_size": float(fill_size) if fill_size is not None else 0.0,
                "market_id": str(market_id) if market_id else "",
                "slot_id": str(slot_id) if slot_id else "",
                "outcome": str(outcome) if outcome else "Up",
                "side": (str(side) if side else "BUY").upper(),
                "tte_bucket": str(tte_bucket) if tte_bucket else None,
                "tte_seconds": float(tte_seconds) if tte_seconds is not None else None,
                "strategy_family": str(strategy_family) if strategy_family else "?",
            })
        conn.close()
    except Exception as exc:
        sys.stderr.write(f"[fill_markout] load_fills error: {exc}\n")
    return fills


# ---------------------------------------------------------------------------
# Step 2 — Load settlements from ledger.db
# ---------------------------------------------------------------------------

def load_settlements() -> dict[str, str]:
    """
    Load slot_settled events and return {market_id: winning_outcome}.
    Uses market_id as key since slot_settled events don't include slot_id.
    """
    if not DB.exists():
        return {}
    result: dict[str, str] = {}
    try:
        conn = sqlite3.connect(str(DB))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                json_extract(payload_json, '$.market_id')       AS market_id,
                json_extract(payload_json, '$.winning_outcome') AS winning_outcome
            FROM ledger_events
            WHERE event_type = 'slot_settled'
              AND json_extract(payload_json, '$.winning_outcome') IS NOT NULL
            """
        )
        for market_id, winning_outcome in cur.fetchall():
            if market_id:
                result[str(market_id)] = str(winning_outcome)
        conn.close()
    except Exception as exc:
        sys.stderr.write(f"[fill_markout] load_settlements error: {exc}\n")
    return result


# ---------------------------------------------------------------------------
# Step 3 — Stream quote.submitted events to build mid_price series
# ---------------------------------------------------------------------------

def build_mid_series(
    target_market_ids: set[str],
) -> dict[tuple[str, str], list[tuple[float, float]]]:
    """
    Stream events.jsonl and collect (ts, mid_price) series per (market_id, outcome).

    Only keeps entries whose market_id is in target_market_ids.

    Returns:
        dict[(market_id, outcome)] -> sorted list of (ts, mid_price)
    """
    series: dict[tuple[str, str], list[tuple[float, float]]] = defaultdict(list)

    if not EVENTS_JSONL.exists():
        sys.stderr.write(
            f"[fill_markout] {EVENTS_JSONL} not found — time-horizon markouts unavailable\n"
        )
        return series

    lines_read = 0
    quotes_kept = 0
    try:
        with EVENTS_JSONL.open() as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                lines_read += 1
                try:
                    evt = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                if evt.get("event_type") != "quote.submitted":
                    continue

                payload = evt.get("payload", {})
                bq = payload.get("book_quality", {})
                market_id = str(payload.get("market_id", ""))
                if not market_id or market_id not in target_market_ids:
                    continue

                outcome = str(bq.get("outcome") or "Up")
                mid_price = bq.get("mid_price")
                ts = evt.get("ts")

                if mid_price is None or ts is None:
                    continue

                series[(market_id, outcome)].append((float(ts), float(mid_price)))
                quotes_kept += 1

    except Exception as exc:
        sys.stderr.write(f"[fill_markout] build_mid_series error: {exc}\n")

    # Sort each series by ts
    for key in series:
        series[key].sort()

    sys.stderr.write(
        f"[fill_markout] streamed {lines_read:,} event lines, "
        f"kept {quotes_kept:,} quote.submitted mid_price samples "
        f"for {len(series):,} (market,outcome) pairs\n"
    )
    return series


def lookup_future_mid(
    series: list[tuple[float, float]],
    target_ts: float,
    max_lag: float = 120.0,
) -> float | None:
    """
    Find the nearest mid_price at or after target_ts in the sorted series.

    max_lag: if the nearest sample is more than this many seconds late, return None.
    """
    if not series:
        return None
    timestamps = [t for t, _ in series]
    idx = bisect.bisect_left(timestamps, target_ts)
    if idx >= len(series):
        return None
    obs_ts, mid = series[idx]
    if obs_ts - target_ts > max_lag:
        return None
    return mid


# ---------------------------------------------------------------------------
# Step 4 — Compute markouts
# ---------------------------------------------------------------------------

def compute_markouts(
    fills: list[dict],
    settlements: dict[str, str],
    mid_series: dict[tuple[str, str], list[tuple[float, float]]],
) -> list[dict]:
    """
    For each fill, compute:
      - markout_final: from settlement outcome (1.0 or 0.0 minus fill_price)
      - markout_Ns:    signed (future_mid - fill_price) for N in HORIZONS

    sign convention: positive = fill moved in our favor
      BUY : markout = future_price - fill_price  (we want price to rise)
      SELL: markout = fill_price - future_price  (we want price to fall)
    """
    enriched = []
    for f in fills:
        market_id = f["market_id"]
        outcome = f["outcome"]
        fill_price = f["fill_price"]
        side = f["side"]
        fill_ts = f["ts"]
        signed = 1.0 if side == "BUY" else -1.0

        rec: dict = {**f}

        # --- Final settlement markout ---
        winning_outcome = settlements.get(market_id)
        if winning_outcome is not None:
            settlement_price = 1.0 if outcome == winning_outcome else 0.0
            rec["markout_final"] = signed * (settlement_price - fill_price)
            rec["settlement_price"] = settlement_price
            rec["winning_outcome"] = winning_outcome
        else:
            rec["markout_final"] = None
            rec["settlement_price"] = None
            rec["winning_outcome"] = None

        # --- Time-horizon markouts ---
        series = mid_series.get((market_id, outcome), [])
        for h in HORIZONS:
            target_ts = fill_ts + h
            future_mid = lookup_future_mid(series, target_ts)
            if future_mid is not None:
                rec[f"markout_{h}s"] = signed * (future_mid - fill_price)
            else:
                rec[f"markout_{h}s"] = None

        enriched.append(rec)

    return enriched


# ---------------------------------------------------------------------------
# Step 5 — Aggregate statistics
# ---------------------------------------------------------------------------

def bucket_stats(
    enriched: list[dict],
    group_key_fn,
    bucket_order: list | None = None,
) -> list[dict]:
    """
    Group enriched fills by group_key_fn(fill) and compute aggregate stats.
    """
    groups: dict[str, dict] = {}

    for fill in enriched:
        key = group_key_fn(fill)
        if key not in groups:
            groups[key] = {
                "fills": 0,
                "fill_price_sum": 0.0,
                "final_markouts": [],
                "horizon_markouts": {h: [] for h in HORIZONS},
                "wins_final": 0,
                "losses_final": 0,
            }
        g = groups[key]
        g["fills"] += 1
        g["fill_price_sum"] += fill["fill_price"]

        mo_final = fill.get("markout_final")
        if mo_final is not None:
            g["final_markouts"].append(mo_final)
            if mo_final >= 0:
                g["wins_final"] += 1
            else:
                g["losses_final"] += 1

        for h in HORIZONS:
            mo = fill.get(f"markout_{h}s")
            if mo is not None:
                g["horizon_markouts"][h].append(mo)

    # Convert to sorted list of rows
    rows = []
    for key, g in groups.items():
        fc = g["fills"]
        avg_fp = g["fill_price_sum"] / fc if fc > 0 else None
        fmo = g["final_markouts"]
        avg_final = safe_avg(fmo)
        wins = g["wins_final"]
        losses = g["losses_final"]
        settled = wins + losses
        adverse_pct = losses / settled if settled > 0 else None

        horizon_avgs = {}
        for h in HORIZONS:
            vals = g["horizon_markouts"][h]
            horizon_avgs[h] = safe_avg(vals) if vals else None

        rows.append({
            "bucket": key,
            "fills": fc,
            "avg_fill_price": avg_fp,
            "avg_final_markout": avg_final,
            "fills_settled": settled,
            "wins": wins,
            "losses": losses,
            "adverse_pct": adverse_pct,
            "horizon_avgs": horizon_avgs,
        })

    # Sort by canonical order if provided, then lexicographic
    def sort_key(r):
        b = r["bucket"]
        if bucket_order and b in bucket_order:
            return (0, bucket_order.index(b), b)
        return (1, 0, b)

    rows.sort(key=sort_key)
    return rows


def price_distribution(enriched: list[dict]) -> list[dict]:
    """
    Group by (outcome, side) and compute fill price stats.
    """
    groups: dict[tuple[str, str], dict] = {}
    for fill in enriched:
        key = (fill["outcome"], fill["side"])
        if key not in groups:
            groups[key] = {"prices": [], "fills": 0}
        groups[key]["prices"].append(fill["fill_price"])
        groups[key]["fills"] += 1

    rows = []
    for (outcome, side), g in sorted(groups.items()):
        prices = g["prices"]
        rows.append({
            "outcome": outcome,
            "side": side,
            "fill_count": len(prices),
            "avg_price": safe_avg(prices),
            "min_price": min(prices),
            "max_price": max(prices),
        })
    return rows


def overall_adverse_pct(enriched: list[dict]) -> tuple[int, int, float | None]:
    """Returns (adverse_count, total_settled, pct) across all fills."""
    adverse = 0
    settled = 0
    for f in enriched:
        mo = f.get("markout_final")
        if mo is not None:
            settled += 1
            if mo < 0:
                adverse += 1
    pct = adverse / settled if settled > 0 else None
    return adverse, settled, pct


# ---------------------------------------------------------------------------
# Step 6 — Render text report
# ---------------------------------------------------------------------------

SEP = "=" * 64


def render_txt(
    enriched: list[dict],
    tte_rows: list[dict],
    all_fills_rows: list[dict],
    dist_rows: list[dict],
    generated_at: str,
) -> list[str]:
    n_fills = len(enriched)
    n_settled = sum(1 for f in enriched if f.get("markout_final") is not None)
    adverse, _, adv_pct = overall_adverse_pct(enriched)

    lines = [
        SEP,
        "FILL MARKOUT REPORT",
        f"Generated: {generated_at}",
        f"Fills analyzed: {n_fills:,}",
        f"Fills with final markout (settled): {n_settled:,}",
        SEP,
        "",
    ]

    # ---- 3B. Markouts by TTE bucket ----------------------------------------
    lines += [
        "3B. FILL MARKOUTS BY TTE BUCKET",
        "    (fills with tte_bucket field only — recent fills)",
        "-" * 64,
    ]

    if not tte_rows:
        lines += [
            "  No fills with tte_bucket found.",
            "  TTE fields are present only in recent fills.",
            "",
        ]
    else:
        # Column header
        h_labels = [f"mo_{h}s" for h in HORIZONS]
        hdr = (
            f"  {'tte_bucket':<12} {'fills':>6} {'avg_price':>10} "
            f"{'avg_final_mo':>13} {'settled':>8} "
            + " ".join(f"{lbl:>9}" for lbl in h_labels)
            + f"  {'adverse':>8}"
        )
        lines.append(hdr)
        lines.append("  " + "-" * (len(hdr) - 2))

        total_tte_fills = 0
        total_tte_settled = 0
        total_tte_wins = 0
        total_tte_losses = 0
        tte_final_all: list[float] = []
        tte_horizon_all: dict[int, list[float]] = {h: [] for h in HORIZONS}

        for r in tte_rows:
            bucket = r["bucket"]
            fc = r["fills"]
            total_tte_fills += fc
            total_tte_settled += r["fills_settled"]
            total_tte_wins += r["wins"]
            total_tte_losses += r["losses"]

            avg_fp = fmt_float(r["avg_fill_price"])
            avg_fm = fmt_float(r["avg_final_markout"], "+.4f")
            adv = fmt_float(r["adverse_pct"], ".0%") if r["adverse_pct"] is not None else "n/a"

            h_strs = []
            for h in HORIZONS:
                v = r["horizon_avgs"].get(h)
                h_strs.append(fmt_float(v, "+.4f") if v is not None else "     n/a")

            lines.append(
                f"  {bucket:<12} {fc:>6} {avg_fp:>10} "
                f"{avg_fm:>13} {r['fills_settled']:>8} "
                + " ".join(f"{s:>9}" for s in h_strs)
                + f"  {adv:>8}"
            )

            if r["avg_final_markout"] is not None:
                tte_final_all.extend(
                    [r["avg_final_markout"]] * r["fills_settled"]
                )
            for h in HORIZONS:
                v = r["horizon_avgs"].get(h)
                if v is not None:
                    tte_horizon_all[h].append(v)

        # TOTAL row
        lines.append("  " + "-" * (len(hdr) - 2))
        total_adv = total_tte_losses / total_tte_settled if total_tte_settled > 0 else None
        total_avg_fm = safe_avg(tte_final_all)
        h_total_strs = []
        for h in HORIZONS:
            v = safe_avg(tte_horizon_all[h])
            h_total_strs.append(fmt_float(v, "+.4f") if v is not None else "     n/a")

        lines.append(
            f"  {'TOTAL':<12} {total_tte_fills:>6} {'--':>10} "
            f"{fmt_float(total_avg_fm, '+.4f'):>13} {total_tte_settled:>8} "
            + " ".join(f"{s:>9}" for s in h_total_strs)
            + f"  {fmt_float(total_adv, '.0%') if total_adv is not None else 'n/a':>8}"
        )
        lines.append("")

    # ---- 3B. ALL fills markouts (by tte bucket, includes no-tte fills) ------
    lines += [
        "3B. FILL MARKOUTS BY TTE BUCKET — ALL FILLS",
        "    (includes fills without tte_bucket; those grouped as 'no_tte')",
        "-" * 64,
    ]

    if not all_fills_rows:
        lines += ["  No fills found.", ""]
    else:
        hdr = (
            f"  {'tte_bucket':<12} {'fills':>7} {'avg_price':>10} "
            f"{'avg_final_mo':>13} {'settled':>8} "
            f"{'wins':>5} {'losses':>7}  {'adverse':>8}"
        )
        lines.append(hdr)
        lines.append("  " + "-" * (len(hdr) - 2))

        grand_fills = 0
        grand_settled = 0
        grand_wins = 0
        grand_losses = 0

        for r in all_fills_rows:
            bucket = r["bucket"]
            fc = r["fills"]
            grand_fills += fc
            grand_settled += r["fills_settled"]
            grand_wins += r["wins"]
            grand_losses += r["losses"]

            avg_fp = fmt_float(r["avg_fill_price"])
            avg_fm = fmt_float(r["avg_final_markout"], "+.4f")
            adv_v = r["adverse_pct"]
            adv = f"{adv_v:.0%}" if adv_v is not None else "n/a"

            lines.append(
                f"  {bucket:<12} {fc:>7} {avg_fp:>10} "
                f"{avg_fm:>13} {r['fills_settled']:>8} "
                f"{r['wins']:>5} {r['losses']:>7}  {adv:>8}"
            )

        lines.append("  " + "-" * (len(hdr) - 2))
        total_adv = grand_losses / grand_settled if grand_settled > 0 else None
        lines.append(
            f"  {'TOTAL':<12} {grand_fills:>7} {'--':>10} "
            f"{'--':>13} {grand_settled:>8} "
            f"{grand_wins:>5} {grand_losses:>7}  "
            f"{fmt_float(total_adv, '.0%') if total_adv is not None else 'n/a':>8}"
        )
        lines.append("")

    # ---- Adverse selection score -------------------------------------------
    adv_str = f"{adv_pct:.1%}" if adv_pct is not None else "n/a"
    lines += [
        "ADVERSE SELECTION SCORE",
        "-" * 64,
        f"  Fills with negative final markout: {adverse:,} / {n_settled:,}  ({adv_str})",
        "  (Score > 50% indicates systematic adverse selection)",
        "",
    ]
    if adv_pct is not None:
        if adv_pct > 0.70:
            lines.append(
                f"  !! HIGH ADVERSE SELECTION: {adv_str} of settled fills are adverse"
            )
        elif adv_pct > 0.50:
            lines.append(
                f"  ! MODERATE ADVERSE SELECTION: {adv_str} of settled fills are adverse"
            )
        else:
            lines.append(
                f"  OK: {adv_str} adverse selection rate (<= 50%)"
            )
        lines.append("")

    # ---- Fill price distribution -------------------------------------------
    lines += [
        "3B. FILL PRICE DISTRIBUTION",
        "-" * 64,
    ]
    if not dist_rows:
        lines += ["  No fills found.", ""]
    else:
        hdr = (
            f"  {'outcome':<8} {'side':<5} "
            f"{'fills':>7} {'avg_price':>10} {'min_price':>10} {'max_price':>10}"
        )
        lines.append(hdr)
        lines.append("  " + "-" * 55)
        for r in dist_rows:
            lines.append(
                f"  {r['outcome']:<8} {r['side']:<5} "
                f"{r['fill_count']:>7} "
                f"{fmt_float(r['avg_price']):>10} "
                f"{fmt_float(r['min_price']):>10} "
                f"{fmt_float(r['max_price']):>10}"
            )
        lines.append("")

    lines += [SEP]
    return lines


# ---------------------------------------------------------------------------
# Step 7 — Build JSON payload
# ---------------------------------------------------------------------------

def build_json(
    enriched: list[dict],
    tte_rows: list[dict],
    all_fills_rows: list[dict],
    dist_rows: list[dict],
    generated_at: str,
    generated_ts: float,
) -> dict:
    n_fills = len(enriched)
    n_settled = sum(1 for f in enriched if f.get("markout_final") is not None)
    adverse, _, adv_pct = overall_adverse_pct(enriched)

    def clean_row(r: dict) -> dict:
        out = {k: v for k, v in r.items() if k != "horizon_avgs"}
        out["horizon_markouts"] = {
            f"{h}s": r["horizon_avgs"].get(h) for h in HORIZONS
        }
        return out

    return {
        "generated_at": generated_at,
        "generated_ts": generated_ts,
        "summary": {
            "fills_analyzed": n_fills,
            "fills_settled": n_settled,
            "adverse_count": adverse,
            "adverse_pct": round(adv_pct, 4) if adv_pct is not None else None,
        },
        "tte_bucket_markouts": [clean_row(r) for r in tte_rows],
        "all_fills_tte_markouts": [clean_row(r) for r in all_fills_rows],
        "fill_price_distribution": dist_rows,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    no_write = "--no-write" in sys.argv

    generated_ts = datetime.now(tz=timezone.utc).timestamp()
    generated_at = fmt_ts(generated_ts)

    sys.stderr.write(f"[fill_markout] Loading fills from {DB} ...\n")
    fills = load_fills()
    sys.stderr.write(f"[fill_markout] Loaded {len(fills):,} fill_applied events\n")

    if not fills:
        msg = "No fill_applied events found in ledger.db\n"
        sys.stdout.write(msg)
        if not no_write:
            OUT_TXT.write_text(msg)
            OUT_JSON.write_text(json.dumps({"error": "no fills", "generated_at": generated_at}))
        return

    sys.stderr.write("[fill_markout] Loading settlements ...\n")
    settlements = load_settlements()
    sys.stderr.write(f"[fill_markout] Loaded {len(settlements):,} settled markets\n")

    # Only fetch mid series for markets that actually have fills
    fill_market_ids = {f["market_id"] for f in fills if f["market_id"]}
    sys.stderr.write(
        f"[fill_markout] Streaming events.jsonl for "
        f"{len(fill_market_ids):,} fill market_ids ...\n"
    )
    mid_series = build_mid_series(fill_market_ids)

    sys.stderr.write("[fill_markout] Computing markouts ...\n")
    enriched = compute_markouts(fills, settlements, mid_series)

    # Aggregate: TTE bucket (only fills with tte_bucket field)
    tte_fills = [f for f in enriched if f.get("tte_bucket") is not None]
    tte_rows = bucket_stats(
        tte_fills,
        group_key_fn=lambda f: f["tte_bucket"] or "?",
        bucket_order=TTE_BUCKET_ORDER,
    )

    # Aggregate: ALL fills by tte_bucket (or "no_tte" for missing)
    def all_fill_bucket(f: dict) -> str:
        return f.get("tte_bucket") or "no_tte"

    all_bucket_order = ["no_tte"] + TTE_BUCKET_ORDER
    all_fills_rows = bucket_stats(
        enriched,
        group_key_fn=all_fill_bucket,
        bucket_order=all_bucket_order,
    )

    # Price distribution
    dist_rows = price_distribution(enriched)

    # Render
    txt_lines = render_txt(
        enriched, tte_rows, all_fills_rows, dist_rows, generated_at
    )
    txt_output = "\n".join(txt_lines) + "\n"

    json_data = build_json(
        enriched, tte_rows, all_fills_rows, dist_rows, generated_at, generated_ts
    )
    json_output = json.dumps(json_data, indent=2)

    sys.stdout.write(txt_output)

    if not no_write:
        OUT_TXT.write_text(txt_output)
        OUT_JSON.write_text(json_output)
        sys.stderr.write(f"[fill_markout] Wrote {OUT_TXT}\n")
        sys.stderr.write(f"[fill_markout] Wrote {OUT_JSON}\n")
    else:
        sys.stderr.write("[fill_markout] --no-write: skipping file output\n")


if __name__ == "__main__":
    main()
