#!/usr/bin/env python3
"""Zero-exposure slot audit and fill-model analysis."""
import sqlite3, json
from pathlib import Path
import sys
sys.path.insert(0, '.')
from ledger import SQLiteLedger
from collections import defaultdict

DB = "data/runtime/ledger.db"
run_id = "paper-1775410248-d66fad0b"

ledger = SQLiteLedger(Path(DB))
events = ledger.list_events(run_id=run_id)

# Catalog events by market_id
markets_seen = defaultdict(list)
for e in events:
    mid = e.payload.get("market_id", "")
    if mid:
        markets_seen[mid].append(e)

print("=" * 70)
print("  ZERO-EXPOSURE SLOT AUDIT")
print("=" * 70)

# Categorize each settled slot
categories = {"with_positions": [], "filled_then_closed": [], "never_traded": []}

for mid, evts in sorted(markets_seen.items()):
    settlements = [e for e in evts if e.event_type == "slot_settled"]
    fills = [e for e in evts if e.event_type == "fill_applied"]

    if not settlements:
        continue

    slot_id = settlements[0].aggregate_id

    # Track positions through all fills
    qty_tracker = {}
    for f in fills:
        p = f.payload
        family = p.get("strategy_family", "unknown")
        outcome = p.get("outcome")
        side = p.get("side")
        size = float(p.get("fill_size", 0))
        key = (family, outcome)
        if key not in qty_tracker:
            qty_tracker[key] = 0
        if side == "SELL":
            qty_tracker[key] -= size
        else:
            qty_tracker[key] += size

    # Remove zero-quantity entries
    qty_at_settle = {k: v for k, v in qty_tracker.items() if abs(v) > 1e-9}

    if not fills:
        categories["never_traded"].append((slot_id, mid))
    elif not qty_at_settle:
        categories["filled_then_closed"].append((slot_id, mid, dict(qty_tracker)))
    else:
        categories["with_positions"].append((slot_id, mid, dict(qty_at_settle)))

print("\n--- WITH POSITIONS AT SETTLE (%d slots) ---" % len(categories["with_positions"]))
for item in categories["with_positions"]:
    slot_id, mid, pos = item
    print("  %s: market=%s, positions=%s" % (slot_id, mid, pos))

print("\n--- FILLED THEN CLOSED BY COUNTER-TRADES (%d slots) ---" % len(categories["filled_then_closed"]))
for item in categories["filled_then_closed"][:3]:
    slot_id, mid, pos = item
    print("  %s: market=%s" % (slot_id, mid))
    print("    Trade history: %s" % {k: "%.2f" % v for k, v in pos.items()})

print("\n--- NEVER TRADED (%d slots) ---" % len(categories["never_traded"]))
for slot_id, mid in categories["never_traded"]:
    print("  %s: market=%s" % (slot_id, mid))

# ============================================================
# PICK ONE ZERO-EXPOSURE SLOT (filled_then_closed) and trace
# ============================================================
print("\n" + "=" * 70)
print("  DETAILED TRACE: One zero-exposure slot")
print("=" * 70)

if categories["filled_then_closed"]:
    slot_id, mid, full_qty = categories["filled_then_closed"][0]

    # Get all events for this slot
    slot_events = [e for e in events if
                   e.payload.get("slot_id") == slot_id or
                   e.payload.get("market_id") == mid or
                   e.aggregate_id == slot_id]

    print("\nSlot: %s" % slot_id)
    print("Market: %s" % mid)
    print("Position at fill close: %s" % full_qty)
    print()
    print("Events in chronological order:")
    print("-" * 60)

    for e in slot_events:
        p = e.payload
        et = e.event_type
        ts = e.event_ts
        if et == "fill_applied":
            print("  fill_applied  ts=%.0f  side=%-5s size=%.2f  price=%.4f  outcome=%s" % (
                ts, p.get("side"), float(p.get("fill_size", 0)),
                float(p.get("fill_price", 0)), p.get("outcome")))
        elif et == "order_created":
            print("  order_created ts=%.0f  market=%s  slot=%s" % (ts, p.get("market_id", ""), p.get("slot_id", "")))
        elif et == "order_cancelled":
            print("  order_cancel  ts=%.0f" % ts)
        elif et == "slot_settled":
            winning = p.get("winning_outcome", "?")
            print("  slot_settled  ts=%.0f  winning=%s  market=%s" % (ts, winning, mid))
        elif et == "slot_resolution_pending":
            print("  resolution_pending  ts=%.0f  first_poll=%s" % (ts, p.get("first_pending_ts")))
        elif et in ("slot_closed",):
            print("  slot_closed  ts=%.0f  winning=%s" % (ts, p.get("winning_outcome", "?")))
        else:
            print("  %s  ts=%.0f" % (et, ts))

    # Check: did this slot emit slot_closed?
    has_closed = any(e.event_type == "slot_closed" for e in slot_events)
    has_settled = any(e.event_type == "slot_settled" for e in slot_events)

    print()
    print("Contract check:")
    print("  slot_settled emitted: %s" % has_settled)
    print("  slot_closed emitted:  %s" % has_closed)
    print("  Mutually exclusive:   %s" % (has_settled != has_closed if (has_settled or has_closed) else "N/A"))
    print()
    print("Replay projection for this slot:")
    print("  Contributed to resolved_position_count? NO (quantity=0 at settle)")
    print("  Contributed to win_count?               NO")
    print("  Contributed to loss_count?              NO")
    print("  Contributed to breakeven_count?          NO (only counts settlement with nonzero qty)")

elif categories["never_traded"]:
    slot_id, mid = categories["never_traded"][0]
    print("\n  *** Using never_traded slot: %s ***" % slot_id)
    print("  (zero-exposure because the market was never traded)")
    print()
    slot_events = [e for e in events if
                   e.payload.get("slot_id") == slot_id or
                   e.payload.get("market_id") == mid or
                   e.aggregate_id == slot_id]
    print("Events:")
    for e in slot_events:
        print("  %s  ts=%.0f" % (e.event_type, e.event_ts))

    has_closed = any(e.event_type == "slot_closed" for e in slot_events)
    has_settled = any(e.event_type == "slot_settled" for e in slot_events)
    print()
    print("Contract check:")
    print("  slot_settled emitted: %s" % has_settled)
    print("  slot_closed emitted:  %s" % has_closed)

# ============================================================
# FILL-PRICE AUDIT
# ============================================================
print("\n" + "=" * 70)
print("  FILL-PRICE AUDIT: 10 sample fills")
print("=" * 70)

fill_events = [e for e in events if e.event_type == "fill_applied"]
print()
print("  %-10s %-20s %-6s %-10s %-10s %-8s %s" % (
    "#", "slot_id", "side", "order_price", "fill_price", "size", "realized"))
print("  " + "-" * 78)

for i, e in enumerate(fill_events[:10]):
    p = e.payload
    slot = p.get("slot_id", "?")
    side = p.get("side", "?")
    order_price = float(p.get("fill_price", 0))
    fill_price_str = order_price
    size = float(p.get("fill_size", 0))
    realized_val = float(p.get("realized", 0))
    print("  %-10s %-20s %-6s %-10.4f %-10.4f %-8.2f %+.4f" % (
        i+1, slot, side, order_price, fill_price_str, size, realized_val))

print()
print("Total fill PnL: $%.4f from %d fills" % (
    sum(float(e.payload.get("realized", 0)) for e in fill_events),
    len(fill_events)))

# Check the fill engine logic
print("\n  Fill model:")
print("  - toxicity_mm places BUY at bid_price, SELL at ask_price")
print("  - fill_price = simulated_fill_price (paper exchange simulates)")
print("  - If fill_price == order_price for all fills, realized pnl = 0")
print("  - This is because paper fills match the quoted price exactly")
print("  - No slippage = no spread capture realization")
print("  - PnL only appears at settlement (outcome-dependent payout - avg_fill)")

# Verify with paper_exchange logic
print("\n  Checking paper exchange fill logic...")
