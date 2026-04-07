#!/usr/bin/env python3
"""Fill model audit and position trace."""
import sqlite3, json
from pathlib import Path
import sys
from collections import defaultdict

DB = "data/runtime/ledger.db"
RUN_ID = "paper-1775410248-d66fad0b"

conn = sqlite3.connect(DB)
c = conn.cursor()

# Get all fill_applied events for this run
c.execute("""SELECT event_ts, payload_json FROM ledger_events 
    WHERE event_type='fill_applied' AND run_id=? ORDER BY event_ts""", (RUN_ID,))
fill_rows = c.fetchall()

print("=" * 78)
print("  FILL-MODEL AUDIT")
print("=" * 78)
print()
print("NOTE: fill_applied events in ledger do NOT contain realized PnL.")
print("We reconstruct by tracking position average price and close prices.")
print()

# Track position state per (slot_id, outcome) and reconstruct PnL
positions = {}  # key -> {"qty", "avg_price", "cost_basis"}
total_fill_realized = 0.0
fill_traces = []

for i, (ts, pj) in enumerate(fill_rows):
    p = json.loads(pj)
    slot_id = p.get("slot_id", "?")
    market_id = p.get("market_id", "")
    outcome = p.get("outcome", "")
    side = p.get("side", "BUY")
    size = float(p.get("fill_size", 0))
    fill_price = float(p.get("fill_price", 0))
    
    key = (slot_id, outcome)
    if key not in positions:
        positions[key] = {"qty": 0.0, "cost_basis": 0.0, "fill_count": 0}
    
    pos = positions[key]
    
    # Calculate realized PnL if closing
    realized = 0.0
    if pos["qty"] != 0 and ((pos["qty"] > 0 and side == "SELL") or (pos["qty"] < 0 and side == "BUY")):
        # This fill is closing some or all of an existing position
        close_size = min(abs(pos["qty"]), size)
        avg_price = pos["cost_basis"] / abs(pos["qty"])
        if pos["qty"] > 0:
            # Long position: selling closes it
            realized = (fill_price - avg_price) * close_size
        else:
            # Short position: buying closes it
            realized = (avg_price - fill_price) * close_size
        total_fill_realized += realized
    
    # Update position
    if side == "SELL":
        pos["qty"] -= size
    else:
        pos["qty"] += size
    pos["fill_count"] += 1
    
    # Update cost basis
    if abs(pos["qty"]) > 1e-9 and pos["qty"] * size > 0:
        # Position increased in same direction
        pos["cost_basis"] += fill_price * size
    elif abs(pos["qty"]) < 1e-9:
        pos["qty"] = 0.0
        pos["cost_basis"] = 0.0
    # If crossing zero, cost_basis becomes 0 (simplified)
    
    if realized != 0 or i < 15:
        fill_traces.append({
            "i": i+1,
            "slot": slot_id,
            "side": side,
            "price": fill_price,
            "size": size,
            "fill_count": pos["fill_count"],
            "qty_after": pos["qty"],
            "realized": realized
        })

# Print trace table
print("%-4s %-20s %-6s %8s %6s %4s %8s %8s" % (
    "#", "slot_id", "side", "fill_px", "size", "fcnt", "qty_after", "realized"))
print("-" * 70)

for ft in fill_traces:
    print("%-4d %-20s %-6s %8.4f %6.2f %4d %8.2f %+8.4f" % (
        ft["i"], ft["slot"], ft["side"], ft["price"], 
        ft["size"], ft["fill_count"], ft["qty_after"], ft["realized"]))

print("-" * 70)
print("Total realized PnL from fill closing: $%.4f" % total_fill_realized)

# Also check: what's in strategy_metrics for realized_pnl?
import json
strat = json.load(open("data/runtime/strategy_metrics.json"))
tox = strat.get("toxicity_mm", {})
print("\nStrategy metrics:")
for k, v in sorted(tox.items()):
    if isinstance(v, float):
        print("  %s: $%.4f" % (k, v))
    else:
        print("  %s: %s" % (k, v))

# Settlement PnL
print("\n=== SETTLEMENT PnL RECONSTRUCTION ===")
positions2 = {}  # (slot_id, outcome) -> {qty, cost_basis}
total_settlement_realized = 0.0

c.execute("""SELECT event_ts, payload_json FROM ledger_events 
    WHERE run_id=? ORDER BY event_ts""", (RUN_ID,))

for ts, pj in c.fetchall():
    e = json.loads(pj)
    et = e.get("event_type", "")
    
    if et == "fill_applied":
        ep = json.loads(str(pj))
        slot_id = ep.get("slot_id", "?")
        outcome = ep.get("outcome", "")
        side = ep.get("side", "BUY")
        size = float(ep.get("fill_size", 0))
        fill_price = float(ep.get("fill_price", 0))
        
        key = (slot_id, outcome)
        if key not in positions2:
            positions2[key] = {"qty": 0.0, "cost_basis": 0.0}
        
        pos = positions2[key]
        if side == "SELL":
            if abs(pos["qty"]) > 1e-9 and pos["qty"] > 0:
                close_size = min(pos["qty"], size)
                avg = pos["cost_basis"] / pos["qty"] if pos["qty"] > 0 else 0
                pos["cost_basis"] -= avg * close_size
                pos["qty"] -= size
            else:
                pos["qty"] -= size
                pos["cost_basis"] = 0  # Simplified: crossing to short
        else:
            if abs(pos["qty"]) > 1e-9 and pos["qty"] < 0:
                close_size = min(abs(pos["qty"]), size)
                avg = pos["cost_basis"] / abs(pos["qty"]) if pos["qty"] < 0 else 0
                pos["cost_basis"] -= avg * close_size
                pos["qty"] += size
            else:
                pos["qty"] += size
                pos["cost_basis"] = fill_price * size if abs(pos["qty"]) > 1e-9 else 0
        
        if abs(pos["qty"]) < 1e-9:
            pos["qty"] = 0.0
            pos["cost_basis"] = 0.0
    
    elif et == "slot_settled":
        ep = json.loads(str(pj))
        slot_id = ep.get("slot_id", "?")
        winning = ep.get("winning_outcome", "")
        payout = 1.0
        
        key = (slot_id, "Up")
        pos = positions2.get(key, {"qty": 0.0, "cost_basis": 0.0})
        
        if abs(pos["qty"]) > 1e-9:
            outcome_won = "Up" == winning
            final_payout = 1.0 if outcome_won else 0.0
            settlement = pos["qty"] * (final_payout - pos["cost_basis"] / pos["qty"])
            total_settlement_realized += settlement

print(f"  Estimated fill realized PnL:  ${total_fill_realized:+.4f}")
print(f"  Estimated settlement PnL:    ${total_settlement_realized:+.4f}")
print(f"  Estimated total:             ${total_fill_realized + total_settlement_realized:+.4f}")

status = json.load(open("data/runtime/status.json"))
print(f"  status.json bankroll:        ${status.get('bankroll', 0):.2f}")
print(f"  status.json realized:        ${status.get('risk',{}).get('realized_pnl_total',0):.4f}")
print(f"  status.json unrealized:      ${status.get('risk',{}).get('unrealized_pnl_total',0):.4f}")

conn.close()
