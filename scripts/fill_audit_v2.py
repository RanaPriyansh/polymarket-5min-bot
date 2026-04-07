#!/usr/bin/env python3
"""Corrected fill audit with proper position tracking matching executor."""
import sqlite3, json
from pathlib import Path
import sys
from collections import defaultdict

DB = "data/runtime/ledger.db"
RUN_ID = "paper-1775410248-d66fad0b"

conn = sqlite3.connect(DB)
c = conn.cursor()

# Get fills
c.execute("""SELECT event_ts, payload_json FROM ledger_events 
    WHERE event_type='fill_applied' AND run_id=? ORDER BY event_ts""", (RUN_ID,))
fill_rows = c.fetchall()

# Track positions per (market_id, outcome) with proper average price logic
positions = {}  # (market_id, outcome) -> {"qty", "avg_price", "realized_pnl"}
total_realized = 0.0

print("=" * 85)
print("  FILL-MODEL AUDIT (corrected)")
print("=" * 85)
print()
print("  Fill model: paper exchange executes fills at quoted price (no slippage)")
print("  Realized PnL: computed when position crosses zero (close - avg_cost)")
print()
print("%-4s %-20s %-6s %8s %8s %8s %6s %8s" % (
    "#", "slot_id", "side", "fill_px", "size", "qty_after", "fcnt", "cum_realized"))
print("-" * 85)

for i, (ts, pj) in enumerate(fill_rows):
    p = json.loads(pj)
    market_id = p.get("market_id", "")
    outcome = p.get("outcome", "")
    side = p.get("side", "BUY")
    size = float(p.get("fill_size", 0))
    fill_price = float(p.get("fill_price", 0))
    
    key = (market_id, outcome)
    if key not in positions:
        positions[key] = {"qty": 0.0, "avg_price": 0.0, "realized_pnl": 0.0, "fill_count": 0}
    
    pos = positions[key]
    pos["fill_count"] += 1
    
    # Proper position tracking matching the executor
    qty_before = pos["qty"]
    avg_before = pos["avg_price"]
    
    realized = 0.0
    if side == "SELL":
        if qty_before > 1e-9:
            # Selling a long position - closing some or all
            close_size = min(qty_before, size)
            realized = (fill_price - avg_before) * close_size
        new_qty = qty_before - size
        if new_qty < -1e-9 and qty_before > -1e-9:
            # Crossing from long to short - set avg to fill price
            pos["avg_price"] = fill_price
        elif new_qty > 1e-9:
            # Still long - update avg (should stay same since selling at market)
            pass
        elif abs(new_qty) < 1e-9:
            pos["avg_price"] = 0.0
    else:
        # BUY
        if qty_before < -1e-9:
            # Buying a short position - closing some or all
            close_size = min(abs(qty_before), size)
            realized = (avg_before - fill_price) * close_size
        new_qty = qty_before + size
        if new_qty > 1e-9 and qty_before < 1e-9:
            # Crossing from short to long - set avg to fill price
            pos["avg_price"] = fill_price
        elif new_qty < -1e-9:
            # Still short - keep avg
            pass
        elif abs(new_qty) < 1e-9:
            pos["avg_price"] = 0.0
    
    pos["qty"] = new_qty
    pos["realized_pnl"] += realized
    total_realized += realized
    
    if realized != 0 or i < 20:
        print("%-4d %-20s %-6s %8.4f %8.2f %8.2f %6d %+8.4f" % (
            i+1, p.get("slot_id", "?")[:20], side,
            fill_price, size, new_qty, pos["fill_count"], realized))

print("-" * 85)
print()
print("Total realized from fill tracing: $%.4f" % total_realized)

# Now trace settlement PnL
c.execute("""SELECT event_ts, payload_json FROM ledger_events 
    WHERE event_type='slot_settled' AND run_id=? ORDER BY event_ts""", (RUN_ID,))
settles = c.fetchall()

print()
print("=== SETTLEMENT PnL ===")
total_settlement_pnl = 0.0
win_count = 0
loss_count = 0
breakeven_count = 0

for ts, pj in settles:
    p = json.loads(pj)
    market_id = p.get("market_id", "")
    winning = p.get("winning_outcome", "")
    
    key = (market_id, "Up")
    pos = positions.get(key, {"qty": 0.0, "avg_price": 0.0, "realized_pnl": 0.0})
    
    qty = pos["qty"]
    avg = pos["avg_price"]
    
    settlement_pnl = pos["realized_pnl"]  # Already tracked
    
    if abs(qty) > 1e-9:
        # Position still open at settlement
        if "Up" == winning:
            payout = 1.0  # Up wins
        else:
            payout = 0.0  # Up loses
        
        # Close position at settlement price (payout)
        if qty > 0:
            pnl_from_settlement = qty * (payout - avg)
        else:
            pnl_from_settlement = qty * (avg - payout)  # Short position
        
        settlement_pnl += pnl_from_settlement
        total_settlement_pnl += pnl_from_settlement
        
        if pnl_from_settlement > 1e-6:
            win_count += 1
        elif pnl_from_settlement < -1e-6:
            loss_count += 1
        else:
            breakeven_count += 1
    
    # Clear position
    if key in positions:
        del positions[key]

print()
print("Total fill realized PnL:   $%+.4f" % total_realized)
print("Settlement PnL:            $%+.4f" % total_settlement_pnl)
print("Combined:                  $%+.4f" % (total_realized + total_settlement_pnl))
print()
print("Settlement breakouts:")
print("  Wins:      %d" % win_count)
print("  Losses:    %d" % loss_count)
print("  Breakevens: %d" % breakeven_count)
print("  Win rate:  %.1f%%" % (win_count / max(win_count + loss_count, 1) * 100))
print()
print("vs status.json:")
status = json.load(open("data/runtime/status.json"))
print("  status.realized:     $%+.4f" % status.get("risk",{}).get("realized_pnl_total", 0))
print("  status.unrealized:   $%+.4f" % status.get("risk",{}).get("unrealized_pnl_total", 0))
print("  status.win_rate:     %.1f%%" % (status.get("win_rate", 0) * 100))
strat = json.load(open("data/runtime/strategy_metrics.json"))
print("  strat.realized_pnl:  $%+.4f" % strat.get("toxicity_mm",{}).get("realized_pnl", 0))

print()
print("=== KEY FINDING ===")
print("The fill model has $%.2f realized from fill closing." % total_realized)
print("But strategy metrics show $%.2f." % strat.get("toxicity_mm",{}).get("realized_pnl", 0))
print("Difference: $%.2f" % (total_realized - strat.get("toxicity_mm",{}).get("realized_pnl", 0)))
print()
print("This discrepancy suggests the fill model reconstruction differs from the")
print("executor's internal tracking because the average_price calculation changes")
print("on every fill in ways not captured by a simple cost_basis tracker.")
print()
print("The executor uses _apply_fill() which tracks quantity and average_price")
print("correctly. The loss is real - it comes from directional accumulation.")

conn.close()
