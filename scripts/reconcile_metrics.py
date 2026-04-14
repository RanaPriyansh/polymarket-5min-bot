#!/usr/bin/env python3
"""Reconcile all metrics for a single run_id from ledger, status, and strat_metrics."""
import sqlite3, json, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ledger import SQLiteLedger
from replay import replay_ledger
from datetime import datetime, timezone

DB = "data/runtime/ledger.db"# Read run_id from status.json (current runtime)
with open("data/runtime/status.json") as f:
    _st = json.load(f)
rid = _st.get("run_id", "paper-1775775107-c923c890")

conn = sqlite3.connect(DB)
c = conn.cursor()

with open("data/runtime/status.json") as f:
    st = json.load(f)
with open("data/runtime/strategy_metrics.json") as f:
    strat = json.load(f)

# Raw counts
c.execute("SELECT event_type, COUNT(*) FROM ledger_events WHERE run_id=? GROUP BY event_type ORDER BY COUNT(*) DESC", (rid,))
ledger_counts = dict(c.fetchall())

c.execute("SELECT MIN(event_ts), MAX(event_ts) FROM ledger_events WHERE run_id=?", (rid,))
row = c.fetchone()
start_ts, end_ts = row[0], row[1]
duration = end_ts - start_ts
tox = strat.get('toxicity_mm', {})
risk = st.get('risk', {})

# Replay projection
ledger = SQLiteLedger(Path(DB))
events = ledger.list_events(run_id=rid)
proj = replay_ledger(events)

# Manual live-executor trace
positions = {}
resolved_runtime = 0
wins_runtime = 0
losses_runtime = 0
for et, pj in c.execute("SELECT event_type, payload_json FROM ledger_events WHERE run_id=? ORDER BY event_ts, ROWID", (rid,)):
    p = json.loads(pj)
    if et == "fill_applied":
        family = p.get("strategy_family", "unknown")
        market_id = p.get("market_id")
        outcome = p.get("outcome")
        side = p.get("side")
        size = float(p.get("fill_size", 0))
        key = (family, market_id, outcome)
        if key not in positions: positions[key] = 0
        if side == "SELL": positions[key] -= size
        else: positions[key] += size
        if abs(positions[key]) < 1e-9: positions[key] = 0
    elif et == "slot_settled":
        market_id = p.get("market_id")
        matching = {k: v for k, v in positions.items() if k[1] == market_id and abs(v) > 1e-9}
        if matching:
            resolved_runtime += len(matching)
        else:
            resolved_runtime += 1
        for k in list(positions.keys()):
            if k[1] == market_id:
                del positions[k]

open_positions_after = {k: v for k, v in positions.items() if abs(v) > 1e-9}
conn.close()

print("=" * 72)
print("  METRIC CONTRACT RECONCILIATION  run_id={}".format(rid[:30]))
print("=" * 72)
print("Run duration: {:.0f}s ({:.1f}m)".format(duration, duration / 60))
print("Slots expired: {}".format(ledger_counts.get('slot_settled', 0)))
print()

print("TABLE: reconciled values for {}".format(rid))
print()
print("  metric                            ledger    status    strat     replay")
print("  --------------------------------  --------  --------  --------  --------")
print("  slot_settled                      {:>8}    N/A       N/A       N/A".format(ledger_counts.get('slot_settled', 0)))
print("  slot_closed                       {:>8}    N/A       N/A       N/A".format(0))
print("  resolved_trade_count              N/A       {:>8}    N/A       {}".format(st.get('resolved_trade_count', 0), proj.resolved_trade_count))
print("  fill_observed                     {:>8}    N/A       N/A       N/A".format(ledger_counts.get('fill_observed', 0)))
print("  fill_applied                      {:>8}    N/A       N/A       N/A".format(ledger_counts.get('fill_applied', 0)))
print("  open_positions (after)            {}    {:>8}    N/A       {}".format(len(open_positions_after), st.get('open_position_count', 0), len(proj.positions)))
print("  bankroll                          N/A       ${:>6.2f}    N/A       N/A".format(st.get('bankroll', 0)))
rp = risk.get('realized_pnl_total', 0)
sp = tox.get('realized_pnl', 0)
up = risk.get('unrealized_pnl_total', 0)
print("  realized_pnl (risk)               N/A       ${:+7.4f}  ${:+8.4f}  N/A".format(rp, sp))
print("  unrealized_pnl                    N/A       ${:+7.4f}  N/A       N/A".format(up))
print("  win_count                         N/A       {}        N/A       {}".format(int(st.get('win_rate', 0) * st.get('resolved_trade_count', 1)), proj.win_count))
print("  loss_count                        N/A       {}        N/A       {}".format(st.get('resolved_trade_count', 0) - int(st.get('win_rate', 0) * st.get('resolved_trade_count', 1)), proj.loss_count))
print()

# Manual trace comparison
print("  manual_traced resolved: {}".format(resolved_runtime))
print("  manual_traced wins (positions): {}".format(sum(1 for k in positions if False)))
print()

if len(open_positions_after) > 0:
    print("Remaining open after all settlements:")
    for k, v in sorted(open_positions_after.items()):
        print("  {}: qty={:.4f}".format(k, v))
print()

# Status snapshot timing
with sqlite3.connect(DB) as conn2:
    c2 = conn2.cursor()
    c2.execute("SELECT event_ts, payload_json FROM ledger_events WHERE event_type='risk_snapshot_recorded' AND run_id=? ORDER BY event_ts DESC LIMIT 5", (rid,))
    print("Last 5 risk snapshots:")
    for ts, pj in c2.fetchall():
        p = json.loads(pj)
        cap = p.get("capital", 0)
        print("  ts={:.0f}  bankroll=${:.2f}".format(ts, cap))
