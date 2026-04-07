#!/usr/bin/env python3
"""As-of reconciliation script."""
import sqlite3, json, sys, subprocess
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ledger import SQLiteLedger
from replay import replay_ledger
from datetime import datetime, timezone

STATUS = "data/runtime/status.json"
STRAT = "data/runtime/strategy_metrics.json"
DB = "data/runtime/ledger.db"

print("=" * 78)
print("  AS-OF RECONCILIATION TABLE")
print("=" * 78)

status = json.load(open(STATUS))
strat = json.load(open(STRAT))
run_id = status["run_id"]
heartbeat_ts = status.get("heartbeat_ts", 0)

print("  run_id:       {}".format(run_id))
print("  heartbeat_ts: {} ({})".format(
    heartbeat_ts,
    datetime.fromtimestamp(heartbeat_ts, tz=timezone.utc).strftime("%H:%M:%S UTC")))

conn = sqlite3.connect(DB)
c = conn.cursor()

# Full run counts
c.execute("SELECT event_type, COUNT(*) FROM ledger_events WHERE run_id=? GROUP BY event_type ORDER BY COUNT(*) DESC", (run_id,))
full = dict(c.fetchall())

# As-of heartbeat counts
c.execute("""SELECT event_type, COUNT(*) FROM ledger_events 
    WHERE run_id=? AND event_ts <= ? GROUP BY event_type ORDER BY COUNT(*) DESC""", (run_id, heartbeat_ts))
aof = dict(c.fetchall())

# Replay: all events
ledger = SQLiteLedger(Path(DB))
all_events = ledger.list_events(run_id=run_id)
proj_full = replay_ledger(all_events)

# Replay: as-of heartbeat
aof_events = [e for e in all_events if e.event_ts <= heartbeat_ts]
proj_aof = replay_ledger(aof_events)

# Events after heartbeat
c.execute("""SELECT event_type, COUNT(*) FROM ledger_events 
    WHERE run_id=? AND event_ts > ? GROUP BY event_type""", (run_id, heartbeat_ts))
after_hb = dict(c.fetchall())

print()
print("{:<34s} {:>8s} {:>8s} {:>8s} {:>12s}  {}".format(
    "Metric", "status", "strat", "replay", "replay_full", "Explanation"))
print("{} {} {} {} {}  {}".format(
    "-" * 34, "-" * 8, "-" * 8, "-" * 8, "-" * 12, "-" * 40))

rows = [
    ("slot_settled",
     "N/A", "N/A", str(aof.get("slot_settled", 0)), str(full.get("slot_settled", 0)),
     "One per market settlement in ledger"),
    ("slot_closed",
     "N/A", "N/A",
     str(len([e for e in aof_events if e.event_type == "slot_closed"])),
     str(len([e for e in all_events if e.event_type == "slot_closed"])),
     "Not in ledger.db, not triggered in this run"),
    ("resolved_trade_count",
     str(status.get("resolved_trade_count", 0)), "N/A",
     str(proj_aof.resolved_trade_count), str(proj_full.resolved_trade_count),
     "Positions settled only"),
    ("win_count",
     "0", "N/A",
     str(proj_aof.win_count), str(proj_full.win_count),
     "Wins from settled positions"),
    ("loss_count",
     "0", "N/A",
     str(proj_aof.loss_count), str(proj_full.loss_count),
     "Losses from settled positions"),
    ("open_position_count",
     str(status.get("open_position_count", 0)), "N/A",
     str(len(proj_aof.positions)), str(len(proj_full.positions)),
     "Settled positions purged from replay"),
]

for row in rows:
    print("{:<34s} {:>8s} {:>8s} {:>8d} {:>12d}  {}".format(*row))

print()
print("  realized_pnl (status.risk):    ${:+.4f}".format(
    status.get("risk", {}).get("realized_pnl_total", 0)))
print("  realized_pnl (strat_metrics):  ${:+.4f}".format(
    strat.get("toxicity_mm", {}).get("realized_pnl", 0)))
print("  unrealized_pnl:                ${:+.4f}".format(
    status.get("risk", {}).get("unrealized_pnl_total", 0)))
print("  bankroll:                      ${:.2f}".format(
    status.get("bankroll", 0)))

print()
print("  Events after heartbeat_ts:")
if after_hb:
    for et, cnt in sorted(after_hb.items(), key=lambda x: -x[1]):
        print("    {}: {}".format(et, cnt))
else:
    print("    (none - heartbeat is current)")

print()
print("  Discrepancy check:")
if aof.get("slot_settled", 0) == full.get("slot_settled", 0):
    print("    slot_settled: MATCH (all {} events before heartbeat)".format(full.get("slot_settled", 0)))
else:
    print("    slot_settled: MISMATCH ({} before heartbeat, {} total)".format(
        aof.get("slot_settled", 0), full.get("slot_settled", 0)))

if status.get("resolved_trade_count") == proj_aof.resolved_trade_count:
    print("    resolved_trade_count: MATCH ({} == {})".format(
        status.get("resolved_trade_count"), proj_aof.resolved_trade_count))
else:
    print("    resolved_trade_count: MISMATCH ({} vs {}) - {} events after heartbeat".format(
        status.get("resolved_trade_count"), proj_aof.resolved_trade_count,
        after_hb.get("slot_settled", 0) + after_hb.get("fill_applied", 0)))

if status.get("open_position_count") == len(proj_aof.positions):
    print("    open_position_count: MATCH ({} positions)".format(
        proj_aof.resolved_trade_count))

conn.close()
