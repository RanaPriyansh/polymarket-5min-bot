#!/usr/bin/env python3
import sqlite3
import json
from pathlib import Path
from datetime import datetime

BASE = Path("/root/obsidian-hermes-vault/projects/polymarket-5min-bot")
DB_PATH = BASE / "data/runtime/ledger.db"
STATUS_PATH = BASE / "data/runtime/status.json"
OUT_PATH = BASE / "data/runtime/settlement_latency_audit_latest.md"

def main():
    # Read status.json
    status = json.loads(STATUS_PATH.read_text())
    run_id = status.get("run_id", "unknown")
    pending_resolution_slots = status.get("pending_resolution_slots", [])
    pending_settlement_count = status.get("risk", {}).get("pending_settlement_count", 0)
    resolved_trade_count = status.get("resolved_trade_count", 0)
    latest_settlement = status.get("latest_settlement", {})
    
    # Read ledger.db
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get all tables
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [t[0] for t in c.fetchall()]
    
    counts = {}
    for t in tables:
        c.execute(f"SELECT COUNT(*) FROM {t}")
        counts[t] = c.fetchone()[0]
    
    # Get slot_settled samples for latency analysis
    latency_samples = []
    try:
        c.execute("SELECT slot_id, payload FROM slot_settled ORDER BY rowid DESC LIMIT 50")
        for row in c.fetchall():
            slot_id = row[0]
            payload = json.loads(row[1]) if row[1] else {}
            settled_ts = payload.get("settled_ts")
            # Extract slot timestamp from slot_id (format: asset:interval:ts)
            parts = slot_id.split(":") if slot_id else []
            if len(parts) >= 3 and settled_ts:
                try:
                    slot_ts = float(parts[2])
                    latency = settled_ts - slot_ts - 300  # subtract interval (5min = 300s)
                    if latency >= 0:
                        latency_samples.append(latency)
                except:
                    pass
    except Exception as e:
        pass
    
    # Get slot_resolution_pending samples
    pending_slots = []
    try:
        c.execute("SELECT slot_id, payload FROM slot_resolution_pending")
        for row in c.fetchall():
            pending_slots.append(row[0])
    except:
        pass
    
    # Get slot_closed samples
    closed_slots = []
    try:
        c.execute("SELECT slot_id FROM slot_closed")
        closed_slots = [r[0] for r in c.fetchall()]
    except:
        pass
    
    conn.close()
    
    # Compute latency stats
    avg_latency = sum(latency_samples) / len(latency_samples) if latency_samples else 0
    max_latency = max(latency_samples) if latency_samples else 0
    min_latency = min(latency_samples) if latency_samples else 0
    
    # Determine verdict
    pending_count = counts.get("slot_resolution_pending", 0)
    settled_count = counts.get("slot_settled", 0)
    closed_count = counts.get("slot_closed", 0)
    
    now_ts = datetime.utcnow().timestamp()
    heartbeat_ts = status.get("heartbeat_ts", 0)
    heartbeat_age = now_ts - heartbeat_ts if heartbeat_ts else 9999
    
    # Verdict logic
    if pending_count > 20:
        verdict = "stuck"
    elif avg_latency > 180 or pending_count > 10:
        verdict = "late"
    elif heartbeat_age > 120:
        verdict = "stale"
    else:
        verdict = "healthy"
    
    # Write report
    report = f"""# Settlement Latency Audit

**Timestamp:** {datetime.utcnow().isoformat()}Z  
**run_id:** `{run_id}`  

## Counts

| Table | Count |
|-------|-------|
| slot_resolution_pending | {pending_count} |
| slot_settled | {settled_count} |
| slot_closed | {closed_count} |

## Settlement Latency

- **Samples:** {len(latency_samples)}
- **Avg latency (post-close):** {avg_latency:.1f}s
- **Min latency:** {min_latency:.1f}s
- **Max latency:** {max_latency:.1f}s

## Status

- **Heartbeat age:** {heartbeat_age:.0f}s
- **Pending resolution slots:** {len(pending_resolution_slots)}
- **Resolved trade count:** {resolved_trade_count}

## Verdict

**{verdict.upper()}**

"""

    OUT_PATH.write_text(report)
    print(report)

if __name__ == "__main__":
    main()