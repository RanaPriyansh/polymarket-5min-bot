#!/usr/bin/env python3
"""Settlement diagnostics analyzer."""
import json
import sys
from collections import defaultdict
from pathlib import Path

EVENTS_FILE = Path("/root/obsidian-hermes-vault/projects/polymarket-5min-bot/data/runtime/events.jsonl")

# Parse events
pending_events = []  # slot_resolution_pending
resolved_events = []  # market.pending_resolution
settled_events = []   # slot_settled
settlement_events = [] # market.settled

total_lines = 0
parse_errors = 0

with open(EVENTS_FILE) as f:
    for line in f:
        total_lines += 1
        try:
            evt = json.loads(line)
            et = evt.get("event_type", "")
            if et == "slot_resolution_pending":
                pending_events.append(evt)
            elif et in ("market.pending_resolution",):
                resolved_events.append(evt)
            elif et == "slot_settled":
                settled_events.append(evt)
            elif et == "market.settled":
                settlement_events.append(evt)
        except json.JSONDecodeError:
            parse_errors += 1

print(f"Total lines parsed: {total_lines}")
print(f"Parse errors: {parse_errors}")
print(f"slot_resolution_pending events: {len(pending_events)}")
print(f"market.pending_resolution events: {len(resolved_events)}")
print(f"slot_settled events: {len(settled_events)}")
print(f"market.settled events: {len(settlement_events)}")
print()

# Analyze pending slots
slots = defaultdict(list)
for evt in pending_events + resolved_events:
    payload = evt.get("payload", {})
    slot_id = payload.get("slot_id", evt.get("aggregate_id", "unknown"))
    ts = evt.get("ts", 0)
    slots[slot_id].append({
        "event_type": evt.get("event_type"),
        "ts": ts,
        "first_pending_ts": payload.get("first_pending_ts", 0),
        "next_poll_ts": payload.get("next_poll_ts", 0),
        "delay_seconds": payload.get("delay_seconds", 0),
        "deferred": payload.get("deferred", False),
    })

print(f"Unique slots with pending events: {len(slots)}")
print()

# For each slot, analyze lifecycle
print("SLOT LIFECYCLE ANALYSIS")
print("=" * 120)
print(f"{'Slot ID':30s} {'Events':>6s} {'Deferred':>8s} {'First':>12s} {'Last':>12s} {'MaxDelay':>8s} {'MaxNextPoll':>14s}")
print("-" * 120)

all_slots_info = []
for slot_id in sorted(slots.keys()):
    events = slots[slot_id]
    first_ts = events[0]["ts"]
    last_ts = events[-1]["ts"]
    max_delay = max(e["delay_seconds"] for e in events)
    max_next_poll = max(e["next_poll_ts"] for e in events)
    has_deferred = any(e["deferred"] for e in events)
    poll_count = len(events)

    print(f"{slot_id:30s} {poll_count:>6d} {str(has_deferred):>8s} {first_ts:>12.0f} {last_ts:>12.0f} {max_delay:>8.0f} {max_next_poll:>14.0f}")
    all_slots_info.append({
        "slot_id": slot_id,
        "poll_count": poll_count,
        "has_deferred": has_deferred,
        "first_pending_ts": first_ts,
        "last_event_ts": last_ts,
        "max_delay": max_delay,
        "max_next_poll": max_next_poll,
        "events": events,
    })

print()

# Check deferred slots in detail
for info in all_slots_info:
    if info["has_deferred"]:
        print(f"DEFERRED SLOT: {info['slot_id']}")
        print(f"  Events: {info['poll_count']}")
        print(f"  First pending: {info['first_pending_ts']}")
        print(f"  Last event: {info['last_event_ts']}")
        print(f"  Max delay between polls: {info['max_delay']:.1f}s")
        print(f"  Max next_poll_ts: {info['max_next_poll']:.1f}")
        print(f"  Last 3 events:")
        for e in info["events"][-3:]:
            print(f"    type={e['event_type']} ts={e['ts']:.0f} delay={e['delay_seconds']:.0f}s next_poll={e['next_poll_ts']:.0f} deferred={e['deferred']}")
        print()

# Resolution poll cap config
print("CONFIG EXPECTATIONS")
print("==================")
print("resolution_initial_poll_seconds: 10 (default)")
print("resolution_poll_cap_seconds: 300 (default, 5 minutes)")
print("After cap: deferred=True, resets to initial poll interval")
print()

# Check if any slot ever reached 'closed' state
print("GAMMA RESOLUTION CHECK")
print("======================")
print("No slot_settled events found in ledger.")
print("No market.settled events found in ledger.")
print()
print("Possible causes:")
print("1. Gamma API never returns closed=true for these markets")
print("2. get_winning_outcome() returns None (outcome_prices not reaching exactly 1.0)")
print("3. Bot restarts break pending resolution continuity")
print("4. Polling cap too short relative to Gamma update frequency")
print()

# Check for restart patterns
print("RESTART PATTERN CHECK")
print("====================")
run_ids = defaultdict(list)
for evt in pending_events + resolved_events + settlement_events:
    run_id = evt.get("run_id", "unknown")
    ts = evt.get("ts", 0)
    run_ids[run_id].append(ts)

for run_id, timestamps in sorted(run_ids.items())[:15]:
    print(f"Run {run_id}: {len(timestamps)} events, range {min(timestamps):.0f} - {max(timestamps):.0f}")

print(f"\nTotal unique run IDs: {len(run_ids)}")
print()

# Check the last event timestamp vs now
import time
now = time.time()
if all_slots_info:
    last_slot_event = max(info["last_event_ts"] for info in all_slots_info)
    print(f"Last pending event was {now - last_slot_event:.0f} seconds ago (~{(now - last_slot_event)/3600:.1f} hours)")
