# Paper Trading Ledger and Replay Contract

Status
Approved design target for the executor-first rebuild. This document defines the source-of-truth contract before implementation.

Goal
Create a truthful paper-trading core where orders, fills, positions, pending settlements, and exposure can be rebuilt exactly from an append-only event ledger after restart.

Core principle
The ledger is the truth.
Everything else is a projection.
If a value cannot be reproduced by replaying events in order, it is not authoritative.

1. Source-of-truth vs projections

Source-of-truth tables
- ledger_events
  - append-only
  - immutable except operational metadata fields like ingested_at
- replay_checkpoints
  - optional optimization only
  - can be discarded and rebuilt from ledger_events

Projection tables
- order_state
- position_state
- settlement_state
- exposure_state
- runtime_health_state

Rule
- Projections are disposable caches.
- On corruption or mismatch, projections are dropped and rebuilt from ledger_events.

2. Required event envelope
Every event must contain:
- event_id: globally unique UUID
- stream: one of run, order, market_slot, settlement, risk
- aggregate_id: stable replay key within a stream
- event_type: defined enum
- event_ts: business timestamp
- recorded_ts: ingestion timestamp
- run_id: paper run identifier
- sequence_num: monotonic integer within aggregate_id
- idempotency_key: stable dedupe key
- causation_id: upstream event that caused this event, nullable
- correlation_id: ties related events in one action chain
- payload_json: canonical event payload
- schema_version: integer

Replay ordering
- Primary order: recorded_ts ascending
- Tie-breakers: aggregate_id ascending, sequence_num ascending, event_id ascending

3. Event types
Run lifecycle
- run_started
- run_recovered
- run_stopped
- replay_completed

Market / slot lifecycle
- slot_discovered
- slot_rejected
- slot_activated
- slot_expired
- slot_resolution_pending
- slot_settled

Order lifecycle
- order_created
- order_submitted
- order_acknowledged
- order_rejected
- order_cancel_requested
- order_cancelled
- order_expired

Fill lifecycle
- fill_observed
- fill_applied

Position lifecycle
- position_opened
- position_increased
- position_reduced
- position_closed

Risk lifecycle
- risk_snapshot_recorded
- risk_limit_breached
- order_blocked_by_risk

4. Idempotency keys
Rules
- Same real-world transition must map to the same idempotency key.
- Re-processing the same external or simulated observation must not alter final state after first application.

Recommended keys
- slot_discovered: discovered:{slot_id}:{market_id}
- order_created: order_created:{client_order_id}
- order_submitted: order_submitted:{client_order_id}
- order_acknowledged: order_ack:{venue_order_id_or_client_order_id}
- fill_observed: fill_obs:{order_id}:{fill_price}:{fill_size}:{fill_ts_bucket}
- fill_applied: fill_apply:{order_id}:{fill_observed_event_id}
- slot_resolution_pending: pending:{slot_id}:{poll_bucket}
- slot_settled: settled:{slot_id}:{winning_outcome}:{resolution_ts}
- risk_snapshot_recorded: risk:{run_id}:{snapshot_ts_bucket}

5. Replay invariants
After replay, the following must be exactly reproducible:
- open orders
- cancelled orders
- filled quantity per order
- average fill price per order
- open positions by slot / market / outcome / strategy family
- pending settlement queue
- settled markets and winning outcomes
- realized pnl
- reserved capital
- available capital
- worst-case open exposure

Hard invariants
- filled_qty(order) <= created_qty(order)
- cancelled + filled cannot exceed created size
- order state transitions must be valid
- position quantity must equal sum(applied fills and settlements) under replay rules
- reserved capital cannot be negative
- available capital + reserved capital + realized cash adjustments must reconcile to bankroll policy
- settlement application must be idempotent
- replaying the same ledger twice yields byte-identical projections

6. Conservative deterministic fill contract
This is part of the ledger design, not a later patch.

Goals
- deterministic
- conservative
- replayable from recorded snapshots
- simple enough to verify

Initial model v1
BUY order fills only if all are true:
- order status is acknowledged/open
- market not expired
- current best_ask exists
- order limit price >= best_ask
- order has rested for at least min_rest_seconds before becoming fill-eligible

SELL order fills only if all are true:
- order status is acknowledged/open
- market not expired
- current best_bid exists
- order limit price <= best_bid
- order has rested for at least min_rest_seconds before becoming fill-eligible

Additional conservative constraints
- max_fill_fraction_per_snapshot to prevent unrealistic full-size fills
- no fill on same snapshot as order creation unless explicitly configured
- no fills after slot expiry timestamp
- if cancel_requested before fill eligibility, order does not fill

Fill events
- fill_observed records the simulated market-cross observation
- fill_applied records the state mutation after dedupe and validation

Why two events
- separates market observation from accounting mutation
- makes replay/debugging cleaner

7. Exposure contract
Exposure must be derived from projections built from ledger events.
At minimum track:
- open orders notional reserved
- resting quote inventory by market/outcome
- slot-level net exposure
- market-level net exposure
- asset bucket exposure
- interval bucket exposure
- pending settlement exposure
- worst-case loss if all open slots resolve adversely

Required risk views
- per-slot worst-case payout
- per-market gross and net exposure
- per-asset gross and net exposure
- per-interval gross and net exposure
- total reserved capital
- total available capital
- unresolved settlement count and notional

8. Minimal physical schema
Table: ledger_events
- event_id TEXT PRIMARY KEY
- stream TEXT NOT NULL
- aggregate_id TEXT NOT NULL
- sequence_num INTEGER NOT NULL
- event_type TEXT NOT NULL
- event_ts REAL NOT NULL
- recorded_ts REAL NOT NULL
- run_id TEXT NOT NULL
- idempotency_key TEXT NOT NULL UNIQUE
- causation_id TEXT
- correlation_id TEXT
- schema_version INTEGER NOT NULL
- payload_json TEXT NOT NULL

Indexes
- (stream, aggregate_id, sequence_num)
- (run_id, recorded_ts)
- (event_type, recorded_ts)

Projection tables can be added later, but must be rebuildable from ledger_events.

9. Acceptance gates before paper run
Gate A: Replay truth
- Deleting projections and replaying ledger recreates exact state.
- Restart replay reproduces open orders, positions, settlements, and bankroll exactly.

Gate B: Settlement truth
- Pending settlements survive restart.
- Applying the same settlement twice changes nothing after first application.

Gate C: Fill truth
- Fill simulation is deterministic for a fixed sequence of snapshots.
- No same-tick magic fills unless explicitly enabled for tests.
- No post-expiry fills.

Gate D: Risk truth
- Risk report includes open orders, resting quotes, slot exposure, pending settlements, and worst-case loss buckets.
- Worst-case exposure reconciles with positions and open orders.

Gate E: Run readiness
- Only after A-D pass do we start the 2-3 day paper run.

10. Anti-patterns forbidden in implementation
- Persisting raw in-memory dicts as the primary state model
- Mutable order/position truth without corresponding ledger event
- Computing bankroll from ad hoc runtime counters
- Applying fills directly without fill_observed/fill_applied separation
- Risk computed only from realized pnl
- Projection-only state with no replay path

Implementation sequence
1. Create ledger_events schema and writer
2. Define event dataclasses / payload schemas
3. Implement replay engine from ledger only
4. Implement order projection
5. Implement fill contract and fill application
6. Implement position / settlement projections
7. Implement exposure / risk projections
8. Add restart recovery tests
9. Replace in-memory executor truth with projections
