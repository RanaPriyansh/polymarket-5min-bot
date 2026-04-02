# Polymarket Bot First-Principles Rebuild Plan

> For Hermes: execute this as a phased rebuild. Do not enable live trading until Phase 5 exit criteria are met.
> For Codex: implement isolated modules behind tests and small commits. No broad refactors without passing phase gates.

Goal
Rebuild the bot from first principles so paper trading becomes trustworthy again, then run 2-3 days of stable paper trading, then use runtime research to improve edge and reduce risk before any live canary.

Architecture
Split the system into 5 hard boundaries:
1. Market discovery
2. Order book snapshot validation
3. Paper execution ledger
4. Risk and exposure engine
5. Runtime research and replay

The contrarian point: the current repo is not mainly broken because “a strategy is weak.” It is broken because the execution truth layer is weak. If orders, fills, positions, settlements, and exposure are not trustworthy, strategy iteration is theater.

Tech stack
- Python 3.12
- Existing repo as scaffold only
- SQLite for durable local state
- JSONL/markdown only for observability artifacts, not source-of-truth state
- systemd for always-on runtime

Current hard findings
- Repo updated to latest origin/main.
- Paper service is currently running and discovering 8 interval markets.
- Live mode is explicitly blocked in cli.py.
- run --strategies defaults to toxicity_mm, so config active strategies are ignored unless explicitly passed.
- Paper execution/risk/settlement persistence is not trustworthy enough for live decisions.
- Autoresearch runs, but outputs are shallow.

Phase 0 — Freeze and simplify
Owner: Hermes
Objective: stop pretending the current stack is production-grade and define a clean baseline.

Tasks
1. Keep live mode blocked.
2. Treat current paper runtime as observational only.
3. Use only one baseline strategy for paper rebuild: toxicity_mm.
4. Demote mean_reversion/opening_range/time_decay to research candidates.
5. Archive current runtime artifacts before major rewrites.

Acceptance
- No code path can place live orders.
- One baseline strategy only in rebuild scope.
- Runtime artifacts archived and preserved for comparison.

Phase 1 — Rebuild market discovery contract
Owner: Codex
Objective: create a strict typed discovery layer that returns explicit reason codes instead of loose dicts.

Files
- Create: interval_market_discovery.py
- Create: tests/test_interval_market_discovery.py
- Modify later: cli.py, market_data.py

Build
- RequestedSlot model:
  - asset
  - interval_minutes
  - slot_start_ts
  - expected_slug
- DiscoveredMarket model:
  - market_id
  - condition_id
  - slug
  - asset
  - interval_minutes
  - slot_start_ts
  - end_ts
  - outcomes
  - token_ids
  - market flags
  - fallback_distance_windows
- DiscoveryResult model with statuses:
  - found_exact
  - found_fallback
  - missing
  - invalid_shape
  - stale
  - closed
  - not_accepting_orders
  - no_orderbook
  - parse_error

Tests
- slug roundtrip
- exact-slot validation
- fallback-slot validation
- malformed payload rejection
- missing token IDs rejection
- wrong end_ts rejection

Acceptance
- One result per expected slot, always.
- Every rejection has an explicit reason code.
- Discovery works for all 8 expected current markets.

Phase 2 — Rebuild order book validation and collector
Owner: Codex
Objective: make book snapshots trustworthy and backtest-compatible.

Files
- Create: orderbook_snapshot.py
- Create: tests/test_orderbook_snapshot.py
- Replace: scripts/collector.py
- Create: data schema doc under docs/

Build
- Validate both token books belong to the same condition/market.
- Validate timestamps are within tolerance.
- Validate both sides and outcome mappings.
- Emit normalized snapshot rows that can be replayed later.
- Stream to disk incrementally; no giant in-memory record list.

Collector output must include
- ts
- asset
- interval_minutes
- slot_id
- market_id
- market_slug
- outcome
- best_bid
- best_ask
- mid_price
- spread_bps
- top_depth
- top_notional
- imbalance
- seconds_to_expiry
- reject_reason if not tradeable

Acceptance
- Collector runs continuously without unbounded memory growth.
- Output schema matches replay engine expectations exactly.
- Snapshot validation catches mismatched or malformed books.

Phase 3 — Rebuild paper execution ledger, replay, fill semantics, and settlement
Owner: Codex
Objective: replace the fake paper portfolio with an append-only event ledger plus deterministic replay and conservative fill rules.

Files
- Create: ledger.py
- Create: paper_exchange.py
- Create: settlement_engine.py
- Create: tests/test_ledger.py
- Create: tests/test_paper_exchange.py
- Create: tests/test_settlement_engine.py
- Modify: execution.py or replace with thin adapter

Build
- Start from the contract in docs/design/ledger-and-replay-contract.md.
- SQLite-backed append-only ledger_events table as source of truth.
- Projection tables only as rebuildable caches.
- Explicit event types for run, slot, order, fill, position, settlement, and risk lifecycle.
- Idempotency keys for every externally or internally observed transition.
- Deterministic replay engine that rebuilds order state, position state, settlement state, and exposure state from ledger only.
- Conservative deterministic fill model implemented at the same time as the ledger.
- No fill after market close.
- No same-snapshot magic fill unless explicitly enabled for tests.
- Pending resolution survives restart.

Acceptance
- Restart replay reproduces state exactly from ledger.
- Replaying the same ledger twice yields identical projections.
- Settlements survive restart and apply idempotently.
- Fill simulation is deterministic for a fixed snapshot stream.
- Only after these pass is the executor core considered trustworthy.

Phase 4 — Rebuild risk from binary-market first principles
Owner: Codex
Objective: compute actual exposure from ledger-backed projections, not fake comfort metrics.

Files
- Create: exposure.py
- Create: tests/test_exposure.py
- Modify: risk.py

Build
- Condition-level exposure, not naive signed inventory only.
- Risk must include:
  - open orders notional reserved
  - resting quote inventory
  - slot-level net exposure
  - market-level exposure
  - asset bucket exposure
  - interval bucket exposure
  - pending settlement exposure
  - available cash
  - reserved cash
  - gross exposure
  - net directional exposure
  - max loss at resolution
- Hard blocks for:
  - max same-direction exposure
  - max per-asset exposure
  - max unresolved positions
  - stale data / stale clock / stale books

Acceptance
- Any portfolio snapshot produces a worst-case loss number by slot, market, asset, and interval bucket.
- Orders breaching limits are rejected before submission.
- Risk reports reconcile to ledger-backed projections after replay.
- Risk state persists across restart.

Phase 5 — Rewire runtime around the new core
Owner: Hermes
Objective: make the bot run continuously on a clean service path using the rebuilt modules.

Files
- Modify: cli.py
- Modify: deploy/systemd/install.sh
- Modify: deploy/systemd/polymarket-paper-bot.service
- Create: docs/runbooks/paper-runtime.md

Build
- run command should honor config strategies when --strategies is omitted.
- runtime status should reflect ledger truth, not in-memory estimates.
- service should point to one canonical path only.
- health outputs should expose:
  - last market discovery time
  - last book fetch time
  - open orders
  - open positions
  - pending settlements
  - bankroll
  - last research cycle

Acceptance
- systemd restart 10x test passes without state loss.
- Paper bot runs for 6+ hours without manual intervention.
- Status artifacts match SQLite truth.

Phase 6 — 2-3 day paper trading protocol
Owner: Hermes
Objective: collect real evidence before touching live.

Runtime policy
- Only toxicity_mm active initially.
- Conservative caps.
- No more than one config family change per day.

Daily loop
1. Export last 24h summary.
2. Review adverse selection, fill quality, directional concentration, settlement reliability.
3. Change only one variable family:
   - filters OR
   - quote width OR
   - exposure caps
4. Run another full day.

Core metrics
- PnL per filled order
- PnL per market touched
- fill rate by asset/interval/time-to-expiry
- adverse selection after 5s/15s/30s
- skip reasons with counterfactual analysis
- same-direction concentration
- pending-settlement backlog
- stale-data incidents

Acceptance
- 2-3 consecutive days with:
  - no state loss
  - no unresolved settlement leak
  - stable runtime
  - bounded concentration risk
  - positive or at least interpretable expectancy after conservative assumptions

Phase 7 — Research loop v2
Owner: Hermes
Objective: make autoresearch produce decision-grade outputs.

Files
- Modify: research/polymarket.py
- Modify: research/loop.py
- Create: tests/test_runtime_research.py

Research questions
- Which asset/interval/regime buckets are positive expectancy?
- Which skip reasons protect edge versus block good trades?
- Which quote widths improve fill quality net of adverse selection?
- Where is PnL concentrated?
- What is the worst clustered loss profile?

Outputs
- keep/kill strategy recommendations
- parameter delta recommendations with sample size and confidence
- no auto-apply until confidence and sample thresholds are met

Acceptance
- Research outputs cite real sample sizes and regime slices.
- Recommendations are traceable to runtime data, not generic summaries.

Hermes / Codex split
Hermes
- Owns phase gates, runtime ops, service management, and daily review.
- Owns research synthesis and decision-making.
- Owns rollout and rollback discipline.

Codex
- Owns isolated module rewrites with tests.
- Owns small commits and exact file-level changes.
- Should not touch service rollout or live settings without Hermes approval.

Immediate next execution sequence
Completed
1. CLI strategy-selection bug fixed in this checkout.
2. Plain pytest green in this checkout.

Active
1. Write and approve the ledger/replay/fill contract before coding.
2. Build SQLite ledger for paper execution, restart replay, and settlement from that contract.
3. Move risk onto persisted exposure including open orders, resting quote inventory, slot-level net exposure, pending settlements, and worst-case loss by market/asset bucket.
4. Implement conservative deterministic fill semantics together with the ledger, not as a later cleanup.
5. Replace collector with canonical replay-compatible schema.
6. Add discovery reason codes after the execution core is trustworthy.
7. Rewire runtime status to ledger truth.
8. Start clean 2-3 day paper run with one baseline strategy only after phase gates pass.
9. Upgrade autoresearch after evidence exists.

Do not do yet
- Do not enable live orders.
- Do not add more strategies.
- Do not trust old performance claims.
- Do not optimize parameters before the ledger and replay pipeline are credible.
