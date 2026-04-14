# POLYMARKET PAPER BOT — FORENSIC COURSE-CORRECT SPEC
# Version: 1.0.0 | Date: 2026-04-11 | Author: Hermes Forensic Lead

---

## RUNTIME REALITY CHECK (basis for this spec)

Verified from live artifacts before writing this document:

- 74 distinct run_ids in ledger.db — experiment is fragmented across 74 mini-runs
- Restart=always in systemd unit — circuit breaker stops are silently eaten, bot restarts immediately
- circuit_breaker_dd threshold = 10% drawdown
- 81 service restart/circuit events in the last 24 hours
- slot_settled schema has: market_id, market_slug, settled_ts, winning_outcome — NO realized_pnl
- win_rate = 3.07% (2 wins, 63 losses out of 65 resolved trades)
- realized_pnl_total = +$32 but this is FILL pnl only, not settlement pnl — settlement schema proves this
- pnl_per_fill = $0.067 looks positive but is contradicted by 3% win_rate
- autoresearch says "confidence=0.7, keep toxicity_mm active" — this is storytelling not verification
- contradiction-first gate: does not exist — autoresearch emits recommendations without any gate
- compiler bridge: not implemented
- fill_applied schema contains: side, outcome, slot_id, strategy_family, fill_price, fill_size — usable for attribution
- slot_settled schema does NOT contain position_pnl, entry_price, hold_duration — NOT usable for attribution
- open_positions = 14-15, 8-10 of them unmarked at any given time

---

## 1. ARCHITECTURE CONTRACTS

### 1A. RESTART SEMANTICS CONTRACT

Current state: BROKEN
systemd Restart=always means circuit breaker is meaningless. Every intentional stop
becomes a fresh run with bankroll reset to 500. Run lineage is 74 fragments.

Required behavior:

  CIRCUIT BREAKER STOP:
    - bot exits with code 2
    - systemd must NOT restart on code 2
    - service remains stopped until operator manually reviews and restarts
    - forensic snapshot must be written before exit (see 1C)

  CRASH/FAILURE STOP:
    - bot exits with non-zero code != 2
    - systemd restarts after RestartSec
    - run_id changes, bankroll continues from last known state (NOT reset to 500)

  OPERATOR STOP:
    - systemctl stop kills with SIGTERM
    - bot handles SIGTERM gracefully: flush artifacts, write forensic snapshot
    - exit code 0
    - systemd does not restart on code 0

  BANKROLL CONTINUITY:
    - on any non-operator restart, bankroll MUST be read from last risk_snapshot_recorded
    - bankroll must NEVER reset to config default on auto-restart
    - if no prior snapshot exists, only then use config default

Implementation contract:
  - cli.py: SystemExit(2) on circuit breaker trigger
  - deploy/systemd/polymarket-paper-bot.service:
      SuccessExitStatus=0
      RestartPreventExitStatus=2
      Restart=on-failure
      RestartSec=15

### 1B. CONTRADICTION-FIRST GATE CONTRACT

Current state: NOT IMPLEMENTED — autoresearch emits freely with no gate

The contradiction-first gate is a stateful check that runs BEFORE any autoresearch
cycle emits an experiment or recommendation. It inspects runtime truth against prior
claims and produces one of three states:

  GREEN:
    - All of the following are true:
      - win_rate >= 0.40 across >= 50 settled trades with pnl attribution
      - realized_settlement_pnl > 0 (not fill_pnl — settlement pnl with winning_outcome known)
      - no open contradiction between research claim and ledger fact
      - run lineage: single uninterrupted run_id for >= 2 hours
    - Gate allows: one evidence-backed experiment recommendation

  YELLOW:
    - Any of the following is true:
      - win_rate between 0.20 and 0.40
      - fewer than 50 settled trades with pnl attribution
      - realized_settlement_pnl unknown (schema missing)
      - run lineage: <= 3 run_ids in last 2 hours
    - Gate allows: one low-confidence hypothesis only — no experiment recommendation
    - Hypothesis MUST list the specific contradictions that put it in YELLOW

  RED:
    - Any of the following is true:
      - win_rate < 0.20 with >= 20 settled trades
      - realized_settlement_pnl is not computable (schema gap exists)
      - contradiction detected: autoresearch prior claim contradicts current ledger fact
      - run lineage: >= 4 run_ids in last 2 hours (fragmented experiment)
      - circuit breaker fired and service restarted automatically
    - Gate allows: NO experiment, NO recommendation
    - Gate MUST emit: contradiction report listing each contradiction found
    - Gate MUST block: any experiment, any hypothesis with confidence > 0

CURRENT GATE STATE: RED
Reasons:
  1. win_rate = 3.07% with 65 settled trades — below RED threshold
  2. slot_settled schema has no realized_pnl — settlement pnl is not computable
  3. 74 run_ids in total, ~12 run_ids in last 24h — fragmented experiment
  4. Restart=always means circuit breaker was ignored
  5. autoresearch emitted "confidence=0.7, keep active" — contradicts ledger evidence

### 1C. FORENSIC SNAPSHOT CONTRACT

A forensic snapshot must be written:
  - Before any circuit breaker exit
  - Before any clean operator stop
  - When gate transitions RED

Snapshot directory: data/forensic-snapshots/{ISO_UTC_TIMESTAMP}/

Files required in snapshot:
  - status.json (copy)
  - strategy_metrics.json (copy)
  - events.jsonl.tail — last 2000 lines of events.jsonl
  - ledger.db (copy)
  - market_samples.jsonl.tail — last 500 lines
  - latest-status.txt (copy)
  - data/research/latest.json (copy)
  - data/research/latest.md (copy)
  - manifest.json:
    {
      "snapshot_ts": UTC ISO string,
      "trigger": "circuit_breaker" | "operator_stop" | "gate_red" | "manual",
      "run_id": current run_id,
      "bankroll": float,
      "realized_pnl": float,
      "win_rate": float,
      "resolved_trade_count": int,
      "gate_state": "GREEN" | "YELLOW" | "RED",
      "artifacts": [{ "source": str, "dest": str, "mtime": float, "size_bytes": int }]
    }

### 1D. DATA COLLECTION CONTRACT

Every event written to ledger.db or events.jsonl MUST include:
  - run_id (string, matches current process run_id)
  - event_ts (float, unix timestamp UTC)
  - event_type (string, from controlled vocabulary below)

Controlled vocabulary for event_type:
  order_created, order_acknowledged, order_cancelled,
  fill_observed, fill_applied,
  slot_resolution_pending, slot_settled, slot_closed,
  risk_snapshot_recorded, circuit_breaker_triggered,
  service_restart, research_cycle_completed,
  contradiction_detected, gate_state_changed

Additional required fields per event_type (see schema section 2).

### 1E. AUTORESEARCH COMPILER BRIDGE CONTRACT

Currently: research/loop.py reads runtime artifacts and emits JSON without any
structured schema or gate check.

Required behavior:

  Step 1: Gate check
    - compute current gate state (GREEN/YELLOW/RED)
    - if RED: write contradiction_report only, stop
    - if YELLOW: may write one hypothesis with confidence < 0.5
    - if GREEN: may write one experiment recommendation

  Step 2: Evidence compilation
    - read only: events.jsonl, ledger.db, strategy_metrics.json, status.json
    - compute: win_rate, realized_settlement_pnl, fill_markout by horizon,
               loss_cluster patterns, win_cluster patterns
    - emit: research_cycle_completed event to ledger

  Step 3: Output schema (see section 3E)
    - compiler output is a typed JSON, not free-form text
    - each field is required or explicitly null

  No recommendation may be emitted that is not backed by a specific ledger fact
  with event_ts, event_type, and count.

### 1F. TRADE ATTRIBUTION CONTRACT

For every settled slot, the system MUST be able to answer:
  - What was the entry price (average_fill_price)?
  - What was the total filled quantity?
  - What was the winning outcome?
  - Did this position win or lose?
  - What was the realized settlement pnl?
  - What was the inventory direction at settlement time?

This requires that slot_settled events include position data.
Currently: slot_settled schema MISSING realized_pnl — this is a P0 bug.
The slot_settled event must be augmented (see schema section 2).

---

## 2. NORMALIZED SCHEMA

All tables are conceptual. Implementation uses ledger.db (SQLite) + JSONL side files.
Ledger events store payload as JSON in payload_json column.

### TABLE: runs
  run_id          TEXT PRIMARY KEY   -- e.g. paper-1775879952-056c7bf9
  started_ts      REAL               -- unix UTC
  ended_ts        REAL               -- null if active
  end_reason      TEXT               -- null | circuit_breaker | operator_stop | crash
  starting_bankroll  REAL
  ending_bankroll    REAL            -- null if active
  restart_count   INTEGER DEFAULT 0  -- how many auto-restarts this run had

### TABLE: run_segments
  segment_id      TEXT PRIMARY KEY   -- uuid
  run_id          TEXT               -- FK to runs
  started_ts      REAL
  ended_ts        REAL
  bankroll_start  REAL
  bankroll_end    REAL
  end_reason      TEXT               -- circuit_breaker | crash | operator_stop | continues

### TABLE: orders
  order_id        TEXT PRIMARY KEY   -- evt order id
  run_id          TEXT
  market_id       TEXT
  market_slug     TEXT
  slot_id         TEXT               -- asset:interval:slot_ts
  strategy_family TEXT
  side            TEXT               -- BUY | SELL
  outcome         TEXT               -- Up | Down
  price           REAL
  size            REAL
  status          TEXT               -- resting | filled | cancelled
  created_ts      REAL
  acknowledged_ts REAL
  terminal_ts     REAL               -- when filled or cancelled

### TABLE: fills
  fill_id         TEXT PRIMARY KEY   -- observed_event_id
  order_id        TEXT               -- FK to orders
  run_id          TEXT
  market_id       TEXT
  market_slug     TEXT
  slot_id         TEXT
  strategy_family TEXT
  side            TEXT
  outcome         TEXT
  fill_price      REAL
  fill_size       REAL
  filled_qty_cumulative  REAL
  remaining_qty   REAL
  status          TEXT               -- partially_filled | filled
  fill_ts         REAL
  -- attribution fields (computed at write time)
  time_to_expiry_seconds  REAL      -- slot_end_ts - fill_ts
  tte_bucket      TEXT               -- <60s | 60-120s | 120-300s | >300s
  mid_price_at_fill  REAL            -- from market_samples.jsonl at nearest ts
  spread_at_fill  REAL               -- best_ask - best_bid at fill_ts
  depth_at_fill   REAL               -- sum of top 3 bid levels notional
  imbalance_at_fill  REAL            -- (bid_depth - ask_depth) / (bid_depth + ask_depth)

### TABLE: positions
  position_key    TEXT PRIMARY KEY   -- strategy_family:market_id:outcome
  run_id          TEXT
  market_id       TEXT
  market_slug     TEXT
  slot_id         TEXT
  strategy_family TEXT
  outcome         TEXT
  quantity        REAL               -- signed: positive=long, negative=short
  average_price   REAL
  notional        REAL               -- abs(quantity) * average_price
  opened_ts       REAL
  closed_ts       REAL               -- null if open
  status          TEXT               -- open | settled | closed_zero_exposure

### TABLE: settlements
  settlement_id   TEXT PRIMARY KEY   -- uuid
  run_id          TEXT
  market_id       TEXT
  market_slug     TEXT
  slot_id         TEXT
  strategy_family TEXT
  position_key    TEXT               -- FK to positions
  winning_outcome TEXT
  position_outcome TEXT              -- the outcome this position held
  position_size   REAL               -- quantity at settlement
  entry_price     REAL               -- average_fill_price at settlement
  settlement_price  REAL             -- 1.0 if win, 0.0 if loss
  realized_pnl    REAL               -- (settlement_price - entry_price) * position_size
  hold_duration_seconds  REAL        -- settled_ts - opened_ts
  settled_ts      REAL
  is_win          INTEGER            -- 1 if realized_pnl > 0, 0 otherwise

### TABLE: market_samples
  sample_id       TEXT PRIMARY KEY
  run_id          TEXT
  market_id       TEXT
  market_slug     TEXT
  slot_id         TEXT
  sample_ts       REAL
  mid_price       REAL
  best_bid        REAL
  best_ask        REAL
  spread          REAL
  bid_depth_top3  REAL
  ask_depth_top3  REAL
  imbalance       REAL
  time_to_expiry  REAL               -- slot_end_ts - sample_ts

### TABLE: risk_snapshots
  snapshot_id     TEXT PRIMARY KEY
  run_id          TEXT
  snapshot_ts     REAL
  bankroll        REAL
  realized_pnl    REAL
  unrealized_pnl  REAL
  peak_bankroll   REAL
  max_drawdown    REAL
  open_positions  INTEGER
  marked_positions INTEGER
  unmarked_positions INTEGER
  gross_exposure  REAL

### TABLE: status_snapshots
  (derived from events.jsonl risk_snapshot_recorded events — no separate table needed)
  source: risk_snapshot_recorded events with full payload

### TABLE: research_cycles
  cycle_id        TEXT PRIMARY KEY
  run_id          TEXT
  cycle_ts        REAL
  gate_state      TEXT               -- GREEN | YELLOW | RED
  contradictions  TEXT               -- JSON array of contradiction strings
  output_type     TEXT               -- none | hypothesis | experiment
  hypothesis_id   TEXT               -- null if none
  confidence      REAL               -- null if RED
  backed_by       TEXT               -- JSON: {event_type, count, ts_range}

### TABLE: service_restarts
  restart_id      TEXT PRIMARY KEY
  prior_run_id    TEXT
  new_run_id      TEXT
  restart_ts      REAL
  trigger         TEXT               -- circuit_breaker | crash | operator | unknown
  prior_bankroll  REAL
  new_bankroll    REAL
  bankroll_delta  REAL               -- should be 0 on auto-restart, -N if reset to default

### TABLE: contradiction_log
  contradiction_id  TEXT PRIMARY KEY
  detected_ts       REAL
  run_id            TEXT
  cycle_id          TEXT
  claim_source      TEXT             -- which research cycle made the claim
  claim_text        TEXT
  ledger_fact       TEXT             -- the specific counter-evidence from ledger
  ledger_event_type TEXT
  ledger_event_count INTEGER
  resolution        TEXT             -- unresolved | retracted | confirmed_correct

---

## 3. REQUIRED ATTRIBUTION OUTPUTS

### 3A. PNL BY ASSET / INTERVAL / SIDE

Source: settlements table (requires schema fix to slot_settled)
Fields: asset, interval_minutes, side (BUY/SELL), outcome (Up/Down)

Report columns:
  asset | interval | side | outcome | trade_count | win_count | loss_count |
  win_rate | total_pnl | avg_pnl_per_trade | avg_hold_seconds

Required rows:
  - One row per (asset, interval, side, outcome) combination
  - Subtotals per asset, per interval
  - Grand total

Gate requirement: only emit this report when settlements.count >= 20

### 3B. FILL MARKOUTS BY HORIZON

Source: fills table JOIN market_samples at matching (slot_id, nearest ts)

For each fill, compute:
  markout_30s   = mid_price_at_fill_ts + 30s  - fill_price  (positive = filled price was good)
  markout_60s   = mid_price_at_fill_ts + 60s  - fill_price
  markout_120s  = mid_price_at_fill_ts + 120s - fill_price
  markout_300s  = mid_price_at_fill_ts + 300s - fill_price
  markout_final = settlement_price            - fill_price

Report columns:
  side | outcome | tte_bucket | fill_count | avg_markout_30s | avg_markout_60s |
  avg_markout_120s | avg_markout_300s | avg_markout_final | adverse_selection_score

adverse_selection_score: percentage of fills where markout_60s < 0 (filled into adverse move)

### 3C. TIME-TO-EXPIRY BUCKETS

Source: fills table, tte_bucket field

Buckets:
  tte_<60s    (last minute — high-urgency fills, likely stale quotes)
  tte_60-120s
  tte_120-300s
  tte_>300s   (early in slot — normal market-making window)

Report per bucket:
  fill_count | avg_fill_price | avg_final_markout | win_rate | pnl_contribution

Hypothesis to test: are tte_<60s fills disproportionately losers?
This distinguishes stale-quote adverse selection from normal MM activity.

### 3D. SPREAD / DEPTH / IMBALANCE BUCKETS

Source: fills JOIN market_samples

Spread buckets: <0.02 | 0.02-0.05 | 0.05-0.10 | >0.10
Depth buckets: <$5 | $5-$20 | $20-$50 | >$50
Imbalance buckets: <-0.3 (bid-heavy) | -0.3 to 0.3 (neutral) | >0.3 (ask-heavy)

Report per bucket:
  fill_count | avg_fill_price | avg_final_markout | win_rate

Purpose: identify which book conditions produce wins vs losses.
A strategy filling into thin books at wide spreads with high imbalance is
systematically losing to informed flow.

### 3E. INVENTORY-STATE BUCKETS

At each fill time, compute:
  inventory_direction = sign of current net position in that slot
    (+1 = long, -1 = short, 0 = flat)
  inventory_magnitude = abs(notional_at_fill_time)

Report: win_rate and pnl by (inventory_direction, inventory_magnitude_bucket)

Purpose: distinguish inventory skew losses from adverse selection losses.
If short positions systematically lose more than longs, this is directional bias.
The current strategy ONLY quotes Up outcome (confirmed bug) — this will show as
100% long inventory with 3% win rate.

### 3F. LOSS CLUSTERS REPORT

Definition: a loss cluster is a set of >= 3 consecutive settled losses within
the same 60-minute window.

For each cluster:
  - cluster start/end ts
  - assets involved
  - market regime at cluster start (Up/Down outcome distribution from slot_settled)
  - avg fill price at cluster fills
  - book imbalance at cluster fills
  - hypothesis: adverse selection | stale quotes | regime mismatch | unknown

### 3G. WIN CLUSTERS REPORT

Same structure as loss clusters but for >= 3 consecutive wins.
If win clusters exist, identify what was different:
  - tte_bucket, spread_bucket, imbalance_bucket distribution vs overall

### 3H. CIRCUIT BREAKER PRECURSOR REPORT

For every forensic snapshot with trigger=circuit_breaker:
  - bankroll at T-60m, T-30m, T-15m, T-5m, T=0
  - unrealized pnl trend in same windows
  - open position count in same windows
  - what assets / slots were open at trigger time
  - last 10 fills before trigger: side, outcome, fill_price, markout if available
  - was drawdown due to unrealized positions or realized losses?

---

## 4. CONTRADICTION-FIRST GATE — EXACT RULES

### 4A. Inputs required for gate computation

  gate_inputs = {
    win_rate: float from status.json (win_count / resolved_trade_count),
    resolved_count: int from status.json resolved_trade_count,
    settlement_pnl_computable: bool (True if slot_settled has realized_pnl),
    run_lineage_fragmentation: int (distinct run_ids in last 2 hours),
    circuit_breaker_fired_unreviewed: bool (any forensic snapshot with unreviewed cb trigger),
    prior_research_claims: list[dict] from research_cycles table,
    current_ledger_facts: {
      win_count: int,
      loss_count: int,
      slot_settled_count: int,
      fill_applied_count: int,
    },
    contradiction_log_open: int (unresolved contradictions in contradiction_log)
  }

### 4B. Gate computation (exact logic, no ambiguity)

  function compute_gate_state(inputs) -> (state, reasons):

    RED conditions (any one triggers RED):
      - inputs.win_rate < 0.20 AND inputs.resolved_count >= 20
      - inputs.settlement_pnl_computable == False
      - inputs.run_lineage_fragmentation >= 4
      - inputs.circuit_breaker_fired_unreviewed == True
      - inputs.contradiction_log_open >= 1

    YELLOW conditions (any one triggers YELLOW, if not RED):
      - inputs.resolved_count < 50
      - inputs.win_rate < 0.40
      - inputs.run_lineage_fragmentation >= 2

    GREEN: none of the above apply

    return state, list of triggered conditions as strings

### 4C. Gate emission rules

  RED:
    - write contradiction_report.json to data/research/contradiction-{ts}.json
    - write gate_state=RED to research_cycles record
    - DO NOT write any hypothesis or experiment to latest.json
    - latest.json MUST contain gate_state=RED and contradiction list
    - system may NOT emit: recommendation, experiment, confidence score

  YELLOW:
    - MAY write one hypothesis to latest.json
    - hypothesis MUST include:
        - gate_state: YELLOW
        - contradictions: list of YELLOW trigger reasons
        - confidence: must be < 0.5
        - backed_by: specific event_type, count, ts_range from ledger
    - MUST NOT include: recommendation to deploy, recommendation to keep active

  GREEN:
    - MAY write one experiment to latest.json
    - experiment MUST include:
        - gate_state: GREEN
        - hypothesis_id: string
        - evidence: {win_rate, resolved_count, realized_settlement_pnl}
        - confidence: 0.0 - 1.0
        - recommendation: specific action
        - backed_by: specific ledger facts

---

## 5. ACCEPTANCE CRITERIA FOR CODEX

The following are the exact file-level acceptance criteria.
Each criterion is pass/fail. All must pass before implementation is complete.

### AC-1: Restart semantics — service file
File: deploy/systemd/polymarket-paper-bot.service

PASS when:
  - grep 'Restart=' shows: Restart=on-failure
  - grep 'RestartPreventExitStatus' shows: RestartPreventExitStatus=2
  - grep 'SuccessExitStatus' shows: SuccessExitStatus=0

FAIL if: Restart=always is present anywhere in the file

### AC-2: Circuit breaker exit code
File: cli.py or execution.py (wherever circuit breaker is triggered)

PASS when:
  - the circuit breaker code path executes: sys.exit(2)  (not sys.exit(1) or raise)
  - journalctl shows: "circuit_breaker" in logs followed by service NOT restarting
  - a new run_id does NOT appear in ledger.db within 60s of circuit breaker exit

FAIL if: service restarts within 30s of a circuit breaker trigger

### AC-3: Bankroll continuity on crash restart
File: cli.py run command startup path

PASS when:
  - when cli.py starts and ledger.db exists with prior risk_snapshot_recorded events,
    it reads the most recent bankroll from that snapshot
  - bankroll in new run's first risk_snapshot_recorded matches prior run's last bankroll
  - config paper_starting_bankroll is NOT used when prior snapshot exists

FAIL if: bankroll resets to 500.0 on any auto-restart

### AC-4: slot_settled schema — pnl attribution
File: execution.py (wherever slot_settled event is written to ledger)

PASS when slot_settled payload contains all of:
  market_id, market_slug, slot_id, settled_ts, winning_outcome,
  position_outcome,    (the outcome this strategy held)
  position_size,       (quantity at settlement)
  entry_price,         (average_fill_price of all fills for this position)
  realized_pnl,        (computed: if winning_outcome == position_outcome: (1.0 - entry_price) * position_size else (-entry_price) * position_size)
  is_win               (1 if realized_pnl > 0, else 0)

FAIL if: slot_settled payload in ledger.db does not contain realized_pnl

Verification SQL:
  SELECT json_extract(payload_json, '$.realized_pnl')
  FROM ledger_events
  WHERE event_type='slot_settled'
  LIMIT 5;
  -- must return non-NULL values

### AC-5: Contradiction-first gate
File: research/gate.py (new file)

PASS when:
  - gate.py exports: compute_gate_state(inputs: dict) -> (state: str, reasons: list[str])
  - compute_gate_state({win_rate: 0.03, resolved_count: 65, ...}) returns ('RED', reasons)
  - reasons list is non-empty and contains human-readable strings
  - research/loop.py calls compute_gate_state before emitting any output
  - when gate returns RED, latest.json contains gate_state=RED and NO experiments field
    (or experiments is an empty list)
  - when gate returns RED, contradiction_report-{ts}.json is written to data/research/

FAIL if:
  - research/loop.py emits experiments when gate_state is RED
  - latest.json contains recommendation when gate_state is RED

Test: python3 -m pytest tests/test_gate.py -v

### AC-6: Fill attribution fields on fill_applied events
File: execution.py (wherever fill_applied is written)

PASS when fill_applied payload contains:
  side, outcome, slot_id, strategy_family, fill_price, fill_size,
  market_id, market_slug,
  AND time_to_expiry_seconds is computed and stored (slot_end_ts - fill_ts)

FAIL if: time_to_expiry_seconds is missing from fill_applied payloads

Verification:
  sqlite3 data/runtime/ledger.db \
    "SELECT json_extract(payload_json,'$.time_to_expiry_seconds') FROM ledger_events WHERE event_type='fill_applied' LIMIT 5"
  -- must return non-NULL floats

### AC-7: Forensic snapshot on circuit breaker
File: cli.py or a new forensic.py module

PASS when:
  - triggering circuit breaker creates a directory: data/forensic-snapshots/{ISO_TS}/
  - directory contains manifest.json with trigger=circuit_breaker
  - directory contains copies of: status.json, ledger.db, strategy_metrics.json
  - system then calls sys.exit(2)

FAIL if: circuit breaker exits without writing forensic snapshot first

### AC-8: Run lineage in service_restart events
File: cli.py startup path

PASS when:
  - on startup, if ledger.db already contains events from a prior run_id,
    a service_restart event is written with:
      prior_run_id, new_run_id, restart_ts, trigger (circuit_breaker | crash | operator | unknown),
      prior_bankroll (from last risk_snapshot_recorded), new_bankroll
  - service_restart events are queryable:
    SELECT COUNT(*) FROM ledger_events WHERE event_type='service_restart'

FAIL if: 74 run_ids exist in ledger.db with no service_restart events linking them

### AC-9: Gate called before every research cycle
File: research/loop.py

PASS when:
  - every call to run_cycle() or equivalent emits a research_cycle_completed event
  - research_cycle_completed payload contains: gate_state, output_type, contradictions
  - output_type is one of: none | hypothesis | experiment
  - when gate_state=RED, output_type=none

FAIL if: research cycle runs and emits experiments without first calling gate check

### AC-10: Tests pass for gate and slot_settled schema
Files: tests/test_gate.py (new), tests/test_settlement_engine.py (existing, update)

PASS when:
  python3 -m pytest tests/test_gate.py tests/test_settlement_engine.py -v
  shows 0 failures

test_gate.py must include:
  test_red_on_low_win_rate()          -- win_rate=0.03, resolved=65 -> RED
  test_red_on_missing_settlement_pnl()  -- settlement_pnl_computable=False -> RED
  test_red_on_fragmentation()         -- run_lineage_fragmentation=5 -> RED
  test_yellow_on_low_count()          -- resolved=30, win_rate=0.4 -> YELLOW
  test_green_baseline()               -- all conditions met -> GREEN
  test_red_blocks_experiments()       -- RED gate returns empty experiments list

test_settlement_engine.py must include:
  test_slot_settled_has_realized_pnl()
  test_slot_settled_has_is_win()
  test_slot_settled_win_computation_correct()
  test_slot_settled_loss_computation_correct()

---

## 6. HIGHEST-PRIORITY IMPLEMENTATION RECOMMENDATION

ONE recommendation only. No others until this is done.

### P0: Fix Restart=always + circuit breaker exit code + slot_settled schema (one PR)

These three must ship together because:

1. Without Restart=on-failure + exit(2): every future circuit breaker is invisible.
   Run lineage will continue fragmenting. No experiment can be trusted.

2. Without slot_settled having realized_pnl: the gate stays RED permanently because
   settlement_pnl_computable=False. No research cycle can ever promote to GREEN.
   All autoresearch output is blocked and rightfully so.

3. They are coupled: once restart semantics are fixed and the gate is implemented,
   gate RED state will block autoresearch. Once slot_settled has pnl, the gate can
   transition to YELLOW when win rate recovers. This is the unlock sequence.

What the implementer must do (exact file targets):
  a) deploy/systemd/polymarket-paper-bot.service:
     - change Restart=always to Restart=on-failure
     - add RestartPreventExitStatus=2
     - add SuccessExitStatus=0

  b) cli.py — wherever circuit breaker fires:
     - write forensic snapshot to data/forensic-snapshots/{ISO_TS}/
     - call sys.exit(2)

  c) execution.py — slot_settled event writer:
     - add fields: position_outcome, position_size, entry_price, realized_pnl, is_win

  d) research/gate.py (new file):
     - implement compute_gate_state() per spec section 4B

  e) research/loop.py:
     - call gate before emitting any output
     - respect RED/YELLOW/GREEN emission rules

  f) tests/test_gate.py (new file):
     - 6 tests per AC-10

Estimated scope: 4-6 files, 200-300 lines of net new code.

Do NOT proceed to attribution reports, markout analysis, or strategy redesign
until AC-1 through AC-10 all pass.

---

END OF SPEC
