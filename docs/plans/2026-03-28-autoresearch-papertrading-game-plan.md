# Autoresearch + Always-On Paper Trading Game Plan

> For Hermes: use superpowers discipline on execution. Brainstorm first, then write plan, then execute via bounded subagents with TDD and review gates.

Goal: turn the canonical Polymarket repo into an always-on paper-trading machine for live 5-15 minute crypto markets while also turning the new autoresearch loop into a domain-agnostic operating system agents can learn once and apply to trading, marketing, websites, and model evaluation.

Architecture: do not start with a pretty dashboard or a giant orchestration framework. First build the telemetry substrate and always-on paper runtime so the system emits trustworthy evidence. Then build the strategy registry and research-comparison layer on top of that evidence. Finally expose the operator surface and promotion pipeline that lets autoresearch safely improve the running system.

Tech stack: Python, Click, YAML, unittest, pandas, structured JSON events, SQLite/Postgres for hot state, markdown/json research artifacts, lightweight dashboard/API, reusable subagent contracts.

---

## Strategic principle

The hidden truth is this:

If paper trading is unstable or unobservable, autoresearch will optimize noise.
If dashboard work comes first, UI will freeze the wrong data model.
If agents learn only Polymarket-specific autoresearch, we lose the monopoly.

So the order is:
1. truth-producing runtime
2. truth-preserving telemetry
3. truth-seeking research OS
4. truth-compressing dashboard
5. truth-governed promotion of new strategies

---

## North-star outcomes

### Outcome A: Always-on paper trading
- one process family running continuously on live Polymarket 5m/15m crypto markets
- persistent paper orders, fills, positions, equity, and run state
- clear health status, kill switches, and restart safety
- strategy-level and market-level metrics available on demand

### Outcome B: Domain-agnostic autoresearch OS
- agents learn one stable loop:
  - intake -> scope -> hypothesize -> experiment -> analyze -> decide -> publish -> update skills/memory
- same loop works for:
  - Polymarket strategies
  - marketing experiments
  - website optimization
  - model eval / prompt optimization

### Outcome C: Research-to-runtime improvement loop
- research artifacts produce candidate strategy/config changes
- candidate registry governs promotion from research -> paper candidate -> active paper candidate
- paper trading produces new labeled evidence for future research

---

## Phase order

## Phase 0: Freeze contracts before coding further

**Objective:** define the shared event, artifact, and role contracts across runtime, dashboard, and autoresearch.

**Why first:** this is the anti-Frankenstein move.

### Deliverables
- canonical runtime event schema
- canonical research artifact schema
- candidate lifecycle states
- subagent handoff contract
- directory conventions for runs, telemetry, research, and dashboards

### Canonical lifecycle states
- backlog
- scoped
- queued
- running
- review
- promising
- validated
- paper_candidate
- active_paper
- paused
- demoted
- archived

### Event families
- system
- market_universe
- market_data
- feature
- strategy
- risk
- execution
- research
- operator

### Required IDs everywhere
- run_id
- cycle_id
- strategy_id
- config_version
- market_id
- signal_id
- order_id
- fill_id
- candidate_id

### Required repo paths
- `data/events/`
- `data/runs/`
- `data/research/`
- `data/candidates/`
- `docs/subagents/`
- `docs/contracts/`

---

## Phase 1: Always-on paper-trading core

**Objective:** get one trustworthy always-on paper trading runtime running before expanding research ambitions.

### Build
1. Runtime supervisor
   - starts/stops monitored paper-trading loop
   - owns heartbeat and restart logic
   - writes run ledger

2. Persistent paper state
   - open orders
   - positions
   - cash/equity snapshots
   - restart-safe state recovery

3. Market-universe service
   - continuously tracks live 5m/15m crypto markets
   - emits selected vs filtered markets with reasons

4. Market-data service
   - normalized order book snapshots
   - freshness tracking
   - stale-data detection

5. Strategy runtime separation
   - strategy engine emits intents only
   - risk engine approves/rejects
   - paper execution engine is sole writer of order/fill state

6. Safety controls
   - stale data breaker
   - repeated error breaker
   - drawdown breaker
   - strategy disable switch
   - observe-only mode

### Success criteria
- paper trader survives restart without losing state
- operator can see if it is healthy in under 30 seconds
- no silent death mode
- no duplicate order mutation across processes

---

## Phase 2: Telemetry substrate and thin operator surface

**Objective:** expose runtime truth without prematurely building a giant UI.

### Build
1. Structured JSON event stream
   - append-only, audit-friendly
2. Hot state store
   - SQLite first, Postgres later if needed
3. Metrics layer
   - loop latency
   - selected markets
   - signals by strategy
   - fills
   - open positions
   - equity / pnl / drawdown
   - stale-data counts
   - last error
4. Thin dashboard/API
   - system health
   - live run status
   - current paper portfolio
   - signal funnel
   - recent alerts
   - research cycle summary

### Design rule
Dashboard is a read-only consumer of telemetry, not a separate source of truth.

### Success criteria
- Steve Jobs simplicity on surface
- Elon telemetry underneath
- every card maps to a durable event or state field

---

## Phase 3: Strategy registry and comparable experiments

**Objective:** make runtime strategies and research strategies the same objects.

### Build
1. Strategy registry
   - `strategy_id`
   - params schema
   - runtime adapter
   - backtest adapter
   - research adapter
2. Port current strategies into registry
   - mean_reversion_5min
   - shock_reversion
   - dislocation_arb
   - toxicity_mm
   - terminal_resolver stays experimental
3. Unified scoring contract
   - pnl
   - win rate
   - trade count
   - drawdown
   - opportunity count
   - fill realism proxy
   - regime breakdown
4. Baselines
   - do nothing
   - current default bundle
   - consensus-following baseline where relevant

### Success criteria
- one command compares multiple strategy families on one dataset
- same strategy IDs appear in runtime, research, and dashboard

---

## Phase 4: Expand autoresearch into a real operating system

**Objective:** teach agents one reusable autoresearch pattern that works beyond Polymarket.

### Kernel rule
The loop stays generic.
Adapters carry domain intelligence.

### Generic roles agents must learn
- sponsor
- planner
- scout
- experimenter
- critic
- analyst
- risk officer
- reporter
- librarian

### What every adapter must provide
1. intake contract
2. unit/hypothesis generator
3. experiment-spec builder
4. evidence collector/executor
5. analyzer
6. decision policy hooks
7. artifact writer
8. skill extraction hooks

### First adapters to support
- `research/polymarket.py`
- `research/marketing.py`
- `research/website.py`
- `research/model_eval.py`

### Universal artifact contract
- `brief.json`
- `units.json`
- `experiments.json`
- `evidence.json`
- `decisions.json`
- `report.md`
- `manifest.json`
- `subagents.json`

### Success criteria
- agent can be taught autoresearch once
- new domain implementation mostly means writing a new adapter
- same promotion/demotion language works across domains

---

## Phase 5: Research-to-paper promotion system

**Objective:** let autoresearch improve the running Polymarket system without letting junk research mutate runtime behavior.

### Build
1. Candidate registry
   - stores candidate configs and lineage
2. Promotion gates
   - minimum sample size
   - robustness across slices
   - acceptable drawdown
   - acceptable fill assumptions
   - config version frozen
3. Paper deployment states
   - shadow
   - paper_candidate
   - active_paper
   - paused
   - retired
4. Demotion logic
   - paper-vs-research drift
   - degraded fill quality
   - breaker interactions
   - repeated stale/no-data incidents

### Success criteria
- no strategy goes active paper without evidence and provenance
- runtime knows exactly which research cycle birthed each candidate

---

## Phase 6: Agent learning system for autoresearch

**Objective:** make agents capable of using and implementing autoresearch on basically anything.

### Build
1. Persistent skill pack
   - `autoresearch-loop-adapters`
   - domain adapter templates
   - evaluation checklists
   - promotion gate checklists
2. Repo-local subagent templates
   - scout
   - experimenter
   - analyst
   - reporter
   - critic
3. Example playbooks
   - optimize a marketing funnel
   - optimize a landing page / website
   - optimize model prompts/evals
   - optimize trading strategy parameters
4. Training examples
   - each example shows:
     - brief
     - experiments
     - evidence
     - decisions
     - extracted skill pattern

### Success criteria
- an agent can spin up a new adapter with a checklist, not a reinvention
- the same operating language is used across all domains

---

## Recommended execution sequence for us

### This week
1. Write contracts and schemas
2. Harden always-on paper runtime
3. Add thin telemetry/status surface
4. Add candidate registry skeleton
5. Add repo-local autoresearch OS docs and templates

### Next week
1. Build strategy registry
2. Port all current strategies into registry
3. Expand research to compare strategy families
4. Connect dashboard to candidate and research outputs

### Week 3+
1. Add first non-Polymarket adapters
   - marketing
   - website
   - model_eval
2. Add promotion/demotion automation
3. Add richer paper-vs-research drift analysis

---

## What should run continuously vs on schedule

### Continuous
- supervisor
- universe selector
- market-data ingestion
- feature computation
- strategy evaluation
- risk engine
- paper execution
- heartbeats / health monitor
- alerts

### Scheduled
- 5m / 15m labeling and markout enrichment
- hourly paper summary
- daily research packaging
- daily candidate scorecard
- daily/weekly strategy sweeps
- retention and compaction

---

## Major risks

1. UI-first trap
- building a dashboard before durable telemetry

2. Noise-optimization trap
- expanding autoresearch before paper runtime emits trustworthy evidence

3. Overfitting trap
- promoting strategy tweaks from tiny sample sizes

4. Framework trap
- turning multi-agent discipline into ceremony instead of leverage

5. Live-trading temptation
- trying to harden live execution before paper mode becomes boringly reliable

---

## Immediate implementation epics

### Epic 1
Always-on paper runtime + state persistence + heartbeat

### Epic 2
Event schema + telemetry store + thin dashboard/API

### Epic 3
Strategy registry + unified comparison framework

### Epic 4
Autoresearch OS contracts + reusable adapter templates

### Epic 5
Candidate registry + promotion/demotion pipeline

---

## Definition of done for the overall program

We win when all of this is true:
- paper trader runs continuously on live 5m/15m crypto markets
- runtime health and portfolio state are always visible
- autoresearch produces machine-readable and human-readable artifacts
- strategy improvements move through explicit promotion gates
- agents can apply the same autoresearch operating system to trading, marketing, websites, and model eval
- repo remains one coherent monopoly codebase, not a graveyard of experiments
