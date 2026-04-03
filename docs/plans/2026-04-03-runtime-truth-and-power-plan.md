# Polymarket Runtime Truth and Power Plan

> For Hermes: execute this plan from first principles. The goal is not more architecture. The goal is a smaller, cleaner, more trustworthy paper machine that can generate durable research and operational leverage.

Goal: turn the repo into a trustworthy paper-runtime system with one proven baseline family, one canonical status surface, and faster iteration loops.

Architecture: keep the event-sourced core, subtract narrative and duplicate policy, and harden the runtime truth layer around `toxicity_mm`. Add no new strategy families until the baseline earns trust over longer settled-paper runs.

Tech stack: Python, Click CLI, SQLite ledger, JSON/JSONL runtime artifacts, systemd.

---

## Phase 0 — Immediate actions already aligned

- Tracked config should contain no secret-shaped fields.
- Telegram should default off in tracked config.
- Baseline runtime remains `toxicity_mm` only.
- `STATUS.md` is now the canonical human-readable status file.

---

## Phase 1 — Remove truth contamination

### Task 1: Make research run-scoped by default

Objective: stop `research` from blending current and prior runs into one recommendation surface.

Files:
- Modify: `runtime_telemetry.py`
- Modify: `research/polymarket.py`
- Test: `tests/test_runtime_features.py`
- Create: `tests/test_research_run_scoping.py`

Implementation targets:
- Add optional `run_id` filtering to event and sample readers.
- Make `cli.py research` default to the current `status.json` run id when available.
- Report explicit scope: current run, sample limit, event count, sample count.

Verification:
- Create mixed-run fixture data.
- Confirm research only analyzes the selected run.
- Confirm summary text names the active run id.

### Task 2: Stop writing duplicate strategy truth without explicit ownership

Objective: prevent `status.json` and `strategy_metrics.json` from silently diverging.

Files:
- Modify: `runtime_telemetry.py`
- Modify: `cli.py`
- Test: `tests/test_runtime_features.py`

Implementation targets:
- Choose one of these patterns:
  1. `status.json` owns strategy metrics and `strategy_metrics.json` is removed, or
  2. `strategy_metrics.json` remains canonical and `status.json` only references a snapshot hash/timestamp.
- Prefer option 1 unless there is a strong reason not to.

Verification:
- Single runtime write path.
- No mismatched counts after repeated loops.

### Task 3: Create one canonical doc map and archive stale docs

Objective: stop maintainers from steering off contradictory markdown.

Files:
- Modify: `README.md`
- Replace/Archive: `PROJECT_STATUS.md`
- Replace/Archive: `DEPLOY.md`
- Slim down: `PLAN.md`

Implementation targets:
- `README.md` should point to `STATUS.md` and the current plan file.
- `PROJECT_STATUS.md` should become a tombstone or archive note.
- `DEPLOY.md` must stop describing live progression.
- `PLAN.md` should only track active execution priorities, not speculative expansion.

Verification:
- Search for live-enablement language and remove it.
- Search for stale performance claims and remove them.

---

## Phase 2 — Unify policy, reduce drift

### Task 4: Build a canonical tradeability policy module

Objective: remove duplicated skip logic and make market acceptance explainable.

Files:
- Create: `tradeability_policy.py`
- Modify: `book_quality.py`
- Modify: `cli.py`
- Modify: `strategies/mean_reversion_5min.py`
- Modify: `strategies/opening_range.py`
- Modify: `strategies/time_decay.py`
- Modify: `strategies/toxicity_mm.py`
- Test: `tests/test_tradeability_policy.py`

Implementation targets:
- One module returns:
  - accept/reject
  - structured reasons
  - strategy-family-aware thresholds
  - outcome-specific view when required
- CLI and strategies should consume the same policy object rather than re-implement thresholds.

Verification:
- Same market + same config => same acceptance decision regardless of caller.
- Skip reasons are identical across runtime and research.

### Task 5: Make strategy family state explicit

Objective: remove ambiguity between baseline, candidate, and experimental families.

Files:
- Modify: `config.yaml`
- Modify: `cli.py`
- Modify: `research/polymarket.py`
- Test: `tests/test_cli_strategy_selection.py`

Implementation targets:
- Add explicit family state metadata if needed:
  - `baseline`
  - `candidate`
  - `experimental`
- Research should not compare inactive families unless explicitly requested.

Verification:
- Default paper run only executes baseline family.
- Research output clearly labels inactive families as inactive.

---

## Phase 3 — Strengthen the baseline

### Task 6: Prove settled-paper behavior for `toxicity_mm`

Objective: gather runtime evidence that survives restart and includes actual settlement outcomes.

Files:
- Modify: `STATUS.md`
- Modify: optional ops scripts/docs
- Possibly modify: `status_utils.py`

Implementation targets:
- Define a 72h evidence packet with:
  - total fills
  - settled trades
  - realized pnl total
  - mark-to-market drawdown
  - restart count / restart equivalence check
  - dominant skip reasons
  - exposure distribution by asset and interval
- Add one command or script to summarize these directly from runtime artifacts.

Verification:
- A human can answer “is baseline getting stronger or weaker?” from one artifact bundle.

### Task 7: Fix one-sided MM exposure policy if runtime evidence confirms it is still structurally biased

Objective: ensure `toxicity_mm` is not blindly accumulating same-direction inventory.

Files:
- Modify: `strategies/toxicity_mm.py`
- Modify: `execution.py` or future executor split files
- Test: `tests/test_runtime_features.py`
- Create: `tests/test_toxicity_mm_inventory_policy.py`

Implementation targets:
- Confirm current quoting/inventory behavior from runtime artifacts first.
- If still one-sided in practice, add explicit inventory policy:
  - dual-sided quoting, or
  - directional alternation, or
  - per-market inventory cap.
- Do not add complexity until the runtime evidence proves the problem is real and recurring.

Verification:
- Exposure concentration should be measurable and lower after the change.

---

## Phase 4 — Speed up learning loops

### Task 8: Institutionalize the 60-second smoke loop

Objective: make every config/policy change verifiable locally in under two minutes.

Files:
- Modify: `README.md`
- Possibly create: `scripts/smoke_runtime.sh`

Implementation targets:
- Standard smoke command:
  - `.venv/bin/python cli.py run --mode paper --max-loops 4 --sleep-seconds 5`
- Print expected outputs and artifact checks.
- Require this before any systemd restart.

Verification:
- Operators use the same short loop for every change.

### Task 9: Separate collector truth from runtime truth

Objective: collect research/backtest data continuously without contaminating the main paper runtime.

Files:
- Review: `scripts/collector.py`
- Create/Modify: `deploy/systemd/polymarket-collector.service`
- Create/Modify: `deploy/systemd/polymarket-collector.timer` or long-running service
- Test: add at least one smoke/integration test if practical

Implementation targets:
- Collector writes to dedicated files under `data/collector/`.
- Collector handles SIGTERM cleanly.
- Collector is not coupled to the trading service lifecycle.

Verification:
- Backtest data grows independently of runtime restarts.

---

## Phase 5 — Shrink the executor without breaking truth

### Task 10: Split `execution.py` only after truth contracts are stable

Objective: reduce maintenance risk without destabilizing the live paper runtime.

Files:
- Current source: `execution.py`
- Future targets:
  - `paper_executor.py`
  - `settlement_runtime.py`
  - `executor_snapshot.py`
- Tests: existing execution/replay/settlement/risk tests plus new focused module tests

Implementation targets:
- Split by responsibility, not style:
  - hot path: order placement, fill application, inventory updates
  - cold path: resolution polling and settlement
  - read path: snapshots, MTM, family metrics, exposure views
- Preserve external behavior and runtime artifacts.

Verification:
- Full test suite passes before and after split.
- Runtime smoke loop produces identical artifact schema.

---

## Promotion policy

A strategy becomes baseline-worthy only if all are true:
- dedicated tests exist
- it runs in paper without operator babysitting
- it produces real fills
- it produces settled-trade evidence
- restart/replay do not distort its metrics
- it outperforms or complements the baseline over a meaningful window

A feature is complete only if all are true:
- wired into the main runtime path
- visible in runtime artifacts
- restart-safe
- tested
- documented in canonical docs

Live mode can only be discussed after all are true:
- env-only secret handling is clean
- canonical docs have no drift
- current-run research truth is clean
- baseline has sustained settled-paper evidence across multiple days
- restart equivalence is proven in real runtime

---

## Execution order

1. Run-scoped research truth
2. Single-owner runtime metrics snapshot
3. Canonical doc cleanup
4. Unified tradeability policy
5. 72h baseline evidence packet
6. Inventory bias fix only if evidence proves the need
7. Collector operationalization
8. Executor modularization

This is how the project gets more powerful: less ambiguity, faster loops, stronger truth, tighter baseline discipline.
