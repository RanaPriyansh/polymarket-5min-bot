# Runtime Truth and Market Alignment Repair Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Repair the canonical Polymarket 5/15m paper bot so runtime telemetry distinguishes slot lifecycle from actual position settlement, market-making is not incorrectly frozen by a directional win-rate gate, and operator surfaces show real-time market eligibility/block reasons clearly enough to trust continuous paper trading again.

**Architecture:** Keep the existing ledger/replay spine. Do not rewrite execution from scratch. Patch the execution/status/governance layers so they report truthful settlement attribution, apply family-aware gate logic, and emit explicit eligibility telemetry for the live market set. The theme is truth first, strategy second.

**Tech Stack:** Python 3.11, click CLI, pytest/unittest, SQLite ledger, JSON runtime telemetry, existing `execution.py`, `cli.py`, `research/gate.py`, operator/status scripts.

---

### Task 1: Make settlement/operator truth distinguish flat slot closes from actual position settlements

**Objective:** Stop `latest_settlement` and related status surfaces from pretending a slot lifecycle event is the same thing as a position settlement with realized PnL.

**Files:**
- Modify: `execution.py`
- Modify: `replay.py`
- Modify: `scripts/ops_status.py` (or operator truth helper if that file owns formatting)
- Test: `tests/test_settlement_status_truth.py`

**Task spec:**
- Add an explicit distinction between:
  - latest slot closure / market resolution event
  - latest settlement with actual position attribution (`realized_pnl`, `position_outcome`, `position_size`)
- Ensure status payloads never present a flat-at-expiry slot close as though it were a filled/settled trade.
- Preserve current ledger schema unless a strictly additive field is needed.
- If `slot_settled` exists with null attribution, operator text should say the slot resolved with no held position rather than showing fake/incomplete settlement details.
- Add a clear field or helper for “latest_position_settlement” (or equivalent naming) backed by replay truth.

**TDD steps:**
1. Write failing tests covering:
   - a slot closes with no open position -> operator/status marks it as slot close only
   - a slot settles with real position attribution -> status exposes realized settlement details
   - latest position settlement prefers the latest attributed settlement, not the latest null-attribution slot event
2. Run targeted tests and verify failure.
3. Implement the minimal status/replay/execution changes.
4. Run targeted tests and verify pass.
5. Run the broader status/ops/replay suite.

**Verification commands:**
- `.venv/bin/python -m pytest tests/test_settlement_status_truth.py -q`
- `.venv/bin/python -m pytest tests/test_operator_truth.py tests/test_replay.py -q`

---

### Task 2: Replace win-rate-based runtime freeze with family-aware gate policy

**Objective:** Prevent the market-making baseline from being paused by a metric that is structurally wrong for MM while keeping hard-stop governance for actual broken states.

**Files:**
- Modify: `cli.py`
- Modify: `research/gate.py`
- Modify: `baseline_evidence.py` and/or status rendering helpers if needed for wording
- Test: `tests/test_runtime_entry_policy.py`
- Test: `tests/test_research_gate_policy.py`

**Task spec:**
- Keep RED/YELLOW/GREEN gate computation, but make entry-pausing family-aware.
- `toxicity_mm` / market-making families must not be paused purely because realized win rate is low when realized PnL / drawdown / continuity are otherwise acceptable.
- Continue pausing for truly dangerous reasons such as:
  - open contradictions
  - unreviewed circuit breaker
  - non-computable settlement truth if that reason is configured as blocking
  - explicit catastrophic PnL / drawdown threshold if already available
- Keep directional families eligible for stricter win-rate-based or evidence-based pauses if intended.
- Status text must explain why new orders are paused or why MM remains allowed despite RED gate.

**TDD steps:**
1. Extend failing tests for:
   - MM family is NOT blocked on low win rate alone when continuity/truth are intact
   - directional family can still be blocked on low win rate if policy says so
   - contradiction/circuit-breaker reasons still block everyone
   - status wording reflects family-aware pause reason correctly
2. Run targeted tests and verify failure.
3. Implement minimal family-aware gate helper(s) and wiring.
4. Run targeted tests and verify pass.
5. Run the broader governance/research/runtime suite.

**Verification commands:**
- `.venv/bin/python -m pytest tests/test_runtime_entry_policy.py -q`
- `.venv/bin/python -m pytest tests/test_research_gate_policy.py tests/test_autoresearch_and_edge.py -q`

---

### Task 3: Add real-time market eligibility telemetry that shows why markets are skipped, quoted, or blocked

**Objective:** Make the live market loop observable enough that an operator can tell whether the bot is refusing markets because of structure, governance, or both.

**Files:**
- Modify: `cli.py`
- Modify: `runtime_telemetry.py` or existing status helper module if needed
- Modify: `scripts/ops_status.py` (or helper)
- Test: `tests/test_market_eligibility_telemetry.py`

**Task spec:**
- Emit and/or summarize counts for each loop bucket:
  - discovered markets
  - structurally untradeable markets
  - governance-blocked markets
  - quoted/entered markets
- Include top skip/block reasons in operator status output.
- Ensure `market.entry_blocked` and baseline structural skip events are aggregated into a compact runtime summary so operators do not need to tail JSONL manually.
- Keep implementation additive and bounded; do not build a dashboard.

**TDD steps:**
1. Write failing tests for:
   - aggregation of structural skip vs governance block counts
   - operator/status text includes dominant reasons
   - no-event edge case renders cleanly
2. Run targeted tests and verify failure.
3. Implement minimal aggregation and status rendering.
4. Run targeted tests and verify pass.
5. Run the broader operator telemetry suite.

**Verification commands:**
- `.venv/bin/python -m pytest tests/test_market_eligibility_telemetry.py -q`
- `.venv/bin/python -m pytest tests/test_operator_truth.py tests/test_runtime_features.py -q`

---

### Task 4: Final integration verification against the live canonical runtime

**Objective:** Prove the repaired code matches the live runtime behavior and improves operator truth.

**Files:**
- Review all modified files only

**Checks:**
- latest settlement surfaces no longer confuse empty slot resolution with actual position settlement
- MM baseline is not frozen solely by realized win rate
- status/ops output exposes structural skip reasons and governance blocks clearly
- targeted tests pass
- smoke-check commands against canonical runtime output make sense

**Verification commands:**
- `.venv/bin/python -m pytest tests/test_settlement_status_truth.py tests/test_runtime_entry_policy.py tests/test_market_eligibility_telemetry.py -q`
- `.venv/bin/python -m pytest -q`
- `.venv/bin/python cli.py health --runtime-dir data/runtime --max-heartbeat-age 180`
- `.venv/bin/python scripts/ops_status.py --write`

---

## Notes

- Do not tune strategy alpha before repairing truth and governance semantics.
- Do not disable safety globally; only remove category-error blocking.
- Prefer additive status fields over breaking schema changes.
- If an existing operator artifact name is misleading, change the wording, not the ledger contract, unless tests prove the contract itself is wrong.
