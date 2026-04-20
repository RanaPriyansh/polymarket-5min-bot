# Profit Engine Upgrades Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Upgrade the Polymarket 5/15m paper bot so it sizes more realistically, directional strategies can choose resting-vs-marketable entry behavior, and autoresearch artifacts become trustworthy enough to run continuously in the background.

**Architecture:** Keep changes minimal and leverage the existing spine. Add a small execution-policy layer for signal orders, improve Kelly/risk sizing with explicit edge/stop-loss semantics, and harden research-loop evidence integrity so background autoresearch can emit trustworthy machine-readable outputs.

**Tech Stack:** Python 3.11, pytest/unittest, click CLI, existing `research/` loop, Polymarket paper executor.

---

### Task 1: Add execution-policy support for directional signal trades

**Objective:** Let signal strategies explicitly choose whether to cross the spread now or rest at a limit/acceptable price.

**Files:**
- Modify: `execution.py`
- Modify: `cli.py`
- Test: `tests/test_execution_signal_policy.py`

**Task spec:**
- Add a small helper in `execution.py` that resolves signal fill behavior from config + signal fields.
- Support at least two directional entry styles:
  - `marketable`: current behavior, cross immediately at best bid/ask
  - `resting_limit`: do not fill worse than `signal.price`; if the order would not cross, leave it open and return a non-filled/opened result with emitted order event(s)
- Preserve current default behavior unless config opts into limit-respecting behavior.
- Allow per-strategy overrides under config, not just one global default.
- Keep `toxicity_mm` quoting flow untouched.

**TDD steps:**
1. Write failing tests for:
   - default marketable BUY still fills at best ask
   - resting_limit BUY uses `signal.price` and does not overpay
   - resting_limit SELL uses `signal.price` and does not undersell
   - per-strategy config override is honored
2. Run targeted pytest and verify failure.
3. Implement minimal execution helper + wiring.
4. Run targeted pytest and verify pass.
5. Run broader relevant suite.

**Verification commands:**
- `.venv/bin/python -m pytest tests/test_execution_signal_policy.py -q`
- `.venv/bin/python -m pytest tests/test_autoresearch_and_edge.py tests/test_runtime_entry_policy.py tests/test_mtm_risk.py -q`

---

### Task 2: Upgrade risk sizing toward real Kelly-style bounded sizing

**Objective:** Replace fake-confidence sizing with bounded edge/risk-budget sizing primitives the strategies can use honestly.

**Files:**
- Modify: `risk.py`
- Modify: `cli.py`
- Test: `tests/test_risk_sizing_policy.py`

**Task spec:**
- Add a bounded sizing helper that uses:
  - bankroll / current capital
  - explicit edge estimate
  - stop-loss or max adverse move estimate
  - fractional Kelly cap
  - existing dollar/notional caps
- Keep backwards compatibility for current callers, but expose a better path for signal strategies.
- Update directional strategy execution path in `cli.py` to use the better sizing helper where enough info exists.
- Do not size from market volume proxies.

**TDD steps:**
1. Write failing tests for:
   - zero/negative edge => zero size
   - larger stop-loss risk => smaller size
   - notional caps still bind
   - per-strategy Kelly fraction still applies
2. Run targeted pytest and verify failure.
3. Implement minimal bounded sizing helper and wire it into directional signal handling.
4. Run targeted pytest and verify pass.
5. Run broader relevant suite.

**Verification commands:**
- `.venv/bin/python -m pytest tests/test_risk_sizing_policy.py -q`
- `.venv/bin/python -m pytest tests/test_mtm_risk.py tests/test_runtime_features.py -q`

---

### Task 3: Harden research-loop evidence integrity and machine-readable experiment output

**Objective:** Make background autoresearch trustworthy and continuously actionable.

**Files:**
- Modify: `research/loop.py`
- Modify: `research/polymarket.py`
- Modify: `cli.py` (only if needed for output wiring)
- Create/Modify: `docs/subagents/autoresearch-subagents.json`
- Test: `tests/test_research_signal_integrity.py`

**Task spec:**
- If gate state is RED, do not leak normal promotion recommendations in insight output.
- Split or label metrics clearly so current-run evidence is not conflated with cumulative evidence.
- Use family-appropriate metrics:
  - market makers: quoted fill rate
  - directional families: signal-to-fill or fills-per-100-markets-seen
- Emit a machine-readable experiment packet artifact, e.g. `latest_experiments.json`, with:
  - family
  - evidence scope
  - blocking contradictions
  - recommended next command
  - promote_if / kill_if thresholds (simple first version)
- Persist reusable autoresearch role templates.

**TDD steps:**
1. Write failing tests for:
   - RED gate suppresses normal recommendations
   - directional families no longer show nonsense fill rates
   - experiment packet JSON is emitted
2. Run targeted pytest and verify failure.
3. Implement minimal changes.
4. Run targeted pytest and verify pass.
5. Run broader relevant suite.

**Verification commands:**
- `.venv/bin/python -m pytest tests/test_research_signal_integrity.py -q`
- `.venv/bin/python -m pytest tests/test_autoresearch_and_edge.py tests/test_research_run_scoping.py -q`

---

### Task 4: Schedule continuous background autoresearch for the canonical runtime

**Objective:** Keep a live research loop running on the VPS continuously with bounded, inspectable outputs.

**Files:**
- No repo code required unless helper script is needed
- Output artifacts: `data/research/continuous-runtime/`

**Task spec:**
- Create one cron job that runs the canonical runtime research snapshot regularly against the live runtime dir.
- Use OpenCode Go by default for cron if supported.
- Prompt must be self-contained and should not schedule more cron jobs.
- Job should write/update continuous research artifacts, not spam chat.
- Prefer local delivery unless the user requests direct delivery.

**Verification steps:**
- List cron jobs and confirm the new job exists.
- Run the job once manually if safe.
- Confirm fresh artifacts appear under the chosen artifact dir.

---

### Final integration review

**Objective:** Verify the combined system is coherent.

**Files:**
- Review all modified files and new tests

**Checks:**
- Directional execution policy works and defaults remain safe
- Risk sizing is bounded and test-covered
- Research output is trustworthy under RED/YELLOW/GREEN gate states
- Background autoresearch job exists and writes artifacts
- Relevant tests pass

**Verification commands:**
- `.venv/bin/python -m pytest tests/test_execution_signal_policy.py tests/test_risk_sizing_policy.py tests/test_research_signal_integrity.py -q`
- `.venv/bin/python -m pytest -q`

---

## Notes

- Keep tasks minimal and evidence-driven.
- Do not refactor unrelated strategy code in this pass.
- Do not touch live trading integration; paper/runtime truth first.
- Commit after each task if the repo state is clean enough to do so.
