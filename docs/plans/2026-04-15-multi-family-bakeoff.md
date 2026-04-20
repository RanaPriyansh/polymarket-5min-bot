# Multi-Family Strategy Bakeoff Implementation Plan

> For Hermes: Use subagent-driven-development skill to implement this plan task-by-task.

Goal: add a proper experiment config plus a bounded bakeoff runner that can trial baseline and candidate strategy families in isolated runtime dirs, then rank them for promotion using durable runtime evidence.

Architecture: keep the trading loop unchanged as the execution truth. Add a thin bakeoff module that reads a YAML trial spec, launches bounded `cli.py run` trials one family at a time in isolated runtime directories, then summarizes results from runtime artifacts and baseline evidence. Promotion remains manual and evidence-backed.

Tech Stack: Python 3.11, click CLI, yaml config, subprocess invocation of existing CLI, unittest.

---

### Task 1: Add strategy bakeoff spec loader and summary model

Objective: create a deterministic experiment manifest format and summary helpers that understand multi-family trials without mutating production config.

Files:
- Create: `strategy_bakeoff.py`
- Create: `configs/strategy-bakeoff.yaml`
- Create: `tests/test_strategy_bakeoff.py`

Step 1: Write failing tests

Add tests that prove:
1. YAML spec loads with an experiment id, default loop/sleep/runtime root, and one trial per family.
2. Missing trial strategies raises a clear `ValueError`.
3. Summary ranking prefers settled evidence first, then realized PnL, then fill count.
4. Rendered markdown/text summary includes winner, promotion recommendation, and per-trial stats.

Step 2: Run test to verify failure

Run: `.venv/bin/pytest tests/test_strategy_bakeoff.py -v`
Expected: FAIL because `strategy_bakeoff.py` does not exist yet.

Step 3: Write minimal implementation

Create `strategy_bakeoff.py` with:
- dataclasses for `BakeoffTrialSpec`, `BakeoffSpec`, `TrialOutcome`
- `load_bakeoff_spec(path)`
- `build_trial_runtime_dir(spec, trial)`
- `score_trial(outcome)` using tuple ordering:
  - settled slots desc
  - resolved trades desc
  - realized pnl desc
  - fills desc
  - fewer toxic skips better
- `rank_trials(outcomes)`
- `render_bakeoff_report(spec, outcomes)`

Create `configs/strategy-bakeoff.yaml` with 4 isolated trials:
- toxicity_mm baseline
- mean_reversion_5min candidate
- opening_range candidate
- time_decay candidate

Defaults:
- `max_loops: 20`
- `sleep_seconds: 5`
- `runtime_root: data/experiments/multi-family-bakeoff`

Step 4: Run test to verify pass

Run: `.venv/bin/pytest tests/test_strategy_bakeoff.py -v`
Expected: PASS.

Step 5: Commit

Run:
`git add strategy_bakeoff.py configs/strategy-bakeoff.yaml tests/test_strategy_bakeoff.py`
`git commit -m "feat: add multi-family bakeoff spec and ranking helpers"`

---

### Task 2: Add CLI bakeoff runner and durable result artifacts

Objective: make the repo able to execute the spec, run isolated bounded trials, and emit one summary packet that says which family deserves promotion.

Files:
- Modify: `cli.py`
- Modify: `tests/test_strategy_bakeoff.py`
- Create: `data/experiments/.gitkeep`

Step 1: Write failing tests

Add tests that prove:
1. A helper builds the exact subprocess command for a trial using isolated `--runtime-dir`, `--strategies`, `--max-loops`, and `--sleep-seconds`.
2. Trial summaries read runtime artifacts plus `build_baseline_evidence()` and expose comparable metrics.
3. The bakeoff command writes `summary.json` and `summary.md` under the experiment root.

Step 2: Run test to verify failure

Run: `.venv/bin/pytest tests/test_strategy_bakeoff.py -v`
Expected: FAIL because runner helpers and command do not exist.

Step 3: Write minimal implementation

In `strategy_bakeoff.py`, add:
- `build_trial_command(...)`
- `collect_trial_outcome(runtime_dir, strategy_family)`
- `write_bakeoff_artifacts(experiment_dir, spec, outcomes)`

In `cli.py`, add `@cli.command(name="bakeoff")` with options:
- `--spec-path` default `configs/strategy-bakeoff.yaml`
- `--python-bin` default `.venv/bin/python`
- `--dry-run/--no-dry-run`

Command behavior:
- load spec
- create experiment root
- for each trial, launch bounded `run` subprocess sequentially
- collect runtime evidence from the isolated runtime dir
- rank outcomes
- print concise terminal summary
- write `summary.json` and `summary.md`
- never change `config.yaml active`

Step 4: Run tests to verify pass

Run:
- `.venv/bin/pytest tests/test_strategy_bakeoff.py -v`
- `.venv/bin/pytest tests/ -q`
Expected: PASS.

Step 5: Commit

Run:
`git add cli.py tests/test_strategy_bakeoff.py data/experiments/.gitkeep`
`git commit -m "feat: add bounded multi-family bakeoff runner"`

---

### Final verification and experiment run

1. Dry run the bakeoff command:
`.venv/bin/python cli.py bakeoff --spec-path configs/strategy-bakeoff.yaml --dry-run`

2. Run the real bakeoff:
`.venv/bin/python cli.py bakeoff --spec-path configs/strategy-bakeoff.yaml`

3. Inspect artifacts:
- `data/experiments/multi-family-bakeoff/summary.json`
- `data/experiments/multi-family-bakeoff/summary.md`
- per-trial runtime dirs under `data/experiments/multi-family-bakeoff/trials/`

4. Promotion rule:
Only recommend promotion if the winner beats baseline on settled evidence or, absent settlement, shows the strongest combination of fills, realized/MTM pnl, and clean risk profile.
