# Trustworthy Always-On Paper Runtime + Telemetry Implementation Plan

> For Hermes: use subagent-driven-development discipline if splitting further. This first slice is intentionally bounded: persistent paper state, heartbeat/status telemetry, and a read-only API surface backed by durable files.

Goal: make the paper trading runtime trustworthy enough to run continuously by giving it persistence, heartbeats, and observable status without rewriting the full architecture in one jump.

Architecture: add a small runtime telemetry layer and broker state snapshotting around the existing CLI loop. The paper broker becomes restart-aware. The runtime emits durable JSON status plus append-only JSONL events. The API wrapper reads these files instead of returning placeholder status.

Tech Stack: Python, Click, unittest, FastAPI, JSON/JSONL, existing broker/runtime loop.

---

### Task 1: Add runtime telemetry module
Objective: create one small source of truth for runtime status and append-only events.

Files:
- Create: `runtime_telemetry.py`
- Test: `tests/test_runtime_telemetry.py`

Required behavior:
- create runtime directory automatically
- write `status.json`
- append `events.jsonl`
- support heartbeat timestamps, phase, loop counters, selected market counts, broker summary, and last error
- provide helper to read latest status safely

### Task 2: Add paper broker snapshot/restore support
Objective: make paper mode restart-aware.

Files:
- Modify: `execution.py`
- Test: `tests/test_runtime_telemetry.py`

Required behavior:
- serialize broker cash, positions, orders, open orders, marks, initial capital
- restore from serialized payload
- preserve order ids/statuses and open orders across restart

### Task 3: Wire telemetry into CLI runtime
Objective: emit status and persist broker state from the actual paper trading loop.

Files:
- Modify: `cli.py`
- Modify: `config.yaml`
- Test: `tests/test_runtime_telemetry.py`

Required behavior:
- create run id
- initialize runtime telemetry store
- emit startup event
- emit heartbeat/status on each loop
- write broker snapshot after each loop in paper mode
- emit error/stop event on exception or shutdown
- add bounded `--max-loops` option for testability

### Task 4: Replace API placeholders with durable runtime reads
Objective: expose real status from runtime files.

Files:
- Modify: `api_wrapper.py`
- Test: `tests/test_runtime_telemetry.py`

Required behavior:
- `/health` reflects whether runtime status heartbeat is fresh
- `/status` returns durable runtime snapshot
- `/logs` remains best-effort but should not be the only status source

### Task 5: Verify end-to-end
Objective: prove this first slice works.

Commands:
- `.venv/bin/python -m unittest discover -s tests -v`
- `.venv/bin/python cli.py run --mode paper --max-loops 1`

Acceptance criteria:
- tests pass
- runtime writes `data/runtime/status.json`
- runtime writes `data/runtime/events.jsonl`
- paper state snapshot exists after a loop
- API helpers can read and report the durable status
