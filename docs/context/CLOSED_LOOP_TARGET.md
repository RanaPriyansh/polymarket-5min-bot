# Closed Loop Target

## Objective

Build a controlled improvement loop:

`market discovery -> paper decisions -> fills/skips/samples -> runtime artifacts -> autoresearch -> experiment specs -> replay/paper validation -> gated promotion`

## Design Principles

- One canonical paper runtime.
- Hermes acts as orchestrator/researcher, not uncontrolled production mutator.
- File-based artifacts first; avoid extra infrastructure unless clearly necessary.
- Config/parameter promotion before any code-level self-modification.
- Live trading remains separately gated and out of scope for early phases.

## Phase Priorities

### Phase 1

- Fix active paper runtime so it produces useful discovery and sampling artifacts.
- Make research output useful even with zero fills.
- Stop dead-end repetitive research cycles.

### Phase 2

- Add experiment specifications and result artifacts.
- Generate candidate parameter deltas from runtime evidence.
- Add replay/backtest validation for proposed deltas.

### Phase 3

- Add a promotion registry for approved config deltas.
- Make runtime read from an auditable active strategy manifest.
- Add rollback and kill-switch controls.

### Phase 4

- Add shadow/live-readiness workflow with stronger validation and manual approval gates.

## Non-Goals Right Now

- No live wallet setup.
- No direct autonomous live trading.
- No self-editing production code without explicit approval.
