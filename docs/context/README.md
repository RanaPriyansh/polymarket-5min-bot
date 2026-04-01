# Project Context Ledger

This directory is the persistent project memory for the Polymarket bot and Hermes-driven VPS work.

## Files

- `VPS_CURRENT_STATE.md`
  - Current verified VPS/runtime state.
- `HERMES_WORKLOG.md`
  - Chronological log of Hermes discovery, fixes, and outcomes.
- `CLOSED_LOOP_TARGET.md`
  - The intended target architecture for the paper -> research -> experiment -> promotion loop.

## Operating Rule

For every meaningful Hermes discovery or repair task:

1. Update `VPS_CURRENT_STATE.md` if verified facts changed.
2. Append a dated entry to `HERMES_WORKLOG.md`.
3. Update `CLOSED_LOOP_TARGET.md` if the target design or implementation order changed.

Keep entries concise, factual, and auditable. Distinguish verified fact from inference.
