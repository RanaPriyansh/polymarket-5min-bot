# Hermes Worklog

## 2026-04-01 - VPS inventory and code truth pack

### Discovery summary

- Confirmed active systemd-managed Polymarket paper bot on the VPS.
- Confirmed separate crypto paper trader and separate legacy Dockerized bot stacks.
- Confirmed Hermes gateway is persistent and has sufficient access to inspect and modify the project on the VPS.

### Verified findings

- Active Polymarket bot repo:
  - `/root/obsidian-hermes-vault/learning/branches/polymarket-paper-runtime/projects/polymarket-5min-bot`
- Active branch and commit:
  - `feat/polymarket-paper-runtime`
  - `5d1465f`
- Active runtime is healthy from systemd's perspective but operationally unproductive:
  - heartbeat updates continue
  - no real paper trades are occurring
- Dashboard timer runs `cli.py research` every 5 minutes.
- Healthcheck timer monitors heartbeat freshness every 2 minutes.

### Current blockers

- Empty tradeable universe under current near-expiry market filter.
- Runtime telemetry is too weak when no trades occur.
- Research loop produces repetitive low-value outputs because the runtime does not emit enough useful artifacts.
- Duplicate crypto trader instances on the VPS are a separate cleanup item.

### Immediate next step

Patch the active Polymarket systemd repo so paper mode always emits useful discovery/sample/research artifacts, even when fills are zero.

## 2026-04-02 - Runtime truth path and replay-derived risk

### Shipped

- Wired runtime execution into the append-only ledger instead of trusting only in-memory order state.
- Enabled runtime persistence and restart restore for fills and settlement events.
- Added replay-derived exposure projection with per-strategy / market / asset / interval buckets.
- Rewired risk reporting to consume executor snapshot + ledger events instead of incremental capital mutation in the CLI loop.
- Added restart-proof tests for exposure, settlement restore, and risk report equivalence after restart.

### Verified

- `pytest -q` now passes with 30 tests.
- Runtime snapshot now includes replay-derived exposure, realized pnl total, open order count, resolved trade stats, and latest settlement.
- CLI now auto-provisions `runtime_dir/ledger.db` and uses the run id as the ledger replay boundary.

### Still missing

- Risk drawdown is still realized-PnL-based, not full mark-to-market.
- Family metrics are still live counters, not yet replay-derived projections.
- No dedicated `exposure.py` -> `risk_snapshot_recorded` ledger event emission yet.

### Next likely slice

- Emit explicit `risk_snapshot_recorded` events on loop ticks.
- Convert family metrics into replay-backed projections.
- Add a full CLI-level restart equivalence test around runtime artifacts + ledger replay.
