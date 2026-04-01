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
