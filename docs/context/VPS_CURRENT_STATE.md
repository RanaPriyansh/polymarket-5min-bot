# VPS Current State

Last updated: 2026-04-01
Source of truth: Hermes reports pasted into Codex and reconciled against the local repo where possible.

## Active Polymarket Runtime

- Active VPS repo:
  - `/root/obsidian-hermes-vault/learning/branches/polymarket-paper-runtime/projects/polymarket-5min-bot`
- Branch:
  - `feat/polymarket-paper-runtime`
- Commit:
  - `5d1465f`
- Supervisor:
  - `polymarket-paper-bot.service`
- Runtime command:
  - `.venv/bin/python cli.py run --mode paper --runtime-dir .../data/runtime --sleep-seconds 60`
- Healthcheck:
  - `polymarket-paper-bot-healthcheck.service` via timer every 2 minutes
- Dashboard/research:
  - `polymarket-paper-bot-dashboard.service` via timer every 5 minutes

## Verified Runtime Problems

- The active runtime executes zero trades.
- The main reason reported by Hermes:
  - market discovery fetches a broad Gamma universe, then filters to zero markets using a near-expiry `minutes=15` constraint.
- `strategy_metrics.json` is effectively empty because the processing path never reaches meaningful market handling.
- `market_samples.jsonl` is absent because sampled/skipped market telemetry is not being produced under the current empty-universe state.
- `data/research/` is filling with repeated low-value artifacts because research keeps running against near-empty runtime inputs.
- Live trading is not operational:
  - no wallet env configured in the active repo
  - `_get_token_id()` remains incomplete according to Hermes

## Other VPS Systems

- Separate crypto paper trader:
  - `/root/projects/crypto-5min-farmer/`
  - not the canonical Polymarket runtime
  - currently has duplicate running instances
- Legacy/parallel Docker stacks exist:
  - `min5-bot`
  - `arbitrage-bot`
  - `edgefinder-bot`
- Hermes gateway is running persistently under systemd and can inspect/write the VPS filesystem.

## Current Working Assumption

The active systemd Polymarket repo is the first repair target, but only for paper/runtime/research stabilization.

We are not enabling live trading during this phase.
