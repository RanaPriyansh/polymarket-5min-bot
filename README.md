# Polymarket 5/15-Minute Paper Bot

Ledger-backed paper trading runtime for Polymarket ultra-short-duration interval markets.

Live trading is blocked.

Read these first:
- `STATUS.md` — canonical human-readable status
- `docs/plans/2026-04-03-runtime-truth-and-power-plan.md` — current execution plan

## Current operating stance

- Paper only
- Baseline runtime family: `toxicity_mm`
- Research-only families: `mean_reversion_5min`, `opening_range`, `time_decay`
- Research is advisory only
- Code + config + runtime artifacts outrank docs

## Core architecture

`discover -> validate -> decide -> execute -> persist -> replay -> report`

Key files:
- `cli.py` — paper runtime entrypoint, status, health, research
- `execution.py` — ledger-backed paper execution, restore, settlement, snapshots
- `risk.py` — mark-to-market and exposure-aware risk reporting
- `ledger.py` / `replay.py` — append-only event storage + deterministic replay
- `runtime_telemetry.py` — durable runtime artifacts
- `research/polymarket.py` — advisory runtime research, now scoped to current run by default

## Runtime artifacts

Written to `data/runtime/`:
- `status.json` — current runtime snapshot
- `events.jsonl` — append-only runtime event stream
- `strategy_metrics.json` — canonical per-family metrics snapshot
- `market_samples.jsonl` — market/book samples
- `ledger.db` — ledger truth store
- `latest-status.txt` — quick operator surface

Research artifacts in `data/research/`:
- `latest.json`
- `latest.md`
- timestamped cycle files

## Quick start

```bash
cd /root/obsidian-hermes-vault/projects/polymarket-5min-bot
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest -q
.venv/bin/python cli.py run --mode paper --max-loops 4 --sleep-seconds 5
```

Status:
```bash
.venv/bin/python cli.py status --runtime-dir data/runtime
.venv/bin/python cli.py health --runtime-dir data/runtime --max-heartbeat-age 180
```

Research:
```bash
.venv/bin/python cli.py research --runtime-dir data/runtime --artifact-dir data/research
```

## Configuration hygiene

Tracked `config.yaml` contains only public defaults.
Secrets belong in `.env` only.

Supported env overrides:
- `POLYMARKET_WALLET_ADDRESS`
- `POLYMARKET_PRIVATE_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Non-negotiable rules

1. Do not enable live mode.
2. Do not promote candidate strategies without settled paper evidence.
3. Do not let research auto-mutate production config.
4. Prefer subtraction over more architecture.
5. Trust runtime artifacts over narrative docs.
