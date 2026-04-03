# Polymarket 5/15-Minute Paper Bot

**Ledger-backed paper trading runtime for Polymarket ultra-short-duration interval markets.**

Live trading is explicitly blocked until the paper runtime is operationally credible.

## Architecture

The bot is designed around execution truth as a first-class concern:

```
discover -> validate -> decide -> execute -> persist -> replay -> report
```

- `market_data.py` — Polymarket CLOB + Gamma API wrapper for interval market discovery
- `cli.py` — runtime entrypoint (paper / backtest / collect / status / health / research); live mode blocked
- `execution.py` — ledger-backed paper execution: orders, fills, positions, settlement, replay, exposure
- `risk.py` — mark-to-market and exposure-aware risk reporting from executor snapshots + ledger events
- `runtime_telemetry.py` — durable status, events (JSONL), strategy metrics, market samples, latest-status text
- `research/loop.py` — generic autoresearch orchestration with deduped latest outputs
- `research/polymarket.py` — domain adapter: family quality, skip-reason analysis, next actions
- `ledger.py` / `replay.py` — append-only event storage and deterministic replay engine
- `paper_exchange.py` — conservative fill engine
- `settlement_engine.py` — idempotent position resolution via ledger events

## Strategy Families

### Active production-paper family
| Family | Role | Status |
|--------|------|--------|
| `toxicity_mm` | VPIN-aware market making | Promoted — real fills and PnL |

### Research candidates
| Family | Role | Barrier to promotion |
|--------|------|---------------------|
| `mean_reversion_5min` | EMA deviation + order-book imbalance | 0 fills — double-screened by book-quality gate |
| `opening_range` | Opening-range breakout on first N ticks | 0 fills — gate issue + was drifting (fixed) |
| `time_decay` | Near-resolution binary convergence plays | 0 fills — gate issue |

Candidates stay wired and tested but are **inactive by default**. Promotion requires:
1. Passing tests
2. Real fills in paper runtime
3. Positive pnl-per-fill evidence

## Quick Start — Paper Runtime

```bash
cd /root/obsidian-hermes-vault/projects/polymarket-5min-bot
.venv/bin/pip install -r requirements.txt
.venv/bin/python cli.py run --mode paper
```

Status:
```bash
.venv/bin/python cli.py status
.venv/bin/python cli.py health --max-heartbeat-age 180
```

Research:
```bash
.venv/bin/python cli.py research
```

## Deployment (systemd)

```bash
sudo bash deploy/systemd/install.sh
```

This installs:
- `polymarket-paper-bot.service` — main bot loop at 5s cadence
- `polymarket-paper-research.service` — one-shot research run
- `polymarket-paper-research.timer` — fires every 15 minutes

Rollback:
```bash
sudo bash deploy/systemd/uninstall.sh
```

## Configuration Secrets

Secrets live in `.env`, never in tracked files:
```
POLYMARKET_WALLET_ADDRESS=
POLYMARKET_PRIVATE_KEY=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

`config.yaml` contains only placeholders and public defaults.

## Runtime Artifacts

Written to `data/runtime/` (gitignored):
- `status.json` — current runtime state
- `events.jsonl` — append-only event stream
- `strategy_metrics.json` — per-family counters
- `market_samples.jsonl` — book-quality samples per loop
- `ledger.db` — SQLite append-only ledger
- `latest-status.txt` — human-readable snapshot

Research artifacts in `data/research/` (gitignored):
- `latest.json` / `latest.md` — newest autoresearch cycle
- timestamped `cycle-*.json` / `cycle-*.md` — history, bounded retention

## Tests

```bash
.venv/bin/python -m pytest -q
```

Covers: ledger, replay, paper exchange, settlement, risk reporting, restore workflow, runtime features, strategy selection, autoresearch cycle, time-decay behavior.

## Operating Rules

1. Trust ledger + replay + runtime artifacts over README or memory
2. Research outputs are advisory; strategies mutate only through config
3. Require tests + runtime evidence + restart safety before promotion
4. Prefer subtraction over adding cleverness
5. Optimize only after the truth layer is clean

## Notes

- 5/15-minute markets only; filters out longer-duration slots
- Market making is the baseline; directional strategies are candidates
- Expect modest PnL in paper mode; the goal is truthful evidence, not profit theater
- Polymarket API rate limits exist; the bot is polite

This software is provided as-is. You are responsible for your own decisions and compliance.
