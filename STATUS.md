# Polymarket 5/15m Bot — Canonical Status

Last updated: 2026-04-03

This file is the canonical project-truth snapshot. Code + runtime artifacts still outrank this document, but this file is the single human-readable status source. Older narrative docs are secondary and may be stale.

## Mission

Build a restart-safe, ledger-backed, replayable paper trading system for Polymarket 5m and 15m interval markets.

Live trading is blocked.

## Current operating stance

- Paper only.
- Baseline runtime family: `toxicity_mm`
- Research-only families: `mean_reversion_5min`, `opening_range`, `time_decay`
- Research is advisory only.
- Promotion requires runtime evidence, restart safety, and settled-paper evidence.

## What is real and useful now

- Ledger-backed order/fill/position/settlement state
- Deterministic replay + restart restore
- Mark-to-market and exposure-aware risk reporting
- Runtime status/health surfaces
- Runtime artifact emission to `data/runtime/`
- Systemd packaging for paper bot
- Passing test suite (`pytest -q`)

## What is still weak

- Strategy truth is weaker than execution truth
- Mixed-run runtime artifacts can contaminate research conclusions
- Replay-backed family metrics are not yet a clean single source of truth
- Settlement is code-complete-ish but still needs stronger runtime proof over longer paper runs
- Book-quality / tradeability logic is duplicated across CLI and strategies
- `opening_range` remains experimental and not trusted
- Docs still drift across multiple markdown files

## Immediate operator rules

1. Do not enable live mode.
2. Do not add more strategies.
3. Do not auto-apply research outputs.
4. Do not trust stale docs over `config.yaml`, runtime artifacts, or tests.
5. Use `toxicity_mm` as the only default runtime family until promotion gates are met.

## Source-of-truth order

1. Code
2. Config
3. Runtime artifacts
4. Tests
5. Internal worklogs / plans
6. README / docs

## Known truth conflicts

- Old docs still imply live-readiness or stale performance claims.
- Research artifacts may blend current and prior runs.
- `status.json` and `strategy_metrics.json` can drift because they are written separately.

## Immediate priorities

- Run-scoped runtime truth
- Canonical tradeability policy
- Secret-free tracked config
- One canonical plan, one canonical status doc
- Stronger settled-paper evidence for `toxicity_mm`
