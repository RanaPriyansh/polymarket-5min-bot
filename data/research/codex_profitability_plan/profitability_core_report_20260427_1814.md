# Profitability Core Report

Generated: 2026-04-27 18:14 UTC

## A. Files Changed

- Safety/runtime: `config.yaml`, `deploy/systemd/polymarket-paper-bot.service`, `cli.py`, `strategy_state.py`
- CLOB V2 read-only data: `market_data.py`, `docs/paper_runtime_runbook.md`, `tests/test_market_data_v2.py`
- Evidence truth: `settlement_engine.py`, `evidence_mart.py`, `tests/test_evidence_mart.py`
- Pessimistic simulator: `paper_exchange.py`, `execution.py`, `tests/test_paper_exchange.py`, `tests/test_restore_workflow.py`
- Scanners/models: `models/terminal_probability.py`, `external_spot.py`, `market_rules.py`, `strategies/terminal_fair_value.py`, `strategies/lag_scanner.py`, `strategies/complement_dislocation.py`, `strategies/toxicity_mm.py`
- Experiment config: `configs/experiments/clean_72h_terminal_fair_value.yaml`

## B. Safety Invariants

- Branch: `codex/profitability-core-v2`
- Preflight snapshot: `data/runtime/ops_snapshots/20260427_175706_pre_profitability_core`
- Local `.venv/bin/python` was absent, so snapshot `cli.py status` and `cli.py health` commands recorded that failure instead of mutating runtime state.
- `systemctl` is not available in this macOS workspace; service state was captured as unavailable.
- `pgrep` snapshot returned `10674`, but `ps -fp 10674` showed no live process by the time it was inspected.
- Live CLI mode remains blocked, and executor live mode now raises before any live session/order/cancel path.
- Packaged paper bot systemd unit changed from `Restart=always` to `Restart=no`.

## C. CLOB V2 Readiness

- Config supports `clob_api_version: v2`, `clob_environment`, and production/staging/read-only CLOB URLs.
- `PolymarketData.clob_read_only_check()` reads only `/ok`, `/version`, and `/time`.
- Orderbook metadata now preserves token id, condition id, levels, best bid/ask, spread, tick size, min order size, timestamp, hash, neg risk, fee fields, and metadata completeness.
- No V2 live order creation is enabled.

## D. Evidence Schema Changes

- `slot_settled` payloads now include `slot_id`, `strategy_family`, `strategy_families`, and `settled_pnl`.
- Order and fill payloads now carry maker/taker, fill reason, simulator assumption, confidence, fees, slippage, and optional model fair/edge fields.
- Added `cli.py build-evidence-mart`, `analyze-family-performance`, `analyze-tte-performance`, and `analyze-markout-vs-settlement`.
- Latest evidence mart: `data/research/evidence_mart/latest.json` and `.md`.
- Current local evidence mart is RED because this workspace has no ledger-backed evidence rows.

## E. Paper Simulator Realism

- Maker fills no longer trigger on midpoint/cross touch alone.
- Maker fills require trade-through, same-price queue exhaustion, or level-consumption evidence.
- Queue ahead, queue fill probability, fill confidence, and fill reasons are recorded.
- Taker fills walk visible book depth, support partial fills, slippage, stale guards, and fee estimates.
- Cancellation latency is modeled at the fill-engine level via `cancel_pending` and `cancel_effective_at`.

## F. Strategy States

- Default config demotes `toxicity_mm` and `time_decay` to `disabled`.
- `terminal_fair_value`, `lag_scanner`, and `complement_dislocation` are `scanner_only`.
- `cli.py run` refuses active strategies unless their configured state is `paper_active`, unless explicit override config is set.

## G. New Scanner Outputs

- Terminal fair-value scanner emits scanner-only decisions with model fair, model edge, book/spot freshness, strike, TTE, and vetoes.
- Lag scanner emits fair delta, market delta, lag, edge before/after latency, suggested side, and expected fill size.
- Complement scanner detects YES/NO ask-sum and bid-sum dislocations with executable size and fee-buffered edge.
- `toxicity_mm` now includes an adverse-selection veto assessment path.

## H. Experiment Config

- Added `configs/experiments/clean_72h_terminal_fair_value.yaml`.
- It is scanner-only by default, paper-only, live blocked, healthcheck no-restart, old `toxicity_mm` disabled, old `time_decay` disabled, 5m crypto only, and TTE <= 60s for terminal candidate decisions.

## I. Tests Run

- `python3 -m py_compile cli.py market_data.py execution.py paper_exchange.py settlement_engine.py evidence_mart.py strategy_state.py external_spot.py market_rules.py models/terminal_probability.py strategies/terminal_fair_value.py strategies/lag_scanner.py strategies/complement_dislocation.py strategies/toxicity_mm.py`
- `python3 -m pytest -q tests/test_paper_exchange.py tests/test_market_data_v2.py tests/test_evidence_mart.py tests/test_terminal_probability.py tests/test_terminal_fair_value_strategy.py tests/test_strategy_state.py`
- `python3 -m pytest -q`

Result: full suite passed, 67 tests.

## J. Remaining Blocked

- Official V2 live signing, pUSD collateral, builder metadata, geoblock checks, and human approval remain blocked.
- The scanner modules are not wired to place orders.
- A clean 24h scanner-only run has not been started from this workspace.
- The evidence mart is structurally available, but current local runtime artifacts do not contain usable evidence rows.

## K. Scanner-Only 72h Commands

Do not run until V2 read-only smoke checks pass:

```bash
python3 cli.py build-evidence-mart --runtime-dir data/runtime
python3 cli.py analyze-family-performance --runtime-dir data/runtime
python3 cli.py analyze-tte-performance --runtime-dir data/runtime
python3 cli.py analyze-markout-vs-settlement --runtime-dir data/runtime
```

The clean experiment config is prepared at:

```bash
configs/experiments/clean_72h_terminal_fair_value.yaml
```

Runtime launch still needs an explicit config-loading path for alternate experiment files before using that YAML directly.

## L. Live Remains Blocked

Live mode remains disabled at CLI and executor level. No live orders were placed or enabled.
