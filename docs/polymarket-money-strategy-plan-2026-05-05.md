# Polymarket Money Strategy Plan — 2026-05-05

## Brutal diagnosis

Current paper runtime before patch was bleeding because it kept spending on broad exploration instead of capital allocation.

Evidence from runtime/events before the patch:

- `toxicity_mm`: 983 settled, -$113.47, -$0.115/trade.
- `mean_reversion_5min`: 150 settled, -$64.90, -$0.433/trade.
- `time_decay`: 404 settled, -$14.73, -$0.036/trade.
- `market_open_probe`: 4,755 settled, -$18.47, -$0.0039/trade; useful for research, bad as active capital.
- `spot_momentum`: zero fills; dead strategy as configured.
- `opening_range`: 75 settled, +$8.53, +$0.114/trade; only current active family with real positive settled evidence.

Contrarian secret from the bookmark corpus: the profitable Polymarket bot claims keep repeating the same pattern — not “predict better,” but “execute market structure faster.” Repeated motifs:

- Short 5m/15m crypto markets misprice because Polymarket quote updates lag CEX spot impulses.
- Limit-order-only arbitrage and maker/taker discipline beat vibe-based directionality.
- Complement pricing dislocations (`YES + NO < 1`) are mechanical, not narrative.
- Wallet/copy-trade intelligence matters, but only after we have a clean execution/evidence layer.
- Broad autonomous exploration without gates becomes tuition, not alpha.

## Immediate implementation performed

### 1. Kill the losers from active paper capital

Active set changed to:

- `opening_range`
- `terminal_fair_value`

Moved to scanner-only:

- `market_open_probe`
- `time_decay`
- `mean_reversion_5min`
- `spot_momentum`

### 2. Fix probe-gate bug

`market_open_probe` previously ignored the normal strategy entry gate. That meant paused negative buckets could keep opening new paper trades. New helper:

- `_paper_market_open_probe_entry_allowed(...)`

It enforces:

- paper mode only;
- strategy active only;
- no existing market exposure;
- runtime gate;
- bucket pause gate.

### 3. Activate terminal fair-value strategy

`terminal_fair_value` now converts scanner decisions into bounded paper signals when:

- time-to-expiry is <= 60s;
- book age <= 500ms;
- spot age <= 1000ms;
- spread <= 6c unless fair is extreme;
- model edge over executable ask >= 12c;
- entry price is between 2c and 88c;
- max notional is $2 before risk manager bound.

This is the direct implementation of the bookmark thesis: buy only when spot-implied terminal probability is materially above Polymarket executable ask.

## Next strategies to implement after this patch settles evidence

### P0 — CEX lag impulse trader

Goal: detect Binance/Coinbase spot jumps before Polymarket quotes move.

Entry:

- Track spot over 1s/3s/5s windows.
- Recompute terminal probability vs market ask.
- Buy only if edge survives latency penalty and book depth check.

Gate:

- paper-only until >= 50 settled trades and +$0.05/trade.

### P1 — Complement dislocation executor

Existing scanner finds `YES + NO < 1` and `YES + NO > 1`; it is not executable yet.

Implementation path:

- Implement atomic-ish two-leg paper order executor.
- Buy both legs only when ask sum + fees + slippage buffer < 0.98.
- Refuse one-leg fills unless hedge can complete inside bounded slippage.

### P2 — Wallet intelligence/copy-trade research layer

Bookmarks repeatedly mention profitable wallets. Do not copy blindly.

Implementation path:

- Ingest public wallet trade history.
- Build wallet performance features by market family/asset/TTE.
- Use as a filter or prior, not as raw auto-copy.

### P3 — Rewards-aware market making

Only after toxicity controls are fixed.

Implementation path:

- Quote only in reward-eligible markets.
- Score expected reward minus adverse selection.
- Flatten before endgame unless holding both sides profitably.

## Promotion rule

No live mode. Paper promotion requires:

- settled trades >= 30;
- win rate >= 45%;
- pnl/trade >= $0.05;
- no hidden stale-artifact or bucket-pause contradictions.

If a strategy fails those gates, it stays scanner-only or gets killed.
