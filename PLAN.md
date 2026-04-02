# Polymarket Bot — Bugs, Bottlenecks, Strategy Roadmap

Date: 2026-04-01

## CRITICAL BUGS FOUND

### 1. Hardcoded book quality thresholds in strategies (FIXED)
- `mean_reversion_5min.py` and `toxicity_mm.py` had `min_top_depth=25` and `min_top_notional=10` hardcoded as defaults in constructor
- Config.yaml sets them to 5 and 1, but if the strategies ever fell back to defaults, they'd reject everything
- FIX: Synced constructor defaults to 5 and 1

### 2. Truncated code in execution.py (FIXED)
- Line 150: `token_id=market...ome)` was truncated from a git merge artifact
- This wouldn't break paper mode (live-only path) but would crash if live is ever enabled
- FIX: Replaced with `token_id = market["token_ids"][outcome] if market else self._get_token_id(market_id, outcome)`

### 3. `research/polymarket.py` missing import (FIXED)
- `from research.loop import make_cycle_id` — function didn't exist
- Autoresearch was completely broken (import crash)
- FIX: Added `make_cycle_id` function to `research/loop.py`

### 4. systemd service using stale .venv python path (FIXED)
- Service file at `/etc/systemd/system/polymarket-paper-bot.service` had `.venv/bin/python` (dead venv)
- Was also using `--sleep-seconds 60` instead of 15
- Was using `Restart=on-failure` instead of `Restart=always`
- FIX: Rewrote to use `/usr/bin/python3` with `Restart=always`, 5s restart delay, 15s sleep

### 5. `telegram_alerts.py` missing from repo (RECREATED)
- File was created locally but not committed to the correct branch
- FIX: Recreated and pushed to main

## BOTTLENECKS (NOT BUGS — PERFORMANCE ISSUES)

### 6. Book quality thresholds too aggressive for 5m prediction markets
- 56% of market checks rejected as "untradeable" (9/16 toxic skips)
- Filters `min_top_depth=5` and `min_top_notional=1` are STILL too high for 5m markets
- These are binary prediction markets with $2-$20 natural depth, not CEX order books
- IMPACT: Mean reversion gets 0 signals, MM only quotes 4/8 markets per cycle
- FIX: Lower to `min_top_depth=2`, `min_top_notional=0.5`, or disable depth checks entirely for 5m markets

### 7. Mean reversion EMA warmup problem
- 5m Windows only give ~20 ticks at 15s polling
- EMA(5) needs 5 ticks = 75s. Then deviation signals need additional time
- Most 5m markets resolve before enough data points accumulate
- IMPACT: 0 signals fired across all testing
- FIX: Use `ema_period_5m=3` or switch to simple moving average for 5m
- OR: Add a "prewarm" mode where the bot subscribes to the previous window's data

### 8. ToxicityMM quote placement frequency
- 15s polling + 6 resting orders = quotes refresh every 30s (per config `quote_refresh_seconds: 30`)
- Too slow for 5m markets that resolve in 300s
- IMPACT: Missed fill opportunities, stale quotes
- FIX: Dynamic refresh based on market age (refresh more often when market is <60s from resolution)

### 9. Simultaneous open positions accumulating
- 5 open positions simultaneously (BTC 5m Up, XRP 5m Up, ETH 15m Up, BTC 15m Up, XRP 15m Up)
- MM is quoting Up on ALL markets (one-sided bias)
- IMPACT: Concentrated directional risk — if "Up" is consistently wrong, all positions lose
- FIX: Add directional balance constraint — limit total net directional exposure

### 10. No backtest data available
- `data/collection_` directory exists but no collected CSVs
- Backtester runs on `data/sample_backtest.csv` as a placeholder
- IMPACT: Can't verify strategy improvements before deployment
- FIX: Run the collector for 24+ hours to build a backtest dataset

## NEW STRATEGIES TO ADD

### 11. Opening Range Breakout (5m crypto)
- Thesis: First 60 seconds of a 5m window sets the range. Break of opening range = momentum continuation
- Implementation: Track price in first 3 ticks after window start. Enter when price breaks above high (buy Up) or below low (buy Down)
- Works on 5m because crypto pumps/dumps tend to persist within windows

### 12. Time Decay Arbitrage
- Thesis: In binary markets, price should decay toward 0 or 1 as resolution approaches
- If "Up" is at 0.52 with 2 minutes left, it should move toward 0 or 1 faster than linear decay
- Implementation: For markets within 60s of resolution, buy the side that's >0.55 at a discount
- Works on both 5m and 15m

### 13. Cross-Market Correlation Signal
- Thesis: If BTC 5m is Up at 0.70 and ETH 5m is Down at 0.30, that's dislocated (crypto correlates)
- Implementation: Track the price spread between correlated assets (BTC/ETH, SOL/XRP). When spread > threshold, bet on convergence
- Only 4 assets, so 4 cross-asset pairs to track

### 14. Resolution Momentum Carryover
- Thesis: If BTC was Up in the last 5m window, probability of Up in next window increases (autocorrelation)
- Implementation: After each settlement, carry the win direction as a prior for the next window's first tick
- Works because crypto trends don't flip direction on 5-minute boundaries

### 15. VPIN Regime Adaptation
- Current `toxicity_mm` uses static VPIN threshold (0.45)
- FIX: Make threshold adaptive based on recent fill rate. If too many fills (VPIN too low), widen threshold. If too few, narrow it.

## AUTORASEARCH LOOP PLAN

### 16. Current state of autoresearch
- `research/loop.py` is a protocol-based framework that runs `ResearchAdapter.run()` and saves results
- `research/polymarket.py` implements `PolymarketRuntimeResearchAdapter` that reads live telemetry and generates insights
- Both are broken: `make_cycle_id` was missing, the adapter only generates trivial insights
- FIX (done): Added `make_cycle_id` function

### 17. Autoresearch v2 — what it should do
```
Every 15 minutes:
1. Read status.json, strategy_metrics.json, events.jsonl
2. For each strategy:
   a. Calculate fill rate, PnL per trade, win rate by asset
   b. Compare current window's strategy performance vs historical average
   c. Identify parameter regimes that are working/bleeding
3. Generate actionable recommendations:
   a. "ToxicityMM: reduce quote size on 5m markets, fill rate too high → VPIN too low"
   b. "Mean Reversion: EMA period 3 gives 3.2% more signals on 5m BTC"
   c. "XRP 15m: 0.85 PnL per fill, increase position size by 50%"
4. Write recommendations to data/research/cycle-XXX.md
5. If a recommendation passes confidence threshold (>80%), auto-apply to config.yaml
```

### 18. Autoresearch v3 — Hermes Agent integration
- Create a cron job that runs `python cli.py research` every 15 minutes
- The research loop reads live PnL and telemetry
- Generate markdown reports with `research/loop.py`
- If Hermes agent is running alongside, it can read research artifacts and update memory/strategy

### 19. Autoresearch v4 — Strategy Evolution
- Store per-run config + PnL in a database
- After 100+ windows, run automated parameter grid search
- Use bandit algorithm (Thompson sampling) to allocate capital to best-performing parameter sets
- Kill strategies that are net negative after 50 windows

## IMMEDIATE ACTION ITEMS (PRIORITY ORDER)

1. **LOWER book quality thresholds to 2/0.5** — this unlocks mean reversion and MM on more markets
2. **Reduce sleep to 5s in systemd** — 15s is OK for discovery but 5s gives more ticks for EMA warmup
3. **Add Opening Range Breakout strategy** — simplest addition, complements existing strategies
4. **Add Time Decay strategy** — works on 15m markets for 60s before resolution, independent of order book depth
5. **Run collector continuously** — `python scripts/collector.py 2>&1 | tee -a data/collector.log` for 24h
6. **Set up parameter sweep cron** — every 4 hours, test EMA(3/5/7/10) and deviation(0.03/0.05/0.08/0.12) combinations
7. **Add direction balance constraint** to toxicity_mm
8. **Add VPIN adaptive threshold** — dynamic based on recent fill rate
9. **Build autoresearch dashboard** — read live PnL from events.jsonl every 15min, generate recommendations
10. **Add Telegram alerts for fills/settlements** — use telegram_alerts.py for real-time updates
