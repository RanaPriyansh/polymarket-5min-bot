# Polymarket Bot Review and Upgrade Inputs — 2026-03-25

## Blunt diagnosis
This repo is no longer just a toy backtest, but it is still not a production execution engine.

What improved now:
- position sizing is capital/risk-budget based instead of volume-proxy based
- mean reversion uses logit-space z-scores instead of naive raw deviation
- realized volatility now comes from recent history
- microprice + imbalance now gate reversal entries
- timeout exits are enforced consistently in backtests
- risk breaker no longer halts on profitable days

## What makes this exceptional next

### 1. Resolution-time edge
The real monopoly is not a generic indicator. It is learning the truth faster than the market reprices.

Build a resolver map per market family:
- sports -> play-by-play / official score feeds
- weather -> METAR / station feeds
- finance -> exchange/index ticks
- politics -> official reporting feeds

Trade only when source confidence is high and the book is stale.

### 2. Microstructure exhaustion classifier
Do not just fade big moves.
Fade failed moves.

Needed features:
- jump size relative to local sigma
- spread expansion vs compression
- top-book refill speed
- imbalance persistence
- cancel bursts / ghost liquidity
- short-horizon post-jump continuation vs exhaustion

### 3. Regime engine
Short-duration prediction markets are different in:
- opening discovery
- middle chop
- terminal minute
- post-resolution stale state

The bot should switch playbooks by regime, not run one rule everywhere.

### 4. Real toxicity model
Current VPIN proxy is not real toxicity.
Need:
- signed trade-flow imbalance
- book-delta imbalance
- adverse selection after fills
- quote fade rate before moves

### 5. Queue-aware execution
Price edge without fill edge is fake alpha.
Need:
- queue ahead estimates
- passive fill probability
- cancel/replace discipline
- stale-quote protection

## Highest-ROI implementation order
1. Safe paper broker / execution abstraction
2. Event-level market microstructure recorder
3. Regime classification
4. Exhaustion-vs-information move classifier
5. Resolver/source map for near-resolution markets
6. Complementary YES/NO dislocation logic
7. Inventory-aware market making with real toxicity inputs

## Candidate strategies to add

### A. Longshot bias fade
Trigger idea:
- YES price > 0.92 or < 0.08
- spread wide
- imbalance one-sided
- no trusted external confirmation
Fade the retail chase cautiously.

### B. Shock-and-revert
Trigger idea:
- move z-score > threshold
- microprice disagrees with last trade direction
- depth refills quickly
Enter fade with tight time stop.

### C. Complementary book dislocation
Exploit cases where YES and NO books imply inconsistent fair values after costs.

### D. Terminal-minute stale quote capture
Specialized strategy for the last 60–180 seconds when information hits faster than quotes are updated.

## Quant principles going forward
- calibrate expected edge, don’t infer it from threshold exceedance alone
- size from capital and worst-case loss, never from venue volume
- work in bounded-probability aware space (logit/z-score), not naive raw percentages
- backtests must include cost and fill realism before you trust them

## Current practical commands
Run tests:
- `. .venv/bin/activate && python -m unittest discover -s tests -v`

Run offline backtest:
- `. .venv/bin/activate && python cli.py backtest --data data/sample_backtest.csv`

## Current limitation
Do not confuse current backtest profitability with durable edge.
The moat is being built in the data + execution + resolver stack, not in one threshold rule.
