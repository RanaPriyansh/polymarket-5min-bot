# Polymarket Strategy Research Roadmap — 2026-04-15

## Brutal truth

The current repo does not need ten random new strategies.
It needs:
1. better execution truth
2. strategy-specific tradeability gates
3. a real external fair-value edge for 5m/15m crypto windows
4. research artifacts that can actually drive promotion decisions

## What we ran

### Runtime research loops executed
- `cli.py research --runtime-dir data/experiments/multi-family-bakeoff-60-retry-final/trials/toxicity_mm --artifact-dir data/research/toxicity_mm-60-retry-final --sample-limit 200`
- `cli.py research --runtime-dir data/experiments/multi-family-bakeoff-60-retry-final/trials/mean_reversion_5min --artifact-dir data/research/mean_reversion_60-retry-final --sample-limit 200`

### Key latest bakeoff result
- Winner: `toxicity_mm`
- Best challenger: `mean_reversion_5min`
- `time_decay` weak
- `opening_range` structurally suspect and likely mis-implemented around slot state

## Evidence-backed conclusions

### Current strongest family
- `toxicity_mm`
- Evidence: 90 fills, +5.7646 realized PnL, +4.4600 MTM, low drawdown

### Best challenger
- `mean_reversion_5min`
- Evidence: +2.4100 realized, +5.2626 MTM, only 13 fills, tiny drawdown
- Conclusion: real candidate, not yet promotable

### Families not worthy of promotion now
- `time_decay`
- `opening_range`

## Highest-leverage repo fixes

### 1. Turn on strategy-specific tradeability config
The code already supports this in `tradeability_policy.py`.
Current repo leaves edge on the floor by using one global gate.

Immediate experiment:
- Loosen directional families separately from MM
- Measure fills, realized pnl, and markouts by family

### 2. Make execution respect `signal.price`
Current directional strategies compute target prices, but execution crosses immediately at best bid/ask.
That means the repo pretends to trade limits while actually paying marketable entry costs.

Immediate experiment:
- for signal families, refuse fills worse than `signal.price`
- or place resting paper limits and score them honestly

### 3. Rebuild `opening_range` around `slot_id`
The strategy claims to trade the opening range of interval windows, but its state is keyed by `market_id` and the reset path is effectively dead.
This is likely why the strategy is garbage right now.

## Best new strategy directions

### A. Crypto reference-lag / fair-value model
This is the most important external edge discovered from X/bookmark mining.
Repeated motif: Polymarket 5m/15m crypto windows lag live external reality.

Build:
- external reference price feed snapshot per loop
- strike-aware probability model for finish-above / finish-below
- trade when Polymarket implied price materially deviates from external fair value

This is the best fit for the current repo.

### B. YES+NO under-$1 arb scanner
Mechanical edge, simple, measurable, crowded but still useful.

Build:
- scan all active short-duration crypto windows
- detect complementary under-sum / dislocation
- paper-trade corrections with strict fill accounting

### C. Adaptive / regime-switching MM
`toxicity_mm` already wins, but wastes a huge amount of opportunity on static rules.

Build:
- dynamic spread tolerance by regime
- dynamic toxicity threshold
- explicit handling for `wide_spread`, `missing_side`, `imbalanced_depth`

### D. Sharp-wallet mining as research, not copy-trading
Don’t blindly copy.
Use profitable wallet timing and market-selection behavior as feature generation for discovery.

### E. Crushed-side overshoot strategy
Only after execution truth improves.
Potentially valuable, but easier to hallucinate than reference-lag edge.

## X/bookmark corpus findings

The strongest recurring usable-now themes were:
1. crypto spot/reference lag
2. YES+NO arbitrage
3. execution alpha > indicator alpha
4. whale/copy-wallet research overlays

Themes that require expanding scope beyond this repo:
- weather forecasting edges
- headline / narrative / mention markets
- resolver-specific non-crypto event markets

## Research-loop problems that must be fixed

### 1. Gate RED still leaks recommendations
If research is RED-gated, the report should stop making promotion-style suggestions.

### 2. Current-run and cumulative metrics are mixed
That makes reports sound more precise than they are.
Need:
- current_run_metrics
- trailing_metrics
- all_time_metrics

### 3. Fill-rate math is invalid for directional families
A family with zero quotes should not show 900% fill rate.

### 4. Promotion still relies on weak short-window evidence
No family in the latest bakeoff had settled evidence.
Do not promote from that alone.

## Minimum next implementation steps

1. Add per-strategy tradeability overrides in `config.yaml`
2. Fix execution to honor `signal.price`
3. Fix `opening_range` state to key/reset on `slot_id`
4. Add fair-value crypto reference model as the next serious candidate family
5. Upgrade research outputs into machine-readable experiment packets and a persistent family scoreboard

## Suggested next commands

### Immediate focused bakeoff after config/execution fixes
- head-to-head only:
  - `toxicity_mm`
  - `mean_reversion_5min`

### Research refresh
- `.venv/bin/python cli.py research --runtime-dir data/runtime --artifact-dir data/research --sample-limit 400`

### Candidate trial research
- `.venv/bin/python cli.py research --runtime-dir data/experiments/<experiment>/trials/<family> --artifact-dir data/research/<family>-<experiment> --sample-limit 200`

## Operator priority order

P0
- execution truth
- tradeability by family
- opening range repair

P1
- external fair-value crypto lag model
- adaptive MM variant

P2
- under-$1 dislocation arb layer
- wallet-mining research overlay

## Final recommendation

Do not scatter attention.
The right sequence is:
1. fix execution + gating truth
2. double down on `toxicity_mm`
3. push `mean_reversion_5min` as the main challenger
4. build the crypto reference-lag fair-value family
5. use X/bookmark mining as a strategy discovery input, not as a substitute for evidence
