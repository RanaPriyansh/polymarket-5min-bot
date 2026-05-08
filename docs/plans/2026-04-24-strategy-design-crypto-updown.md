# Strategy design: Polymarket 5m/15m crypto up/down markets

Scope: paper-only strategy candidates intended to move the runtime from YELLOW to GREEN within 72h. This document is deliberately ruthless: strategies that cannot be validated quickly with current repo primitives are demoted or killed.

Local repo context checked 2026-04-24:
- Runtime code path: `cli.py` has MarketData, orderbook snapshots, seconds-to-resolution, regime classifier, RiskManager, paper executor, telemetry, market event recorder.
- Existing strategy modules: `toxicity_mm`, `mean_reversion_5min`, `shock_reversion`, `dislocation_arb`, `terminal_resolver`.
- Current config actively runs mean_reversion_5min, shock_reversion, dislocation_arb, toxicity_mm.
- Latest local `status.json` shows runtime error from official CLI market listing, no positions, 10 open paper orders, total_pnl 0.0.
- Latest local `market_events.csv`: 1,270 rows, 20 markets, zero active_signal_family rows, regimes: 745 stressed, 283 calm, 221 one_sided, 14 closing, 7 terminal. YES spread bps median ~351, mean ~569. This file is mostly non-crypto/all-liquid sports/general observations, so it is useful for implementation constraints but not edge proof.
- Known evidence from prompt/context: `toxicity_mm` is slightly negative on settled attribution despite surface PnL. Treat maker quoting as guilty until proven otherwise.

Ranking key:
- Expected edge: High/Med/Low relative to current bot, not absolute certainty.
- Difficulty: repo implementation + data requirement.
- Risk: adverse selection, stale oracle/resolution risk, inventory risk, hidden fill bias.
- Speed to validate: can we get meaningful paper evidence in 72h?

## Ranked candidates

| Rank | Candidate | Verdict | Expected edge | Difficulty | Risk | Speed to validate | Why |
|---:|---|---|---|---|---|---|---|
| 1 | Terminal fair-value taker/leaner from spot return into close | BUILD FIRST | High | Med | Med | Fast | 5m/15m crypto markets settle from spot/index; near close, fair probability collapses toward binary outcome. Current repo already has TTE, orderbook, paper executor; only needs SpotProvider + signal wrapper. |
| 2 | Adverse-selection-aware passive skew, not symmetric MM | BUILD SECOND | Med-High | Med | Med | Fast | Current toxicity_mm likely loses because it quotes symmetric stale fair. Replace with one-sided maker only when spot, microprice, and inventory all agree; cancel aggressively on spot moves. |
| 3 | Opening 20-60s volatility underreaction momentum | BUILD THIRD | Med | Low-Med | Med-High | Fast | Immediately after market opens, books often anchor near 50 while spot already trends. Validate with paper quickly, but kill if fill rate low or markouts negative. |
| 4 | Shock fade after non-informational book jump | KEEP AS NARROW FILTER | Med | Low | High | Medium | Existing shock_reversion is close; only works when spot does not confirm. Must add spot-confirmation veto; otherwise it fades real information. |
| 5 | YES/NO complement capture | DEMOTE | Low-Med | Low | Med | Medium | In local data NO side often absent/zero. Good when both books are real, but current adapter may not provide enough NO depth. Use as diagnostic/arb only, not core edge. |
| 6 | Time-decay / 98-99% carry | KILL FOR NOW | Low | Low | Tail High | Slow/False | Small apparent carry masks resolution/tail loss and capital lock. Prompt says settled attribution is weak; do not promote without large resolved sample. |
| 7 | Generic mean reversion without spot | KILL/REWRITE | Low | Low | High | Fast | Crypto up/down is path-to-terminal, not stationary mean reversion. Reversion should only be traded when spot says book overshot. |
| 8 | Cross-exchange latency arb vs Binance/Coinbase | DEFER | High theoretical | High | High | Slow | Needs low-latency websocket, precise resolver source, fill queue model, and live execution. Not 72h paper-ready. |

# 1. Terminal fair-value taker/leaner from spot return into close

Thesis:
In 5m/15m crypto up/down markets, the true fair probability is a function of current spot relative to market start/reference price, remaining time, realized short-horizon volatility, and resolver convention. In the final 30-90s, if spot is already above/below the reference by more than expected residual noise, the fair price should move rapidly toward 0/1. Polymarket books can lag, especially if market makers avoid terminal adverse selection.

Conditions:
- Crypto-only up/down markets with parsable asset and expiry.
- Need market start/reference price. If not available from market metadata, record first spot tick seen after market discovery as `reference_price` and mark confidence lower for markets discovered late.
- TTE between 15s and 120s. Prefer 20-75s.
- Spot feed fresh: max age <= 2s for live/paper signal; <= 5s for paper-only initial validation.
- Spread <= 250 bps for taker entries or <= 500 bps for passive-leaning entries.
- Top ask/bid has enough size for intended paper order.
- No CLI/data-source errors; skip if market source is not crypto filtered.

Signals:
- `spot_return = ln(spot / reference_spot)`.
- `rv_short = std(log returns over last 60-180s)` annualization irrelevant; use per-second/per-sample residual.
- `distance_z = spot_return / (rv_short * sqrt(TTE_seconds / sample_interval_seconds))` with floor sigma.
- Fair win probability: `p_fair = normal_cdf(distance_z)` for UP/YES; `1-p_fair` for DOWN/NO if needed.
- Edge to buy YES: `p_fair - yes_ask - fees/slippage_buffer`.
- Edge to sell YES or buy NO: `yes_bid - p_fair - buffer`.
- Confirmation: microprice direction agrees with edge or at least does not strongly disagree.

Entry:
- Taker version: BUY YES at ask when `p_fair - ask >= 0.025` and `distance_z >= 1.25`; SELL/short only if repo supports clean sell of inventory; otherwise buy NO when available.
- Passive-leaner version: post BUY one tick above best bid only if edge >= 0.04 and TTE > 35s; cancel/reprice every loop/tick.
- Do not enter new terminal positions if TTE < 12s because fill/settlement simulation becomes unreliable.

Exit:
- If paper executor supports selling, exit when edge flips negative by > 1c or when TTE < 8s and mark has not converged.
- Otherwise hold to settlement, but cap size as full binary loss.
- Cancel all resting orders when spot crosses reference against position or spot age stale.

Risk controls:
- Max one open terminal position per market.
- Max notional per market: 0.5-1.0% bankroll until settled evidence exists.
- Max terminal strategy daily loss: 1.5% bankroll.
- Require `distance_z` buffer bigger when TTE longer; at 90s use >=1.5, at 30s use >=1.0.
- Never average down near terminal.
- Market discovery late: halve size if reference_spot was not captured within first 15s of market life.

Failure modes:
- Wrong reference price or resolver source.
- Spot provider differs from Polymarket resolver index.
- Paper fills overstate taker/slippage or ignore queue priority.
- Book is right because it knows reference/resolver nuance we missed.
- Gap in spot feed creates false confidence.

Paper-test plan:
- Run crypto-only universe for 72h with terminal strategy in shadow + paper mode.
- Log every evaluated market once per loop: reference_spot, current_spot, TTE, p_fair, bid, ask, edge, action, fill/no-fill.
- Required promotion evidence: >=50 terminal decisions or >=20 fills; resolved/settled PnL positive after binary settlement; 60s/terminal markouts positive; no single loss > planned binary risk; performance positive separately for BTC and ETH if both traded.
- Kill if 20+ fills have negative settled expectancy or if more than 2 losses come from wrong reference mapping.

Exact implementation steps:
1. Add `spot_provider.py` with async provider for Binance/Coinbase public ticker websocket or REST fallback. Minimal first pass: REST every 1-2s for BTC, ETH, SOL, XRP if websocket too much for 72h.
2. Add `MarketContext`/runtime cache in `cli.py` keyed by market_id: asset, direction/up-down mapping, discovered_ts, start_ts, end_ts, reference_spot, reference_confidence.
3. Add parser helper in `market_universe.py` or new `crypto_market_parser.py` for titles/slugs containing Bitcoin/Ethereum/Solana/XRP + Up/Down + 5M/15M window.
4. Add `strategies/terminal_spot_fair.py` with dataclass signal and `generate_signal(market, ob, context, spot, risk_manager)`.
5. Add config block `terminal_spot_fair`: enabled params for `min_edge`, `max_seconds_to_resolution`, `min_seconds_to_resolution`, `min_distance_z`, `max_spread_bps`, `spot_max_age_sec`, `kelly_fraction`.
6. Wire into `cli.py` before `toxicity_mm`; if terminal signal fires, do not allow maker quote on same market.
7. Extend `EventRecorder` columns or telemetry event with spot/reference/fair fields. If changing CSV is risky, append JSON events `strategy.terminal_spot_fair.evaluated` and `strategy.terminal_spot_fair.signal`.
8. Add tests with synthetic orderbook + context: profitable UP, no trade stale spot, no trade wide spread, no trade wrong TTE, size capped.
9. Run `python cli.py run --mode paper --strategies terminal_spot_fair --universe-mode short_horizon_crypto` for smoke with `--max-loops 2` before 72h.

# 2. Adverse-selection-aware passive skew (replacement for toxicity_mm)

Thesis:
Symmetric market making is selected against in crypto binary markets because takers hit stale quotes when spot moves. A maker can have edge only by quoting one side, far enough from fair, while canceling on spot moves and skewing inventory. The goal is not spread capture everywhere; it is selective liquidity provision when fair value is stable and book microstructure pays us to wait.

Conditions:
- Crypto-only, TTE 90s to 12m. Avoid final 60s unless terminal_spot_fair owns the action.
- Spot return over last 5-15s below threshold; no jump > 0.5 residual sigma.
- Spread >= 1c or >= 150 bps; enough spread to be paid.
- Top-book imbalance not toxic against quote: for BUY quote require microprice >= mid or stable; for SELL quote require microprice <= mid or stable.
- Existing inventory small or quote reduces risk.

Signals:
- Compute spot-implied fair `p_fair` as above but for longer TTE, with wider uncertainty.
- Buy quote only if `bid_price <= p_fair - min_maker_edge`.
- Sell quote only if `ask_price >= p_fair + min_maker_edge` and we own inventory or repo supports short safely. Prefer inventory-reducing sells only.
- Cancel if fair moves within 1c of quote or spot age > 2s.
- Inventory skew: if long YES, lower buy price and make sells more aggressive; if short/long NO, inverse.

Entry:
- Post-only BUY at `min(best_bid + tick, p_fair - edge_buffer)`, not crossing.
- Post-only SELL only against existing inventory at `max(best_ask - tick, p_fair + edge_buffer)`.
- Quote one side only. Never bid and ask simultaneously until evidence says adverse selection is controlled.

Exit:
- Inventory-reducing limit orders when price exceeds fair by >=1c.
- Cancel all quotes on spot jump, spread collapse, TTE < 75s, or market parser uncertainty.

Risk controls:
- Quote TTL <= 5s; since current loop sleeps 60s, this strategy requires either shorter loop for crypto mode or quote timestamp cancellation at next tick. With 60s loop, keep it shadow-only or use immediate cancel/replace each loop with very conservative prices.
- Max open passive orders per market: 1.
- Max inventory per market: 0.5% bankroll pre-validation.
- No quote if previous fill in same market has negative 10s/30s markout.

Failure modes:
- Current paper broker fills resting orders only when later book crosses; it may not model queue priority or adverse selection accurately.
- 60s runtime loop is too slow; live quotes would be stale.
- Spot-implied fair misspecified.

Paper-test plan:
- Run in shadow first: log hypothetical quotes and whether subsequent mid would have crossed/adversely selected.
- Then paper with max size tiny and quote only every loop. Compare 10s/30s/60s markout by quote side.
- Promote only if filled paper orders have positive 30s/60s markout and settled PnL over >=50 fills. If no fills in 72h, keep as research but not GREEN blocker.

Exact implementation steps:
1. Do not modify `toxicity_mm`; create `strategies/spot_skew_mm.py` so attribution is clean.
2. Reuse terminal fair-value/context code.
3. Add `cancel_all_market` before every quote; never leave old quotes across loops.
4. Add quote metadata: fair, edge, spot_move_5s, quote_ttl, inventory.
5. Add config `spot_skew_mm` with `min_maker_edge: 0.02`, `max_tte: 720`, `min_tte: 75`, `spot_jump_veto_bps`, `max_inventory_usd`.
6. Wire after directional strategies but before legacy `toxicity_mm`; disable legacy toxicity_mm during validation to avoid mixed attribution.

# 3. Opening 20-60s volatility underreaction momentum

Thesis:
At market open, books often initialize around 50/50 or stale from prior window, while crypto spot may already have momentum relative to the new reference. Early trend continuation can be monetized before the market reprices, especially in 15m markets where TTE is long enough for exits.

Conditions:
- Market age 15-75s since start/reference captured.
- Current spot already moved from reference by `abs(return) >= 0.4 * short_sigma` or fixed min bps floor.
- 5s and 15s spot momentum same sign.
- Spread <= 300 bps; avoid one-sided illiquid books unless edge huge.
- Book price not already extreme: 0.20 < YES mid < 0.80.

Signals:
- `ret_since_open`, `ret_5s`, `ret_15s`.
- `p_fair` from spot/ref/TTE.
- `underreaction = p_fair - yes_ask` for up/YES. Trade only if underreaction >= 2-3c.
- Microprice confirmation: for BUY, microprice >= mid or bid imbalance positive.

Entry:
- BUY YES/UP at ask or passive near bid depending spread. For DOWN use NO if available; if not, only trade UP markets where YES is the up outcome.
- Prefer taker only when edge >= 4c; otherwise passive.

Exit:
- Exit on momentum decay: ret_5s reverses or p_fair edge < 0.5c.
- Time stop: after 90s from market start if no improvement.
- Hard stop: spot crosses reference against position.

Risk controls:
- Smaller size than terminal: 0.25-0.5% bankroll.
- Max one trade per market in opening window.
- Do not trade after major scheduled macro/news unless intentionally tagged; volatility can overwhelm signal.

Failure modes:
- Reference capture wrong by a few seconds makes signal fake.
- Momentum reverses because crypto microstructure is noisy.
- Wide spreads consume edge.

Paper-test plan:
- 72h shadow + paper for crypto-only. Need >=30 opening windows evaluated; fills may be fewer.
- Track 30s/60s markouts and final settlement separately. Opening momentum may show good early markouts but bad hold-to-settle; require exit simulation to be credible.
- Kill if positive signals are mostly no-fill passive orders or if taker entries lose > spread on 60s markout.

Exact implementation steps:
1. Add `strategies/opening_spot_momentum.py` using the same MarketContext and SpotProvider.
2. Require context `market_age_sec` and `reference_confidence`.
3. Add state per market to prevent duplicate opening trades.
4. Wire before mean_reversion; opening momentum and mean reversion should not both trade same market.
5. Add tests for age gates, momentum agreement, edge threshold, duplicate prevention.

# 4. Shock fade with spot-confirmation veto

Verdict: keep only as a narrow liquidity-shock reversion strategy. Existing `shock_reversion` fades book jumps using microprice/imbalance, but for crypto the missing critical feature is spot confirmation. If spot confirms the book jump, fading is adverse selection.

Upgrade thesis:
Fade a Polymarket price jump only when external spot did not move enough to justify it and the book shows reversal pressure.

Conditions/signals:
- Existing `jump_zscore` in book price >= threshold.
- External spot-implied `p_fair` moved less than 30-40% of book move.
- Microprice disagrees with jump direction.
- Spread <= 250-300 bps.
- TTE > 90s; do not fade terminal information.

Entry/exit:
- Same as current shock_reversion, but action vetoed unless `book_move - fair_move >= min_overshoot`.
- Exit when mid reverts half the jump, or after 60s, or if spot begins confirming book direction.

Risk controls:
- Tiny size until proven; shocks are where toxic flow hides.
- No trade if spot feed stale.
- No trade around exchange outages/news candles.

Paper-test plan:
- Implement as modification or new `spot_shock_reversion.py` for clean attribution.
- Compare current shock_reversion signals vs spot-vetoed signals in shadow. If the veto removes most losers, keep. If it removes all trades, demote.

Exact implementation steps:
1. Clone current `shock_reversion.py` to `spot_shock_reversion.py` and add spot/context args.
2. Add fair-value time series to estimate fair_move between last loop and current loop.
3. Add `min_book_fair_overshoot` config, default 0.025.
4. Wire but do not run simultaneously with old shock_reversion in paper attribution.

# 5. Complementary YES/NO dislocation

Verdict: demote as opportunistic diagnostic. The strategy is conceptually valid when YES+NO books are both populated and token mapping is correct. Local event data shows NO side often zero in current adapter observations, so this cannot be a primary 72h GREEN path.

Use only when:
- Both YES and NO have bid/ask depth.
- `yes_ask + no_ask < 0.98` gives direct buy-both arb to settlement, or `yes_bid + no_bid > 1.02` gives sell-both if shorting/collateral mechanics are supported. Prefer true locked complement over mid/microprice dislocation.
- Token mapping verified.

Kill weak version:
- Do not trade microprice sum dislocation alone if it requires shorting or assumes executable mid. That is not robust.

Implementation steps:
1. Modify `dislocation_arb` to check executable best bid/ask sums, not just microprice sum.
2. Add `requires_both_books=True` and skip if NO book empty.
3. Record skipped reason counts; if skip rate >95%, keep demoted.

# 6. Time-decay / high-probability carry

Verdict: kill for live authority now.

Reason:
Buying 98-99c outcomes for tiny yield is exactly where hidden tail/resolution risk and fees dominate. Prompt says toxicity/time_decay evidence is not GREEN; settled attribution matters more than surface PnL. In 5m crypto, a 99c side can become 0 on one candle. Without a demonstrably superior fair model and settlement sample, this is a yield trap.

Only future use:
- As an exit/settlement helper for positions already held, not as a fresh-entry alpha.

# 7. Generic mean reversion without spot

Verdict: kill or rewrite into spot-conditioned reversion.

Reason:
The current `mean_reversion_5min` uses Polymarket price history, z-score, microprice and imbalance. For crypto up/down, Polymarket price is largely a transformation of external spot distance to reference, not a stationary price series. A price away from EMA may be correct, not overextended.

Salvage:
- Replace EMA anchor with spot-implied fair. Revert only when `market_price - p_fair` is extreme and spot momentum is flat/opposite.

# 8. Deferred high-theoretical strategies

Killed/deferred for 72h:
- True cross-exchange latency arb: likely highest theoretical edge but needs websocket latency discipline, precise resolver mapping, live order placement, and queue model.
- News/event timing for crypto macro prints: edge may exist around CPI/FOMC/exchange outages, but sample in 72h too low and risk high.
- Funding/open-interest predictors: too slow for 5m/15m and needs external derivatives data not in repo.
- Martingale/averaging around binary terminal: hard kill.

# 72h validation operating plan

Phase 0: fix measurement before alpha
- Run only crypto universe. Current local config is `all_liquid` and latest CSV is mostly sports/general; that cannot validate crypto up/down strategies.
- Fix official CLI listing error or switch to gamma fallback for paper if CLI remains unstable.
- Disable legacy `toxicity_mm` during new strategy validation to avoid mixed attribution.

Phase 1: shadow all, trade one
- Day 1: run `terminal_spot_fair` in paper, `spot_skew_mm` and `opening_spot_momentum` in shadow events only.
- Day 2: if terminal has sane fills/markouts, add opening momentum paper with tiny size.
- Day 3: add spot_skew only if loop frequency and cancellation semantics are adequate; otherwise keep shadow.

Promotion metrics:
- Strategy-attributed settled PnL > 0 after costs/slippage assumptions.
- 60s markout > 0 for non-terminal entries; terminal/final markout > 0 for terminal entries.
- Edge monotonicity: higher predicted edge bucket should have higher realized markout/PnL.
- No reliance on one lucky max-win trade. Median markout by fill should be non-negative.
- Skip/fill reasons available; no black-box trades.

Kill metrics:
- Negative settled expectancy after >=20 fills or >=50 evaluated actionable signals.
- Markouts positive but settlement negative for hold-to-settle strategies.
- More than 10% signals fail due to stale spot/reference ambiguity.
- Strategy cannot produce enough evaluated opportunities in 72h.
- Paper fills are mostly artifacts of unrealistic crossing/queue assumptions.

Recommended immediate implementation order:
1. Crypto market parser + SpotProvider + MarketContext/reference capture.
2. `terminal_spot_fair` strategy and event logging.
3. Smoke tests and 2-loop paper run in crypto-only universe.
4. 72h paper run with legacy toxicity_mm disabled.
5. Add `opening_spot_momentum` after terminal evidence/logging is stable.
6. Add `spot_skew_mm` only after loop/cancel latency is reduced below 5s or explicitly modeled in shadow.

Bottom line:
The fastest plausible path to GREEN is not more generic market making. It is spot-grounded fair-value trading near terminal plus strict adverse-selection avoidance. Anything not using external spot/reference is probably measuring Polymarket microstructure noise while being picked off by traders who do.