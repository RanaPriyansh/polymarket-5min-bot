# Liquidity rewards / market-making viability research

Generated: 2026-04-27 16:46:07 UTC
Scope: read-only public API research; no orders/trading.

## Sources checked

- Polymarket docs llms-full.txt, section `Liquidity Rewards` and rewards API references.
- Public CLOB endpoints:
  - `GET https://clob.polymarket.com/rewards/markets/current`
  - `GET https://clob.polymarket.com/rewards/markets/current?sponsored=true`
  - `GET https://clob.polymarket.com/rewards/markets/multi`
- Repo runtime markout artifacts:
  - `data/runtime/fill_markout_report_latest.json`
  - `data/runtime/fill_markout_report_latest.txt`
  - `data/runtime/fill_markout_audit_latest.md`
- Bot config: `config.yaml`.

## Current active rewards snapshot

As of the query:

- Native/current rewards configs: 6,296 markets.
- Native/current total daily rate: about $131,569/day.
- Native median daily rate: $2/day; p90 about $30.19/day; max $3,840/day.
- Sponsored-inclusive configs: 975 markets.
- Sponsored-inclusive total daily rate: about $39,609/day.
- Sponsored-inclusive median daily rate: about $4/day; p90 about $55.50/day; max about $1,805/day.
- Common `rewards_min_size`: 50 shares (2,917 markets), 20 shares (2,239), 100 (409), 500 (237), 200 (236), 1000 (215).
- Common `rewards_max_spread`: 4.5 cents (3,880), 3.5 cents (1,463), 2.5 cents (538), 5.5 cents (358). A few markets have 0.2 or 0, not usable for normal maker farming.

Important for this 5-minute crypto bot:

- Querying rewards markets for `bitcoin up down 5m` returned current BTC 5m slots with `rewards_min_size=50`, `rewards_max_spread=4.5`, but effective `rate_per_day=0` in `rewards_config`.
- I did not find active nonzero reward rate on the current 5m crypto Up/Down markets sampled. Treat the 5m bot’s native target universe as non-rewarded for now.

## Polymarket reward scoring model

Docs say makers earn by posting resting limit orders. Rewards are sampled every minute and paid daily at midnight UTC. Minimum payout is $1.

Variables:

- `v` = market `max_incentive_spread` / `rewards_max_spread`, in cents.
- `s` = order distance from the size-cutoff-adjusted midpoint, in cents.
- `b` = in-game/live multiplier, if applicable.
- `c` = single-sided scaling factor, currently 3.0.
- `min_incentive_size` / `rewards_min_size` = minimum size cutoff used to determine adjusted midpoint and scoring eligibility.

Order score:

- `S(v,s) = ((v - s) / v)^2 * b`, for orders within the max spread.
- If `s >= v`, score is zero.
- Tightening from the edge matters quadratically.

For binary complement books, Polymarket computes two synthetic side scores:

- `Q_one = sum(S * bid_size on market) + sum(S * ask_size on complement)`
- `Q_two = sum(S * ask_size on market) + sum(S * bid_size on complement)`

Two-sided score:

- If midpoint is in [0.10, 0.90], single-sided liquidity can score but is penalized:
  - `Q_min = max(min(Q_one, Q_two), max(Q_one/c, Q_two/c))`
- If midpoint is outside [0.10, 0.90], liquidity must be double-sided:
  - `Q_min = min(Q_one, Q_two)`

Reward share:

- Per sample: `Q_normal = my_Q_min / sum(all_makers_Q_min)`.
- Per epoch/day: sum normalized sample scores, then normalize again across makers.
- Approx daily reward for stable quoting:
  - `my_reward_day ~= market_rate_day * my_avg_Q_min / (market_competition_Q + my_avg_Q_min)`

Approx score for one order at distance `s`:

- `score_per_share = ((v-s)/v)^2`.
- Examples with `v=4.5c`:
  - at midpoint (`s=0`): 1.000
  - 1c away: ((4.5-1)/4.5)^2 = 0.605
  - 2c away: 0.309
  - 3c away: 0.111
  - 4c away: 0.012
  - 4.5c away: 0
- Examples with `v=3.5c`:
  - 1c away: 0.510
  - 2c away: 0.184
  - 3c away: 0.020

Practical implication: being barely inside `max_spread` is usually not worth it; reward capture needs quotes tight to adjusted midpoint and present on both sides.

## Estimated rewards per quoted dollar

I sampled `GET /rewards/markets/multi` and used the returned `market_competitiveness` as a proxy for existing aggregate Q. Approximation:

- Quote `q = rewards_min_size` on both reward sides.
- Assume zero-spread scoring, so `my_Q_min ~= q`.
- Approx capital / quoted dollar denominator: `q` dollars for balanced binary two-sided maker inventory at around 50c. This is optimistic; actual locked collateral and fill inventory risk can be higher depending on order structure and prices.
- `share ~= q / (market_competitiveness + q)`.
- `reward_per_day ~= total_daily_rate * share`.
- `reward_yield_day ~= reward_per_day / q`.

High apparent yields from the sample are real API outputs but should be haircutted heavily: the denominator is optimistic, `market_competitiveness` can move after entry, and low-competition/high-rate markets are usually toxic or headline/event-risk markets.

Top sampled candidates by optimistic reward yield:

| Market | Rate/day | Min size | Max spread | Competition proxy | Optimistic reward/day | Optimistic reward/quoted-$/day |
|---|---:|---:|---:|---:|---:|---:|
| MegaETH FDV > $2B one day after launch | $800 | 200 | 3.5c | 22.0 | $720.58 | 360.3% |
| MegaETH FDV > $1.5B one day after launch | $800 | 200 | 3.5c | 35.5 | $679.33 | 339.7% |
| MegaETH FDV > $1B one day after launch | $800 | 200 | 3.5c | 61.6 | $611.57 | 305.8% |
| Tom Steyer CA governor 2026 | $213 | 50 | 4.5c | 44.2 | $113.04 | 226.1% |
| Starmer out by May 15, 2026 | $150 | 20 | 4.5c | 71.0 | $32.98 | 164.9% |
| U.S. invades Iran before 2027 | $500 | 200 | 4.5c | 119.3 | $313.20 | 156.6% |
| MegaETH FDV > $800M one day after launch | $300 | 200 | 3.5c | 76.8 | $216.75 | 108.4% |
| Xavier Becerra CA governor 2026 | $50 | 50 | 4.5c | 46.6 | $25.87 | 51.7% |
| Weed rescheduled by Dec 31 | $100 | 200 | 3.5c | 24.6 | $89.06 | 44.5% |
| Trump visits China by May 15 | $100 | 200 | 4.5c | 28.2 | $87.64 | 43.8% |

Candidate-market interpretation:

- The strongest reward farming opportunities are not the bot’s 5m crypto markets. They are discrete event/news/sports/esports markets.
- Some have enormous nominal reward APR, but the adverse-selection risk is also enormous: launch FDV, geopolitics, election, and breaking-news markets can gap through passive quotes.
- Sports/live markets may have explicit large incentives, but require live odds models and fast cancel/reprice; otherwise they are likely very toxic.

## Local adverse-selection evidence

Repo markout report (`data/runtime/fill_markout_report_latest.json/txt`) shows:

- Fills analyzed: 56,914.
- Settled fills: 46,530.
- Negative final markout: 27,412 / 46,530 = 58.9% adverse.
- Average final markout by TTE:
  - <60s: -0.0052, 64% adverse.
  - 60-120s: -0.0032, 59% adverse.
  - 120-300s: -0.0210, 59% adverse.
  - >300s: -0.0079, 55% adverse.
- Total average final markout reported: about -0.0126/share.

For the active config:

- `execution.mm_paper_max_notional_usd=6` and `max_risk_per_trade_usd=10` are far below common reward `min_size` values (20/50/200 shares). The current bot cannot meaningfully farm rewards without a separate capital/risk mode.
- Current runtime status shows gate RED / new orders paused and no current-run fills, so this is research-only and not actionable for live deployment.

Adverse-selection break-even intuition:

- If expected fill rate per day is `f` fraction of resting shares and expected markout loss is `L` dollars/share, expected adverse loss per day is approximately `q * f * L` for one side, or larger when both sides are active.
- With local final markout `L ~= $0.0126/share`, a 200-share quote that gets filled once loses about `$2.52` in markout expectation before fees/slippage. Frequent toxic fills can quickly consume low daily rewards, but the very high-rate sampled candidates could still cover markouts if fill frequency is controlled and quotes are pulled around news.
- For 5m crypto, because current reward rate is effectively 0, the adverse-selection evidence makes reward farming non-viable: there is no rebate to offset the negative markout.

## Viability verdict

For this repo’s current 5m crypto bot: NOT VIABLE as a reward-farming strategy today.

Reasons:

1. The target 5m crypto markets sampled have reward configs but zero daily reward rate.
2. Current risk sizing is below reward minimum sizes.
3. Existing markouts show systematic adverse selection: 58.9% adverse and about -1.26c/share final markout on average.
4. Reward scoring requires tight, continuously resting, two-sided quotes; that conflicts with the current short-horizon crypto toxicity controls unless the bot has a strong fair-value/cancel engine.

For a separate reward-aware maker on non-5m markets: CONDITIONALLY VIABLE ONLY as a tightly capped, model-backed experiment.

Minimum requirements:

- Separate universe from 5m crypto.
- Nonzero `total_daily_rate`, `rewards_min_size` affordable, `rewards_max_spread >= 3.5c`, and competition proxy low enough to produce at least `$5-$10/day` expected reward after haircuts.
- Strong external fair-value model for the market category. Do not quote event/news/live sports markets without domain models.
- Kill switches around news, live game state, market gaps, and fills.

## Reward-aware maker implementation plan

1. Add read-only rewards discovery module:
   - Fetch `/rewards/markets/current` and `/rewards/markets/multi` with pagination.
   - Join by `condition_id` to market metadata, tokens, spread, volume, end date, tags/category if available.
   - Store snapshot in runtime/research artifact for audit.

2. Candidate filter:
   - `total_daily_rate > 0`.
   - `rewards_min_size <= reward_cap_shares` and estimated collateral <= bankroll cap.
   - `rewards_max_spread >= 3.5` unless strategy has high-confidence fair value.
   - Exclude current bot’s 5m slots while rate is 0.
   - Exclude midpoint outside [0.10,0.90] unless quoting truly double-sided.
   - Exclude markets with imminent exogenous binary news if no model.

3. Scoring estimator:
   - For candidate quote levels, compute `S(v,s)` per order.
   - Estimate `my_Q_min` using both sides and single-sided penalty.
   - Estimate `reward_day = rate_day * my_Q_min/(competition_Q + my_Q_min)`.
   - Haircut reward by uptime, queue position, sampling miss probability, and competition growth. Start with 70-90% haircut on apparent yields.

4. Quote construction:
   - Quote both sides near fair value, not merely near book midpoint.
   - Initial size should be `min_incentive_size`, not larger, until realized reward share and markouts are known.
   - Place no closer than the model edge permits: `expected_edge_per_share >= adverse_markout_buffer + fees + risk_margin - reward_per_expected_fill_share`.
   - Prefer markets where fair value is stable and exogenous jump risk is low.

5. Runtime controls:
   - Reprice/cancel on mid move >= 0.5c or fair-value move >= 0.25c.
   - Quote TTL <= 5-10s for toxic/live markets; <= 30s for slow politics/macro markets.
   - Inventory skew: widen or pull the side that increases existing exposure.
   - Hard cap per condition, category, and correlated event.
   - Stop quoting after adverse fill burst or markout breach.

6. Measurement:
   - Track per-market reward estimator vs actual `/rewards/user/percentages` once authenticated read-only is available.
   - Track fill markouts at 30s/60s/120s/300s/final separately for reward-maker fills.
   - Attribute PnL as spread capture + reward accrual - markout - fees - unresolved inventory risk.

## Anti-adverse-selection controls

- Fair-value gate: never quote simply because reward score is high; require independent fair value.
- Toxicity gates: pause on high VPIN/order-flow imbalance, sudden top-of-book depletion, spread blowout, or one-sided market sweeps.
- News/live-state gate: pause around scheduled announcements, sports game starts/halftime/endgame, crypto launches, legal/political deadlines.
- Momentum gate: for crypto/sports/live, pull quotes when underlying/fair value velocity exceeds threshold.
- TTE gate: avoid late-stage binary markets unless the resolution model is extremely strong.
- Midpoint range gate: if midpoint <0.10 or >0.90, do not rely on single-sided scoring; require true two-sided offsets or skip.
- Queue-quality gate: if already crowded at best level and estimated score share is low, do not join unless expected reward covers queue/fill toxicity.
- Fill-response gate: after any fill, immediately cancel sibling/stale quotes, recompute fair value, and reduce/recenter size.
- Daily kill switch: stop candidate if realized+markout PnL plus conservative reward estimate falls below threshold, or adverse markout share exceeds 55-60%.

## Bottom line

Do not retrofit reward farming into the current 5m crypto bot as-is. Add a separate reward-aware maker research module first. The public reward program is large and some markets show attractive apparent rewards per quoted dollar, but the current target universe has zero effective rewards and the bot’s observed fill markouts are adverse enough that rebates cannot be assumed to rescue profitability.
