# Creative strategy opportunity scout — Subagent 10

Generated: 2026-04-27
Scope: idea ranking only, grounded in current repo/runtime facts. No live trading, no risk-limit loosening, no geoblock bypass, no fabricated edge.

## Grounded constraints used

- Repo stance is paper-only; live blocked.
- Current families: `toxicity_mm` and `time_decay` active; `mean_reversion_5min`, `opening_range`, `spot_momentum` are candidates/research.
- Historical paper evidence is broadly poor; adverse selection is the central problem.
- Local markout research says about 56,914 fills analyzed, 46,530 settled, 58.9% adverse final markouts, average final markout about -1.26c/share.
- TTE buckets 120-300s and >300s are strongly negative. Only `<60s` is even directionally promising, but it is still dangerous because fill simulation is optimistic and current queue/fee/rebate modeling is absent.
- Market universe is currently BTC/ETH/SOL/XRP x {5m,15m}; discovery stack uses Gamma slug + CLOB book + Binance spot.
- Current simulator is optimistic for maker fills and lacks fee/rebate and queue models, so maker ideas must be discounted heavily.
- Current 5m crypto reward configs sampled are effectively zero-rate, so reward farming is not viable inside the present universe.

## Ranking legend

- scanner-only: alerts/research surface, not a trade strategy yet
- candidate-only: plausible strategy family worth model/shadow work
- paper-active: appropriate for paper experiment after prerequisites
- rejected: should not be pursued in current form

## Ranked strategy slate

### 1) Crypto short-duration terminal probability model
- Class: paper-active
- Thesis: 5m/15m crypto Up/Down is fundamentally a terminal-probability problem, not a generic market-making problem. Fair value should come from slot-open price, current spot deviation, remaining time, and conditional volatility, then only trade when market price is materially away from that fair.
- Why edge may exist: Polymarket binaries can lag the underlying spot path, especially when participants anchor on book midpoint or simple momentum instead of calibrated terminal odds.
- Why exploitable by small bot: only 8 active symbols in current universe, so a lightweight fair-value engine can cover all markets with low infra burden.
- Why competitors may miss it: many participants quote off local midpoint or intuition; small, short-dated binaries are often too niche for heavier options-style probability modeling.
- Data needed: slot open, Binance spot ticks, realized vol by asset/time-of-day, time-to-expiry, Polymarket bid/ask/mid, settlement truth.
- Implementation complexity: medium.
- Expected capacity: medium within 5m/15m majors; limited by liquidity and signal frequency.
- Risk: model misspecification, oracle mismatch, stale spot, correlated exposure, edge disappearing once modeled correctly.
- Paper-test design: first run full shadow calibration with no trades; score every loop with Brier/log loss, calibration bins, and implied-vs-realized terminal outcomes. Then paper directional entries only when edge exceeds conservative buffer.
- Minimum evidence required: >=200 scored slots per asset cluster, stable calibration, >=50 settled paper trades, positive EV after conservative slippage haircut, no dependence on optimistic maker-fill assumptions.
- Notes: this is the keystone prerequisite for most non-fantasy ideas.

### 2) Adverse-selection pause model
- Class: paper-active
- Thesis: since historical losses are dominated by toxic fills, the fastest path to less-bad performance may be a predictive veto model that pauses quoting/trading during microstructure states associated with bad markouts.
- Why edge may exist: loss avoidance is often easier than alpha generation; even a modest filter can improve every existing family.
- Why exploitable by small bot: uses existing runtime artifacts and current universe only; no need for broad market coverage.
- Why competitors may miss it: many bots focus on signal generation and underinvest in explicit “do not trade now” models.
- Data needed: spread, top-depth, imbalance, sweep activity, short-horizon spot velocity, tte, fill timestamps, markout labels.
- Implementation complexity: medium.
- Expected capacity: high as a shared risk/control layer.
- Risk: over-pausing causes no fills; historical labels are contaminated by optimistic fill simulation and prior bad strategy behavior.
- Paper-test design: train offline on fill-markout labels; run shadow veto on current paper loop; compare with/without filter on opportunity count, adverse share, and final markout.
- Minimum evidence required: material reduction in adverse final markout and negative tail without collapsing all candidate trades.
- Notes: strongest near-term research ROI after fair value.

### 3) Opening-range with external spot confirmation
- Class: paper-active
- Thesis: opening-range breakouts are only interesting if spot confirms that the move is real relative to slot-open and consistent with terminal fair value; pure endogenous market breakout is too fragile.
- Why edge may exist: initial interval repricing can underreact when Polymarket traders are slower than spot.
- Why exploitable by small bot: existing `opening_range` family already provides a scaffold.
- Why competitors may miss it: many short-term breakout traders ignore external confirmation or slot-specific fair odds.
- Data needed: first-N-second market prints, slot open, Binance spot open/current, fair model output, depth/spread, settlement labels.
- Implementation complexity: low/medium.
- Expected capacity: medium, one-fire-per-slot style.
- Risk: false breakouts, chop, delayed spot confirmation, existing fixed-size logic overtrading weak signals.
- Paper-test design: compare raw opening_range vs spot-confirmed variant on same slots; record incremental uplift by asset and tte bucket.
- Minimum evidence required: >=50 settled confirmed trades with better PnL/trade and markout than unconfirmed opening_range.
- Notes: fast-to-implement because code path already exists.

### 4) New-market stale quote scanner
- Class: candidate-only
- Thesis: fresh 5m/15m markets may briefly show stale/manual/inherited prices during listing/roll transitions, producing obvious dislocations versus spot-implied terminal fair.
- Why edge may exist: newly created markets can lag the first spot move and book refresh.
- Why exploitable by small bot: universe is tiny and timing windows are short; small bot can focus on slot-roll moments.
- Why competitors may miss it: requires precise slot lifecycle awareness plus fair-value comparison at creation time.
- Data needed: market creation/discovery timestamps, first several book snapshots, spot at slot-open/current, fair model, optional quote age.
- Implementation complexity: medium.
- Expected capacity: low/medium; rare but potentially high-quality signals.
- Risk: apparent staleness may reflect hidden queue dynamics or delayed discovery on our side.
- Paper-test design: scanner-only first; log all flagged dislocations and subsequent book/settlement evolution; then latency-haircut simulation.
- Minimum evidence required: repeated dislocations that persist long enough to be actionable after latency/slippage penalties.
- Notes: great asymmetry, but only after fair model exists.

### 5) Complementary YES/NO dislocation scanner
- Class: scanner-only
- Thesis: complementary binary books should obey parity. If YES ask + NO ask or YES bid + NO bid implies impossible package pricing, that is a clean mechanical anomaly.
- Why edge may exist: fragmented liquidity and asynchronous quote updates can violate complement parity briefly.
- Why exploitable by small bot: current universe is tiny; parity checks are computationally trivial.
- Why competitors may miss it: many discretionary traders look at one side only; some bots do not map complements robustly.
- Data needed: token mapping for both sides, best bid/ask, size, tick rules, latency estimates.
- Implementation complexity: low.
- Expected capacity: low, but signal quality can be very high.
- Risk: partial fills, token mapping errors, package not actually executable at posted size.
- Paper-test design: scanner-only with executable-size simulation and partial-fill penalties.
- Minimum evidence required: multiple post-latency package opportunities with robust net edge >1-2c.
- Notes: excellent scanner surface, not a core strategy.

### 6) Polymarket-vs-exchange lag arb scanner
- Class: candidate-only
- Thesis: if Polymarket binary price materially lags spot-derived fair probability from exchange moves, there may be short-lived directional arbitrage.
- Why edge may exist: exchange price discovery is continuous and deeper; Polymarket can reprice more slowly.
- Why exploitable by small bot: only Binance spot plus current 8 markets needed.
- Why competitors may miss it: some treat binary price as independent sentiment rather than a function of spot path and remaining time.
- Data needed: high-frequency Binance spot, fair model, Polymarket book/mid, slot-open, realized vol, latency timestamps.
- Implementation complexity: medium.
- Expected capacity: medium but concentrated during fast spot moves.
- Risk: this devolves into terminal probability model with worse discipline if thresholds are too loose; exchange lead can vanish before executable fill.
- Paper-test design: pure scanner first; compare lag magnitude to next 5s/15s/30s Polymarket repricing and final settlement.
- Minimum evidence required: stable predictive lag relation after conservative latency haircut.
- Notes: likely same underlying engine as rank #1, but expressed as a scanner.

### 7) Maker-only fair-value quoting
- Class: candidate-only
- Thesis: maker quotes should only exist where both sides around fair are positive EV after adverse-selection buffer; midpoint quoting is invalid.
- Why edge may exist: some wide-spread moments may allow collecting spread if quoting is skewed by fair and toxicity state.
- Why exploitable by small bot: current universe is small enough to maintain rapid repricing.
- Why competitors may miss it: many market makers quote symmetric spreads without terminal-probability anchoring.
- Data needed: fair-value engine, order book, inventory, short-horizon markouts, fill data, spot velocity.
- Implementation complexity: high because queue/fill realism is currently weak.
- Expected capacity: medium/high if real, but paper estimates likely inflated.
- Risk: current simulator overestimates maker fills and ignores queue/fees/rebates; historical adverse selection is already ugly.
- Paper-test design: only after queue-aware conservative fill model; compare fill-markout distribution to toxicity_mm.
- Minimum evidence required: >=100 conservative simulated fills or settled paper fills with non-negative final markout after buffers.
- Notes: interesting eventually, but dangerous before model realism improves.

### 8) Volatility-regime quote widening
- Class: paper-active
- Thesis: quote widths and thresholds should expand when spot volatility/velocity rises and contract when calm; static bands invite toxic fills.
- Why edge may exist: volatility clustering is real and current strategies likely use overly static thresholds.
- Why exploitable by small bot: simple overlay on current families.
- Why competitors may miss it: some bots implement toxicity gating but not dynamic width by realized/instantaneous vol.
- Data needed: rolling spot volatility, spot velocity, spread/depth, tte, fill outcomes by regime.
- Implementation complexity: low.
- Expected capacity: high as a general control layer.
- Risk: widening too much kills fills; narrowing in calm regimes can still be toxic without fair value.
- Paper-test design: shadow recommended widths per regime; then A/B against static-width candidate quoting in backtests.
- Minimum evidence required: lower adverse-share or better markout than static thresholds without full collapse in opportunity count.
- Notes: fast-to-implement control, but not a standalone edge.

### 9) Cross-market consistency scanner
- Class: scanner-only
- Thesis: related Polymarket markets or duplicated condition formulations may imply inconsistent probabilities across a market graph.
- Why edge may exist: taxonomy fragmentation and manual market creation can create rule-near-duplicates or consistency breaks.
- Why exploitable by small bot: scanner can operate asynchronously with low cost.
- Why competitors may miss it: requires ontology/market-linking work more than raw speed.
- Data needed: market metadata, condition IDs, rule text, token mapping, prices, resolution semantics.
- Implementation complexity: medium/high.
- Expected capacity: low in current 5m crypto universe, higher if universe expands.
- Risk: false equivalence due to non-identical rules; low current scope fit.
- Paper-test design: build consistency graph and alert only on exact/near-exact semantic matches.
- Minimum evidence required: repeated, rule-clean inconsistencies surviving manual review.
- Notes: useful research infrastructure, weak near-term monetization in current universe.

### 10) Whale/orderflow follower/fader
- Class: scanner-only
- Thesis: certain wallets or flow archetypes may be informed; others may create overreaction worth fading.
- Why edge may exist: public orderflow may contain persistent participant-specific skill or urgency.
- Why exploitable by small bot: small bot can specialize in a few validated wallets/flow patterns.
- Why competitors may miss it: wallet attribution/clustering is messy; many avoid the research burden.
- Data needed: public trades/order events, wallet IDs, market/time context, subsequent markouts, clustering heuristics.
- Implementation complexity: high.
- Expected capacity: medium if signal exists, but likely low in current short-duration crypto due to latency.
- Risk: delayed data, survivorship bias, whales hedging elsewhere, overfitting to anecdotes.
- Paper-test design: wallet leaderboard by forward markout and settlement outcomes; then test follow/fade rules out-of-sample.
- Minimum evidence required: stable wallet cohorts with statistically meaningful predictive value after delay assumptions.
- Notes: strong scanner, weak first implementation.

### 11) Agent-assisted manual trade recommendation system
- Class: paper-active
- Thesis: because data is noisy and universe is small, an advisory engine that ranks the best 1-3 opportunities per slot for human review may outperform premature automation.
- Why edge may exist: human judgment can reject obvious bad context while the agent supplies disciplined, repeatable filtering.
- Why exploitable by small bot: no need for low-latency auto-execution; can focus on ranking quality and explanation quality.
- Why competitors may miss it: many builders jump straight to full automation and skip high-signal human-in-the-loop workflows.
- Data needed: all scanner outputs, fair values, regime flags, book state, rationale summaries, outcome tracking.
- Implementation complexity: low/medium.
- Expected capacity: medium for a solo operator; low operational scale, high learning value.
- Risk: human inconsistency, hindsight bias, selective execution.
- Paper-test design: generate timestamped recommendations and compare recommended-vs-not-recommended outcomes with no execution discretion hidden.
- Minimum evidence required: recommendation bucket clearly outperforms background universe on forward markout and settlement EV.
- Notes: very practical given current red-gated, paper-only posture.

### 12) Human-in-the-loop edge dashboard
- Class: paper-active
- Thesis: the main near-term bottleneck is not more raw strategies but better observability of fair value, toxicity, parity, stale quotes, and post-trade attribution in one operator surface.
- Why edge may exist: faster diagnosis improves strategy selection and prevents repeated deployment of bad ideas.
- Why exploitable by small bot: concentrated universe makes dashboarding manageable.
- Why competitors may miss it: dashboards seem non-alpha, yet they are often necessary to harvest sparse, fragile edge.
- Data needed: runtime artifacts, current book/spot snapshots, fair model, markout metrics, alerts, settlement audit.
- Implementation complexity: low/medium.
- Expected capacity: indirect but high leverage.
- Risk: can become vanity analytics with no decision impact.
- Paper-test design: measure whether dashboard-driven recommendations improve candidate-selection hit rate and reduce toxic periods.
- Minimum evidence required: clear reduction in false-positive deployments or measurable improvement in manual recommendations.
- Notes: one of the best practical uses for GPT-5.5 Pro.

### 13) News/event shock scanner
- Class: scanner-only
- Thesis: event-driven shocks create temporary mispricings and stale quotes, but only if external news is faster than local repricing.
- Why edge may exist: Polymarket participants may be slower to process breaking information.
- Why exploitable by small bot: only as an alerting tool; full automated trading is likely too risky without premium news infra.
- Why competitors may miss it: some pure price-action bots ignore external news context.
- Data needed: news/calendar feed, spot shock detection, book change detection, market metadata.
- Implementation complexity: medium/high.
- Expected capacity: bursty and rare.
- Risk: headline interpretation error, fake/bad sources, extreme latency disadvantage.
- Paper-test design: event tagging and post-event repricing study before any trade simulation.
- Minimum evidence required: repeated cases where alert precedes repricing enough to matter.
- Notes: attractive conceptually, but likely scanner-only in current setup.

### 14) Resolution/settlement latency scanner
- Class: scanner-only
- Thesis: there may be edge in markets whose de facto outcome is clear before formal Polymarket settlement or where settlement timing itself creates temporary inventory/funding opportunities.
- Why edge may exist: operational lag between observable resolution and final settlement.
- Why exploitable by small bot: monitoring is cheap.
- Why competitors may miss it: settlement mechanics are operationally boring and under-researched.
- Data needed: market close times, oracle timestamps, final spot windows, settlement timestamps, post-resolution quotes.
- Implementation complexity: medium.
- Expected capacity: low in current 5m crypto, since outcomes resolve mechanically from spot quickly.
- Risk: edge may be non-existent for this universe; could tie up capital for trivial gain.
- Paper-test design: measure delay from economic certainty to quote disappearance/settlement.
- Minimum evidence required: recurring, actionable post-resolution price staleness.
- Notes: probably not worth priority in BTC/ETH/SOL/XRP 5m/15m.

### 15) Reward-aware LP
- Class: rejected for current universe
- Thesis: use liquidity rewards plus spread capture to offset adverse selection.
- Why edge may exist: rewards can subsidize passive quoting in some markets.
- Why exploitable by small bot: only in a separate non-5m project with category-specific fair models.
- Why competitors may miss it: reward math and competition proxies are nontrivial.
- Data needed: rewards APIs, competition proxies, fair-value model, actual reward attribution.
- Implementation complexity: high.
- Expected capacity: potentially high outside current universe.
- Risk: current 5m crypto rates are effectively zero; current bot sizing is below common reward minima; event/reward markets can be extremely toxic.
- Paper-test design: read-only reward scanner first, then separate-paper attribution experiment outside the current bot.
- Minimum evidence required: nonzero reward rates, affordable min size, positive net EV after huge haircuts and toxicity buffers.
- Notes: grounded repo research already says “not viable today” for current 5m bot.

### 16) Long-dated carry/reward scanner
- Class: scanner-only
- Thesis: long-dated markets may offer carry from rewards or slow convergence to fair value.
- Why edge may exist: slower information diffusion and reward carry.
- Why exploitable by small bot: small size can sit in niche long-dated markets if a separate bankroll is allowed.
- Why competitors may miss it: many short-term bots ignore long-dated carry.
- Data needed: rewards snapshots, external fair models by category, calendar/news risk, capital lock metrics.
- Implementation complexity: medium/high.
- Expected capacity: potentially medium outside current scope.
- Risk: completely out-of-scope for present 5m/15m crypto bot; capital tie-up and event jumps.
- Paper-test design: read-only scanner with manual review queue.
- Minimum evidence required: verified positive reward/carry after realistic capital and event-risk haircut.
- Notes: interesting future research, not a fit now.

### 17) Sports late-line drift scanner
- Class: rejected for current repo scope
- Thesis: sportsbook or exchange closing-line moves may predict late Polymarket repricing in sports binaries.
- Why edge may exist: Polymarket may lag mature sports books.
- Why exploitable by small bot: only if the universe expands into sports and reliable odds feeds exist.
- Why competitors may miss it: cross-venue sports mapping is annoying.
- Data needed: sports odds feeds, Polymarket sports markets, line history, game-state timing.
- Implementation complexity: high.
- Expected capacity: potentially high in another project.
- Risk: not applicable to current BTC/ETH/SOL/XRP 5m/15m universe; would require a new stack.
- Paper-test design: not recommended inside current repo.
- Minimum evidence required: separate project charter and data sources.
- Notes: fantasy for this bot right now.

### 18) Market creation/sponsorship scanner
- Class: scanner-only
- Thesis: newly sponsored or newly created markets may have transient low competition, stale setup, or unusually attractive reward/value combinations.
- Why edge may exist: sponsorship changes alter maker incentives before the crowd adapts.
- Why exploitable by small bot: can passively monitor rewards/current listings.
- Why competitors may miss it: requires joining market metadata, rewards, and fair-value viability.
- Data needed: market discovery feed, sponsorship/reward endpoints, metadata, category filters, fair-value feasibility tags.
- Implementation complexity: low/medium.
- Expected capacity: low in current crypto interval universe, higher for future expansion.
- Risk: most newly sponsored markets will still be toxic or outside modeled categories.
- Paper-test design: scanner that ranks markets by reward rate, low competition, and modelability.
- Minimum evidence required: several repeatedly attractive setups that pass manual review.
- Notes: good expansion scout, not near-term alpha in current universe.

## Top 5 opportunities best suited for GPT-5.5 Pro

1. Crypto short-duration terminal probability model
   - Best use of stronger reasoning, calibration planning, and decomposition of fair-value assumptions.
2. Adverse-selection pause model
   - Benefits from richer feature ideation, label hygiene scrutiny, and contradiction-first evaluation.
3. Human-in-the-loop edge dashboard
   - GPT can synthesize cross-signal explanations and operator-facing summaries well.
4. Agent-assisted manual trade recommendation system
   - High leverage from ranking, confidence wording, and explicit rationale generation.
5. Cross-market consistency scanner
   - Semantic matching of market rules and consistency graphs is LLM-friendly.

## Top 3 fast-to-implement

1. Volatility-regime quote widening
   - Mostly an overlay/control problem with existing data.
2. Opening-range with external spot confirmation
   - Existing `opening_range` scaffold reduces implementation time.
3. Complementary YES/NO dislocation scanner
   - Mechanically simple and low-risk as a scanner.

## Top 3 high-upside

1. Crypto short-duration terminal probability model
   - Highest upside because it can underpin directional entries, stale-quote detection, and safer maker skew.
2. New-market stale quote scanner
   - Rare but potentially very asymmetric if real.
3. Maker-only fair-value quoting
   - Upside is large if fair-value and pause controls become good enough, but current realism gaps make it second-phase only.

## Fantasies to reject now

- Pure reward-aware LP in current 5m crypto universe: sampled reward rate effectively zero, current size caps below reward minima, and adverse markout dominates.
- Sports late-line drift scanner inside this repo: outside current universe and would require an entirely different data/model stack.
- Fully automated news/event shock trading: likely loses to latency and headline-quality disadvantages; scanner-only at most.
- Resolution/settlement latency trading as a primary edge in 5m crypto: likely too little edge for too much operational complexity.
- Standalone midpoint-based maker quoting or generic `toxicity_mm` scaling: contradicted by local adverse markout evidence and optimistic fill simulator.

## Bottom line

Best grounded build order:
1. terminal probability / fair-value model
2. adverse-selection pause model
3. spot-confirmed opening-range candidate
4. stale-quote + complement scanners
5. only then consider fair-value-skewed maker quoting

This ordering respects the repo’s actual failure mode: not lack of idea variety, but lack of a trustworthy independent fair value and robust toxicity filter.