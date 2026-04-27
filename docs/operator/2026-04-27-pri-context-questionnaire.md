# Pri context questionnaire for canonical paper-only Polymarket repo

Purpose: exact questions to ask Pri so GPT-5.5 Pro can make better decisions for the canonical paper-only repo at `/root/obsidian-hermes-vault/projects/polymarket-5min-bot`.

Hard constraints that do not change even if unanswered:
- No live trading.
- No geoblock bypass.
- No loosening of risk controls.
- Research/advisory changes must not auto-promote into production.

Repo-grounded defaults if Pri does not answer:
- Paper only.
- Starting paper bankroll: $500.
- Risk limits remain conservative (`circuit_breaker_dd=10%`, `max_daily_loss=5%`, `max_risk_per_trade_usd=$10`, `mm_paper_max_notional_usd=$6`).
- Default universe stays crypto interval markets already in config: BTC, ETH, SOL, XRP on 5m and 15m.
- Telegram remains disabled.
- Baseline runtime family remains `toxicity_mm`; other families stay research-only unless paper evidence gates are met.

## 1) Capital / risk tolerance

Question 1:
What paper bankroll should we treat as the canonical reference size for evaluation: keep $500, lower it, or raise it?
Why it matters:
Position sizing, drawdown interpretation, and whether strategy results scale realistically all depend on the assumed bankroll.
Default if unanswered:
Keep the current repo default of $500 paper bankroll.

Question 2:
Should strategy recommendations optimize for preserving capital first, or for maximizing expected paper PnL within the existing conservative risk limits?
Why it matters:
This decides whether GPT-5.5 should prefer fewer trades and stronger vetoes versus broader experimentation within the same hard risk ceiling.
Default if unanswered:
Preserve capital first.

## 2) Acceptable drawdown

Question 3:
What maximum paper drawdown would make Pri uncomfortable enough to pause or reject a strategy even if the config allows it?
Why it matters:
The repo has a 10% circuit-breaker threshold, but Pri may want a stricter human review threshold for recommendations.
Default if unanswered:
Use the repo’s current 10% drawdown threshold as the hard stop and treat anything above 5% drawdown as requiring extra caution in recommendations.

Question 4:
What maximum daily paper loss would Pri consider acceptable before manual review is required?
Why it matters:
The repo already uses a 5% daily loss limit; Pri’s human tolerance determines whether GPT-5.5 should recommend tighter operational guardrails.
Default if unanswered:
Use the current 5% max daily loss and require manual review at that point.

## 3) Profit target

Question 5:
Is there a target paper return goal Pri cares about, such as “prove positive expectancy,” “beat no-trade baseline,” or a specific monthly paper PnL target?
Why it matters:
Without a stated objective, GPT-5.5 can only optimize for robustness, not for a concrete success criterion.
Default if unanswered:
Optimize for proving positive expectancy versus a no-trade baseline, not for an aggressive return target.

## 4) Desired markets

Question 6:
Should the canonical paper bot stay focused on crypto interval markets only, or should research also consider non-crypto Polymarket categories as scanner-only opportunities?
Why it matters:
The current config is crypto-interval focused, while reward research suggests non-crypto markets may matter only as separate research tracks.
Default if unanswered:
Stay focused on crypto interval markets only for the canonical bot; non-crypto remains separate research/scanner-only.

Question 7:
Within crypto interval markets, should we keep all four assets in config (BTC, ETH, SOL, XRP), or prioritize a narrower subset for cleaner evidence?
Why it matters:
A narrower universe can reduce noise, simplify analysis, and make paper evidence more trustworthy.
Default if unanswered:
Keep BTC, ETH, SOL, and XRP as configured.

## 5) Acceptable market categories

Question 8:
Are there any market categories Pri wants explicitly allowed for read-only research or scanner alerts beyond crypto intervals, such as politics, macro, sports, or launches?
Why it matters:
Some categories may be analytically interesting but operationally noisy or ethically undesirable.
Default if unanswered:
Allow only crypto interval markets for canonical recommendations; anything else is out of scope unless treated as separate read-only research.

## 6) Jurisdictions to avoid

Question 9:
Are there specific jurisdictions, political topics, or event types Pri wants the system to avoid even for read-only research summaries?
Why it matters:
This affects what GPT-5.5 should surface, summarize, or ignore when scanning broader Polymarket contexts.
Default if unanswered:
Avoid expanding into sensitive categories and stay within crypto interval market analysis only.

## 7) Time horizon

Question 10:
Should GPT-5.5 optimize recommendations mainly for the current 5m/15m horizon, or is Pri open to a separate longer-dated paper research track?
Why it matters:
The canonical repo is built around short-duration interval markets; longer-dated ideas change data needs, risk framing, and evaluation timelines.
Default if unanswered:
Optimize for 5m/15m only.

## 8) Automation

Question 11:
Does Pri want GPT-5.5 to recommend only read-only analysis and paper-trading-safe config changes, or also operational automation such as scheduled digests, health checks, and restart-safe supervision?
Why it matters:
This determines whether recommendations should focus purely on strategy logic or also on unattended paper operations.
Default if unanswered:
Allow paper-safe operational automation only; no autonomous promotion or risky unattended behavior.

## 9) Telegram / reporting

Question 12:
Does Pri want Telegram enabled for paper-only alerts and digests, and if so what events matter most: fills, settlements, gate changes, circuit breakers, daily summaries, or research digests?
Why it matters:
The repo already has Telegram hooks, but alert volume and event type shape operator burden and review quality.
Default if unanswered:
Keep Telegram disabled.

Question 13:
What reporting cadence would Pri actually read: real-time alerts, hourly digests, daily summaries, or only on failures/gate changes?
Why it matters:
A report that is too frequent gets ignored; too infrequent and important paper evidence gets missed.
Default if unanswered:
Prefer daily summary plus immediate alerts only for gate degradation, circuit breakers, or service health failures.

## 10) Legal / geographic constraints

Question 14:
From what legal/geographic environment is Pri operating, and are there any constraints that mean we should avoid even paper workflows that depend on account-linked Polymarket access?
Why it matters:
Even paper-only research may touch account, API, or platform access questions; we must not suggest anything that implies geoblock bypass or regulatory exposure.
Default if unanswered:
Assume we must stay strictly within clearly lawful, read-only or already-authorized paper-access workflows and never suggest bypasses.

## 11) VPS / resources

Question 15:
What is the actual deployment environment for the canonical paper bot: single VPS, home machine, or mixed environments, and what CPU/RAM/network constraints should GPT-5.5 assume?
Why it matters:
Resource limits affect polling frequency, telemetry retention, digest generation, and whether more scanners are practical.
Default if unanswered:
Assume a modest single VPS and avoid resource-heavy additions.

## 12) Accounts / data feeds

Question 16:
What accounts and data feeds are already legitimately available for this repo: Polymarket paper-access credentials only, public APIs only, spot market data providers, Telegram bot credentials, or none?
Why it matters:
GPT-5.5 should not recommend features that depend on credentials or feeds Pri does not already have.
Default if unanswered:
Assume only the current repo-configured public/API access needed for paper mode, with no new paid feeds and no missing secret-dependent features turned on.

## 13) Willingness to pay

Question 17:
Is Pri willing to pay for any additional infrastructure or data, such as better spot feeds, monitoring, or hosted reporting, or should recommendations assume zero added spend?
Why it matters:
Some stronger fair-value or monitoring ideas require external services; budget changes what is realistic.
Default if unanswered:
Assume zero additional spend.

## 14) Manual oversight

Question 18:
How much manual oversight is Pri willing to provide: daily review, multiple times per day, only when alerted, or very minimal?
Why it matters:
This affects how conservative unattended paper operation should be and how much burden can be shifted onto alerts and run gates.
Default if unanswered:
Assume limited daily review with manual attention when alerted.

Question 19:
Should any gate RED state, circuit breaker, or run-lineage anomaly require explicit human acknowledgment before paper trading resumes?
Why it matters:
The repo’s recent history shows restart/circuit-breaker supervision issues; a human-ack rule reduces false confidence.
Default if unanswered:
Yes—require explicit human acknowledgment before resuming after any RED gate or circuit-breaker event.

## 15) Strategy philosophy

Question 20:
Does Pri prefer interpretable, thesis-driven strategies even if slower to develop, or is Pri comfortable with more empirical scorer/guard models so long as they remain paper-only and auditable?
Why it matters:
This changes whether GPT-5.5 should prioritize transparent rule-based logic or more model-heavy ranking/guard layers.
Default if unanswered:
Prefer interpretable, auditable strategies first.

Question 21:
Does Pri want the canonical bot to remain narrowly focused on one baseline family until evidence is strong, or to run multiple paper families in parallel for faster learning?
Why it matters:
Parallel experimentation can speed research but may contaminate evidence and complicate attribution.
Default if unanswered:
Keep one trusted baseline family by default and treat other families as research-only.

## 16) Rewards farming

Question 22:
Does Pri want GPT-5.5 to treat liquidity rewards as entirely out of scope for this canonical 5m/15m repo, or as a separate scanner-only research track for future consideration?
Why it matters:
Current research says 5m crypto rewards are effectively non-viable, but a separate non-5m reward track could still be studied.
Default if unanswered:
Treat rewards farming as out of scope for the canonical bot and, at most, scanner-only research in a separate track.

## 17) Paper/live promotion requirements

Question 23:
What exact evidence threshold would Pri require before even promoting a strategy from research-only to canonical paper runtime: settled trade count, positive markout, uptime proof, restart-safety proof, or time-in-production?
Why it matters:
The repo already blocks live trading and requires settled paper evidence, but Pri’s promotion standard determines how strict GPT-5.5 should be.
Default if unanswered:
Use the repo’s conservative stance: require GREEN gate state, restart safety, settled-paper evidence, and meaningful sample size before promotion within paper mode; never recommend live promotion.

Question 24:
Should GPT-5.5 assume that no strategy may become the default canonical paper family unless it clearly beats `toxicity_mm` or another established baseline on settled paper evidence?
Why it matters:
This clarifies whether “interesting candidate” is enough, or whether replacement needs baseline-outperformance proof.
Default if unanswered:
Yes—require clear settled-paper outperformance over the incumbent baseline before changing the default family.

## 18) Legal/geographic promotion boundary

Question 25:
Even if paper evidence becomes strong, should GPT-5.5 treat the project as permanently paper-only unless Pri explicitly changes the mission in writing?
Why it matters:
This prevents recommendation drift toward live-readiness or operational steps that exceed the current mandate.
Default if unanswered:
Yes—treat the project as permanently paper-only.

## 19) Concise operator preference question

Question 26:
If Pri answers only one meta-question, what should dominate decision-making: safety/robustness, learning speed, or paper PnL?
Why it matters:
This gives GPT-5.5 a tie-breaker when evidence is mixed.
Default if unanswered:
Safety/robustness dominates.

## Suggested short form to send Pri

If only a short checklist is practical, ask Pri to answer these fields:
- Canonical paper bankroll:
- Preferred assets/markets:
- Any categories/jurisdictions/topics to avoid:
- Max acceptable drawdown:
- Max acceptable daily loss:
- Reporting channel/cadence:
- Manual oversight availability:
- Willingness to pay for data/infrastructure:
- Whether rewards farming is out of scope or scanner-only research:
- Promotion rule from research-only to canonical paper runtime:
- Confirmation that project remains permanently paper-only:
