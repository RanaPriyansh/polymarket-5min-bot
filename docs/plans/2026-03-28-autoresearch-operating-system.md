# Autoresearch Operating System

## Purpose

Design a domain-agnostic operating system for disciplined autonomous research that agents can learn once and reuse across domains such as marketing, websites, model evaluation, and trading strategies.

This design uses the existing `research/loop.py` pattern as the minimal execution kernel and treats domain implementations such as `research/polymarket.py` as adapters. The goal is to separate:

1. The universal research lifecycle
2. The domain adapter contract
3. The artifact and evaluation contracts
4. The promotion/demotion governance model
5. The reusable role and skill system

## Reading the current baseline

### What `research/loop.py` already provides

The existing loop is a clean kernel with four main pluggable stages:

- hypothesis generation
- experiment execution
- result analysis
- report writing

It also provides:

- a shared `ResearchContext`
- shared result objects for hypotheses, experiments, insights, and cycle result
- artifact persistence via JSON
- event publication hooks

This is a good base for a generic OS because the structure is already domain-neutral.

### What `research/polymarket.py` adds

The Polymarket module demonstrates how an adapter can bind a domain to the kernel:

- provides default hypotheses
- converts hypotheses into domain experiments/backtests
- compares outputs and ranks them
- emits a report and subagent template file

It also already hints at reusable role taxonomy via:

- scout
- experimenter
- analyst
- reporter

That role set is useful but incomplete for a general operating system.

## Core design principle

The autoresearch OS should be taught to agents as:

Question -> Hypothesis -> Test -> Evidence -> Decision -> State transition -> Reuse

Agents should learn one stable operating model and only swap adapters, metrics, tools, and domain-specific constraints.

## 1. Operating system layers

### Layer 0: Research kernel

The kernel is the irreducible loop that exists in every domain.

Canonical phases:

1. intake
2. map the search space
3. propose hypotheses/tasks
4. design experiments/evaluations
5. execute evidence gathering
6. analyze and score
7. decide promotion/demotion/archive
8. publish artifacts
9. update memory/skills
10. schedule the next cycle

This is broader than the current implementation, but the current loop maps cleanly into phases 3 through 8.

### Layer 1: Domain adapter

The adapter defines how a generic hypothesis becomes a domain-valid action.

Examples:

- marketing: turn a hypothesis into a campaign test, creative comparison, or audience segmentation experiment
- websites: turn a hypothesis into a UX audit, funnel analysis, SEO crawl, or page-speed intervention
- model eval: turn a hypothesis into benchmark runs, prompt sweeps, failure clustering, or regression checks
- trading: turn a hypothesis into a backtest, sensitivity sweep, risk check, or execution simulation

### Layer 2: Policy and governance

The OS needs explicit rules for what counts as:

- valid evidence
- enough evidence
- promotion
- demotion
- rollback
- retirement

Without this layer, agents generate activity but not disciplined progress.

### Layer 3: Reusable skills and subagents

Agents should not relearn domain process each time. They should call stable role modules and domain skills.

## 2. Universal object model

The current dataclasses are a good starting point, but the generic OS should standardize a slightly richer ontology.

### A. ResearchBrief

Replaces or extends `ResearchContext`.

Fields:

- `domain`: marketing | website | model_eval | trading | custom
- `program_id`: long-running research program or portfolio
- `cycle_id`: current cycle id
- `objective`: what outcome matters
- `decision_question`: concrete question to answer this cycle
- `constraints`: budgets, risk limits, time limits, tool limits, brand/legal constraints
- `inputs`: datasets, URLs, configs, prior artifacts, competitor lists, benchmark suites
- `success_metrics`: primary and secondary metrics
- `failure_guardrails`: thresholds that invalidate a candidate
- `time_horizon`: immediate, weekly, quarterly, etc.
- `operator_preferences`: exploration vs exploitation, aggressiveness, acceptable uncertainty

### B. ResearchUnit

A universal work candidate. This generalizes “hypothesis” so the system works even when the work item is not literally a scientific hypothesis.

Fields:

- `id`
- `type`: hypothesis | opportunity | bug | segment | strategy | prompt | creative | intervention
- `statement`
- `rationale`
- `source`: human, prior cycle, anomaly detector, competitor intelligence, model suggestion
- `metadata`
- `parent_ids`: lineage from prior units
- `priority`
- `expected_value`
- `cost_estimate`
- `risk_estimate`
- `state`

For backwards compatibility, `ResearchHypothesis` can remain one valid subtype.

### C. ExperimentSpec

A normalized test plan.

Fields:

- `experiment_id`
- `unit_id`
- `method`: backtest, ab_test, benchmark_run, crawl, survey, simulation, manual audit
- `inputs`
- `procedure`
- `sample_definition`
- `metrics`
- `stopping_conditions`
- `quality_checks`
- `estimated_cost`
- `estimated_duration`
- `repro_instructions`

### D. EvidenceBundle

Captures raw and processed evidence.

Fields:

- `unit_id`
- `experiment_id`
- `status`
- `observations`
- `metrics`
- `artifacts`
- `errors`
- `warnings`
- `sample_size`
- `data_coverage`
- `quality_score`
- `reproducibility_score`
- `timestamp`

### E. DecisionRecord

The explicit governance output.

Fields:

- `unit_id`
- `decision`: promote | hold | iterate | demote | archive | deploy | rollback
- `reason`
- `confidence`
- `evidence_strength`
- `next_stage`
- `owner_role`
- `followup_actions`
- `expiry`

### F. SkillArtifact

Stores reusable know-how produced by cycles.

Fields:

- `skill_id`
- `name`
- `domain_scope`: universal, marketing, website, model_eval, trading
- `trigger_conditions`
- `playbook`
- `tool_requirements`
- `evaluation_pattern`
- `known_failure_modes`
- `examples`
- `version`

## 3. Role taxonomy

A generic autoresearch OS needs two parallel taxonomies:

1. execution roles for a cycle
2. capability roles that persist across cycles

### A. Execution roles

These are the roles an agent or subagent can take in a cycle.

#### 1. Sponsor

Purpose:
- defines the decision that matters
- sets budget/risk/time constraints
- approves promotion to production or capital deployment

Typical human-in-the-loop role, but can be partially delegated.

#### 2. Planner

Purpose:
- converts objectives into a cycle plan
- decomposes the decision into testable units
- assigns roles, tools, and budgets

#### 3. Scout

Purpose:
- explores search space
- finds anomalies, opportunities, competitors, and candidate units
- broad recall over precision

Cross-domain equivalents:
- marketing: trends, audience pockets, competitor campaigns
- websites: broken flows, slow pages, SERP gaps
- model eval: benchmark holes, adversarial prompts, regression clusters
- trading: market regimes, event catalysts, microstructure anomalies

#### 4. Synthesizer

Purpose:
- turns noisy findings into candidate research units
- deduplicates and clusters similar ideas
- proposes ranking and rationale

#### 5. Experimenter

Purpose:
- operationalizes a unit into an `ExperimentSpec`
- runs tests or orchestrates domain tools
- ensures instrumentation and logging

#### 6. Critic

Purpose:
- stress-tests assumptions
- searches for confounders, leakage, overfitting, bias, survivorship, and p-hacking
- attempts falsification before promotion

This role is missing from the current Polymarket subagent template and should be first-class.

#### 7. Analyst

Purpose:
- interprets evidence bundles
- compares candidates
- estimates confidence and transferability

#### 8. Risk officer

Purpose:
- enforces guardrails
- blocks unsafe deployment
- tracks downside scenarios and policy violations

Examples:
- legal/brand risk in marketing
- accessibility/privacy/security risk for websites
- benchmark contamination or unsafe behavior for model eval
- drawdown/liquidity/slippage risk for trading

#### 9. Librarian

Purpose:
- curates artifacts
- maintains lineage, memory, and reusable skills
- updates canonical benchmarks, datasets, and playbooks

#### 10. Reporter

Purpose:
- emits short, operator-facing summaries
- writes machine-readable and human-readable outputs

### B. Persistent capability modules

These are reusable skills agents should learn once.

- problem framing
- hypothesis writing
- experiment design
- sampling and controls
- measurement selection
- falsification
- confidence calibration
- error taxonomy construction
- artifact hygiene
- memo writing
- promotion policy application

The OS should teach these as transferable skills rather than domain-specific prompts.

## 4. Adapter contract

The adapter contract is the most important abstraction. A new domain should only need to implement a known set of methods and schemas.

### Minimal adapter contract

A domain adapter should implement:

- `intake(brief) -> normalized brief`
- `enumerate_units(brief, limit) -> list[ResearchUnit]`
- `design_experiment(unit, brief) -> ExperimentSpec`
- `run_experiment(spec, brief) -> EvidenceBundle`
- `analyze(units, evidence, brief) -> list[DecisionRecord]`
- `write_artifacts(cycle_state) -> artifact index`
- `update_memory(cycle_state) -> memory delta`

### Compatibility with current code

Current mapping:

- `HypothesisGenerator.generate` ~= `enumerate_units`
- `ExperimentRunner.run` mixes `design_experiment` and `run_experiment`
- `ResultAnalyzer.analyze` partially covers `analyze`
- `ReportWriter.write` partially covers `write_artifacts`

So the current implementation is a viable v0 kernel, but the adapter boundary should be made more explicit.

### Recommended extended adapter interface

```python
class ResearchAdapter(Protocol):
    def intake(self, brief: ResearchBrief) -> ResearchBrief: ...
    def enumerate_units(self, brief: ResearchBrief, limit: int) -> list[ResearchUnit]: ...
    def design_experiment(self, unit: ResearchUnit, brief: ResearchBrief) -> ExperimentSpec: ...
    def run_experiment(self, spec: ExperimentSpec, brief: ResearchBrief) -> EvidenceBundle: ...
    def critique(self, unit: ResearchUnit, evidence: EvidenceBundle, brief: ResearchBrief) -> dict: ...
    def analyze(
        self,
        brief: ResearchBrief,
        units: list[ResearchUnit],
        evidence: list[EvidenceBundle],
        critiques: list[dict],
    ) -> list[DecisionRecord]: ...
    def artifact_manifest(self, cycle_state: dict) -> dict: ...
    def skill_extraction(self, cycle_state: dict) -> list[SkillArtifact]: ...
```

### Rules for adapters

Every adapter must define:

1. valid unit types
2. valid experiment methods
3. metric definitions and sign conventions
4. guardrails and disqualifiers
5. confidence heuristics
6. cost model
7. promotion policy
8. artifact types it emits

### Examples of adapter specialization

#### Marketing adapter

Units:
- audience segment
- offer hypothesis
- creative angle
- landing page variant

Experiments:
- ad creative test
- email subject test
- funnel drop-off analysis
- competitor messaging scrape

Primary metrics:
- CTR, CPA, CVR, revenue per click, CAC payback

Guardrails:
- brand safety, policy rejection rate, low sample size, attribution gaps

#### Website adapter

Units:
- UX friction hypothesis
- SEO opportunity
- performance bottleneck
- content gap

Experiments:
- page-speed audit
- navigation task completion test
- crawl/indexability check
- funnel instrumentation audit

Primary metrics:
- conversion rate, bounce rate, CWV, time to task, indexed pages

Guardrails:
- accessibility regressions, broken paths, analytics uncertainty

#### Model eval adapter

Units:
- benchmark gap
- prompt strategy
- model configuration
- failure cluster

Experiments:
- benchmark run
- pairwise eval
- judge model comparison
- adversarial probe sweep

Primary metrics:
- pass rate, pairwise win rate, latency, cost, safety score, regression rate

Guardrails:
- contamination, judge bias, flaky harness, insufficient task diversity

#### Trading adapter

Units:
- signal hypothesis
- execution rule
- filter
- regime detector

Experiments:
- backtest
- walk-forward validation
- slippage simulation
- sensitivity sweep

Primary metrics:
- pnl, sharpe, drawdown, turnover, win rate, capacity

Guardrails:
- data leakage, regime concentration, liquidity mismatch, unstable parameter surface

## 5. Artifact contract

The OS should produce machine-readable artifacts first and human-readable summaries second.

### Required artifacts for every cycle

1. `brief.json`
   - normalized cycle brief

2. `units.json`
   - all candidate research units with lineage and ranking

3. `experiments.json`
   - experiment specs and execution metadata

4. `evidence.json`
   - evidence bundles and raw metric summaries

5. `decisions.json`
   - promotion/demotion decisions with reasons

6. `report.md`
   - operator memo

7. `manifest.json`
   - index of all artifacts plus checksums/paths

8. `memory_delta.json`
   - what reusable knowledge changed this cycle

### Recommended optional artifacts

- `critiques.json`
- `leaderboard.json`
- `skill_artifacts.json`
- `failures.json`
- `event_log.jsonl`
- `trace.json`
- domain-specific raw outputs such as benchmark tables, screenshots, crawls, backtests, notebooks

### Artifact quality requirements

Every artifact should satisfy:

- deterministic ids
- explicit schema version
- lineage fields (`parent_cycle_id`, `source_artifact_ids`)
- provenance (`who/what generated it`, tool version, model version)
- timestamps
- reproducibility metadata

## 6. Evaluation criteria

The generic OS needs a universal scorecard, with domain metrics plugged in beneath it.

### A. Universal evaluation dimensions

Every unit should be evaluated on some mix of:

1. impact
   - if true and deployed, how much value could it create?

2. evidence quality
   - sample size, validity, noise, reproducibility, instrumentation quality

3. robustness
   - does it hold across segments, regimes, datasets, seeds, or time windows?

4. efficiency
   - value relative to experiment cost and operator attention

5. safety/risk
   - downside if wrong or deployed prematurely

6. novelty/orthogonality
   - adds new information versus repeating known patterns

7. transferability
   - likely to generalize to adjacent contexts

### B. Recommended scoring decomposition

A generic decision score could be:

`decision_score = expected_impact * evidence_strength * robustness - risk_penalty - cost_penalty`

Where each term is domain-normalized to a fixed range.

### C. Confidence model

Confidence should not just be the top metric. It should combine:

- metric strength
- sample adequacy
- consistency across slices
- absence of confounders
- historical reliability of similar units
- critic findings

Suggested confidence buckets:

- `exploratory`
- `promising`
- `validated`
- `production_ready`
- `degraded`

### D. Disqualifiers

The OS should support hard blocks such as:

- low sample size
- instrumentation invalid
- confounded comparison
- failed reproducibility
- risk threshold breached
- violation of legal/policy constraints

## 7. Promotion and demotion lifecycle

This is the governance backbone.

### Canonical states

1. `backlog`
   - discovered but not yet scoped

2. `scoped`
   - clarified into a valid unit with acceptance criteria

3. `queued`
   - approved for experimentation

4. `running`
   - experiment in progress

5. `review`
   - evidence available, awaiting analysis/critique

6. `promising`
   - positive but incomplete evidence

7. `validated`
   - sufficiently robust for limited real-world exposure

8. `paper`
   - simulated or shadow deployment only

9. `canary`
   - limited live deployment

10. `production`
   - active default strategy/process

11. `degraded`
   - was good, now underperforming or invalidating

12. `demoted`
   - explicitly stepped down from a higher state

13. `archived`
   - retained for memory, not active

### Promotion rules

Each promotion should require explicit threshold checks.

Example generic ladder:

- `queued -> running`: experiment spec complete, budget approved
- `review -> promising`: positive signal above baseline and no hard disqualifier
- `promising -> validated`: replicated across at least N slices/regimes/segments
- `validated -> paper/canary`: risk officer approval and rollback plan exists
- `canary -> production`: live metrics confirm offline expectation within tolerance

### Demotion rules

Demotion should happen automatically or semi-automatically when:

- performance falls below baseline for a rolling window
- confidence decays due to contradictory new evidence
- risk profile worsens
- key assumptions no longer hold
- implementation drift breaks reproducibility

### Retirement versus archive

- `retired`: concept intentionally no longer pursued due to strategic irrelevance
- `archived`: inactive but searchable and potentially reusable later

### Domain examples

Marketing:
- creative idea -> offline messaging review -> small audience canary -> scaled campaign

Website:
- UX fix idea -> staging validation -> limited traffic experiment -> full rollout

Model eval:
- prompt recipe -> eval holdout validation -> limited integration into agent -> default policy

Trading:
- alpha idea -> backtest -> walk-forward -> paper trading -> low-capital live -> production sizing

## 8. Reusable skill and subagent organization

The OS should organize skills in a way that separates universal cognition from domain implementation.

### A. Skill stack structure

#### Level 1: universal skills

Applies everywhere.

- framing a decision question
- generating candidate units
- writing experiment specs
- identifying confounders
- scoring evidence quality
- making promotion decisions
- writing concise reports

#### Level 2: domain skills

Applies within a domain.

- marketing audience segmentation
- website funnel inspection
- model eval harness operation
- trading walk-forward validation

#### Level 3: tool skills

Applies to a tool or platform.

- GA4 analysis
- Ahrefs/Search Console
- Playwright/Lighthouse
- eval harnesses
- backtest engine
- exchange or broker simulator

#### Level 4: environment skills

Applies to an organization’s workflow.

- artifact naming conventions
- team scorecards
- deployment review checklist
- escalation policy

### B. Subagent pack design

A “subagent pack” should include:

- role name
- mission
- required inputs
- expected outputs
- allowed tools
- hard constraints
- evaluation rubric
- failure patterns to watch for
- handoff format

### C. Recommended default subagent pack

1. planner
2. scout
3. synthesizer
4. experimenter
5. critic
6. analyst
7. risk_officer
8. librarian
9. reporter

This expands the current Polymarket set and makes the missing governance functions explicit.

### D. Handoff discipline

Every subagent output should contain:

- claimed conclusion
- evidence cited
- uncertainty
- open questions
- recommended next action
- artifact references

This prevents “vibes-only” delegation.

## 9. Suggested execution template for agents

Teach every agent this sequence:

### Step 1: Frame

Ask:
- what decision are we trying to make?
- what metric defines success?
- what guardrails define failure?
- what time/budget/tool constraints apply?

### Step 2: Explore

Generate a broad but tagged list of candidate units.

### Step 3: Prioritize

Rank candidates by expected value, cost, and uncertainty reduction.

### Step 4: Specify

Write an explicit experiment for each selected unit.

### Step 5: Execute

Run evidence collection with logging and reproducibility.

### Step 6: Critique

Try to falsify the result before celebrating it.

### Step 7: Decide

Assign a lifecycle state and follow-up action.

### Step 8: Externalize memory

Store artifacts, reusable patterns, and failure modes.

### Step 9: Queue next cycle

Choose whether to exploit, replicate, expand, or archive.

## 10. Implementation roadmap from the current codebase

### Phase 1: formalize contracts without breaking current code

Add new schemas or dataclasses for:

- `ResearchBrief`
- `ResearchUnit`
- `ExperimentSpec`
- `EvidenceBundle`
- `DecisionRecord`
- `SkillArtifact`

Keep current dataclasses as compatibility wrappers.

### Phase 2: separate experiment design from execution

Split current runner responsibilities into:

- experiment designer
- experiment executor

This matters in every domain because it makes experiment plans auditable before execution.

### Phase 3: add critic and risk roles to the loop

Extend the kernel:

- `critic.review(...)`
- `risk.evaluate(...)`
- `decision_policy.apply(...)`

### Phase 4: standardize artifact manifest and memory delta

Make every adapter emit:

- `manifest.json`
- `memory_delta.json`
- `skill_artifacts.json`

### Phase 5: build adapter starter kits

Create template adapters for:

- marketing
- websites
- model eval
- trading

Each starter kit should include:

- example unit types
- example experiments
- default metrics
- default guardrails
- sample report
- sample promotion ladder

## 11. What should stay generic versus domain-specific

### Generic across all domains

- cycle state machine
- role taxonomy
- artifact schemas
- evaluation dimensions
- promotion/demotion policy structure
- memory/skill extraction format
- event model and observability

### Domain-specific

- unit types
- experiment methods
- metrics and thresholds
- data sources and tools
- risk guardrails
- production rollout mechanics

## 12. Minimal doctrine for “superpowers-style disciplined workflow”

If this system is to stay disciplined, agents should obey the following doctrine:

1. never promote without explicit evidence
2. never trust a top-line metric without a critic pass
3. every cycle must leave reusable artifacts
4. every claim must point to evidence or be marked speculative
5. every promoted candidate must have rollback/demotion criteria
6. measure uncertainty reduction, not just positive outcomes
7. prefer shallow broad exploration early, deeper exploitation later
8. preserve lineage so later agents can understand why something exists
9. separate hypothesis quality from execution quality
10. optimize for cumulative learning rate, not raw experiment count

## 13. Concrete recommended v1 architecture

### Kernel

`ResearchProgram`
- manages many cycles over time

`ResearchCycle`
- executes a single brief through the canonical stages

### Services

- `AdapterRegistry`
- `ArtifactStore`
- `DecisionPolicy`
- `MemoryStore`
- `EventBus`
- `SkillRegistry`

### Interfaces

- `ResearchAdapter`
- `Critic`
- `RiskEvaluator`
- `DecisionScorer`
- `ReportWriter`

### Outputs

- cycle artifacts
- skill artifacts
- promotion ledger
- reusable benchmark/history store

## 14. Recommended teaching message for agents

Agents should be taught the following invariant:

“You are not trying to be clever once. You are trying to improve the organization’s evidence-backed decision quality over repeated cycles. Your job is to convert uncertainty into governed artifacts that can be promoted, demoted, and reused.”

That makes the OS portable across marketing, websites, model eval, and trading.

## 15. Bottom line

The current `research/loop.py` is a strong execution kernel. `research/polymarket.py` is a useful example adapter. To become a true generic autoresearch operating system, the architecture should be expanded around five durable abstractions:

1. a richer universal object model
2. a strict adapter contract
3. a first-class artifact and memory contract
4. explicit promotion/demotion governance
5. a reusable role and skill taxonomy

With those additions, agents can learn one disciplined operating model and apply it across domains by swapping adapters rather than relearning the whole process each time.
