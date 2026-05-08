# Polymarket CLI Universe Adapter + Tonight Launch Plan

> For Hermes: execute aggressively. Use test-first for new behavior and prefer the official `polymarket` CLI as venue truth.

Goal: replace brittle market discovery with an official Polymarket CLI-backed adapter, support multiple universe modes, and launch always-on paper trading tonight with $100 initial capital in broad liquid-market mode while preserving the final goal of strict 5/15-minute Polymarket windows.

Architecture: add a dedicated market-universe adapter that shells out to the official `polymarket` CLI for market discovery and order-book snapshots, normalize those payloads into the existing runtime shape, then wire CLI/runtime config to select one of three universes: strict 5/15 crypto, short-horizon crypto, or all-liquid markets. Launch the runtime in tmux so it survives the shell.

Tech stack: Python, Click CLI, unittest, official `polymarket` CLI, tmux.

---

## Task 1: Add failing tests for universe selection and CLI-backed discovery
Objective: lock the new behavior before touching implementation.

Files:
- Create: `tests/test_market_universe.py`
- Modify: none

Checks:
- universe mode `strict_crypto_5m_15m` only keeps crypto markets inside 15 minutes
- universe mode `short_horizon_crypto` keeps crypto markets inside a configurable horizon
- universe mode `all_liquid` keeps liquid markets regardless of topic
- CLI payload normalization produces existing fields like `id`, `question`, `endDate`, `clobTokenIds`, `bestBid`, `bestAsk`

## Task 2: Implement CLI-backed market universe adapter
Objective: add one clean adapter file rather than infecting the whole codebase.

Files:
- Create: `market_universe.py`
- Modify: `market_data.py`

Behavior:
- `OfficialPolymarketCliAdapter` runs `polymarket -o json markets list ...` and `polymarket -o json clob book TOKEN_ID`
- parse outcomes/token IDs safely
- normalize market payloads
- support modes:
  - `strict_crypto_5m_15m`
  - `short_horizon_crypto`
  - `all_liquid`
- expose a single `fetch_markets()` and `fetch_orderbook()` path for runtime use

## Task 3: Wire runtime and collect paths to universe config
Objective: let one runtime choose different universes without forking the bot.

Files:
- Modify: `cli.py`, `config.yaml`

Behavior:
- replace hardcoded `get_markets_by_duration(5/15)` loop with adapter-driven fetch
- add status telemetry fields for `universe_mode` and `market_source`
- add CLI overrides for `--universe-mode` and optional `--market-source`
- set paper initial capital default to $100

## Task 4: Add launch tooling for always-on tmux runtime
Objective: make tonight’s runtime survivable and inspectable.

Files:
- Create: `scripts/launch_paper_tmux.sh`
- Create: `scripts/runtime_snapshot.sh`
- Modify: `README.md`

Behavior:
- start/restart tmux session `pm-paper`
- run venv python command with broad liquid mode tonight
- save stdout/stderr to `logs/paper_runtime.log`
- snapshot helper prints tmux pane tail + runtime status

## Task 5: Validate, launch, and schedule 48h follow-up research
Objective: prove the runtime works and set the research flywheel.

Files:
- none or docs updates if needed

Checks:
- full tests pass
- one `--max-loops 1` smoke run works in `all_liquid` mode
- tmux session launches and status file updates
- create a 48h cron follow-up that reads runtime artifacts and runs/recommends autoresearch promotion decisions
