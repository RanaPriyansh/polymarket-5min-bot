# VPS Architecture Discovery Brief

## 1. What is already confirmed locally

### Runtime entrypoints
- Main runtime command is `python cli.py run --mode paper`.
- `live` mode is intentionally blocked in the CLI until paper workflow is proven.
- Other supported commands:
  - `python cli.py collect`
  - `python cli.py backtest --data ...`
  - `python cli.py research --runtime-dir data/runtime --artifact-dir data/research`

### Runtime loop shape
1. Load config from `config.yaml` plus env overrides.
2. Create runtime directories under `data/`, `logs/`, and `data/runtime/`.
3. Discover active 5m/15m interval markets from Polymarket Gamma.
4. Fetch both outcome books from the Polymarket CLOB.
5. Assess book quality and skip toxic/illiquid books.
6. Run strategies:
   - `mean_reversion_5min`
   - `toxicity_mm`
7. Simulate paper order placement/fills through `PolymarketExecutor`.
8. Poll for market resolution and settle paper positions.
9. Persist telemetry to runtime artifacts for later research.

### Data sources and execution path
- Market discovery uses Gamma API.
- Order books use CLOB `/book?token_id=...`.
- Paper mode does not hit live order APIs for execution.
- Live-mode order placement code exists in `execution.py`, but CLI currently prevents running it.
- Settlement is derived by polling refreshed market state after market end.

### Runtime artifacts already produced
- `data/runtime/status.json`
- `data/runtime/events.jsonl`
- `data/runtime/strategy_metrics.json`
- `data/runtime/market_samples.jsonl`

### Current autoresearch scope in repo
- `python cli.py research` runs a local research cycle over runtime artifacts.
- Current adapter mainly surfaces:
  - fill rate
  - toxic-book skip rate
  - realized pnl by strategy family
  - dominant skip reasons
  - latest fill artifact
- Subagent roles are sketched in `docs/subagents/autoresearch-subagents.json`:
  - scout
  - experimenter
  - analyst
  - reporter

### Deployment assumptions found in repo
- Dockerfile exists and defaults to paper mode.
- README suggests Docker deployment.
- `DEPLOY.md` suggests ad hoc background processes via `nohup`.
- No committed runner config was found for:
  - `systemd`
  - `pm2`
  - `supervisord`
  - `docker-compose`
  - cron/timers

## 2. What the local repo does NOT tell us

These facts must come from the VPS:

- Exact repo path on server
- Whether the bot runs bare-metal, in Docker, or both
- Which process manager is actually used
- How Hermes/Thielon is installed and invoked
- Whether bot and Hermes share the same workspace/data directory
- Whether there is Redis/Postgres/SQLite or only flat files
- Whether logs are local files, journald, Docker logs, or mixed
- Whether the bot is currently running at all
- Whether paper mode is the only active execution path
- Whether there is any external runner wrapping `cli.py`
- Whether any other service is placing real Polymarket orders
- How secrets are injected
- Whether there is monitoring/restart logic

## 3. Local architecture summary

### Components
- `cli.py`: orchestrates run/backtest/collect/research
- `market_data.py`: Polymarket Gamma + CLOB access
- `execution.py`: paper/live order abstraction, fills, settlement, positions
- `strategies/mean_reversion_5min.py`: signal generation
- `strategies/toxicity_mm.py`: quoting logic
- `risk.py`: position caps and circuit breakers
- `runtime_telemetry.py`: durable status/events/metrics/sample storage
- `research/polymarket.py`: converts runtime artifacts into research insights
- `research/loop.py`: writes research results to JSON/Markdown

### Important current constraint
- The repo is architected as a single-process runtime loop.
- Research is currently offline/post-run or sidecar, not a continuously governed self-improvement system.
- There is no safe promotion pipeline yet from research output -> parameter change -> validation -> deployment.

## 4. Target direction for the closed loop

The strongest version of this system should be:

1. Paper trading bot writes structured runtime artifacts every cycle.
2. Hermes/Thielon reads those artifacts on a separate cadence.
3. Hermes runs autoresearch to generate hypotheses, anomaly reports, and experiment proposals.
4. Proposed changes are tested on replay/backtest data before promotion.
5. Only bounded config/strategy deltas are eligible for rollout.
6. Promotions are written to a versioned config/experiment registry, not directly hot-patched into production logic.
7. Live trading remains gated behind explicit approval plus paper/replay evidence.

## 5. Hermes discovery task

Ask Hermes/Thielon to collect only facts first. Do not let it change config, restart services, or place trades yet.

### Prompt for Hermes

```text
You are helping inventory the VPS architecture for the Polymarket bot and its autoresearch loop. Do not modify anything. Do not restart anything. Do not place trades. Only collect facts and report them clearly.

I need a server architecture report covering:
1. Where the Polymarket bot repo lives
2. Whether the bot is running now, and under what command
3. Whether it runs in Docker, systemd, pm2, nohup, cron, or another runner
4. Where logs, runtime artifacts, configs, env files, and credentials are located
5. How paper trades are currently executed/simulated
6. Whether any component is capable of live trading right now
7. How Hermes/Thielon is installed, launched, and connected to this project
8. Whether there is shared storage between the bot and Hermes
9. Whether Redis/Postgres/SQLite or any queue/message bus is running
10. What restart/healthcheck/monitoring setup exists

Return:
- a process inventory
- a filesystem inventory
- a network/service inventory
- the exact bot execution path from scheduler/runner -> Python entrypoint -> runtime artifacts
- a gap list of what is missing for a continuous autoresearch loop

Run commands like these as needed and summarize results:
- pwd
- hostname
- whoami
- date -Is
- uname -a
- python3 --version
- docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
- docker inspect <container>
- ps aux | egrep 'python|pm2|node|qwen|thielon|hermes|polymarket|cli.py' | grep -v grep
- systemctl list-units --type=service --all | egrep 'hermes|thielon|polymarket|bot|docker'
- systemctl status <candidate-service> --no-pager
- crontab -l
- sudo crontab -l
- pm2 ls
- ss -tulpn
- find ~ -maxdepth 5 \\( -name 'polymarket-5min-bot' -o -name 'config.yaml' -o -name '.env' -o -name 'paper_trading.out' -o -name 'collector.out' -o -name 'status.json' -o -name 'events.jsonl' -o -name 'strategy_metrics.json' -o -name 'market_samples.jsonl' \\)
- ls -la <repo>
- git -C <repo> status --short
- git -C <repo> rev-parse --abbrev-ref HEAD
- git -C <repo> log -1 --oneline
- sed -n '1,220p' <repo>/config.yaml
- sed -n '1,260p' <repo>/cli.py
- tail -n 100 <logfile>
- cat <runtime status file>

Also identify:
- the exact command that starts Hermes/Thielon
- the model/provider used for Thielon
- how much autonomy it currently has
- whether it can read/write this bot repo
- whether it can run scheduled jobs or only respond interactively

Output format:
1. Executive summary
2. Bot runtime
3. Hermes runtime
4. Storage and artifacts
5. Services and runners
6. Risks / missing pieces
7. Exact evidence snippets
```

## 6. What I expect Hermes to come back with

At minimum, we need these exact facts before architecture planning:

- Bot repo absolute path
- Bot branch and last commit
- Current runner command
- Whether process is persistent across reboot
- Whether runtime artifacts are updating in real time
- Whether Hermes can see the same runtime directory
- Whether research artifacts already exist anywhere on disk
- Whether there is one bot instance or many
- Whether container volume mounts persist `data/runtime`
- Whether any env file contains live wallet credentials

## 7. Early architecture recommendation

Unless the VPS already has a cleaner setup, the safest first production shape is:

- one persistent paper bot service
- one persistent Hermes research service
- one shared artifact directory
- one experiment registry directory
- one promotion gate requiring replay validation before applying deltas

Recommended storage split:
- `data/runtime/` for raw loop artifacts
- `data/research/` for Hermes reports
- `data/experiments/` for candidate parameter sets and results
- `data/promotions/` for approved config deltas

Recommended control split:
- Bot process only trades and records facts
- Hermes only researches and proposes
- A separate promotion step decides whether changes become active

This keeps the loop improving every cycle without letting the research agent directly mutate the production strategy in an uncontrolled way.
