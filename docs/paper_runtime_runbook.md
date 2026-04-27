# Polymarket Paper Runtime Runbook

Last updated: 2026-04-27
Canonical repo: `/root/obsidian-hermes-vault/projects/polymarket-5min-bot`
Mode: PAPER ONLY

## 1. Hard rules

- Live trading remains disabled.
- Never run `cli.py` in `--mode live`.
- Never loosen circuit breakers, drawdown limits, or daily-loss gates to make the runtime look healthy.
- Never bypass RED/YELLOW governance just to keep the loop alive.
- Never run two paper bots at once.
- Never let the healthcheck auto-restart after a protective stop.

## 2. Current architecture

Main runtime
- Service: `polymarket-paper-bot.service`
- Command:
  `/root/obsidian-hermes-vault/projects/polymarket-5min-bot/.venv/bin/python /root/obsidian-hermes-vault/projects/polymarket-5min-bot/cli.py run --mode paper --runtime-dir /root/obsidian-hermes-vault/projects/polymarket-5min-bot/data/runtime --sleep-seconds 5`
- Restart policy:
  - `Restart=on-failure`
  - `RestartPreventExitStatus=2`
- Meaning:
  - crash/failure may restart
  - circuit-breaker exit code 2 must stay stopped

Runtime truth artifacts
- `data/runtime/status.json`
- `data/runtime/latest-status.txt`
- `data/runtime/strategy_metrics.json`
- `data/runtime/events.jsonl`
- `data/runtime/market_samples.jsonl`
- `data/runtime/ledger.db`

Research artifacts
- `data/research/latest.json`
- `data/research/latest.md`
- `data/research/family_scoreboard.json`
- `data/research/bucket_scoreboard.json`
- timestamped `cycle-runtime-*.json/.md`
- contradiction reports `contradiction-*.json`

Forensic snapshots
- `data/forensic-snapshots/<timestamp>/manifest.json`
- used for circuit-breaker/operator-stop evidence preservation

## 3. Services and timers

Main service
- `polymarket-paper-bot.service`

Healthcheck
- `polymarket-paper-bot-healthcheck.timer`
- `polymarket-paper-bot-healthcheck.service`
- Healthcheck now uses:
  `scripts/paper_healthcheck.py`
- Safety contract:
  - may restart only for genuine unhealthy crash/hang cases
  - must not restart after protective stops
  - must not amplify duplicate/orphan paper processes

Research
- `polymarket-paper-research.timer`
- `polymarket-paper-research.service`
- cadence: every 15 minutes

Ops evidence
- `polymarket-paper-ops-hourly.timer`
- `polymarket-paper-ops-hourly.service`
- cadence: hourly
- generates runtime/operator evidence files such as:
  - `data/runtime/ops_status.txt`
  - `data/runtime/ops_evidence.txt`
  - `data/runtime/ops_settlement_diagnostics.txt`
  - `data/runtime/fill_markout_audit_latest.md`
  - `data/runtime/settlement_latency_audit_latest.md`
  - `data/runtime/reconcile_metrics_latest.txt`
  - `data/runtime/attribution_report_latest.txt`
  - `data/runtime/fill_markout_report_latest.txt`

## 4. Cron and external supervisor jobs

Telegram scheduled evidence report
- cron entry:
  `28 4,10,16,22 * * * [ $(date +\%s) -le 1777537696 ] && /usr/bin/flock -n /tmp/polymarket_telegram_evidence.lock /root/.hermes/scripts/polymarket_telegram_evidence.py >> /root/.hermes/logs/polymarket_telegram_evidence.log 2>&1`
- cadence: every 6 hours at 04:28, 10:28, 16:28, 22:28 local server time
- intended role: scheduled evidence update only

72h supervisor
- script: `/root/.hermes/scripts/polymarket_72h_supervisor.py`
- intended role: observe-only critical alerts, not scheduled reporting
- current design contract:
  - no restarts
  - no config changes
  - no scheduled 6h reports from supervisor
  - heartbeat-stale alert suppressed for protective stopped states

tmux
- Current tmux sessions are not the paper bot runtime path.
- Do not start another paper bot in tmux.

## 5. Telegram/reporting cadence

Scheduled reports
- owner: cron job running `polymarket_telegram_evidence.py`
- cadence: every 6 hours

Immediate alerts
- owner: `polymarket_72h_supervisor.py`
- scope: critical safety/runtime alerts only
- dedupe/cooldown: enforced inside supervisor state machine
- sends are logged in:
  - `/root/.hermes/logs/polymarket_72h_supervision.log`
  - `/root/.hermes/logs/polymarket_telegram_evidence.log`
  - state files under `/root/.hermes/state/`

## 6. Strategy state

Active
- `toxicity_mm`
- `time_decay`

Candidates (not active by default)
- `mean_reversion_5min`
- `opening_range`
- `spot_momentum`

Operational truth
- `toxicity_mm` is the baseline family with actual fills.
- `time_decay` has been mechanically active but has not proven economic value.
- Candidate families are not approved for activation during debug/hardening.

## 7. RED / YELLOW / GREEN meaning

GREEN
- enough settled evidence
- acceptable profitability threshold
- no unresolved contradiction / breaker-review problems

YELLOW
- system may be mechanically alive but evidence is still insufficient or weak
- common causes:
  - low settled-trade count
  - win rate below target threshold
  - run-lineage fragmentation
- YELLOW is not permission to claim edge

RED
- contradiction or safety stop state
- examples:
  - unreviewed circuit-breaker stop
  - settlement PnL not computable
  - severe fragmentation
  - explicit contradiction log
- RED is a stop-and-review state, not a parameter-tweaking invitation

## 8. Status checks

Fast human status
```bash
cd /root/obsidian-hermes-vault/projects/polymarket-5min-bot
.venv/bin/python cli.py status --runtime-dir data/runtime
```

Health check
```bash
cd /root/obsidian-hermes-vault/projects/polymarket-5min-bot
.venv/bin/python cli.py health --runtime-dir data/runtime --max-heartbeat-age 180
```

Raw latest status
```bash
cd /root/obsidian-hermes-vault/projects/polymarket-5min-bot
cat data/runtime/latest-status.txt
```

Service status
```bash
systemctl status --no-pager polymarket-paper-bot.service
systemctl status --no-pager polymarket-paper-bot-healthcheck.timer polymarket-paper-bot-healthcheck.service
systemctl status --no-pager polymarket-paper-research.timer polymarket-paper-research.service
systemctl status --no-pager polymarket-paper-ops-hourly.timer polymarket-paper-ops-hourly.service
```

Current timers
```bash
systemctl list-timers --all 'polymarket-paper*' --no-pager
```

Process truth
```bash
ps -eo pid,ppid,etimes,cmd | grep -E 'polymarket|paper_trader|cli.py run' | grep -v grep
```

Journal truth
```bash
journalctl -u polymarket-paper-bot.service -n 120 --no-pager -o short-iso
journalctl -u polymarket-paper-bot-healthcheck.service -n 80 --no-pager -o short-iso
```

## 9. Stop/start commands

Paper bot
```bash
systemctl stop polymarket-paper-bot.service
systemctl start polymarket-paper-bot.service
systemctl restart polymarket-paper-bot.service
```

Healthcheck timer
```bash
systemctl stop polymarket-paper-bot-healthcheck.timer
systemctl start polymarket-paper-bot-healthcheck.timer
systemctl start polymarket-paper-bot-healthcheck.service
```

Research timer
```bash
systemctl stop polymarket-paper-research.timer
systemctl start polymarket-paper-research.timer
systemctl start polymarket-paper-research.service
```

Ops hourly timer
```bash
systemctl stop polymarket-paper-ops-hourly.timer
systemctl start polymarket-paper-ops-hourly.timer
systemctl start polymarket-paper-ops-hourly.service
```

Stop supervisor
```bash
pkill -f '/root/.hermes/scripts/polymarket_72h_supervisor.py'
```

Check supervisor still running
```bash
pgrep -af 'polymarket_72h_supervisor.py'
```

Disable scheduled Telegram evidence report
```bash
crontab -l | grep -v 'polymarket_telegram_evidence.py' | crontab -
```

## 10. When NOT to restart

Do NOT restart automatically or manually just because the service stopped if any of the following are true:
- `stop_reason=circuit_breaker`
- daily-loss or drawdown stop fired
- runtime is in RED due to unresolved contradiction / breaker-review state
- duplicate paper bot processes exist
- a rogue non-systemd paper process exists
- evidence review has not been completed after a protective stop

The correct response to a protective stop is forensic review, not blind restart.

## 11. How to regenerate research

One-shot research run
```bash
cd /root/obsidian-hermes-vault/projects/polymarket-5min-bot
.venv/bin/python cli.py research --runtime-dir data/runtime --artifact-dir data/research
```

One-shot ops evidence refresh
```bash
systemctl start polymarket-paper-ops-hourly.service
```

## 12. How to read scoreboards

Family scoreboard
- file: `data/research/family_scoreboard.json`
- use for family-level verdicts, promotion state, settled-trade counts, realized PnL
- this is governance/evidence, not permission to auto-activate candidates during hardening

Bucket scoreboard
- file: `data/research/bucket_scoreboard.json`
- use for bucket pause decisions by family/asset/interval/TTE bucket
- runtime may consume this to block or warn on weak buckets

Operator evidence
- `data/runtime/ops_evidence.txt`
- `data/runtime/attribution_report_latest.txt`
- `data/runtime/fill_markout_report_latest.txt`
- `data/runtime/reconcile_metrics_latest.txt`

Use settlement/mark-to-market truth, not just fill-level realized deltas.

## 13. Current truth model

What counts as real progress
- consistent settled-trade evidence
- acceptable settlement/MTM PnL
- stable run lineage
- no unresolved breaker-review contradictions

What does NOT count
- merely being `active (running)`
- quote volume without economic edge
- fill-level micro-PnL if settlement truth is weak/negative
- repeated restarts that reset narrative but not risk reality

## 14. Live trading remains disabled

CLI behavior
```bash
cd /root/obsidian-hermes-vault/projects/polymarket-5min-bot
.venv/bin/python cli.py live
```
Expected result: failure with live-mode blocked message.

Do not change this during debug/hardening.
