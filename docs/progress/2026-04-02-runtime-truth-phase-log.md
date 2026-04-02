# Runtime Truth Phase Log - 2026-04-02

Status
- Phase 3 executor/runtime truth path is now live.
- Phase 4 risk truth has started with replay-derived exposure and replay-derived runtime risk reporting.

Completed in this slice
- Added `exposure.py` as the first reusable exposure projection layer.
- Extended `replay.py` to derive:
  - resolved_trade_count
  - win_count
  - loss_count
  - latest_settlement
  - realized_pnl_timeline
  - exposure snapshot
- Extended `execution.py` to expose:
  - `get_ledger_events()`
  - `get_replay_projection()`
  - replay-derived runtime snapshot fields
  - `record_risk_snapshot()` for durable `risk_snapshot_recorded` ledger events
- Reworked `risk.py` so reporting can be built from executor snapshot + ledger events.
- Removed CLI dependence on incremental realized-delta bankroll mutation.
- Added tests:
  - replay exposure projection
  - replay-derived risk report stability after restart
  - replay-derived circuit breaker trigger
  - runtime snapshot resolved stats restored after settlement replay
  - risk snapshot event emission

Verification
- Targeted suite: passed
- Full suite: `pytest -q` -> `31 passed`

Strategic meaning
- Runtime no longer just stores fills and settlements; it now reads them back into exposure and risk reporting.
- The paper bot is materially closer to a truth spine instead of a vibes engine.

Still missing
- Mark-to-market drawdown/exposure using live marks rather than only realized PnL.
- Replay-derived family metrics.
- Full CLI-level restart equivalence harness.

Deployment status
- `polymarket-paper-bot.service` is now running live under systemd from the canonical repo path.
- Service unit was cleaned to a single authoritative `ExecStart` with explicit `--runtime-dir`.
- Added CLI ops surfaces:
  - `python3 cli.py status --runtime-dir data/runtime`
  - `python3 cli.py health --runtime-dir data/runtime --max-heartbeat-age 180`
- Added `latest-status.txt` emission beside `status.json` for fast terminal/systemd inspection.
- Live runtime verification confirmed:
  - 8 strict interval markets discovered
  - ledger writes occurring in `data/runtime/ledger.db`
  - `risk_snapshot_recorded` events present for active run ids
  - `status.json` heartbeat updating under systemd

Recommended next move
1. Emit `risk_snapshot_recorded` each loop.
2. Add replay-backed family metrics.
3. Add full runtime restart equivalence test using runtime_dir + ledger.db.
4. Only then harden dashboard/runtime surfaces around replay-backed truth.
