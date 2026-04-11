#!/usr/bin/env python3
"""
Polymarket 5/15-minute bot CLI.
Commands: run, backtest, paper, live, collect, research
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List

import click
import yaml

from baseline_evidence import build_baseline_evidence, render_baseline_evidence_text
from research.loop import ResearchLoop
from research.polymarket import PolymarketRuntimeResearchAdapter
from runtime_telemetry import RuntimeTelemetry
from status_utils import render_status_text, runtime_health_payload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)

logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Polymarket 5/15-minute trading bot."""
    pass


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:
        return
    load_dotenv()


def _ensure_runtime_dirs(cfg: dict) -> None:
    Path("data").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    runtime_dir = Path(cfg.get("runtime", {}).get("dir", "data/runtime"))
    runtime_dir.mkdir(parents=True, exist_ok=True)
    log_file = Path(cfg.get("logging", {}).get("file", "logs/bot.log"))
    log_file.parent.mkdir(parents=True, exist_ok=True)


def _apply_env_overrides(cfg: dict) -> dict:
    import os

    env_mapping = {
        ("polymarket", "wallet_address"): "POLYMARKET_WALLET_ADDRESS",
        ("polymarket", "private_key"): "POLYMARKET_PRIVATE_KEY",
        ("telegram", "bot_token"): "TELEGRAM_BOT_TOKEN",
        ("telegram", "chat_id"): "TELEGRAM_CHAT_ID",
    }
    for path, env_name in env_mapping.items():
        value = os.getenv(env_name)
        if value:
            cursor = cfg
            for key in path[:-1]:
                cursor = cursor.setdefault(key, {})
            cursor[path[-1]] = value
    return cfg


def load_cfg() -> dict:
    _load_dotenv()
    with open("config.yaml", "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    cfg = _apply_env_overrides(cfg)
    _ensure_runtime_dirs(cfg)
    return cfg


def _emit_events(runtime: RuntimeTelemetry, events: Iterable[Dict], *, run_id: str | None = None) -> None:
    for event in events:
        payload = dict(event)
        event_type = payload.pop("event_type", "runtime.event")
        runtime.append_event(event_type, payload, run_id=run_id)


def _resolve_active_strategies(cfg: dict, strategies: str | None) -> List[str]:
    if strategies and strategies.strip():
        return [item.strip() for item in strategies.split(",") if item.strip()]
    configured = cfg.get("strategies", {}).get("active", [])
    return [str(item).strip() for item in configured if str(item).strip()]


def _active_slot_summary(markets: List[Dict]) -> List[Dict]:
    return [
        {
            "slot_id": market["slot_id"],
            "market_id": market["id"],
            "market_slug": market["slug"],
            "asset": market.get("asset"),
            "interval_minutes": market.get("interval_minutes"),
            "end_ts": market["end_ts"],
        }
        for market in markets
    ]


def _strategy_governance(cfg: dict, active_strategies: List[str]) -> Dict[str, object]:
    candidates = [str(item).strip() for item in cfg.get("strategies", {}).get("candidates", []) if str(item).strip()]
    baseline = active_strategies[0] if len(active_strategies) == 1 else None
    return {
        "baseline_strategy": baseline,
        "research_candidates": candidates,
    }


def _enforce_clock_drift(drift_seconds: float) -> None:
    if drift_seconds > 5:
        logger.warning("Clock drift is %.2fs vs Polymarket API time", drift_seconds)
    if drift_seconds > 30:
        logger.warning("Clock drift exceeds 30s; paper trading may query stale windows")
    if drift_seconds > 60:
        raise click.ClickException(f"Clock drift too large for interval markets: {drift_seconds:.2f}s")


def _read_prior_bankroll(ledger_db_path: str) -> "float | None":
    """Read the most recent bankroll from risk_snapshot_recorded events in the ledger.

    Returns the bankroll float if found, or None if the ledger does not exist or has no
    risk_snapshot_recorded events yet.
    """
    import json
    import sqlite3

    from pathlib import Path as _Path

    p = _Path(ledger_db_path)
    if not p.exists():
        return None
    try:
        conn = sqlite3.connect(str(p))
        cur = conn.cursor()
        cur.execute(
            "SELECT payload_json FROM ledger_events"
            " WHERE event_type='risk_snapshot_recorded'"
            " ORDER BY event_ts DESC LIMIT 1"
        )
        row = cur.fetchone()
        conn.close()
        if row is None:
            return None
        payload = json.loads(row[0])
        bankroll = payload.get("capital")
        if bankroll is not None:
            return float(bankroll)
        return None
    except Exception:
        return None


def _detect_prior_run_id(ledger_db_path: str) -> "tuple[str | None, float | None]":
    """Return (prior_run_id, prior_bankroll) from the most recent risk_snapshot_recorded event.

    Returns (None, None) when the ledger does not exist or contains no such events.
    """
    import json
    import sqlite3

    from pathlib import Path as _Path

    p = _Path(ledger_db_path)
    if not p.exists():
        return None, None
    try:
        conn = sqlite3.connect(str(p))
        cur = conn.cursor()
        cur.execute(
            "SELECT run_id, payload_json FROM ledger_events"
            " WHERE event_type='risk_snapshot_recorded'"
            " ORDER BY event_ts DESC LIMIT 1"
        )
        row = cur.fetchone()
        conn.close()
        if row is None:
            return None, None
        prior_run_id = row[0]
        payload = json.loads(row[1])
        prior_bankroll = float(payload.get("capital", 0.0))
        return prior_run_id, prior_bankroll
    except Exception:
        return None, None


@cli.command()
@click.option("--mode", type=click.Choice(["backtest", "paper", "live"]), default="paper", help="Execution mode")
@click.option("--strategies", default=None, help="Comma-separated active strategies (defaults to config.yaml strategies.active)")
@click.option("--max-loops", type=int, default=0, help="Bounded loop count for smoke tests")
@click.option("--runtime-dir", default="data/runtime", help="Directory for durable runtime telemetry")
@click.option("--sleep-seconds", type=int, default=15, help="Loop sleep duration")
def run(mode, strategies, max_loops, runtime_dir, sleep_seconds):
    """Run the trading bot in specified mode."""
    from execution import PolymarketExecutor
    from tradeability_policy import assess_tradeability
    from market_data import PolymarketData
    from risk import RiskManager
    from strategies.mean_reversion_5min import MeanReversion5Min
    from strategies.opening_range import OpeningRangeBreakout
    from strategies.time_decay import TimeDecay
    from strategies.toxicity_mm import ToxicityMM

    if mode == "live":
        raise click.ClickException("Live mode is deferred until the restored paper workflow is proven.")

    cfg = load_cfg()
    active_strategies = _resolve_active_strategies(cfg, strategies)
    cfg.setdefault("strategies", {})["active"] = active_strategies
    initial_capital = float(cfg.get("execution", {}).get("paper_starting_bankroll", 500.0))
    ledger_db_path = str(Path(runtime_dir) / "ledger.db")
    prior_bankroll = _read_prior_bankroll(ledger_db_path)
    if prior_bankroll is not None:
        click.echo(f"Bankroll continuity: resuming from prior bankroll ${prior_bankroll:.2f} (not resetting to config default)")
        initial_capital = prior_bankroll
    else:
        click.echo(f"No prior ledger found — starting with config bankroll ${initial_capital:.2f}")
    strategy_governance = _strategy_governance(cfg, active_strategies)

    click.echo(f"Starting bot in {mode} mode with strategies: {','.join(active_strategies)}")
    click.echo("Press Ctrl+C to stop.")

    async def main_loop():
        runtime = RuntimeTelemetry(runtime_dir)
        run_id = runtime.make_run_id(mode)
        stop_reason = "completed"
        stop_snapshot_dir: str | None = None

        # AC-8: emit service_restart lineage event when a prior run exists in the ledger
        ledger_path = str(Path(runtime_dir) / "ledger.db")
        prior_run_id, prior_bankroll_at_stop = _detect_prior_run_id(ledger_path)
        if prior_run_id and prior_run_id != run_id:
            runtime.append_event("service_restart", {
                "prior_run_id": prior_run_id,
                "new_run_id": run_id,
                "restart_ts": time.time(),
                "trigger": "unknown",
                "prior_bankroll": prior_bankroll_at_stop,
                "new_bankroll": initial_capital,
                "bankroll_delta": round(initial_capital - (prior_bankroll_at_stop or 0.0), 6),
            })

        runtime.append_event("runtime.started", {"run_id": run_id, "mode": mode, "strategies": active_strategies, **strategy_governance})
        runtime.update_status(run_id=run_id, phase="starting", mode=mode, strategies=active_strategies, loop_count=0, **strategy_governance)

        async with PolymarketData(cfg) as md:
            startup = await md.smoke_check()
            _enforce_clock_drift(startup["clock_drift_seconds"])
            runtime.append_event("runtime.startup_check", startup)

            cfg.setdefault("execution", {})
            cfg["execution"].setdefault("ledger_db_path", str(Path(runtime_dir) / "ledger.db"))

            mean_rev = MeanReversion5Min(cfg)
            opening_range = OpeningRangeBreakout(cfg)
            time_decay = TimeDecay(cfg)
            mm = ToxicityMM(cfg)
            risk_mgr = RiskManager(cfg, initial_capital=initial_capital)
            executor = PolymarketExecutor(cfg, md, mode=mode, run_id=run_id)
            await executor.__aenter__()
            loop_count = 0

            try:
                while True:
                    loop_count += 1
                    loop_now = time.time()
                    all_markets = await md.get_markets_by_duration(minutes=max(md.intervals))
                    click.echo(f"Discovered {len(all_markets)} strict interval markets")

                    for market in all_markets:
                        executor.register_market(market)
                        runtime.append_event("market.discovered", {
                            "slot_id": market["slot_id"],
                            "market_id": market["id"],
                            "market_slug": market["slug"],
                            "asset": market.get("asset"),
                            "interval_minutes": market.get("interval_minutes"),
                            "end_ts": market["end_ts"],
                        })

                    order_books = await asyncio.gather(
                        *[md.get_orderbook(market) for market in all_markets],
                        return_exceptions=True,
                    )

                    processed_markets = 0
                    toxic_skips = 0
                    fill_events: List[Dict] = []
                    orderbooks_by_market: Dict[str, object] = {}

                    for idx, market in enumerate(all_markets):
                        market_id = market["id"]
                        primary_outcome = market["outcomes"][0]
                        ob_or_exc = order_books[idx]
                        if isinstance(ob_or_exc, Exception):
                            click.echo(f"Error fetching order book for {market_id}: {ob_or_exc}")
                            runtime.append_event("market.fetch_error", {"market_id": market_id, "error": str(ob_or_exc)})
                            continue

                        orderbook = ob_or_exc
                        orderbooks_by_market[market_id] = orderbook
                        book_quality = assess_tradeability(
                            cfg,
                            "runtime_baseline",
                            orderbook,
                            primary_outcome,
                        )
                        runtime.append_market_sample({
                            "run_id": run_id,
                            "market_id": market_id,
                            "market_slug": market["slug"],
                            "slot_id": market["slot_id"],
                            "volume": float(market.get("volume", 0)),
                            "book_spread_bps": round(book_quality.spread_bps, 3),
                            "book_depth": round(book_quality.top_depth, 3),
                            "book_notional": round(book_quality.top_notional, 3),
                            "book_reasons": list(book_quality.reasons),
                            "is_tradeable": book_quality.is_tradeable,
                        })

                        fill_events.extend(executor.evaluate_market_orders(market_id, orderbook))
                        mid = md.mid_price(orderbook, primary_outcome)
                        mean_rev.update_price(
                            market_id,
                            mid,
                            loop_now,
                            float(market.get("volume", 0)),
                            interval_minutes=market.get("interval_minutes"),
                        )

                        for strategy_family in active_strategies:
                            executor.note_market_seen(strategy_family)

                        if not book_quality.is_tradeable:
                            toxic_skips += 1
                            for strategy_family in active_strategies:
                                executor.note_toxic_book_skip(strategy_family)
                            runtime.append_event("market.skipped_toxic_book", {
                                "market_id": market_id,
                                "market_slug": market["slug"],
                                "reasons": list(book_quality.reasons),
                                "spread_bps": book_quality.spread_bps,
                                "top_depth": book_quality.top_depth,
                            })
                            continue

                        processed_markets += 1
                        volume = float(market.get("volume", 0))

                        if "mean_reversion_5min" in active_strategies:
                            signal = mean_rev.generate_signal(
                                market_id,
                                primary_outcome,
                                mid,
                                orderbook,
                                volume,
                                interval_minutes=market.get("interval_minutes"),
                            )
                            if signal:
                                signal.size = risk_mgr.cap_requested_size(max(mid, 0.01), signal.size)
                                if signal.size > 0:
                                    click.echo(
                                        f"SIGNAL: {market['slug']} {signal.outcome} {signal.action} {signal.size}@{mid:.4f} ({signal.reason})"
                                    )
                                    result = await executor.execute_signal_trade(market, orderbook, signal)
                                    _emit_events(runtime, result.get("events", []), run_id=run_id)

                        if "opening_range" in active_strategies:
                            opening_range.update_price(market_id, mid, volume)
                            signal = opening_range.generate_signal(
                                market_id,
                                primary_outcome,
                                mid,
                                orderbook,
                                volume,
                                slot_id=market.get("slot_id", ""),
                            )
                            if signal:
                                signal.size = risk_mgr.cap_requested_size(max(signal.price, 0.01), signal.size)
                                if signal.size > 0 and not executor.has_strategy_market_exposure("opening_range", market_id):
                                    click.echo(
                                        f"OPENING RANGE: {market['slug']} {signal.outcome} {signal.action} {signal.size}@{signal.price:.4f} ({signal.reason})"
                                    )
                                    result = await executor.execute_signal_trade(market, orderbook, signal, strategy_family="opening_range")
                                    _emit_events(runtime, result.get("events", []), run_id=run_id)

                        if "time_decay" in active_strategies:
                            signal = time_decay.generate_signal(market_id, market, orderbook, current_time=loop_now)
                            if signal:
                                signal.size = risk_mgr.cap_requested_size(max(signal.price, 0.01), signal.size)
                                if signal.size > 0 and not executor.has_strategy_market_exposure("time_decay", market_id):
                                    click.echo(
                                        f"TIME DECAY: {market['slug']} {signal.outcome} {signal.action} {signal.size}@{signal.price:.4f} ({signal.reason})"
                                    )
                                    result = await executor.execute_signal_trade(market, orderbook, signal, strategy_family="time_decay")
                                    _emit_events(runtime, result.get("events", []), run_id=run_id)

                        if "toxicity_mm" in active_strategies:
                            quote_yes, _, quality = mm.generate_quotes(market_id, orderbook)
                            if not quote_yes:
                                executor.note_toxic_book_skip("toxicity_mm")
                                runtime.append_event("quote.skipped", {
                                    "market_id": market_id,
                                    "market_slug": market["slug"],
                                    "strategy_family": "toxicity_mm",
                                    "reasons": list(quality.reasons),
                                })
                            elif executor.has_strategy_market_exposure("toxicity_mm", market_id):
                                runtime.append_event("quote.skipped", {
                                    "market_id": market_id,
                                    "market_slug": market["slug"],
                                    "strategy_family": "toxicity_mm",
                                    "reasons": ["existing_market_exposure"],
                                })
                            else:
                                await executor.cancel_family_market(market_id, "toxicity_mm")
                                bid_id = await executor.place_order(
                                    market_id,
                                    quote_yes.outcome,
                                    "BUY",
                                    quote_yes.bid_size,
                                    quote_yes.bid_price,
                                    strategy_family="toxicity_mm",
                                    order_kind="quote",
                                    market=market,
                                )
                                ask_id = await executor.place_order(
                                    market_id,
                                    quote_yes.outcome,
                                    "SELL",
                                    quote_yes.ask_size,
                                    quote_yes.ask_price,
                                    strategy_family="toxicity_mm",
                                    order_kind="quote",
                                    market=market,
                                )
                                runtime.append_event("quote.submitted", {
                                    "market_id": market_id,
                                    "market_slug": market["slug"],
                                    "strategy_family": "toxicity_mm",
                                    "bid_order_id": bid_id,
                                    "ask_order_id": ask_id,
                                    "reason": quote_yes.reason,
                                    "book_quality": quote_yes.book_quality,
                                })

                    _emit_events(runtime, [{**fill, "event_type": "order.filled"} for fill in fill_events], run_id=run_id)
                    settlement_events = await executor.process_pending_resolutions(now_ts=time.time())
                    _emit_events(runtime, settlement_events, run_id=run_id)

                    positions = await executor.refresh_positions()
                    strategy_metrics = executor.get_family_metrics()
                    runtime.write_strategy_metrics(strategy_metrics)

                    snapshot_ts = time.time()
                    executor_snapshot = executor.get_runtime_snapshot(now_ts=snapshot_ts, orderbooks_by_market=orderbooks_by_market)
                    risk_report = risk_mgr.get_risk_report(
                        executor_snapshot=executor_snapshot,
                        ledger_events=executor.get_ledger_events(),
                        now_ts=snapshot_ts,
                    )
                    executor.record_risk_snapshot(risk_report, snapshot_ts=snapshot_ts)
                    runtime.write_runtime_snapshot(
                        run_id=run_id,
                        phase="running",
                        mode=mode,
                        loop_count=loop_count,
                        fetched_markets=len(all_markets),
                        processed_markets=processed_markets,
                        toxic_skips=toxic_skips,
                        bankroll=round(risk_report["capital"], 6),
                        open_position_count=executor_snapshot["open_position_count"],
                        resolved_trade_count=executor_snapshot["resolved_trade_count"],
                        win_rate=round(executor_snapshot["win_rate"], 6),
                        active_slots=_active_slot_summary(all_markets),
                        pending_resolution_slots=executor_snapshot["pending_resolution_slots"],
                        latest_settlement=executor_snapshot["latest_settlement"],
                        positions=positions,
                        risk=risk_report,
                        **strategy_governance,
                    )
                    click.echo(
                        "Paper bankroll ${capital:.2f} | open={open} | resolved={resolved} | pending={pending}".format(
                            capital=risk_report["capital"],
                            open=executor_snapshot["open_position_count"],
                            resolved=executor_snapshot["resolved_trade_count"],
                            pending=len(executor_snapshot["pending_resolution_slots"]),
                        )
                    )

                    if risk_mgr.check_circuit_breakers(risk_report):
                        stop_reason = "circuit_breaker"
                        runtime.append_event("runtime.circuit_breaker", {"run_id": run_id, "risk": risk_report})
                        runtime.update_status(
                            run_id=run_id,
                            phase="stopping",
                            mode=mode,
                            loop_count=loop_count,
                            stop_reason=stop_reason,
                            risk=risk_report,
                        )
                        evidence_manifest = runtime.preserve_run_evidence(trigger=stop_reason, run_id=run_id)
                        stop_snapshot_dir = str(evidence_manifest["snapshot_dir"])
                        runtime.append_event(
                            "runtime.evidence_preserved",
                            {"run_id": run_id, "trigger": stop_reason, "snapshot_dir": stop_snapshot_dir},
                        )
                        click.echo(f"Preserved stop evidence at {stop_snapshot_dir}")
                        click.echo("CIRCUIT BREAKER TRIGGERED — stopping trading")
                        break

                    if max_loops and loop_count >= max_loops:
                        stop_reason = "max_loops"
                        runtime.append_event("runtime.max_loops_reached", {"run_id": run_id, "max_loops": max_loops})
                        break

                    await asyncio.sleep(sleep_seconds)
            except KeyboardInterrupt:
                stop_reason = "keyboard_interrupt"
                click.echo("Shutting down...")
                runtime.append_event("runtime.interrupted", {"run_id": run_id})
            finally:
                runtime.update_status(run_id=run_id, phase="stopped", mode=mode, loop_count=loop_count, stop_reason=stop_reason)
                runtime.append_event(
                    "runtime.stopped",
                    {"run_id": run_id, "loop_count": loop_count, "stop_reason": stop_reason, "snapshot_dir": stop_snapshot_dir},
                )
                await executor.__aexit__(None, None, None)
                _stop_context["reason"] = stop_reason  # AC-2: surface stop_reason to outer scope

    _stop_context: dict = {"reason": "completed"}
    asyncio.run(main_loop())

    # AC-2: circuit breaker must exit with code 2 so systemd RestartPreventExitStatus=2 suppresses restart
    if _stop_context["reason"] == "circuit_breaker":
        click.echo("Exiting with code 2 (circuit_breaker) — service will not auto-restart")
        sys.exit(2)


@cli.command()
@click.option("--data", default="data/sample_backtest.csv", help="Path to historical order book data CSV")
def backtest(data):
    """Run backtest on historical data."""
    from backtest_engine import Backtester

    cfg = load_cfg()
    bt = Backtester(cfg, initial_capital=float(cfg.get("execution", {}).get("paper_starting_bankroll", 500.0)))
    click.echo(f"Loading data from {data}")
    df = bt.load_historical_orderbooks(data)
    if df.empty:
        click.echo("No data. Exiting.")
        return

    market_ids = df["market_id"].unique()[:5]
    results = []
    for mid in market_ids:
        result = bt.simulate_mean_reversion(df, mid)
        if result.total_trades > 0:
            results.append(result)
            click.echo(f"{mid}: {result.total_trades} trades, WR {result.win_rate:.1%}, PnL ${result.total_pnl:.2f}")

    if results:
        total_pnl = sum(r.total_pnl for r in results)
        total_trades = sum(r.total_trades for r in results)
        wins = sum(int(r.win_rate * r.total_trades) for r in results)
        overall_wr = wins / total_trades if total_trades > 0 else 0
        click.echo(f"AGGREGATE: {total_trades} trades, WR {overall_wr:.1%}, Total PnL ${total_pnl:.2f}")
    else:
        click.echo("No trades generated. Adjust parameters or data?")


@cli.command()
def paper():
    """Run paper trading simulation (alias for run --mode=paper)."""
    ctx = click.get_current_context()
    ctx.invoke(run, mode="paper")


@cli.command()
def live():
    """Run live trading (currently disabled pending paper validation)."""
    ctx = click.get_current_context()
    ctx.invoke(run, mode="live")


@cli.command()
def collect():
    """Collect real-time order book data for later backtesting."""
    import pandas as pd

    from market_data import PolymarketData

    cfg = load_cfg()
    click.echo("Starting data collection. Press Ctrl+C to stop and save.")

    async def collect_loop():
        async with PolymarketData(cfg) as md:
            records = []
            try:
                while True:
                    markets = await md.get_markets_by_duration(minutes=max(md.intervals))
                    orderbooks = await asyncio.gather(*[md.get_orderbook(market) for market in markets], return_exceptions=True)
                    for market, orderbook in zip(markets, orderbooks):
                        if isinstance(orderbook, Exception):
                            continue
                        for outcome in market["outcomes"]:
                            records.append({
                                "timestamp": time.time(),
                                "market_id": market["id"],
                                "market_slug": market["slug"],
                                "slot_id": market["slot_id"],
                                "asset": market.get("asset"),
                                "interval_minutes": market.get("interval_minutes"),
                                "outcome": outcome,
                                "best_bid": md.best_bid(orderbook, outcome),
                                "best_ask": md.best_ask(orderbook, outcome),
                                "mid_price": md.mid_price(orderbook, outcome),
                                "imbalance": md.calculate_imbalance(orderbook, outcome),
                                "volume": float(market.get("volume", 0)),
                            })
                    click.echo(f"Collected {len(records)} snapshots")
                    await asyncio.sleep(30)
            except KeyboardInterrupt:
                pass
            finally:
                fname = Path("data") / f"collection_{int(time.time())}.csv"
                pd.DataFrame(records).to_csv(fname, index=False)
                click.echo(f"Saved {len(records)} records to {fname}")

    asyncio.run(collect_loop())


@cli.command()
@click.option("--runtime-dir", default="data/runtime", help="Directory containing live runtime artifacts")
def status(runtime_dir):
    """Render the current replay-backed runtime status."""
    click.echo(render_status_text(runtime_dir))


@cli.command(name="health")
@click.option("--runtime-dir", default="data/runtime", help="Directory containing live runtime artifacts")
@click.option("--max-heartbeat-age", default=180, help="Max acceptable heartbeat age in seconds")
def health_cmd(runtime_dir, max_heartbeat_age):
    """Check runtime health from durable heartbeat artifacts."""
    payload = runtime_health_payload(runtime_dir, max_heartbeat_age=max_heartbeat_age)
    click.echo(render_status_text(runtime_dir))
    click.echo(f"Healthy: {payload['healthy']} (threshold={max_heartbeat_age}s)")
    if not payload["healthy"]:
        raise SystemExit(1)


@cli.command(name="evidence")
@click.option("--runtime-dir", default="data/runtime", help="Directory containing runtime artifacts")
@click.option("--strategy-family", default="toxicity_mm", help="Strategy family to summarize")
@click.option("--event-limit", default=5000, help="Max events to inspect for current run")
@click.option("--sample-limit", default=5000, help="Max market samples to inspect for current run")
def evidence_cmd(runtime_dir, strategy_family, event_limit, sample_limit):
    """Render a baseline evidence packet from runtime artifacts and ledger history."""
    payload = build_baseline_evidence(
        runtime_dir,
        strategy_family=strategy_family,
        event_limit=event_limit,
        sample_limit=sample_limit,
    )
    click.echo(render_baseline_evidence_text(payload))


@cli.command()
@click.option("--runtime-dir", default="data/runtime", help="Directory containing live runtime artifacts")
@click.option("--artifact-dir", default="data/research", help="Directory to write research outputs")
@click.option("--sample-limit", default=200, help="How many market samples to analyze")
def research(runtime_dir, artifact_dir, sample_limit):
    """Run autoresearch on live runtime artifacts, scoped to the current run by default."""
    telemetry = RuntimeTelemetry(runtime_dir)
    adapter = PolymarketRuntimeResearchAdapter(
        runtime_dir,
        sample_limit=sample_limit,
        run_id=telemetry.current_run_id(),
    )
    loop = ResearchLoop(artifact_dir)
    result = loop.run_cycle(adapter)
    click.echo(result.summary)
    for insight in result.insights:
        click.echo(f"- {insight.title}: {insight.recommendation} [{insight.confidence:.0%}]")
    click.echo(f"Artifacts written to {artifact_dir}/{result.cycle_id}.json and .md")


if __name__ == "__main__":
    cli()
