#!/usr/bin/env python3
"""
Polymarket 5/15-minute bot CLI.
Commands: run, backtest, paper, live, collect, research
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Dict, Iterable, List

import click
import yaml

from research.loop import ResearchLoop
from research.polymarket import PolymarketRuntimeResearchAdapter
from runtime_telemetry import RuntimeTelemetry

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


def _emit_events(runtime: RuntimeTelemetry, events: Iterable[Dict]) -> None:
    for event in events:
        payload = dict(event)
        event_type = payload.pop("event_type", "runtime.event")
        runtime.append_event(event_type, payload)


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


def _enforce_clock_drift(drift_seconds: float) -> None:
    if drift_seconds > 5:
        logger.warning("Clock drift is %.2fs vs Polymarket API time", drift_seconds)
    if drift_seconds > 30:
        logger.warning("Clock drift exceeds 30s; paper trading may query stale windows")
    if drift_seconds > 60:
        raise click.ClickException(f"Clock drift too large for interval markets: {drift_seconds:.2f}s")


@cli.command()
@click.option("--mode", type=click.Choice(["backtest", "paper", "live"]), default="paper", help="Execution mode")
@click.option("--strategies", default=None, help="Comma-separated active strategies (defaults to config.yaml strategies.active)")
@click.option("--max-loops", type=int, default=0, help="Bounded loop count for smoke tests")
@click.option("--runtime-dir", default="data/runtime", help="Directory for durable runtime telemetry")
@click.option("--sleep-seconds", type=int, default=15, help="Loop sleep duration")
def run(mode, strategies, max_loops, runtime_dir, sleep_seconds):
    """Run the trading bot in specified mode."""
    from book_quality import assess_book_quality
    from execution import PolymarketExecutor
    from market_data import PolymarketData
    from risk import RiskManager
    from strategies.mean_reversion_5min import MeanReversion5Min
    from strategies.toxicity_mm import ToxicityMM

    if mode == "live":
        raise click.ClickException("Live mode is deferred until the restored paper workflow is proven.")

    cfg = load_cfg()
    active_strategies = _resolve_active_strategies(cfg, strategies)
    cfg.setdefault("strategies", {})["active"] = active_strategies
    filters = cfg.get("filters", {})
    initial_capital = float(cfg.get("execution", {}).get("paper_starting_bankroll", 500.0))

    # Relax book-quality filters for paper mode so ToxicityMM can actually place quotes
    if mode == "paper":
        filters.setdefault("max_book_spread_bps", 1000)
        filters.setdefault("min_top_depth", 5)
        filters.setdefault("min_top_notional", 1)
        filters.setdefault("max_depth_ratio", 30)

    click.echo(f"Starting bot in {mode} mode with strategies: {','.join(active_strategies)}")
    click.echo("Press Ctrl+C to stop.")

    async def main_loop():
        runtime = RuntimeTelemetry(runtime_dir)
        run_id = runtime.make_run_id(mode)
        runtime.append_event("runtime.started", {"run_id": run_id, "mode": mode, "strategies": active_strategies})
        runtime.update_status(run_id=run_id, phase="starting", mode=mode, strategies=active_strategies, loop_count=0)

        async with PolymarketData(cfg) as md:
            startup = await md.smoke_check()
            _enforce_clock_drift(startup["clock_drift_seconds"])
            runtime.append_event("runtime.startup_check", startup)

            cfg.setdefault("execution", {})
            cfg["execution"].setdefault("ledger_db_path", str(Path(runtime_dir) / "ledger.db"))

            mean_rev = MeanReversion5Min(cfg)
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

                    for idx, market in enumerate(all_markets):
                        market_id = market["id"]
                        primary_outcome = market["outcomes"][0]
                        ob_or_exc = order_books[idx]
                        if isinstance(ob_or_exc, Exception):
                            click.echo(f"Error fetching order book for {market_id}: {ob_or_exc}")
                            runtime.append_event("market.fetch_error", {"market_id": market_id, "error": str(ob_or_exc)})
                            continue

                        orderbook = ob_or_exc
                        book_quality = assess_book_quality(
                            orderbook,
                            primary_outcome,
                            max_spread_bps=filters.get("max_book_spread_bps", 250),
                            min_top_depth=filters.get("min_top_depth", 25),
                            min_top_notional=filters.get("min_top_notional", 10),
                            max_depth_ratio=filters.get("max_depth_ratio", 12),
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

                        if "mean_reversion_5min" in active_strategies:
                            executor.note_market_seen("mean_reversion_5min")
                        if "toxicity_mm" in active_strategies:
                            executor.note_market_seen("toxicity_mm")

                        if not book_quality.is_tradeable:
                            toxic_skips += 1
                            if "mean_reversion_5min" in active_strategies:
                                executor.note_toxic_book_skip("mean_reversion_5min")
                            if "toxicity_mm" in active_strategies:
                                executor.note_toxic_book_skip("toxicity_mm")
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
                                    _emit_events(runtime, result.get("events", []))

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

                    _emit_events(runtime, [{**fill, "event_type": "order.filled"} for fill in fill_events])
                    settlement_events = await executor.process_pending_resolutions(now_ts=time.time())
                    _emit_events(runtime, settlement_events)

                    positions = await executor.refresh_positions()
                    strategy_metrics = executor.get_family_metrics()
                    runtime.write_strategy_metrics(strategy_metrics)

                    snapshot_ts = time.time()
                    executor_snapshot = executor.get_runtime_snapshot(now_ts=snapshot_ts)
                    risk_report = risk_mgr.get_risk_report(
                        executor_snapshot=executor_snapshot,
                        ledger_events=executor.get_ledger_events(),
                        now_ts=snapshot_ts,
                    )
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
                        strategy_metrics=strategy_metrics,
                        risk=risk_report,
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
                        runtime.append_event("runtime.circuit_breaker", {"run_id": run_id, "risk": risk_report})
                        click.echo("CIRCUIT BREAKER TRIGGERED — stopping trading")
                        break

                    if max_loops and loop_count >= max_loops:
                        runtime.append_event("runtime.max_loops_reached", {"run_id": run_id, "max_loops": max_loops})
                        break

                    await asyncio.sleep(sleep_seconds)
            except KeyboardInterrupt:
                click.echo("Shutting down...")
                runtime.append_event("runtime.interrupted", {"run_id": run_id})
            finally:
                runtime.update_status(run_id=run_id, phase="stopped", mode=mode, loop_count=loop_count)
                runtime.append_event("runtime.stopped", {"run_id": run_id, "loop_count": loop_count})
                await executor.__aexit__(None, None, None)

    asyncio.run(main_loop())


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
@click.option("--artifact-dir", default="data/research", help="Directory to write research outputs")
@click.option("--sample-limit", default=200, help="How many market samples to analyze")
def research(runtime_dir, artifact_dir, sample_limit):
    """Run autoresearch on live runtime artifacts, not just backtest CSVs."""
    adapter = PolymarketRuntimeResearchAdapter(runtime_dir, sample_limit=sample_limit)
    loop = ResearchLoop(artifact_dir)
    result = loop.run_cycle(adapter)
    click.echo(result.summary)
    for insight in result.insights:
        click.echo(f"- {insight.title}: {insight.recommendation} [{insight.confidence:.0%}]")
    click.echo(f"Artifacts written to {artifact_dir}/{result.cycle_id}.json and .md")


if __name__ == "__main__":
    cli()
