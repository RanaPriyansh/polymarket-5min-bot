#!/usr/bin/env python3
"""
Polymarket 5/15-minute bot CLI.
Commands: run, backtest, paper, live, collect, research
"""

from __future__ import annotations

import asyncio
import click
import yaml
import logging
import time
from datetime import datetime
from pathlib import Path

from runtime_telemetry import RuntimeTelemetry
from research.loop import ResearchLoop
from research.polymarket import PolymarketRuntimeResearchAdapter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s: %(message)s",
)


@click.group()
def cli():
    """Polymarket 5/15-minute trading bot."""
    pass


def load_cfg() -> dict:
    with open("config.yaml", "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@cli.command()
@click.option("--mode", type=click.Choice(["backtest", "paper", "live"]), default="paper", help="Execution mode")
@click.option("--strategies", default="mean_reversion_5min,toxicity_mm", help="Comma-separated active strategies")
@click.option("--max-loops", type=int, default=0, help="Bounded loop count for smoke tests")
@click.option("--runtime-dir", default="data/runtime", help="Directory for durable runtime telemetry")
@click.option("--sleep-seconds", type=int, default=60, help="Loop sleep duration")
def run(mode, strategies, max_loops, runtime_dir, sleep_seconds):
    """Run the trading bot in specified mode."""
    from book_quality import assess_book_quality
    from market_data import PolymarketData
    from strategies.mean_reversion_5min import MeanReversion5Min
    from strategies.toxicity_mm import ToxicityMM
    from execution import PolymarketExecutor
    from risk import RiskManager

    cfg = load_cfg()
    active_strategies = [item.strip() for item in strategies.split(",") if item.strip()]
    cfg["strategies"]["active"] = active_strategies
    filters = cfg.get("filters", {})

    click.echo(f"Starting bot in {mode} mode with strategies: {','.join(active_strategies)}")
    click.echo("Press Ctrl+C to stop.")

    async def main_loop():
        runtime = RuntimeTelemetry(runtime_dir)
        run_id = runtime.make_run_id(mode)
        runtime.append_event("runtime.started", {"run_id": run_id, "mode": mode, "strategies": active_strategies})
        runtime.update_status(run_id=run_id, phase="starting", mode=mode, strategies=active_strategies, loop_count=0)

        async with PolymarketData(cfg) as md:
            mean_rev = MeanReversion5Min(cfg)
            mm = ToxicityMM(cfg)
            risk_mgr = RiskManager(cfg, initial_capital=1000.0)
            executor = None
            last_realized_total = 0.0

            if mode in ("paper", "live"):
                executor = PolymarketExecutor(cfg, md, mode=mode)
                await executor.__aenter__()

            loop_count = 0
            try:
                while True:
                    loop_count += 1
                    all_markets = await md.get_markets_by_duration(minutes=15)
                    click.echo(f"[{datetime.utcnow()}] Fetched {len(all_markets)} markets (within 15m)")
                    market_ids = [market["id"] for market in all_markets]
                    order_book_tasks = [md.get_orderbook(market_id, "YES") for market_id in market_ids]
                    order_books = await asyncio.gather(*order_book_tasks, return_exceptions=True)

                    processed_markets = 0
                    toxic_skips = 0
                    fill_events = []

                    for idx, market in enumerate(all_markets):
                        market_id = market["id"]
                        ob_or_exc = order_books[idx]
                        if isinstance(ob_or_exc, Exception):
                            click.echo(f"Error fetching order book for {market_id}: {ob_or_exc}")
                            runtime.append_event("market.fetch_error", {"market_id": market_id, "error": str(ob_or_exc)})
                            continue

                        orderbook = ob_or_exc
                        book_quality = assess_book_quality(
                            orderbook,
                            "YES",
                            max_spread_bps=filters.get("max_book_spread_bps", 250),
                            min_top_depth=filters.get("min_top_depth", 25),
                            min_top_notional=filters.get("min_top_notional", 10),
                            max_depth_ratio=filters.get("max_depth_ratio", 12),
                        )
                        runtime.append_market_sample({
                            "run_id": run_id,
                            "market_id": market_id,
                            "volume": float(market.get("volume", 0)),
                            "book_spread_bps": round(book_quality.spread_bps, 3),
                            "book_depth": round(book_quality.top_depth, 3),
                            "book_notional": round(book_quality.top_notional, 3),
                            "book_reasons": list(book_quality.reasons),
                            "is_tradeable": book_quality.is_tradeable,
                        })

                        if executor:
                            fill_events.extend(executor.evaluate_market_orders(market_id, orderbook))

                        mid = md.mid_price(orderbook, "YES")
                        mean_rev.update_price(market_id, mid, time.time(), float(market.get("volume", 0)))

                        if "mean_reversion_5min" in active_strategies and executor:
                            executor.note_market_seen("mean_reversion_5min")
                        if "toxicity_mm" in active_strategies and executor:
                            executor.note_market_seen("toxicity_mm")

                        if not book_quality.is_tradeable:
                            toxic_skips += 1
                            if executor:
                                if "mean_reversion_5min" in active_strategies:
                                    executor.note_toxic_book_skip("mean_reversion_5min")
                                if "toxicity_mm" in active_strategies:
                                    executor.note_toxic_book_skip("toxicity_mm")
                            runtime.append_event("market.skipped_toxic_book", {
                                "market_id": market_id,
                                "reasons": list(book_quality.reasons),
                                "spread_bps": book_quality.spread_bps,
                                "top_depth": book_quality.top_depth,
                            })
                            continue

                        processed_markets += 1
                        volume = float(market.get("volume", 0))

                        if "mean_reversion_5min" in active_strategies:
                            signal = mean_rev.generate_signal(market_id, "YES", mid, orderbook, volume)
                            if signal:
                                click.echo(
                                    f"SIGNAL: {market_id} {signal.outcome} {signal.action} {signal.size}@{signal.price} ({signal.reason})"
                                )
                                if executor:
                                    order_id = await executor.place_order(
                                        market_id,
                                        signal.outcome,
                                        signal.action,
                                        signal.size,
                                        signal.price,
                                        strategy_family="mean_reversion_5min",
                                        order_kind="signal",
                                    )
                                    runtime.append_event("order.submitted", {
                                        "market_id": market_id,
                                        "order_id": order_id,
                                        "strategy_family": "mean_reversion_5min",
                                        "side": signal.action,
                                        "price": signal.price,
                                        "size": signal.size,
                                        "reason": signal.reason,
                                    })

                        if "toxicity_mm" in active_strategies and executor:
                            quote_yes, quote_no, quality = mm.generate_quotes(market_id, orderbook)
                            if not quote_yes:
                                executor.note_toxic_book_skip("toxicity_mm")
                                runtime.append_event("quote.skipped", {
                                    "market_id": market_id,
                                    "strategy_family": "toxicity_mm",
                                    "reasons": list(quality.reasons),
                                })
                            else:
                                await executor.cancel_family_market(market_id, "toxicity_mm")
                                click.echo(
                                    f"MM QUOTE: {quote_yes.outcome} bid {quote_yes.bid_price} ask {quote_yes.ask_price} size {quote_yes.bid_size}"
                                )
                                bid_id = await executor.place_order(
                                    market_id,
                                    quote_yes.outcome,
                                    "BUY",
                                    quote_yes.bid_size,
                                    quote_yes.bid_price,
                                    strategy_family="toxicity_mm",
                                    order_kind="quote",
                                )
                                ask_id = await executor.place_order(
                                    market_id,
                                    quote_yes.outcome,
                                    "SELL",
                                    quote_yes.ask_size,
                                    quote_yes.ask_price,
                                    strategy_family="toxicity_mm",
                                    order_kind="quote",
                                )
                                runtime.append_event("quote.submitted", {
                                    "market_id": market_id,
                                    "strategy_family": "toxicity_mm",
                                    "bid_order_id": bid_id,
                                    "ask_order_id": ask_id,
                                    "reason": quote_yes.reason,
                                    "book_quality": quote_yes.book_quality,
                                })

                    if executor:
                        for fill in fill_events:
                            runtime.append_event("order.filled", fill)
                        realized_total = executor.get_realized_pnl_total()
                        realized_delta = realized_total - last_realized_total
                        if realized_delta:
                            risk_mgr.update_capital(realized_delta)
                        last_realized_total = realized_total
                        positions = await executor.refresh_positions()
                        strategy_metrics = executor.get_family_metrics()
                    else:
                        positions = {}
                        strategy_metrics = {}

                    runtime.write_strategy_metrics(strategy_metrics)
                    risk_report = risk_mgr.get_risk_report()
                    runtime.update_status(
                        run_id=run_id,
                        phase="running",
                        mode=mode,
                        loop_count=loop_count,
                        fetched_markets=len(all_markets),
                        processed_markets=processed_markets,
                        toxic_skips=toxic_skips,
                        risk=risk_report,
                        positions=positions,
                        strategy_metrics=strategy_metrics,
                    )
                    click.echo(
                        f"Risk: Capital ${risk_report['capital']:.2f}, DD {risk_report['max_drawdown']:.2%}, toxic_skips={toxic_skips}"
                    )

                    if risk_mgr.check_circuit_breakers():
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
                if executor:
                    await executor.__aexit__(None, None, None)

    asyncio.run(main_loop())


@cli.command()
@click.option("--data", default="data/sample_backtest.csv", help="Path to historical order book data CSV")
def backtest(data):
    """Run backtest on historical data."""
    from backtest_engine import Backtester

    cfg = load_cfg()
    bt = Backtester(cfg, initial_capital=1000.0)
    click.echo(f"Loading data from {data}")
    df = bt.load_historical_orderbooks(data)
    if df.empty:
        click.echo("No data. Exiting.")
        return

    market_ids = df["market_id"].unique()[:5]
    results = []
    for mid in market_ids:
        result = bt.simulate_mean_reversion(df, mid, outcome="YES")
        if result.total_trades > 0:
            results.append(result)
            click.echo(f"{mid}-YES: {result.total_trades} trades, WR {result.win_rate:.1%}, PnL ${result.total_pnl:.2f}")

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
    """Run live trading (requires wallet configured)."""
    ctx = click.get_current_context()
    ctx.invoke(run, mode="live")


@cli.command()
def collect():
    """Collect real-time order book data for later backtesting."""
    import pandas as pd

    from book_quality import assess_book_quality
    from market_data import PolymarketData

    cfg = load_cfg()
    click.echo("Starting data collection. Press Ctrl+C to stop and save.")

    async def collect_loop():
        async with PolymarketData(cfg) as md:
            records = []
            try:
                while True:
                    markets = await md.get_markets_by_duration(minutes=15)
                    for market in markets:
                        market_id = market["id"]
                        orderbook = await md.get_orderbook(market_id, "YES")
                        quality = assess_book_quality(
                            orderbook,
                            "YES",
                            max_spread_bps=cfg.get("filters", {}).get("max_book_spread_bps", 250),
                            min_top_depth=cfg.get("filters", {}).get("min_top_depth", 25),
                            min_top_notional=cfg.get("filters", {}).get("min_top_notional", 10),
                            max_depth_ratio=cfg.get("filters", {}).get("max_depth_ratio", 12),
                        )
                        records.append({
                            "timestamp": datetime.utcnow(),
                            "market_id": market_id,
                            "outcome": "YES",
                            "best_bid": orderbook.yes_bids[0][0] if orderbook.yes_bids else 0,
                            "best_ask": orderbook.yes_asks[0][0] if orderbook.yes_asks else 0,
                            "bid_size": orderbook.yes_bids[0][1] if orderbook.yes_bids else 0,
                            "ask_size": orderbook.yes_asks[0][1] if orderbook.yes_asks else 0,
                            "mid_price": md.mid_price(orderbook, "YES"),
                            "volume": float(market.get("volume", 0)),
                            "book_spread_bps": quality.spread_bps,
                            "book_depth": quality.top_depth,
                            "book_notional": quality.top_notional,
                            "book_reasons": "|".join(quality.reasons),
                        })
                    click.echo(f"[{datetime.utcnow()}] Collected {len(records)} snapshots")
                    await asyncio.sleep(30)
            except KeyboardInterrupt:
                Path("data").mkdir(exist_ok=True)
                df = pd.DataFrame(records)
                fname = f"data/collection_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(fname, index=False)
                click.echo(f"Saved {len(df)} records to {fname}")

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
