#!/usr/bin/env python3
"""
Polymarket 5/15-minute bot CLI
Commands: run, backtest, paper, live, collect
"""

import asyncio
import click
import yaml
import logging
from datetime import datetime
from market_data import PolymarketData
from strategies.mean_reversion_5min import MeanReversion5Min
from strategies.toxicity_mm import ToxicityMM
from execution import PolymarketExecutor
from risk import RiskManager
from backtest_engine import Backtester
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s: %(message)s'
)

@click.group()
def cli():
    """Polymarket 5/15-minute trading bot."""
    pass

@cli.command()
@click.option('--mode', type=click.Choice(['backtest', 'paper', 'live']), default='paper', help='Execution mode')
@click.option('--strategies', default='mean_reversion_5min,toxicity_mm', help='Comma-separated active strategies')
def run(mode, strategies):
    """Run the trading bot in specified mode."""
    cfg = yaml.safe_load(open("config.yaml"))
    cfg["strategies"]["active"] = strategies.split(",")

    click.echo(f"Starting bot in {mode} mode with strategies: {strategies}")
    click.echo("Press Ctrl+C to stop.")

    async def main_loop():
        async with PolymarketData(cfg) as md:
            # Initialize strategies
            mean_rev = MeanReversion5Min(cfg)
            mm = ToxicityMM(cfg)
            risk_mgr = RiskManager(cfg, initial_capital=1000.0)

            # If paper or live, wire executor
            executor = None
            if mode in ('paper', 'live'):
                executor = PolymarketExecutor(cfg, md)
                await executor.__aenter__()

            try:
                while True:
                    # Fetch all markets matching duration criteria (5m and 15m)
                    markets_5m = await md.get_markets_by_duration(minutes=5)
                    markets_15m = await md.get_markets_by_duration(minutes=15)
                    all_markets = markets_5m + markets_15m
                    click.echo(f"[{datetime.utcnow()}] Fetched {len(all_markets)} markets (5m+15m)")

                    for market in all_markets:
                        market_id = market["id"]
                        # Fetch order book for YES outcome
                        ob = await md.get_orderbook(market_id, "YES")
                        mid = md.mid_price(ob, "YES")
                        imbalance = md.calculate_imbalance(ob, "YES")

                        # Update mean reversion strategy price history
                        mean_rev.update_price(market_id, mid, time.time(), float(market.get("volume", 0)))

                        # Generate signals
                        if "mean_reversion_5min" in cfg["strategies"]["active"]:
                            signal = mean_rev.generate_signal(
                                market_id, "YES", mid, ob, float(market.get("volume", 0))
                            )
                            if signal:
                                click.echo(f"SIGNAL: {market_id} {signal.outcome} {signal.action} {signal.size}@{signal.price} ({signal.reason})")
                                # In paper/live mode, execute
                                if executor:
                                    order_id = await executor.place_order(
                                        market_id, signal.outcome, signal.action, signal.size, signal.price
                                    )
                        # Market making could run continuously (but step back if VPIN high)
                        if "toxicity_mm" in cfg["strategies"]["active"]:
                            quote_yes, quote_no = mm.generate_quotes(market_id, ob)
                            if quote_yes:
                                click.echo(f"MM QUOTE: {quote_yes.outcome} bid {quote_yes.bid_price} ask {quote_yes.ask_price} size {quote_yes.bid_size}")
                                # In paper/live: place both bid and ask orders
                                # await executor.place_order(...)

                    # Update risk and capital
                    if executor:
                        positions = await executor.refresh_positions()
                        # Approximate PnL from positions (simplified)
                        risk_report = risk_mgr.get_risk_report()
                        click.echo(f"Risk: Capital ${risk_report['capital']:.2f}, DD {risk_report['max_drawdown']:.2%}")

                    # Circuit breakers
                    if risk_mgr.check_circuit_breakers():
                        click.echo("CIRCUIT BREAKER TRIGGERED — stopping trading")
                        break

                    await asyncio.sleep(60)  # check every minute for 5/15min opportunities

            except KeyboardInterrupt:
                click.echo("Shutting down...")
                if executor:
                    await executor.__aexit__(None, None, None)

    asyncio.run(main_loop())

@cli.command()
@click.option('--data', default='data/sample_backtest.csv', help='Path to historical order book data CSV')
def backtest(data):
    """Run backtest on historical data."""
    cfg = yaml.safe_load(open("config.yaml"))
    bt = Backtester(cfg, initial_capital=1000.0)
    click.echo(f"Loading data from {data}")
    df = bt.load_historical_orderbooks(data)
    if df.empty:
        click.echo("No data. Exiting.")
        return

    # Test on a random market with sufficient data
    market_ids = df['market_id'].unique()[:5]
    results = []
    for mid in market_ids:
        for outcome in ["YES"]:
            result = bt.simulate_mean_reversion(df, mid, outcome)
            if result.total_trades > 0:
                results.append(result)
                click.echo(f"{mid}-{outcome}: {result.total_trades} trades, WR {result.win_rate:.1%}, PnL ${result.total_pnl:.2f}")

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
    ctx.invoke(run, mode='paper')

@cli.command()
def live():
    """Run live trading (requires wallet configured)."""
    ctx = click.get_current_context()
    ctx.invoke(run, mode='live')

@cli.command()
def collect():
    """Collect real-time order book data for later backtesting."""
    cfg = yaml.safe_load(open("config.yaml"))
    click.echo("Starting data collection. Press Ctrl+C to stop and save.")
    async def collect_loop():
        async with PolymarketData(cfg) as md:
            records = []
            try:
                while True:
                    markets = await md.get_markets_by_duration(minutes=15)  # 5m+15m
                    for m in markets:
                        mid = m["id"]
                        ob = await md.get_orderbook(mid, "YES")
                        rec = {
                            "timestamp": datetime.utcnow(),
                            "market_id": mid,
                            "outcome": "YES",
                            "best_bid": ob.yes_bids[0][0] if ob.yes_bids else 0,
                            "best_ask": ob.yes_asks[0][0] if ob.yes_asks else 0,
                            "bid_size": ob.yes_bids[0][1] if ob.yes_bids else 0,
                            "ask_size": ob.yes_asks[0][1] if ob.yes_asks else 0,
                            "mid_price": md.mid_price(ob, "YES"),
                            "volume": float(m.get("volume", 0))
                        }
                        records.append(rec)
                    click.echo(f"[{datetime.utcnow()}] Collected {len(records)} snapshots")
                    await asyncio.sleep(30)
            except KeyboardInterrupt:
                click.echo("Saving data to data/collection_<timestamp>.csv")
                df = pd.DataFrame(records)
                fname = f"data/collection_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
                df.to_csv(fname, index=False)
                click.echo(f"Saved {len(df)} records to {fname}")
    asyncio.run(collect_loop())

if __name__ == '__main__':
    cli()