#!/usr/bin/env python3
"""
Polymarket 5/15-minute bot CLI.
Commands: run, backtest, paper, live, collect
"""

import asyncio
import logging
import time
from datetime import datetime
from pathlib import Path

import click
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s: %(message)s'
)


def merge_unique_markets(*market_lists):
    merged = {}
    for market_list in market_lists:
        for market in market_list:
            market_id = market.get("id")
            if market_id and market_id not in merged:
                merged[market_id] = market
    return list(merged.values())


def parse_market_end_ts(market: dict):
    end_fields = ["end_date_iso", "endDate", "end_date", "end_time"]
    for field in end_fields:
        value = market.get(field)
        if not value:
            continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
    return None


def classify_runtime_regime(market: dict, spread_bps: float, imbalance: float, realized_vol: float):
    end_ts = parse_market_end_ts(market)
    if end_ts is not None:
        seconds_to_end = end_ts - time.time()
        if seconds_to_end <= 60:
            return "terminal"
        if seconds_to_end <= 180:
            return "closing"
    if spread_bps > 300:
        return "stressed"
    if realized_vol > 0.12:
        return "volatile"
    if abs(imbalance) > 0.6:
        return "one_sided"
    return "calm"


def seconds_to_resolution(market: dict):
    end_ts = parse_market_end_ts(market)
    if end_ts is None:
        return None
    return end_ts - time.time()


REPO_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = REPO_ROOT / "config.yaml"


@click.group()
def cli():
    """Polymarket 5/15-minute trading bot."""
    pass


@cli.command()
@click.option('--mode', type=click.Choice(['backtest', 'paper', 'live']), default='paper', help='Execution mode')
@click.option('--strategies', default='mean_reversion_5min,shock_reversion,dislocation_arb,toxicity_mm', help='Comma-separated active strategies')
def run(mode, strategies):
    """Run the trading bot in specified mode."""
    from event_recorder import EventRecorder
    from execution import create_broker
    from market_data import PolymarketData
    from resolver_map import ResolverMap
    from risk import RiskManager
    from strategies.dislocation_arb import ComplementaryDislocationStrategy
    from strategies.mean_reversion_5min import MeanReversion5Min
    from strategies.shock_reversion import ShockReversionStrategy
    from strategies.terminal_resolver import TerminalResolverStrategy
    from strategies.toxicity_mm import ToxicityMM

    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    cfg["strategies"]["active"] = strategies.split(",")

    click.echo(f"Starting bot in {mode} mode with strategies: {strategies}")
    click.echo("Press Ctrl+C to stop.")

    async def main_loop():
        async with PolymarketData(cfg) as md:
            mean_rev = MeanReversion5Min(cfg)
            shock_rev = ShockReversionStrategy(cfg)
            dislocation = ComplementaryDislocationStrategy(cfg)
            terminal_resolver = TerminalResolverStrategy(cfg)
            mm = ToxicityMM(cfg)
            risk_mgr = RiskManager(cfg, initial_capital=1000.0)
            recorder = EventRecorder(REPO_ROOT / 'data' / 'market_events.csv')
            resolver_map = ResolverMap(REPO_ROOT / 'data' / 'resolver_map.json')

            executor = None
            if mode in ('paper', 'live'):
                executor = create_broker(mode, cfg, md)
                await executor.__aenter__()

            try:
                while True:
                    markets_5m = await md.get_markets_by_duration(minutes=5)
                    markets_15m = await md.get_markets_by_duration(minutes=15)
                    all_markets = merge_unique_markets(markets_5m, markets_15m)
                    click.echo(f"[{datetime.utcnow()}] Fetched {len(all_markets)} markets (5m+15m)")

                    for market in all_markets:
                        market_id = market["id"]
                        market_order_submitted = False
                        resolver_info = resolver_map.upsert_market(market)
                        ob = await md.get_orderbook(market_id, "YES")
                        mid = md.mid_price(ob, "YES")
                        imbalance = md.calculate_imbalance(ob, "YES")

                        mean_rev.update_price(market_id, mid, time.time(), float(market.get("volume", 0)))
                        shock_rev.update_price(market_id, mid, time.time(), float(market.get("volume", 0)))

                        realized_vol = mean_rev.estimate_realized_volatility(market_id)
                        spread_bps = mean_rev.spread_bps(ob, "YES")
                        regime = classify_runtime_regime(market, spread_bps, imbalance, realized_vol)
                        secs_to_res = seconds_to_resolution(market)
                        base_volume = float(market.get("volume", 0))

                        if regime == "stressed":
                            recorder.record(
                                ob,
                                base_volume,
                                regime,
                                resolver_info=resolver_info,
                                seconds_to_resolution=secs_to_res if secs_to_res is not None else -1.0,
                            )
                            click.echo(f"SKIP[{regime}]: {market_id} spread={spread_bps:.1f}bps")
                            continue

                        recorder.record(
                            ob,
                            base_volume,
                            regime,
                            resolver_info=resolver_info,
                            seconds_to_resolution=secs_to_res if secs_to_res is not None else -1.0,
                        )

                        if "mean_reversion_5min" in cfg["strategies"]["active"]:
                            signal = mean_rev.generate_signal(
                                market_id,
                                "YES",
                                mid,
                                ob,
                                base_volume,
                                risk_manager=risk_mgr,
                            )
                            if signal:
                                recorder.record(
                                    ob,
                                    base_volume,
                                    regime,
                                    resolver_info=resolver_info,
                                    seconds_to_resolution=secs_to_res if secs_to_res is not None else -1.0,
                                    active_signal_family="mean_reversion_5min",
                                )
                                click.echo(
                                    f"SIGNAL[{regime}]: {market_id} {signal.outcome} {signal.action} "
                                    f"{signal.size}@{signal.price} ({signal.reason})"
                                )
                                if executor:
                                    await executor.place_order(
                                        market_id, signal.outcome, signal.action, signal.size, signal.price
                                    )
                                    if mode == 'paper' and hasattr(executor, 'process_orderbook'):
                                        await executor.process_orderbook(market_id, ob)
                                    market_order_submitted = True

                        if (not market_order_submitted and "shock_reversion" in cfg["strategies"]["active"]
                                and regime in {"volatile", "closing", "calm"}):
                            shock_signal = shock_rev.generate_signal(
                                market_id,
                                "YES",
                                mid,
                                ob,
                                base_volume,
                                risk_manager=risk_mgr,
                            )
                            if shock_signal:
                                recorder.record(
                                    ob,
                                    base_volume,
                                    regime,
                                    resolver_info=resolver_info,
                                    seconds_to_resolution=secs_to_res if secs_to_res is not None else -1.0,
                                    active_signal_family="shock_reversion",
                                )
                                click.echo(
                                    f"SHOCK[{shock_signal.regime}]: {market_id} {shock_signal.outcome} {shock_signal.action} "
                                    f"{shock_signal.size}@{shock_signal.price} ({shock_signal.reason})"
                                )
                                if executor:
                                    await executor.place_order(
                                        market_id,
                                        shock_signal.outcome,
                                        shock_signal.action,
                                        shock_signal.size,
                                        shock_signal.price,
                                    )
                                    if mode == 'paper' and hasattr(executor, 'process_orderbook'):
                                        await executor.process_orderbook(market_id, ob)
                                    market_order_submitted = True

                        if (not market_order_submitted and "dislocation_arb" in cfg["strategies"]["active"]
                                and regime in {"calm", "closing", "volatile"}):
                            dislocation_signal = dislocation.generate_signal(
                                market_id,
                                ob,
                                base_volume,
                                risk_manager=risk_mgr,
                            )
                            if dislocation_signal:
                                recorder.record(
                                    ob,
                                    base_volume,
                                    regime,
                                    resolver_info=resolver_info,
                                    seconds_to_resolution=secs_to_res if secs_to_res is not None else -1.0,
                                    active_signal_family="dislocation_arb",
                                )
                                click.echo(
                                    f"DISLOCATION[{regime}]: {market_id} {dislocation_signal.outcome} {dislocation_signal.action} "
                                    f"{dislocation_signal.size}@{dislocation_signal.price} ({dislocation_signal.reason})"
                                )
                                if executor:
                                    await executor.place_order(
                                        market_id,
                                        dislocation_signal.outcome,
                                        dislocation_signal.action,
                                        dislocation_signal.size,
                                        dislocation_signal.price,
                                    )
                                    if mode == 'paper' and hasattr(executor, 'process_orderbook'):
                                        await executor.process_orderbook(market_id, ob)
                                    market_order_submitted = True

                        if (not market_order_submitted and "terminal_resolver" in cfg["strategies"]["active"]
                                and regime in {"terminal", "closing"}):
                            resolver_signal = terminal_resolver.generate_signal(
                                market_id,
                                market,
                                ob,
                                base_volume,
                                resolver_info=resolver_info,
                                seconds_to_resolution=secs_to_res,
                                risk_manager=risk_mgr,
                            )
                            if resolver_signal:
                                recorder.record(
                                    ob,
                                    base_volume,
                                    regime,
                                    resolver_info=resolver_info,
                                    seconds_to_resolution=secs_to_res if secs_to_res is not None else -1.0,
                                    active_signal_family="terminal_resolver",
                                )
                                click.echo(
                                    f"RESOLVER[{regime}]: {market_id} {resolver_signal.outcome} {resolver_signal.action} "
                                    f"{resolver_signal.size}@{resolver_signal.price} ({resolver_signal.reason})"
                                )
                                if executor:
                                    await executor.place_order(
                                        market_id,
                                        resolver_signal.outcome,
                                        resolver_signal.action,
                                        resolver_signal.size,
                                        resolver_signal.price,
                                    )
                                    if mode == 'paper' and hasattr(executor, 'process_orderbook'):
                                        await executor.process_orderbook(market_id, ob)
                                    market_order_submitted = True

                        if "toxicity_mm" in cfg["strategies"]["active"] and regime in {"calm", "closing"}:
                            quote_yes, quote_no = mm.generate_quotes(market_id, ob)
                            if quote_yes:
                                click.echo(
                                    f"MM[{regime}]: {quote_yes.outcome} bid {quote_yes.bid_price} ask {quote_yes.ask_price} "
                                    f"size {quote_yes.bid_size}"
                                )

                    if executor:
                        positions = await executor.refresh_positions()
                        if isinstance(positions, dict) and "equity" in positions:
                            risk_mgr.sync_equity(float(positions["equity"]))
                        risk_report = risk_mgr.get_risk_report()
                        click.echo(f"Risk: Capital ${risk_report['capital']:.2f}, DD {risk_report['max_drawdown']:.2%}")
                        click.echo(f"Broker summary: {positions}")

                    if risk_mgr.check_circuit_breakers():
                        click.echo("CIRCUIT BREAKER TRIGGERED — stopping trading")
                        break

                    await asyncio.sleep(60)

            except KeyboardInterrupt:
                click.echo("Shutting down...")
            finally:
                if executor:
                    await executor.__aexit__(None, None, None)

    asyncio.run(main_loop())


@cli.command()
@click.option(
    '--data',
    default=str(REPO_ROOT / 'data' / 'sample_backtest.csv'),
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help='Path to historical order book data CSV',
)
def backtest(data):
    """Run backtest on historical data."""
    from backtest_engine import Backtester

    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    bt = Backtester(cfg, initial_capital=1000.0)
    click.echo(f"Loading data from {data}")
    try:
        df = bt.load_historical_orderbooks(str(data))
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    if df.empty:
        click.echo("No data. Exiting.")
        return

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
    import pandas as pd

    from market_data import PolymarketData

    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    click.echo("Starting data collection. Press Ctrl+C to stop and save.")

    async def collect_loop():
        async with PolymarketData(cfg) as md:
            records = []
            try:
                while True:
                    markets = merge_unique_markets(
                        await md.get_markets_by_duration(minutes=5),
                        await md.get_markets_by_duration(minutes=15),
                    )
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
                            "volume": float(m.get("volume", 0)),
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
