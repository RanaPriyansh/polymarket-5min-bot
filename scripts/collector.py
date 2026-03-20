#!/usr/bin/env python3
"""
Dedicated collector for 5/15-minute market order books.
Saves CSV snapshots for backtesting.
"""

import asyncio
import pandas as pd
from datetime import datetime
from market_data import PolymarketData
import yaml
import sys

async def main():
    cfg = yaml.safe_load(open("../config.yaml"))
    records = []

    async with PolymarketData(cfg) as md:
        click.echo("Collector started. Ctrl+C to stop and save.")
        while True:
            try:
                markets_5m = await md.get_markets_by_duration(minutes=5)
                markets_15m = await md.get_markets_by_duration(minutes=15)
                all_markets = markets_5m + markets_15m
                click.echo(f"[{datetime.utcnow()}] Found {len(all_markets)} active markets")

                for m in all_markets:
                    market_id = m["id"]
                    try:
                        ob = await md.get_orderbook(market_id, "YES")
                        rec = {
                            "timestamp": datetime.utcnow(),
                            "market_id": market_id,
                            "outcome": "YES",
                            "best_bid": ob.yes_bids[0][0] if ob.yes_bids else 0.0,
                            "best_ask": ob.yes_asks[0][0] if ob.yes_asks else 0.0,
                            "bid_size": ob.yes_bids[0][1] if ob.yes_bids else 0.0,
                            "ask_size": ob.yes_asks[0][1] if ob.yes_asks else 0.0,
                            "mid_price": md.mid_price(ob, "YES"),
                            "volume": float(m.get("volume", 0)),
                            "liquidity": float(m.get("liquidity", 0))
                        }
                        records.append(rec)
                    except Exception as e:
                        click.echo(f"Error on {market_id}: {e}")

                # Save snapshot every 30 seconds, sleep between
                if len(records) % 100 == 0:
                    temp_df = pd.DataFrame(records)
                    temp_df.to_csv("data/collection_latest.csv", index=False)
                await asyncio.sleep(30)
            except KeyboardInterrupt:
                break
            except Exception as e:
                click.echo(f"Loop error: {e}")
                await asyncio.sleep(10)

    # Save all collected data
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"data/collection_{timestamp}.csv"
    df = pd.DataFrame(records)
    df.to_csv(fname, index=False)
    click.echo(f"Saved {len(df)} order book snapshots to {fname}")

if __name__ == "__main__":
    import click
    asyncio.run(main())