#!/usr/bin/env python3
"""
Dedicated collector for 5/15-minute market order books.
Saves CSV snapshots for backtesting.
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path

import click
import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from market_data import PolymarketData


def merge_unique_markets(*market_lists):
    merged = {}
    for market_list in market_lists:
        for market in market_list:
            market_id = market.get("id")
            if market_id and market_id not in merged:
                merged[market_id] = market
    return list(merged.values())


async def main():
    with (REPO_ROOT / "config.yaml").open("r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle)
    records = []
    data_dir = REPO_ROOT / "data"
    data_dir.mkdir(exist_ok=True)

    async with PolymarketData(cfg) as md:
        click.echo("Collector started. Ctrl+C to stop and save.")
        while True:
            try:
                markets_5m = await md.get_markets_by_duration(minutes=5)
                markets_15m = await md.get_markets_by_duration(minutes=15)
                all_markets = merge_unique_markets(markets_5m, markets_15m)
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
                            "liquidity": float(m.get("liquidity", 0)),
                        }
                        records.append(rec)
                    except Exception as exc:
                        click.echo(f"Error on {market_id}: {exc}")

                if records and len(records) % 100 == 0:
                    pd.DataFrame(records).to_csv(data_dir / "collection_latest.csv", index=False)
                await asyncio.sleep(30)
            except KeyboardInterrupt:
                break
            except Exception as exc:
                click.echo(f"Loop error: {exc}")
                await asyncio.sleep(10)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = data_dir / f"collection_{timestamp}.csv"
    df = pd.DataFrame(records)
    df.to_csv(fname, index=False)
    click.echo(f"Saved {len(df)} order book snapshots to {fname}")


if __name__ == "__main__":
    asyncio.run(main())
