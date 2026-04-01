#!/usr/bin/env python3
"""
Collector for strict 5m/15m crypto interval markets.
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import click
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from market_data import PolymarketData


async def main():
    cfg = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    records = []

    async with PolymarketData(cfg) as md:
        click.echo("Collector started. Ctrl+C to stop and save.")
        while True:
            try:
                markets = await md.get_markets_by_duration(minutes=max(md.intervals))
                orderbooks = await asyncio.gather(*[md.get_orderbook(market) for market in markets], return_exceptions=True)
                click.echo(f"Tracking {len(markets)} active interval markets")

                for market, orderbook in zip(markets, orderbooks):
                    if isinstance(orderbook, Exception):
                        click.echo(f"Error on {market['slug']}: {orderbook}")
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

                if records and len(records) % 50 == 0:
                    pd.DataFrame(records).to_csv(ROOT / "data" / "collection_latest.csv", index=False)
                await asyncio.sleep(30)
            except KeyboardInterrupt:
                break
            except Exception as exc:
                click.echo(f"Loop error: {exc}")
                await asyncio.sleep(10)

    timestamp = int(time.time())
    fname = ROOT / "data" / f"collection_{timestamp}.csv"
    pd.DataFrame(records).to_csv(fname, index=False)
    click.echo(f"Saved {len(records)} order book snapshots to {fname}")


if __name__ == "__main__":
    asyncio.run(main())
