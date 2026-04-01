#!/usr/bin/env python3
"""
Collector for 5/15-minute market order books.
Saves CSV snapshots for backtesting.
Usage: cd /path/to/polymarket-5min-bot && python3 scripts/collector.py [--minutes 5 --minutes 15]
"""
import asyncio
import csv
import sys
import time
from datetime import datetime
from pathlib import Path

import click
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from market_data import PolymarketData


async def main():
    cfg_path = ROOT / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    data_dir = ROOT / "data"
    data_dir.mkdir(exist_ok=True)

    async with PolymarketData(cfg) as md:
        smoke = await md.smoke_check()
        click.echo(f"Smoke check OK: {smoke['slug']} (drift {smoke['clock_drift_seconds']:.1f}s)")

        records = []
        while True:
            try:
                markets = await md.get_markets_by_duration(minutes=max(md.intervals))
                click.echo(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Discovered {len(markets)} markets")

                orderbooks = await asyncio.gather(
                    *[md.get_orderbook(m) for m in markets],
                    return_exceptions=True,
                )

                for market, ob_or_exc in zip(markets, orderbooks):
                    if isinstance(ob_or_exc, Exception):
                        continue
                    ob = ob_or_exc
                    for outcome in market["outcomes"]:
                        bids, asks = [], []
                        if outcome.upper() in (ob.outcome_labels[0].upper(),):
                            bids, asks = ob.yes_bids, ob.yes_asks
                        else:
                            bids, asks = ob.no_bids, ob.no_asks

                        best_bid = bids[0][0] if bids else 0
                        best_ask = asks[0][0] if asks else 0
                        bid_size = bids[0][1] if bids else 0
                        ask_size = asks[0][1] if asks else 0

                        records.append({
                            "ts": time.time(),
                            "ts_iso": datetime.utcnow().isoformat(),
                            "market_id": market["id"],
                            "market_slug": market["slug"],
                            "slot_id": market["slot_id"],
                            "asset": market.get("asset"),
                            "interval": market.get("interval_minutes"),
                            "end_ts": market["end_ts"],
                            "outcome": outcome,
                            "best_bid": round(best_bid, 4),
                            "best_ask": round(best_ask, 4),
                            "bid_size": round(bid_size, 2),
                            "ask_size": round(ask_size, 2),
                            "mid": round((best_bid + best_ask) / 2, 4) if best_bid and best_ask else 0,
                            "volume": float(market.get("volume", 0)),
                            "liquidity": float(market.get("liquidity", 0)),
                            "closed": market.get("closed", False),
                        })

                if len(records) % 100 == 0 and len(records) > 0:
                    fname = data_dir / "collection_latest.csv"
                    with open(fname, "w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=records[0].keys())
                        writer.writeheader()
                        writer.writerows(records[-500:])  # Keep last 500

                click.echo(f"  Collected {len(records)} total snapshots")
                await asyncio.sleep(10)

            except KeyboardInterrupt:
                break
            except Exception as e:
                click.echo(f"  Loop error: {e}")
                await asyncio.sleep(15)

    # Final save
    fname = data_dir / f"collection_{int(time.time())}.csv"
    with open(fname, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys() if records else [])
        writer.writeheader()
        writer.writerows(records)
    click.echo(f"Saved {len(records)} snapshots to {fname}")


if __name__ == "__main__":
    asyncio.run(main())
