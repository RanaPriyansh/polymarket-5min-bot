"""
Polymarket Data Collector for 5/15-minute markets.
Connects to CLOB and Gamma APIs, maintains order book snapshots.
"""

from __future__ import annotations

import asyncio
import aiohttp
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

try:
    import redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None

logger = logging.getLogger(__name__)


@dataclass
class OrderBook:
    market_id: str
    yes_asks: List[Tuple[float, float]]
    yes_bids: List[Tuple[float, float]]
    no_asks: List[Tuple[float, float]]
    no_bids: List[Tuple[float, float]]
    timestamp: float
    sequence: int


class PolymarketData:
    def __init__(self, config: dict, redis_client=None):
        self.clob_url = config["polymarket"]["clob_api_url"]
        self.gamma_url = config["polymarket"]["gamma_api_url"]
        self.session = None
        if redis_client is not None:
            self.redis = redis_client
        elif redis is not None:
            try:
                self.redis = redis.Redis(host="localhost", port=6379, db=0)
            except Exception:
                self.redis = None
        else:
            self.redis = None
        self.markets_cache = {}
        self.orderbooks = {}
        self.running = False

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    async def get_markets_by_duration(self, minutes: int) -> List[Dict]:
        now = datetime.utcnow()
        end_by = now + timedelta(minutes=minutes)
        params = {
            "end_date_min": now.isoformat() + "Z",
            "end_date_max": end_by.isoformat() + "Z",
            "sort": "volume",
            "order": "desc",
            "limit": 100,
        }
        async with self.session.get(f"{self.gamma_url}/markets", params=params) as resp:
            data = await resp.json()
            return data.get("markets", [])

    async def get_orderbook(self, market_id: str, outcome: str = "YES") -> OrderBook:
        url = f"{self.clob_url}/book?market_id={market_id}"
        async with self.session.get(url) as resp:
            data = await resp.json()

        ob = OrderBook(
            market_id=market_id,
            yes_asks=[(float(p), float(s)) for p, s in data.get("yes_asks", [])],
            yes_bids=[(float(p), float(s)) for p, s in data.get("yes_bids", [])],
            no_asks=[(float(p), float(s)) for p, s in data.get("no_asks", [])],
            no_bids=[(float(p), float(s)) for p, s in data.get("no_bids", [])],
            timestamp=time.time(),
            sequence=data.get("sequence", 0),
        )
        self.orderbooks[market_id] = ob
        return ob

    def calculate_imbalance(self, ob: OrderBook, outcome: str = "YES") -> float:
        if outcome == "YES":
            bids = sum(size for _, size in ob.yes_bids[:10])
            asks = sum(size for _, size in ob.yes_asks[:10])
        else:
            bids = sum(size for _, size in ob.no_bids[:10])
            asks = sum(size for _, size in ob.no_asks[:10])
        total = bids + asks
        if total == 0:
            return 0.0
        return (bids - asks) / total

    def mid_price(self, ob: OrderBook, outcome: str = "YES") -> float:
        if outcome == "YES":
            best_bid = ob.yes_bids[0][0] if ob.yes_bids else 0.0
            best_ask = ob.yes_asks[0][0] if ob.yes_asks else 0.0
        else:
            best_bid = ob.no_bids[0][0] if ob.no_bids else 0.0
            best_ask = ob.no_asks[0][0] if ob.no_asks else 0.0
        if best_bid and best_ask:
            return (best_bid + best_ask) / 2
        return 0.0

    async def subscribe_orderbook_stream(self, market_ids: List[str], callback):
        ws_url = self.clob_url.replace("https", "wss") + "/ws"
        async with self.session.ws_connect(ws_url) as ws:
            await ws.send_json({"type": "subscribe", "markets": market_ids})
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await callback(json.loads(msg.data))
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error("WebSocket error")
                    break

    async def collect_historical_prices(self, market_id: str, duration: int = 24 * 60):
        try:
            import pandas as pd  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("pandas required for historical collection") from exc
        prices = []
        return pd.DataFrame(prices, columns=["timestamp", "price", "volume"])


def load_config(config_path: str) -> dict:
    import yaml
    with open(config_path) as fh:
        return yaml.safe_load(fh)


if __name__ == "__main__":
    cfg = load_config("config.yaml")
    print("Market Data Module loaded. Use as library.")
