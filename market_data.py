"""
Polymarket market-data layer.
Supports legacy direct API access plus official `polymarket` CLI-backed market discovery.
"""

import aiohttp
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import redis
from dataclasses import dataclass
import logging

from market_universe import OfficialPolymarketCliAdapter

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
        self.config = config
        self.clob_url = config["polymarket"]["clob_api_url"]
        self.gamma_url = config["polymarket"]["gamma_api_url"]
        self.market_source = config.get("market_source", "official_cli")
        self.universe_cfg = dict(config.get("market_universe", {}) or {})
        self.session = None
        self.redis = redis_client or redis.Redis(host='localhost', port=6379, db=0)
        self.markets_cache: Dict[str, Dict] = {}
        self.orderbooks = {}
        self.running = False
        self.cli_adapter = OfficialPolymarketCliAdapter(self.universe_cfg)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def get_markets_for_universe(self) -> List[Dict]:
        if self.market_source == "official_cli":
            markets = await self._get_markets_official_cli()
        else:
            markets = await self._get_markets_gamma_fallback()
        self.markets_cache = {market.get("id"): market for market in markets if market.get("id")}
        return markets

    async def _get_markets_official_cli(self) -> List[Dict]:
        self.cli_adapter.config = dict(self.universe_cfg)
        return await __import__("asyncio").to_thread(self.cli_adapter.fetch_markets)

    async def _get_markets_gamma_fallback(self) -> List[Dict]:
        minutes = int(self.universe_cfg.get("max_minutes", 15))
        now = datetime.utcnow()
        end_by = now + timedelta(minutes=minutes)
        params = {
            "end_date_min": now.isoformat() + "Z",
            "end_date_max": end_by.isoformat() + "Z",
            "sort": "volume",
            "order": "desc",
            "limit": int(self.universe_cfg.get("limit", 100)),
        }
        async with self.session.get(f"{self.gamma_url}/markets", params=params) as resp:
            data = await resp.json()
            return data.get("markets", [])

    async def get_markets_by_duration(self, minutes: int) -> List[Dict]:
        """Backward-compatible API used by older code paths."""
        if self.market_source == "official_cli":
            original = dict(self.universe_cfg)
            try:
                self.universe_cfg["mode"] = "short_horizon_crypto"
                self.universe_cfg["max_minutes"] = minutes
                return await self.get_markets_for_universe()
            finally:
                self.universe_cfg = original
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
        if self.market_source == "official_cli":
            market = self.markets_cache.get(market_id)
            if market is None:
                raise ValueError(f"Unknown market_id {market_id}; market cache is empty or stale")
            data = await __import__("asyncio").to_thread(self.cli_adapter.fetch_orderbook, market, outcome)
            ob = OrderBook(
                market_id=market_id,
                yes_asks=data.get("yes_asks", []),
                yes_bids=data.get("yes_bids", []),
                no_asks=data.get("no_asks", []),
                no_bids=data.get("no_bids", []),
                timestamp=data.get("timestamp", time.time()),
                sequence=data.get("sequence", 0),
            )
            self.orderbooks[market_id] = ob
            return ob

        url = f"{self.clob_url}/book?market_id={market_id}"
        async with self.session.get(url) as resp:
            data = await resp.json()

        yes_asks = [(float(p), float(s)) for p, s in data.get("yes_asks", [])]
        yes_bids = [(float(p), float(s)) for p, s in data.get("yes_bids", [])]
        no_asks = [(float(p), float(s)) for p, s in data.get("no_asks", [])]
        no_bids = [(float(p), float(s)) for p, s in data.get("no_bids", [])]

        ob = OrderBook(
            market_id=market_id,
            yes_asks=yes_asks,
            yes_bids=yes_bids,
            no_asks=no_asks,
            no_bids=no_bids,
            timestamp=time.time(),
            sequence=data.get("sequence", 0),
        )
        self.orderbooks[market_id] = ob
        return ob

    @staticmethod
    def calculate_imbalance(ob: OrderBook, outcome: str = "YES") -> float:
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

    @staticmethod
    def mid_price(ob: OrderBook, outcome: str = "YES") -> float:
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
            sub_msg = {"type": "subscribe", "markets": market_ids}
            await ws.send_json(sub_msg)
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await callback(data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error("WebSocket error")
                    break

    async def collect_historical_prices(self, market_id: str, duration: int = 24 * 60):
        prices = []
        return pd.DataFrame(prices, columns=['timestamp', 'price', 'volume'])


def load_config(config_path: str) -> dict:
    import yaml
    with open(config_path) as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    cfg = load_config("config.yaml")
    print("Market Data Module loaded. Use as library.")