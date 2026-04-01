"""
Polymarket data access for strict 5m/15m crypto interval markets.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp

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
    outcome_labels: Tuple[str, str] = ("YES", "NO")
    market_slug: str = ""
    slot_id: str = ""
    end_ts: float = 0.0
    token_ids: Dict[str, str] = field(default_factory=dict)


class PolymarketData:
    def __init__(self, config: dict, redis_client=None):
        polymarket_cfg = config["polymarket"]
        self.clob_url = polymarket_cfg["clob_api_url"]
        self.gamma_url = polymarket_cfg["gamma_api_url"]
        self.assets = list(polymarket_cfg.get("assets", ["btc", "eth", "sol", "xrp"]))
        self.intervals = [int(value) for value in polymarket_cfg.get("intervals", [5, 15])]
        self.fallback_windows = int(polymarket_cfg.get("fallback_windows", 1))
        self.session: aiohttp.ClientSession | None = None
        self.headers = {
            "Accept": "application/json",
            "User-Agent": "polymarket-5min-bot/restore",
        }
        if redis_client is not None:
            self.redis = redis_client
        elif redis is not None:
            try:
                self.redis = redis.Redis(host="localhost", port=6379, db=0)
            except Exception:
                self.redis = None
        else:
            self.redis = None
        self.markets_cache: Dict[str, Dict] = {}
        self.market_index_by_id: Dict[str, Dict] = {}
        self.orderbooks: Dict[str, OrderBook] = {}

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @staticmethod
    def slot_start(ts: float, interval_minutes: int) -> int:
        interval_seconds = interval_minutes * 60
        return int(ts) // interval_seconds * interval_seconds

    @staticmethod
    def build_slug(asset: str, interval_minutes: int, slot_start_ts: int) -> str:
        return f"{asset}-updown-{interval_minutes}m-{slot_start_ts}"

    @staticmethod
    def parse_slug(slug: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
        parts = slug.split("-")
        if len(parts) < 4 or parts[1] != "updown":
            return None, None, None
        asset = parts[0]
        interval_text = parts[2]
        try:
            interval_minutes = int(interval_text[:-1]) if interval_text.endswith("m") else None
            slot_start_ts = int(parts[3])
        except ValueError:
            return asset, None, None
        return asset, interval_minutes, slot_start_ts

    @staticmethod
    def _decode_list(value: Any) -> List[Any]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
                if isinstance(decoded, list):
                    return decoded
            except json.JSONDecodeError:
                return []
        return []

    @staticmethod
    def _parse_end_ts(end_date: str) -> float:
        return datetime.fromisoformat(end_date.replace("Z", "+00:00")).timestamp()

    @staticmethod
    def _levels_for_outcome(ob: OrderBook, outcome: str = "YES") -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        normalized = (outcome or "").strip().upper()
        first_label = ob.outcome_labels[0].upper()
        second_label = ob.outcome_labels[1].upper()
        if normalized in {"YES", first_label}:
            return ob.yes_bids, ob.yes_asks
        if normalized in {"NO", second_label}:
            return ob.no_bids, ob.no_asks
        if outcome == ob.outcome_labels[0]:
            return ob.yes_bids, ob.yes_asks
        if outcome == ob.outcome_labels[1]:
            return ob.no_bids, ob.no_asks
        raise KeyError(f"Outcome {outcome!r} is not present in order book {ob.outcome_labels}")

    @staticmethod
    def calculate_imbalance(ob: OrderBook, outcome: str = "YES") -> float:
        bids, asks = PolymarketData._levels_for_outcome(ob, outcome)
        bid_depth = sum(size for _, size in bids[:10])
        ask_depth = sum(size for _, size in asks[:10])
        total = bid_depth + ask_depth
        if total <= 0:
            return 0.0
        return (bid_depth - ask_depth) / total

    @staticmethod
    def best_bid(ob: OrderBook, outcome: str = "YES") -> float:
        bids, _ = PolymarketData._levels_for_outcome(ob, outcome)
        return bids[0][0] if bids else 0.0

    @staticmethod
    def best_ask(ob: OrderBook, outcome: str = "YES") -> float:
        _, asks = PolymarketData._levels_for_outcome(ob, outcome)
        return asks[0][0] if asks else 0.0

    @staticmethod
    def mid_price(ob: OrderBook, outcome: str = "YES") -> float:
        best_bid = PolymarketData.best_bid(ob, outcome)
        best_ask = PolymarketData.best_ask(ob, outcome)
        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2.0
        return 0.0

    @staticmethod
    def get_winning_outcome(market: Dict) -> Optional[str]:
        prices = market.get("outcome_prices", [])
        outcomes = market.get("outcomes", [])
        for outcome, price in zip(outcomes, prices):
            if abs(float(price) - 1.0) <= 1e-9:
                return outcome
        return None

    async def _fetch_json(self, url: str) -> Tuple[Any, aiohttp.typedefs.LooseHeaders]:
        if not self.session:
            raise RuntimeError("PolymarketData session not initialized")
        async with self.session.get(url) as resp:
            payload = await resp.json(content_type=None)
            if resp.status >= 400:
                raise RuntimeError(f"Polymarket request failed ({resp.status}) for {url}: {payload}")
            return payload, resp.headers

    def _normalize_market_payload(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        slug = raw.get("slug") or raw.get("ticker")
        if not slug:
            raise RuntimeError("Polymarket API changed: market payload missing slug")

        condition_id = raw.get("conditionId") or raw.get("condition_id")
        outcomes = [str(item) for item in self._decode_list(raw.get("outcomes") or raw.get("outcome_labels"))]
        outcome_prices = [float(item) for item in self._decode_list(raw.get("outcomePrices") or raw.get("outcome_prices"))]
        tokens = raw.get("tokens") or []
        if tokens:
            normalized_tokens = [
                {
                    "outcome": str(token["outcome"]),
                    "token_id": str(token["token_id"]),
                    "price": float(token.get("price", 0.0)),
                }
                for token in tokens
                if token.get("token_id") is not None and token.get("outcome") is not None
            ]
        else:
            token_ids = [str(item) for item in self._decode_list(raw.get("clobTokenIds") or raw.get("clob_token_ids"))]
            normalized_tokens = []
            for index, token_id in enumerate(token_ids):
                outcome = outcomes[index] if index < len(outcomes) else f"Outcome{index + 1}"
                price = outcome_prices[index] if index < len(outcome_prices) else 0.0
                normalized_tokens.append({"outcome": outcome, "token_id": token_id, "price": price})

        accepting_orders_value = raw.get("acceptingOrders")
        if accepting_orders_value is None:
            accepting_orders_value = raw.get("accepting_orders")
        enable_order_book_value = raw.get("enableOrderBook")
        if enable_order_book_value is None:
            enable_order_book_value = raw.get("enable_order_book")

        if not outcomes:
            outcomes = [token["outcome"] for token in normalized_tokens]

        if len(outcomes) < 2 or len(normalized_tokens) < 2:
            raise RuntimeError("Polymarket API changed: expected exactly two outcomes with token ids")

        end_date = raw.get("endDate") or raw.get("end_date")
        if not end_date:
            raise RuntimeError("Polymarket API changed: market payload missing endDate")

        asset, interval_minutes, slot_start_ts = self.parse_slug(slug)
        market = {
            "id": str(raw.get("id") or condition_id or slug),
            "condition_id": str(condition_id or ""),
            "slug": slug,
            "question": raw.get("question") or raw.get("title") or slug,
            "end_date": end_date,
            "end_ts": self._parse_end_ts(end_date),
            "active": bool(raw.get("active", False)),
            "closed": bool(raw.get("closed", False)),
            "accepting_orders": bool(accepting_orders_value if accepting_orders_value is not None else not raw.get("closed", False)),
            "enable_order_book": bool(enable_order_book_value if enable_order_book_value is not None else True),
            "volume": float(raw.get("volume") or raw.get("volumeNum") or 0.0),
            "liquidity": float(raw.get("liquidity") or raw.get("liquidityNum") or 0.0),
            "outcomes": outcomes[:2],
            "outcome_prices": outcome_prices[:2] if outcome_prices else [float(token.get("price", 0.0)) for token in normalized_tokens[:2]],
            "tokens": normalized_tokens[:2],
            "token_ids": {token["outcome"]: token["token_id"] for token in normalized_tokens[:2]},
            "asset": asset,
            "interval_minutes": interval_minutes,
            "slot_start_ts": slot_start_ts,
            "slot_id": f"{asset}:{interval_minutes}:{slot_start_ts}" if asset and interval_minutes and slot_start_ts else str(raw.get("id") or slug),
        }
        return market

    async def get_market_by_slug(self, slug: str, *, use_cache: bool = False) -> Dict[str, Any]:
        if use_cache and slug in self.markets_cache:
            return self.markets_cache[slug]
        payload, _ = await self._fetch_json(f"{self.gamma_url}/markets/slug/{slug}")
        market = self._normalize_market_payload(payload)
        self.markets_cache[slug] = market
        self.market_index_by_id[market["id"]] = market
        return market

    async def smoke_check(self) -> Dict[str, Any]:
        now_ts = time.time()
        slug = self.build_slug("btc", 5, self.slot_start(now_ts, 5))
        payload, headers = await self._fetch_json(f"{self.gamma_url}/markets/slug/{slug}")
        market = self._normalize_market_payload(payload)
        if not market["condition_id"]:
            raise RuntimeError("Polymarket API changed: normalized market missing condition_id")
        api_date = headers.get("Date")
        if not api_date:
            raise RuntimeError("Polymarket API changed: missing Date header for clock drift check")
        api_now = parsedate_to_datetime(api_date).astimezone(UTC).timestamp()
        drift_seconds = abs(now_ts - api_now)
        return {
            "slug": slug,
            "market": market,
            "clock_drift_seconds": drift_seconds,
            "clock_header": api_date,
        }

    async def discover_current_markets(self, max_minutes: Optional[int] = None) -> List[Dict[str, Any]]:
        discovered: List[Dict[str, Any]] = []
        now_ts = time.time()
        allowed_intervals = [interval for interval in self.intervals if max_minutes is None or interval <= max_minutes]
        for interval_minutes in sorted(allowed_intervals):
            slot_start_ts = self.slot_start(now_ts, interval_minutes)
            for asset in self.assets:
                chosen_market: Optional[Dict[str, Any]] = None
                for fallback_index in range(self.fallback_windows + 1):
                    candidate_start = slot_start_ts - (fallback_index * interval_minutes * 60)
                    slug = self.build_slug(asset, interval_minutes, candidate_start)
                    try:
                        market = await self.get_market_by_slug(slug)
                    except Exception as exc:
                        logger.debug("Failed to fetch %s: %s", slug, exc)
                        continue
                    if market["active"] and not market["closed"] and market["accepting_orders"] and market["enable_order_book"]:
                        chosen_market = market
                        break
                if chosen_market:
                    discovered.append(chosen_market)
        discovered.sort(key=lambda item: (int(item.get("interval_minutes") or 0), str(item.get("asset") or ""), item["slug"]))
        return discovered

    async def get_markets_by_duration(self, minutes: int) -> List[Dict]:
        return await self.discover_current_markets(max_minutes=minutes)

    async def _fetch_token_book(self, token_id: str) -> Dict[str, Any]:
        payload, _ = await self._fetch_json(f"{self.clob_url}/book?token_id={token_id}")
        return payload

    @staticmethod
    def _normalize_levels(levels: Iterable[Dict[str, Any]], *, reverse: bool) -> List[Tuple[float, float]]:
        pairs = [
            (float(level["price"]), float(level["size"]))
            for level in levels
            if level.get("price") is not None and level.get("size") is not None
        ]
        return sorted(pairs, key=lambda item: item[0], reverse=reverse)

    async def get_orderbook(self, market: Dict | str, outcome: str = "YES") -> OrderBook:
        if isinstance(market, str):
            market_data = self.market_index_by_id.get(market) or self.markets_cache.get(market)
            if not market_data:
                raise KeyError(f"Unknown market {market!r}")
        else:
            market_data = market
            self.markets_cache[market_data["slug"]] = market_data
            self.market_index_by_id[market_data["id"]] = market_data

        books = await asyncio.gather(
            *[self._fetch_token_book(token["token_id"]) for token in market_data["tokens"]],
        )
        first_outcome, second_outcome = market_data["outcomes"][:2]
        first_book, second_book = books[:2]
        timestamp_ms = float(first_book.get("timestamp") or second_book.get("timestamp") or (time.time() * 1000))
        ob = OrderBook(
            market_id=market_data["id"],
            yes_asks=self._normalize_levels(first_book.get("asks", []), reverse=False),
            yes_bids=self._normalize_levels(first_book.get("bids", []), reverse=True),
            no_asks=self._normalize_levels(second_book.get("asks", []), reverse=False),
            no_bids=self._normalize_levels(second_book.get("bids", []), reverse=True),
            timestamp=timestamp_ms / 1000.0,
            sequence=int(timestamp_ms),
            outcome_labels=(first_outcome, second_outcome),
            market_slug=market_data["slug"],
            slot_id=market_data["slot_id"],
            end_ts=float(market_data["end_ts"]),
            token_ids=market_data["token_ids"],
        )
        self.orderbooks[market_data["id"]] = ob
        return ob

    async def subscribe_orderbook_stream(self, asset_ids: List[str], callback):
        if not self.session:
            raise RuntimeError("PolymarketData session not initialized")
        ws_url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        async with self.session.ws_connect(ws_url) as ws:
            await ws.send_json({
                "type": "market",
                "assets_ids": asset_ids,
                "custom_feature_enabled": True,
            })
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

    with open(config_path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


if __name__ == "__main__":
    cfg = load_config("config.yaml")
    print("Market Data Module loaded. Use as library.")
