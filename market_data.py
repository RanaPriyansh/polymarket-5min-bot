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
    token_id: str = ""
    condition_id: str = ""
    best_bid: float = 0.0
    best_ask: float = 0.0
    spread: float = 0.0
    tick_size: Optional[float] = None
    min_order_size: Optional[float] = None
    hash: str = ""
    neg_risk: Optional[bool] = None
    fees_enabled: Optional[bool] = None
    fee_schedule: Dict[str, Any] = field(default_factory=dict)
    last_trade_price: Optional[float] = None
    last_trade_size: float = 0.0
    book_age_ms: float = 0.0
    ws_lag_ms: float = 0.0
    market_metadata_incomplete: bool = False
    token_book_metadata: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class PolymarketData:
    def __init__(self, config: dict, redis_client=None):
        polymarket_cfg = config["polymarket"]
        self.clob_api_version = str(polymarket_cfg.get("clob_api_version", "v1")).lower()
        self.clob_environment = str(polymarket_cfg.get("clob_environment", "production")).lower()
        self.clob_urls = dict(polymarket_cfg.get("clob_urls", {}) or {})
        self.clob_url = self._resolve_clob_url(polymarket_cfg)
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

    def _resolve_clob_url(self, polymarket_cfg: Dict[str, Any]) -> str:
        if self.clob_environment in self.clob_urls:
            return str(self.clob_urls[self.clob_environment]).rstrip("/")
        if self.clob_api_version == "v2" and polymarket_cfg.get("clob_v2_read_only_url"):
            return str(polymarket_cfg["clob_v2_read_only_url"]).rstrip("/")
        return str(polymarket_cfg.get("clob_api_url", "https://clob.polymarket.com")).rstrip("/")

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

    async def _fetch_json(self, url: str, max_retries: int = 3) -> Tuple[Any, aiohttp.typedefs.LooseHeaders]:
        """Fetch JSON with DNS/connection retry resilience.
        Transient network errors don't crash the bot -- they log and retry."""
        if not self.session:
            raise RuntimeError("PolymarketData session not initialized")

        import socket
        last_exc: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                async with self.session.get(url) as resp:
                    payload = await resp.json(content_type=None)
                    if resp.status >= 400:
                        raise RuntimeError(f"Polymarket request failed ({resp.status}) for {url}: {payload}")
                    return payload, resp.headers
            except (socket.gaierror, OSError, ConnectionError) as exc:
                last_exc = exc
                logger.warning("Network error fetching %s (attempt %d/%d): %s", url, attempt + 1, max_retries, exc)
                await asyncio.sleep(2 ** attempt)
        raise last_exc  # type: ignore[misc]

    async def _fetch_public(self, path: str, max_retries: int = 3) -> Tuple[Any, aiohttp.typedefs.LooseHeaders]:
        if not path.startswith("/"):
            path = f"/{path}"
        if not self.session:
            raise RuntimeError("PolymarketData session not initialized")
        url = f"{self.clob_url}{path}"
        import socket

        last_exc: Optional[Exception] = None
        for attempt in range(max_retries):
            try:
                async with self.session.get(url) as resp:
                    text = await resp.text()
                    if resp.status >= 400:
                        raise RuntimeError(f"Polymarket request failed ({resp.status}) for {url}: {text}")
                    try:
                        return json.loads(text), resp.headers
                    except json.JSONDecodeError:
                        return text.strip(), resp.headers
            except (socket.gaierror, OSError, ConnectionError) as exc:
                last_exc = exc
                logger.warning("Network error fetching %s (attempt %d/%d): %s", url, attempt + 1, max_retries, exc)
                await asyncio.sleep(2 ** attempt)
        raise last_exc  # type: ignore[misc]

    async def clob_read_only_check(self) -> Dict[str, Any]:
        """Read-only CLOB smoke check for V2 cutover readiness.

        This intentionally touches only public endpoints.
        """
        ok_payload, _ = await self._fetch_public("/ok")
        version_payload, _ = await self._fetch_public("/version")
        time_payload, _ = await self._fetch_public("/time")
        server_time = self._normalize_server_time(time_payload)
        return {
            "clob_api_version": self.clob_api_version,
            "clob_environment": self.clob_environment,
            "clob_url": self.clob_url,
            "ok": ok_payload,
            "version": version_payload,
            "server_time": server_time,
            "clock_drift_seconds": abs(time.time() - server_time) if server_time else None,
        }

    @staticmethod
    def _normalize_server_time(payload: Any) -> float:
        if isinstance(payload, (int, float)):
            return float(payload)
        if isinstance(payload, str):
            try:
                return float(payload)
            except ValueError:
                return 0.0
        if isinstance(payload, dict):
            for key in ("time", "server_time", "timestamp"):
                if key in payload:
                    return PolymarketData._normalize_server_time(payload[key])
        return 0.0

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

    @staticmethod
    def _optional_float(value: Any) -> Optional[float]:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _book_metadata(cls, payload: Dict[str, Any], token_id: str) -> Dict[str, Any]:
        fee_schedule = payload.get("fee_schedule") or payload.get("fees") or {}
        if not isinstance(fee_schedule, dict):
            fee_schedule = {"raw": fee_schedule}
        return {
            "token_id": str(payload.get("asset_id") or payload.get("token_id") or token_id),
            "condition_id": str(payload.get("condition_id") or payload.get("market") or ""),
            "timestamp": cls._optional_float(payload.get("timestamp")),
            "hash": str(payload.get("hash") or ""),
            "min_order_size": cls._optional_float(payload.get("min_order_size")),
            "tick_size": cls._optional_float(payload.get("tick_size")),
            "neg_risk": payload.get("neg_risk"),
            "fees_enabled": payload.get("fees_enabled"),
            "fee_schedule": fee_schedule,
            "last_trade_price": cls._optional_float(payload.get("last_trade_price")),
            "last_trade_size": cls._optional_float(payload.get("last_trade_size") or payload.get("last_trade_qty") or payload.get("last_trade_quantity")),
        }

    @staticmethod
    def _metadata_incomplete(*metadata_items: Dict[str, Any]) -> bool:
        for metadata in metadata_items:
            if metadata.get("tick_size") is None or metadata.get("min_order_size") is None:
                return True
        return False

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
        first_token_id = str(market_data["tokens"][0]["token_id"])
        second_token_id = str(market_data["tokens"][1]["token_id"])
        first_metadata = self._book_metadata(first_book, first_token_id)
        second_metadata = self._book_metadata(second_book, second_token_id)
        timestamp_ms = float(first_book.get("timestamp") or second_book.get("timestamp") or (time.time() * 1000))
        yes_bids = self._normalize_levels(first_book.get("bids", []), reverse=True)
        yes_asks = self._normalize_levels(first_book.get("asks", []), reverse=False)
        no_bids = self._normalize_levels(second_book.get("bids", []), reverse=True)
        no_asks = self._normalize_levels(second_book.get("asks", []), reverse=False)
        best_bid = yes_bids[0][0] if yes_bids else 0.0
        best_ask = yes_asks[0][0] if yes_asks else 0.0
        timestamp_s = timestamp_ms / 1000.0
        tick_size = first_metadata.get("tick_size")
        min_order_size = first_metadata.get("min_order_size")
        ob = OrderBook(
            market_id=market_data["id"],
            yes_asks=yes_asks,
            yes_bids=yes_bids,
            no_asks=no_asks,
            no_bids=no_bids,
            timestamp=timestamp_s,
            sequence=int(timestamp_ms),
            outcome_labels=(first_outcome, second_outcome),
            market_slug=market_data["slug"],
            slot_id=market_data["slot_id"],
            end_ts=float(market_data["end_ts"]),
            token_ids=market_data["token_ids"],
            token_id=first_metadata["token_id"],
            condition_id=str(market_data.get("condition_id") or first_metadata.get("condition_id") or ""),
            best_bid=best_bid,
            best_ask=best_ask,
            spread=max(best_ask - best_bid, 0.0) if best_bid and best_ask else 0.0,
            tick_size=tick_size,
            min_order_size=min_order_size,
            hash=str(first_metadata.get("hash") or ""),
            neg_risk=first_metadata.get("neg_risk"),
            fees_enabled=first_metadata.get("fees_enabled"),
            fee_schedule=dict(first_metadata.get("fee_schedule") or {}),
            last_trade_price=first_metadata.get("last_trade_price"),
            last_trade_size=float(first_metadata.get("last_trade_size") or 0.0),
            book_age_ms=max(0.0, (time.time() - timestamp_s) * 1000.0),
            market_metadata_incomplete=self._metadata_incomplete(first_metadata, second_metadata),
            token_book_metadata={
                first_token_id: first_metadata,
                second_token_id: second_metadata,
            },
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
