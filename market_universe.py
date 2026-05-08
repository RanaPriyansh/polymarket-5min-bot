from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence


UNIVERSE_MODES = {
    "strict_crypto_5m_15m",
    "short_horizon_crypto",
    "all_liquid",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_jsonish_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _normalize_iso(value: Any) -> str | None:
    if not value:
        return None
    text = str(value)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return text


def normalize_cli_market(raw: Dict[str, Any]) -> Dict[str, Any]:
    outcomes = _parse_jsonish_list(raw.get("outcomes"))
    token_ids = _parse_jsonish_list(raw.get("clobTokenIds"))
    outcome_prices = _parse_jsonish_list(raw.get("outcomePrices"))
    tags = _parse_jsonish_list(raw.get("tags")) if isinstance(raw.get("tags"), str) else (raw.get("tags") or [])

    yes_idx = 0
    no_idx = 1 if len(token_ids) > 1 else 0
    yes_price = None
    if outcome_prices:
        yes_price = _safe_float(outcome_prices[0], default=0.0)
    best_bid = _safe_float(raw.get("bestBid"), default=0.0)
    best_ask = _safe_float(raw.get("bestAsk"), default=0.0)
    last_trade = _safe_float(raw.get("lastTradePrice"), default=0.0)
    midpoint = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 0.0
    price = last_trade or yes_price or midpoint

    market = dict(raw)
    market.update(
        {
            "id": str(raw.get("id")),
            "conditionId": raw.get("conditionId") or raw.get("condition_id"),
            "question": raw.get("question") or raw.get("title") or raw.get("slug") or "",
            "description": raw.get("description") or "",
            "endDate": _normalize_iso(raw.get("endDate") or raw.get("end_date_iso")),
            "price": price,
            "yes_price": price,
            "bestBid": best_bid,
            "bestAsk": best_ask,
            "last_trade_price": last_trade or price,
            "volume": _safe_float(raw.get("volumeNum") or raw.get("volume"), default=0.0),
            "liquidity": _safe_float(raw.get("liquidityNum") or raw.get("liquidity"), default=0.0),
            "tags": tags,
            "outcomes": outcomes,
            "clobTokenIds": token_ids,
            "yes_token_id": token_ids[yes_idx] if len(token_ids) > yes_idx else None,
            "no_token_id": token_ids[no_idx] if len(token_ids) > no_idx else None,
            "market_source": "official_cli",
            "acceptingOrders": bool(raw.get("acceptingOrders", True)),
            "active": bool(raw.get("active", True)),
            "closed": bool(raw.get("closed", False)),
        }
    )
    return market


def market_minutes_to_end(market: Dict[str, Any]) -> float | None:
    end_date = market.get("endDate") or market.get("end_date_iso")
    if not end_date:
        return None
    try:
        dt = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (dt - datetime.now(timezone.utc)).total_seconds() / 60.0
    except ValueError:
        return None


def market_is_crypto(market: Dict[str, Any]) -> bool:
    text_parts = [
        str(market.get("question") or ""),
        str(market.get("description") or ""),
        str(market.get("slug") or ""),
        " ".join(str(tag) for tag in (market.get("tags") or [])),
    ]
    haystack = " ".join(text_parts).lower()
    keywords = {
        "crypto", "bitcoin", "btc", "ethereum", "eth", "solana", "sol", "xrp",
        "doge", "polygon", "base", "airdrop", "altcoin", "memecoin",
    }
    return any(keyword in haystack for keyword in keywords)


def select_universe_markets(markets: Sequence[Dict[str, Any]], config: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    cfg = dict(config or {})
    mode = cfg.get("mode", "all_liquid")
    if mode not in UNIVERSE_MODES:
        raise ValueError(f"Unsupported universe mode: {mode}")

    min_liquidity = float(cfg.get("min_liquidity", 1000.0))
    max_minutes = float(cfg.get("max_minutes", 180.0))
    limit = int(cfg.get("limit", 100))

    selected: List[Dict[str, Any]] = []
    for market in markets:
        mins = market_minutes_to_end(market)
        if mins is not None and mins < 0:
            continue
        liquidity = _safe_float(market.get("liquidity"), default=0.0)
        volume = _safe_float(market.get("volume"), default=0.0)
        effective_liquidity = max(liquidity, volume)
        if effective_liquidity < min_liquidity:
            continue
        if not market.get("acceptingOrders", True):
            continue
        if mode == "strict_crypto_5m_15m":
            if not market_is_crypto(market):
                continue
            if mins is None or mins > 15:
                continue
        elif mode == "short_horizon_crypto":
            if not market_is_crypto(market):
                continue
            if mins is None or mins > max_minutes:
                continue
        elif mode == "all_liquid":
            pass
        selected.append(market)

    def sort_key(market: Dict[str, Any]):
        mins = market_minutes_to_end(market)
        liquidity = _safe_float(market.get("liquidity"), default=0.0)
        volume = _safe_float(market.get("volume"), default=0.0)
        urgency = mins if mins is not None else 1e12
        return (-max(liquidity, volume), urgency)

    selected.sort(key=sort_key)
    return list(selected[:limit])


class OfficialPolymarketCliAdapter:
    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or {}

    def _run_json(self, *args: str) -> Any:
        command = ["polymarket", "-o", "json", *args]
        output = subprocess.check_output(command, text=True)
        return json.loads(output)

    def fetch_markets(self) -> List[Dict[str, Any]]:
        cfg = self.config
        raw_markets = self._run_json(
            "markets",
            "list",
            "--active",
            str(cfg.get("active", True)).lower(),
            "--closed",
            str(cfg.get("closed", False)).lower(),
            "--limit",
            str(cfg.get("limit", 200)),
            "--order",
            str(cfg.get("order", "liquidityNum")),
        )
        normalized = [normalize_cli_market(market) for market in raw_markets]
        return select_universe_markets(normalized, cfg)

    def fetch_orderbook(self, market: Dict[str, Any], outcome: str = "YES") -> Dict[str, Any]:
        yes_token_id = market.get("yes_token_id")
        no_token_id = market.get("no_token_id")
        if not yes_token_id and not no_token_id:
            raise ValueError(f"Market {market.get('id')} missing token ids")

        def parse_book(token_id: str | None):
            if not token_id:
                return [], [], 0.0
            raw_book = self._run_json("clob", "book", str(token_id))
            bids = [
                (float(level["price"]), float(level["size"]))
                for level in (raw_book.get("bids") or [])
            ]
            asks = [
                (float(level["price"]), float(level["size"]))
                for level in (raw_book.get("asks") or [])
            ]
            bids.sort(key=lambda item: item[0], reverse=True)
            asks.sort(key=lambda item: item[0])
            timestamp = _safe_float(raw_book.get("timestamp"), default=0.0) / 1000.0
            return bids, asks, timestamp

        yes_bids, yes_asks, yes_ts = parse_book(yes_token_id)
        no_bids, no_asks, no_ts = parse_book(no_token_id)
        timestamp = max(yes_ts, no_ts, 0.0)
        token_id = yes_token_id if outcome.upper() == "YES" else no_token_id
        return {
            "market_id": market.get("id"),
            "outcome": outcome.upper(),
            "token_id": token_id,
            "yes_bids": yes_bids,
            "yes_asks": yes_asks,
            "no_bids": no_bids,
            "no_asks": no_asks,
            "timestamp": timestamp,
            "sequence": 0,
        }
