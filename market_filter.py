from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple


DEFAULT_FILTER_CONFIG = {
    "enabled": True,
    "scan_limit": 25,
    "blocked_keywords": [
        "meme", "tweet", "elon", "will elon", "viral", "tiktok",
        "celebrity", "drama", "beef", "feud",
    ],
    "preferred_keywords": [
        "bitcoin", "btc", "ethereum", "eth", "crypto",
        "fed", "interest rate", "cpi", "inflation", "gdp",
        "tennis", "football", "soccer", "basketball", "baseball", "nfl", "nba", "mlb",
        "election", "primary", "senate", "house",
    ],
    "extreme_low": 0.05,
    "extreme_high": 0.95,
    "min_volume_usd": 5_000,
    "min_days_to_resolution": 0.0,
    "max_days_to_resolution": 90.0,
    "prefer_midpoint_prices": True,
}


def _cfg(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    merged = dict(DEFAULT_FILTER_CONFIG)
    if config:
        merged.update(config)
    return merged


def market_question(market: Dict[str, Any]) -> str:
    return str(
        market.get("question")
        or market.get("title")
        or market.get("name")
        or market.get("slug")
        or ""
    )


def market_price(market: Dict[str, Any]) -> float | None:
    candidates = [
        market.get("yes_price"),
        market.get("price"),
        market.get("last_trade_price"),
        market.get("probability"),
        market.get("current_probability"),
    ]
    for value in candidates:
        try:
            if value is None or value == "":
                continue
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def market_volume(market: Dict[str, Any]) -> float:
    candidates = [
        market.get("volume"),
        market.get("volume24hr"),
        market.get("volume_24h"),
        market.get("liquidity"),
    ]
    for value in candidates:
        try:
            if value is None or value == "":
                continue
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def market_end_iso(market: Dict[str, Any]) -> str | None:
    for field in ("end_date_iso", "endDate", "end_date", "end_time"):
        value = market.get(field)
        if value:
            return str(value)
    return None


def days_to_resolution(market: Dict[str, Any]) -> float | None:
    end_value = market_end_iso(market)
    if not end_value:
        return None
    try:
        end_dt = datetime.fromisoformat(end_value.replace("Z", "+00:00"))
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (end_dt - now).total_seconds() / 86400.0
    except ValueError:
        return None


def should_trade_market(market: Dict[str, Any], config: Dict[str, Any] | None = None) -> Tuple[bool, str]:
    cfg = _cfg(config)
    question_lower = market_question(market).lower()

    for keyword in cfg["blocked_keywords"]:
        if keyword in question_lower:
            return False, f"blocked keyword '{keyword}'"

    price = market_price(market)
    if price is not None:
        if price < cfg["extreme_low"]:
            return False, f"extreme low probability {price:.1%}"
        if price > cfg["extreme_high"]:
            return False, f"extreme high probability {price:.1%}"

    volume = market_volume(market)
    if volume < cfg["min_volume_usd"]:
        return False, f"low volume ${volume:,.0f}"

    dtr = days_to_resolution(market)
    if dtr is not None:
        if dtr > cfg["max_days_to_resolution"]:
            return False, f"too far out ({dtr:.1f}d)"
        if dtr < cfg["min_days_to_resolution"]:
            return False, f"already resolving ({dtr:.2f}d)"

    if any(keyword in question_lower for keyword in cfg["preferred_keywords"]):
        return True, "preferred category"
    return True, "passes baseline filters"


def score_market(market: Dict[str, Any], config: Dict[str, Any] | None = None) -> float:
    cfg = _cfg(config)
    should_trade, _ = should_trade_market(market, cfg)
    if not should_trade:
        return 0.0

    score = 50.0
    question_lower = market_question(market).lower()
    volume = market_volume(market)
    price = market_price(market)

    if any(keyword in question_lower for keyword in cfg["preferred_keywords"]):
        score += 20.0
    if volume >= 50_000:
        score += 15.0
    elif volume >= 10_000:
        score += 10.0
    elif volume >= cfg["min_volume_usd"]:
        score += 5.0

    if cfg.get("prefer_midpoint_prices", True) and price is not None and 0.30 <= price <= 0.70:
        score += 15.0

    dtr = days_to_resolution(market)
    if dtr is not None:
        if 0 < dtr <= 7:
            score += 10.0
        elif dtr <= 30:
            score += 5.0

    return score


def rank_markets(markets: Iterable[Dict[str, Any]], config: Dict[str, Any] | None = None) -> List[Tuple[Dict[str, Any], bool, str, float]]:
    cfg = _cfg(config)
    ranked: List[Tuple[Dict[str, Any], bool, str, float]] = []
    for market in markets:
        should_trade, reason = should_trade_market(market, cfg)
        ranked.append((market, should_trade, reason, score_market(market, cfg)))
    ranked.sort(key=lambda item: item[3], reverse=True)
    return ranked


def filter_ranked_markets(markets: Iterable[Dict[str, Any]], config: Dict[str, Any] | None = None):
    cfg = _cfg(config)
    ranked = rank_markets(markets, cfg)
    passed = [market for market, should_trade, _, _ in ranked if should_trade]
    filtered = [(market, reason) for market, should_trade, reason, _ in ranked if not should_trade]
    if cfg.get("scan_limit"):
        passed = passed[: int(cfg["scan_limit"])]
    return passed, filtered, ranked


def summarize_filter_results(ranked: List[Tuple[Dict[str, Any], bool, str, float]]) -> Dict[str, Any]:
    passed = [item for item in ranked if item[1]]
    filtered = [item for item in ranked if not item[1]]
    top_passed = [
        {
            "market_id": market.get("id"),
            "question": market_question(market),
            "score": round(score, 2),
            "reason": reason,
        }
        for market, _, reason, score in passed[:5]
    ]
    top_filtered = [
        {
            "market_id": market.get("id"),
            "question": market_question(market),
            "reason": reason,
        }
        for market, _, reason, _ in filtered[:5]
    ]
    return {
        "passed": len(passed),
        "filtered": len(filtered),
        "top_passed": top_passed,
        "top_filtered": top_filtered,
    }
