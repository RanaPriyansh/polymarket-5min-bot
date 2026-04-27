from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


PRICE_PATTERNS = [
    re.compile(r"(?:open|opening|start|reference|strike)\s*(?:price)?\s*(?:of|is|:)?\s*\$?([0-9][0-9,]*(?:\.[0-9]+)?)", re.I),
    re.compile(r"\$([0-9][0-9,]*(?:\.[0-9]+)?)"),
]


@dataclass(frozen=True)
class MarketRule:
    asset: str | None
    timeframe_minutes: int | None
    strike_or_open_price: float | None
    expiry_ts: float | None
    uncertain: bool
    reason: str = ""


def parse_market_rule(market: dict[str, Any]) -> MarketRule:
    asset = market.get("asset")
    timeframe = market.get("interval_minutes")
    strike = market.get("strike_or_open_price") or market.get("open_price")
    if strike is None:
        text = " ".join(str(market.get(key) or "") for key in ("question", "title", "slug"))
        for pattern in PRICE_PATTERNS:
            match = pattern.search(text)
            if match:
                strike = float(match.group(1).replace(",", ""))
                break
    strike_value = float(strike) if strike is not None else None
    expiry_ts = market.get("end_ts")
    uncertain = not asset or not timeframe or strike_value is None or not expiry_ts
    reason = "missing_asset_timeframe_strike_or_expiry" if uncertain else ""
    return MarketRule(
        asset=str(asset).lower() if asset else None,
        timeframe_minutes=int(timeframe) if timeframe else None,
        strike_or_open_price=strike_value,
        expiry_ts=float(expiry_ts) if expiry_ts else None,
        uncertain=uncertain,
        reason=reason,
    )
