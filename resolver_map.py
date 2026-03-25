"""
Resolver/source map scaffolding.

Purpose:
- attach markets to source families and resolution sources
- start building the metadata layer required for true stale-price capture

This is scaffolding, not a full resolver engine yet.
"""

from dataclasses import dataclass, asdict
from pathlib import Path
import json
from typing import Dict, Optional


@dataclass
class ResolverInfo:
    market_id: str
    source_family: str
    resolver: str
    confidence: float
    notes: str = ""


class ResolverMap:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, dict] = {}
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                self._data = {}

    def infer_source_family(self, market: dict) -> ResolverInfo:
        title = " ".join(
            str(market.get(key, "")) for key in ["question", "title", "description", "slug"]
        ).lower()

        if any(token in title for token in ["nba", "nfl", "soccer", "mlb", "goal", "touchdown", "match", "game"]):
            return ResolverInfo(market_id=market.get("id", ""), source_family="sports", resolver="official_sports_feed", confidence=0.6)
        if any(token in title for token in ["temperature", "rain", "snow", "weather", "wind", "hurricane"]):
            return ResolverInfo(market_id=market.get("id", ""), source_family="weather", resolver="official_weather_feed", confidence=0.65)
        if any(token in title for token in ["cpi", "fed", "rate", "inflation", "stocks", "spx", "btc", "ethereum", "nasdaq"]):
            return ResolverInfo(market_id=market.get("id", ""), source_family="finance", resolver="official_market_data", confidence=0.55)
        if any(token in title for token in ["election", "senate", "president", "vote", "poll"]):
            return ResolverInfo(market_id=market.get("id", ""), source_family="politics", resolver="official_election_reporting", confidence=0.55)
        return ResolverInfo(market_id=market.get("id", ""), source_family="general", resolver="manual_review_required", confidence=0.2)

    def upsert_market(self, market: dict) -> ResolverInfo:
        market_id = market.get("id", "")
        info = self.infer_source_family(market)
        existing = self._data.get(market_id)
        if existing:
            return ResolverInfo(**existing)
        self._data[market_id] = asdict(info)
        self.save()
        return info

    def get(self, market_id: str) -> Optional[ResolverInfo]:
        row = self._data.get(market_id)
        return ResolverInfo(**row) if row else None

    def save(self):
        self.path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
