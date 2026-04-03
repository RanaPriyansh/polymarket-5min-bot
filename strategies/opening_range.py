"""
Opening Range Breakout for Polymarket 5m/15m crypto markets.
Enters when price breaks above/below the opening range (first 3 ticks).
Thesis: In binary crypto markets, initial momentum tends to persist through the window.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from book_quality import assess_book_quality
from market_data import OrderBook


@dataclass
class Signal:
    market_id: str
    outcome: str
    action: str  # BUY or SELL
    price: float
    confidence: float
    size: float
    reason: str
    book_quality: Dict


class OpeningRangeBreakout:
    def __init__(self, config: dict):
        params = config["strategies"]["opening_range"]
        filters = config.get("filters", {})
        self.tick_count = params.get("opening_range_ticks", 3)
        self.breakout_pct = params.get("breakout_pct", 0.02)
        self.min_volume = params.get("min_volume", 5000)
        self.max_book_spread_bps = filters.get("max_book_spread_bps", 500)
        self.min_top_depth = filters.get("min_top_depth", 2)
        self.min_top_notional = filters.get("min_top_notional", 0.5)
        self.max_depth_ratio = filters.get("max_depth_ratio", 12)
        # Per-slot state: market_id -> {high, low, ticks, broken}
        self.opening_ranges = {}

    def update_price(self, market_id: str, price: float, volume: float = 0):
        state = self.opening_ranges.setdefault(market_id, {
            "high": price, "low": price,
            "ticks": 0, "prices": [],
            "broken": False, "broken_direction": None
        })
        state["ticks"] += 1
        state["prices"].append(price)
        state["high"] = max(state["high"], price)
        state["low"] = min(state["low"], price)
        # Keep only last 100 prices to prevent memory leak
        if len(state["prices"]) > 100:
            state["prices"] = state["prices"][-100:]

    def generate_signal(self, market_id: str, primary_outcome: str,
                       price: float, orderbook: OrderBook,
                       volume: float, slot_id: str = "") -> Optional[Signal]:
        if volume < self.min_volume:
            return None

        quality = assess_book_quality(
            orderbook, primary_outcome,
            max_spread_bps=self.max_book_spread_bps,
            min_top_depth=self.min_top_depth,
            min_top_notional=self.min_top_notional,
            max_depth_ratio=self.max_depth_ratio,
        )
        if not quality.is_tradeable:
            return None

        state = self.opening_ranges.get(market_id)
        if state is None:
            return None

        # Opening range not yet established
        if state["ticks"] < self.tick_count:
            return None

        # Already broke out this window
        if state["broken"]:
            return None

        opening_high = state["high"]
        opening_low = state["low"]
        opening_range = opening_high - opening_low

        if opening_range < 0.005:  # range too small, no breakout possible
            return None

        breakout_threshold = opening_range * self.breakout_pct

        if price > opening_high + breakout_threshold:
            state["broken"] = True
            state["broken_direction"] = "up"
            # BUY the primary outcome (e.g., "Up")
            best_ask = orderbook.yes_asks[0][0] if orderbook.yes_asks else price
            return Signal(
                market_id=market_id,
                outcome=primary_outcome,
                action="BUY",
                price=round(best_ask * 1.002, 4),
                confidence=min((price - opening_high) / opening_range * 2, 0.9),
                size=10.0,
                reason=f"Range breakout up: {price:.4f} > {opening_high:.4f}+{breakout_threshold:.4f}",
                book_quality=quality.to_dict(),
            )
        elif price < opening_low - breakout_threshold:
            state["broken"] = True
            state["broken_direction"] = "down"
            # BUY the opposite outcome
            opposite = orderbook.outcome_labels[1] if primary_outcome == orderbook.outcome_labels[0] else orderbook.outcome_labels[0]
            best_ask = orderbook.no_asks[0][0] if orderbook.no_asks else price
            return Signal(
                market_id=market_id,
                outcome=opposite,
                action="BUY",
                price=round(best_ask * 1.002, 4),
                confidence=min((opening_low - price) / opening_range * 2, 0.9),
                size=10.0,
                reason=f"Range breakout down: {price:.4f} < {opening_low:.4f}-{breakout_threshold:.4f}",
                book_quality=quality.to_dict(),
            )

        return None

    def reset_window(self, market_id: str):
        """Reset when a new market slot is detected."""
        self.opening_ranges.pop(market_id, None)
