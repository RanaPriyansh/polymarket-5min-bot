"""
Time Decay Strategy for Polymarket binary markets.
When markets are near resolution (<60s), prices should accelerate toward 0 or 1.
If one side is >0.55 with <60s left, buy it at a discount.

Thesis: Binary markets resolve at exactly 0 or 1. Linear decay is wrong —
deceleration curves are concave. The market maker prices linear but prices
should compress faster than time remaining suggests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from market_data import OrderBook
from tradeability_policy import assess_tradeability


@dataclass  
class Signal:
    market_id: str
    outcome: str
    action: str
    price: float
    confidence: float
    size: float
    reason: str
    book_quality: Dict


class TimeDecay:
    def __init__(self, config: dict):
        self.config = config
        params = config["strategies"]["time_decay"]
        self.min_seconds_left = params.get("min_seconds_left", 10)
        self.max_seconds_left = params.get("max_seconds_left", 60)
        self.min_price = params.get("min_price", 0.55)

    def generate_signal(self, market_id: str, market: dict, orderbook: OrderBook,
                       current_time: float) -> Optional[Signal]:
        end_ts = market.get("end_ts", 0)
        if end_ts <= 0:
            return None

        seconds_left = end_ts - current_time
        if seconds_left < self.min_seconds_left or seconds_left > self.max_seconds_left:
            return None

        quality = assess_tradeability(self.config, "time_decay", orderbook, orderbook.outcome_labels[0])
        if not quality.is_tradeable:
            return None

        # Check both outcomes
        outcomes = market.get("outcomes", ["Up", "Down"])
        best_signal = None

        for i, outcome in enumerate(outcomes):
            if i == 0:
                bids, asks = orderbook.yes_bids, orderbook.yes_asks
            else:
                bids, asks = orderbook.no_bids, orderbook.no_asks
            if not bids or not asks:
                continue

            mid = (bids[0][0] + asks[0][0]) / 2
            if mid < self.min_price:
                continue

            # Discount: if mid=0.70, buy at 0.695 (0.5% below mid)
            discount = 0.005
            buy_price = round(mid - discount, 4)

            # Confidence increases as seconds_left decreases
            urgency = max(0, 1 - (seconds_left / self.max_seconds_left))
            confidence = min((mid - 0.5) * 2 * (1 + urgency), 0.95)

            sig = Signal(
                market_id=market_id,
                outcome=outcome,
                action="BUY",
                price=buy_price,
                confidence=confidence,
                size=5.0,
                reason=f"Time decay: {outcome}={mid:.4f} with {seconds_left:.0f}s left, urgency={urgency:.2f}",
                book_quality=quality.to_dict(),
            )

            if best_signal is None or sig.confidence > best_signal.confidence:
                best_signal = sig

        return best_signal
