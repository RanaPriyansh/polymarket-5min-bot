"""Spot-momentum directional candidate for 5m crypto up/down markets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from market_context import MarketContext


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


class SpotMomentum:
    """Buy the underpriced side when spot has moved materially mid-slot."""

    def __init__(self, config: dict):
        params = config.get("strategies", {}).get("spot_momentum", {})
        self.min_abs_spot_move_pct = float(params.get("min_abs_spot_move_pct", 0.20))
        self.min_tte_pct = float(params.get("min_tte_pct", 0.20))
        self.max_tte_pct = float(params.get("max_tte_pct", 0.70))
        self.max_entry_price = float(params.get("max_entry_price", 0.70))
        self.base_notional_usd = float(params.get("base_notional_usd", 2.0))
        self.move_notional_multiplier = float(params.get("move_notional_multiplier", 4.0))
        self.max_notional_usd = float(params.get("max_notional_usd", 6.0))
        self._fired_slots: set[str] = set()

    def mark_fired(self, slot_id: str) -> None:
        self._fired_slots.add(str(slot_id))

    def mark_signal_fired(self, context: MarketContext) -> None:
        self.mark_fired(context.slot_id)

    def generate_signal(self, context: MarketContext) -> Optional[Signal]:
        if int(context.interval_minutes) != 5:
            return None
        if context.slot_id in self._fired_slots:
            return None
        if not (self.min_tte_pct <= float(context.tte_pct) <= self.max_tte_pct):
            return None

        move = float(context.spot_move_pct_window or 0.0)
        if abs(move) < self.min_abs_spot_move_pct:
            return None

        if move > 0:
            outcome = context.outcome_labels[0] if context.outcome_labels else "Up"
            price = float(context.mid_price_yes or 0.0)
            direction = "positive"
        else:
            outcome = context.outcome_labels[1] if len(context.outcome_labels) > 1 else "Down"
            price = float(context.mid_price_no or 0.0)
            direction = "negative"

        if price <= 0.0 or price >= self.max_entry_price:
            return None

        notional = min(self.max_notional_usd, self.base_notional_usd + self.move_notional_multiplier * abs(move))
        size = notional / price
        confidence = min(0.95, 0.50 + min(abs(move), 1.0) * 0.30 + abs(float(context.momentum_score or 0.0)) * 0.10)

        return Signal(
            market_id=context.market_id,
            outcome=outcome,
            action="BUY",
            price=round(price, 4),
            confidence=round(confidence, 4),
            size=size,
            reason=(
                f"Spot momentum {direction}: {context.asset} move={move:.3f}% "
                f"tte_pct={context.tte_pct:.2f} price={price:.4f}"
            ),
            book_quality={
                "is_tradeable": True,
                "strategy_family": "spot_momentum",
                "spot_move_pct_window": move,
                "tte_pct": context.tte_pct,
                "momentum_score": context.momentum_score,
            },
        )
