from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class LagOpportunity:
    strategy_family: str
    strategy_variant: str
    slot_id: str
    market_slug: str
    outcome: str
    fair_delta: float
    market_delta: float
    lag_ms: float
    edge_before_latency: float
    edge_after_latency: float
    suggested_side: str
    best_executable_price: float
    expected_fill_size: float
    action: str = "scanner_only"

    def to_event(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["event_type"] = "scanner.opportunity"
        return payload


class LagScanner:
    strategy_family = "lag_scanner"

    def __init__(self, config: dict):
        params = config.get("strategies", {}).get("lag_scanner", {}) or {}
        self.min_fair_delta = float(params.get("min_fair_delta", 0.12))
        self.min_edge_after_latency = float(params.get("min_edge_after_latency", 0.05))
        self.latency_penalty = float(params.get("latency_penalty", 0.02))

    def evaluate(
        self,
        *,
        market: dict[str, Any],
        outcome: str,
        previous_fair: float,
        current_fair: float,
        previous_market_probability: float,
        current_best_ask: float,
        current_ask_size: float,
        spot_move_ts: float,
        book_update_ts: float,
    ) -> LagOpportunity | None:
        fair_delta = current_fair - previous_fair
        market_delta = current_best_ask - previous_market_probability
        lag = max(0.0, (book_update_ts - spot_move_ts) * 1000.0)
        edge_before = current_fair - current_best_ask
        edge_after = edge_before - self.latency_penalty
        if fair_delta < self.min_fair_delta:
            return None
        if fair_delta - market_delta < self.min_fair_delta:
            return None
        if edge_after < self.min_edge_after_latency:
            return None
        return LagOpportunity(
            strategy_family=self.strategy_family,
            strategy_variant="fair_delta_vs_book_delta",
            slot_id=market.get("slot_id", ""),
            market_slug=market.get("slug", ""),
            outcome=outcome,
            fair_delta=fair_delta,
            market_delta=market_delta,
            lag_ms=lag,
            edge_before_latency=edge_before,
            edge_after_latency=edge_after,
            suggested_side="BUY",
            best_executable_price=current_best_ask,
            expected_fill_size=current_ask_size,
        )
