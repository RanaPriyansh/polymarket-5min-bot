from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from book_quality import BookQuality, assess_book_quality
from market_data import OrderBook


@dataclass(frozen=True)
class TradeabilityPolicy:
    strategy_family: str
    max_spread_bps: float
    min_top_depth: float
    min_top_notional: float
    max_depth_ratio: float

    def assess(self, orderbook: OrderBook, outcome: str = "YES") -> BookQuality:
        return assess_book_quality(
            orderbook,
            outcome,
            max_spread_bps=self.max_spread_bps,
            min_top_depth=self.min_top_depth,
            min_top_notional=self.min_top_notional,
            max_depth_ratio=self.max_depth_ratio,
        )


def _filters_from_config(config: Dict[str, Any], strategy_family: str | None = None) -> Dict[str, Any]:
    base_filters = dict(config.get("filters", {}) or {})
    policy_cfg = config.get("tradeability", {}) or {}
    per_strategy = policy_cfg.get("per_strategy", {}) or {}
    if strategy_family and strategy_family in per_strategy:
        overrides = per_strategy.get(strategy_family) or {}
        merged = dict(base_filters)
        merged.update(overrides)
        return merged
    return base_filters


def tradeability_policy(config: Dict[str, Any], strategy_family: str = "default") -> TradeabilityPolicy:
    filters = _filters_from_config(config, strategy_family)
    return TradeabilityPolicy(
        strategy_family=strategy_family,
        max_spread_bps=float(filters.get("max_book_spread_bps", 500)),
        min_top_depth=float(filters.get("min_top_depth", 2)),
        min_top_notional=float(filters.get("min_top_notional", 0.5)),
        max_depth_ratio=float(filters.get("max_depth_ratio", 12)),
    )


def assess_tradeability(
    config: Dict[str, Any],
    strategy_family: str,
    orderbook: OrderBook,
    outcome: str = "YES",
) -> BookQuality:
    return tradeability_policy(config, strategy_family).assess(orderbook, outcome)
