"""
Terminal-minute resolver strategy.

Goal:
- focus only on markets close to resolution
- use resolver/source-family metadata as a confidence prior
- act only when the book still looks stale enough to exploit

This is scaffolding for future stale-truth capture.
"""

from dataclasses import dataclass
from typing import Optional

from market_data import OrderBook, PolymarketData
from risk import RiskManager


@dataclass
class ResolverSignal:
    market_id: str
    outcome: str
    action: str
    price: float
    confidence: float
    size: float
    reason: str
    seconds_to_resolution: float
    expected_edge: float


class TerminalResolverStrategy:
    def __init__(self, config: dict):
        params = config["strategies"]["terminal_resolver"]
        self.max_seconds_to_resolution = params["max_seconds_to_resolution"]
        self.min_resolver_confidence = params["min_resolver_confidence"]
        self.max_spread_bps = params["max_spread_bps"]
        self.min_volume = params["min_volume"]
        self.min_imbalance = params["min_imbalance"]
        self.kelly_fraction = params["kelly_fraction"]
        self.reference_capital = params.get("reference_capital", 1000.0)

    @staticmethod
    def spread_bps(orderbook: OrderBook, outcome: str = "YES") -> float:
        mid = PolymarketData.mid_price(orderbook, outcome)
        if mid <= 0:
            return 0.0
        if outcome == "YES":
            if not orderbook.yes_bids or not orderbook.yes_asks:
                return 0.0
            spread = orderbook.yes_asks[0][0] - orderbook.yes_bids[0][0]
        else:
            if not orderbook.no_bids or not orderbook.no_asks:
                return 0.0
            spread = orderbook.no_asks[0][0] - orderbook.no_bids[0][0]
        return (spread / mid) * 10000

    def generate_signal(
        self,
        market_id: str,
        market: dict,
        orderbook: OrderBook,
        volume: float,
        resolver_info=None,
        seconds_to_resolution: Optional[float] = None,
        risk_manager: Optional[RiskManager] = None,
    ) -> Optional[ResolverSignal]:
        if volume < self.min_volume:
            return None
        if resolver_info is None or getattr(resolver_info, "confidence", 0.0) < self.min_resolver_confidence:
            return None
        if seconds_to_resolution is None or seconds_to_resolution > self.max_seconds_to_resolution or seconds_to_resolution < 0:
            return None

        mid = PolymarketData.mid_price(orderbook, "YES")
        if mid <= 0:
            return None
        spread = self.spread_bps(orderbook, "YES")
        if spread <= 0 or spread > self.max_spread_bps:
            return None

        imbalance = PolymarketData.calculate_imbalance(orderbook, "YES")
        if abs(imbalance) < self.min_imbalance:
            return None

        # Heuristic stale-truth capture prior:
        # near resolution + stronger resolver confidence + one-sided book = more edge.
        action = "BUY" if imbalance > 0 else "SELL"
        expected_edge = max((self.max_seconds_to_resolution - seconds_to_resolution) / self.max_seconds_to_resolution * 0.04, 0.005)
        expected_edge *= max(getattr(resolver_info, "confidence", 0.0), self.min_resolver_confidence)
        confidence = min(expected_edge / 0.03, 1.0)

        sizing_engine = risk_manager or RiskManager(
            {
                "risk": {"max_daily_loss": 0.05, "max_position_size": 0.1, "circuit_breaker_dd": 0.1},
                "strategies": {"terminal_resolver": {"kelly_fraction": self.kelly_fraction, "min_edge": 0.005}},
            },
            initial_capital=self.reference_capital,
        )
        sizing = sizing_engine.calculate_position_size(
            "terminal_resolver",
            confidence=confidence,
            price=mid,
            volatility=max(abs(imbalance) * 0.05, 0.02),
            stop_loss=max(abs(imbalance) * 0.03, 0.02),
            edge=expected_edge,
        )
        if sizing.size <= 0:
            return None

        if action == "BUY":
            best_bid = orderbook.yes_bids[0][0] if orderbook.yes_bids else mid
            limit_price = min(mid * 0.999, best_bid + 0.001)
        else:
            best_ask = orderbook.yes_asks[0][0] if orderbook.yes_asks else mid
            limit_price = max(mid * 1.001, best_ask - 0.001)
        limit_price = min(max(limit_price, 0.01), 0.99)

        return ResolverSignal(
            market_id=market_id,
            outcome="YES",
            action=action,
            price=round(limit_price, 4),
            confidence=confidence,
            size=round(sizing.size, 2),
            reason=f"resolver={resolver_info.resolver} conf={resolver_info.confidence:.2f} imb={imbalance:.2f}",
            seconds_to_resolution=seconds_to_resolution,
            expected_edge=expected_edge,
        )
