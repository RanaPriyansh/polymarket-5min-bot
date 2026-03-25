"""
YES/NO complementary dislocation strategy.

Goal:
Exploit temporary inconsistencies between YES and NO books in binary markets.
For a clean binary market, YES fair probability + NO fair probability should be close to 1.
When the books imply a strong dislocation after spread/microstructure checks, trade the richer side.
"""

from dataclasses import dataclass
from typing import Optional

from market_data import OrderBook, PolymarketData
from risk import RiskManager


@dataclass
class DislocationSignal:
    market_id: str
    outcome: str
    action: str
    price: float
    confidence: float
    size: float
    reason: str
    dislocation: float
    expected_edge: float


class ComplementaryDislocationStrategy:
    def __init__(self, config: dict):
        params = config["strategies"]["dislocation_arb"]
        self.min_volume = params["min_volume"]
        self.min_dislocation = params["min_dislocation"]
        self.max_spread_bps = params["max_spread_bps"]
        self.kelly_fraction = params["kelly_fraction"]
        self.reference_capital = params.get("reference_capital", 1000.0)

    @staticmethod
    def spread_bps(orderbook: OrderBook, outcome: str) -> float:
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

    @staticmethod
    def microprice(orderbook: OrderBook, outcome: str) -> float:
        if outcome == "YES":
            if not orderbook.yes_bids or not orderbook.yes_asks:
                return 0.0
            bid_price, bid_size = orderbook.yes_bids[0]
            ask_price, ask_size = orderbook.yes_asks[0]
        else:
            if not orderbook.no_bids or not orderbook.no_asks:
                return 0.0
            bid_price, bid_size = orderbook.no_bids[0]
            ask_price, ask_size = orderbook.no_asks[0]
        total = bid_size + ask_size
        if total <= 0:
            return 0.0
        return (ask_price * bid_size + bid_price * ask_size) / total

    def generate_signal(
        self,
        market_id: str,
        orderbook: OrderBook,
        volume: float,
        risk_manager: Optional[RiskManager] = None,
    ) -> Optional[DislocationSignal]:
        if volume < self.min_volume:
            return None

        yes_mid = PolymarketData.mid_price(orderbook, "YES")
        no_mid = PolymarketData.mid_price(orderbook, "NO")
        yes_micro = self.microprice(orderbook, "YES")
        no_micro = self.microprice(orderbook, "NO")
        if min(yes_mid, no_mid, yes_micro, no_micro) <= 0:
            return None

        yes_spread = self.spread_bps(orderbook, "YES")
        no_spread = self.spread_bps(orderbook, "NO")
        if yes_spread > self.max_spread_bps or no_spread > self.max_spread_bps:
            return None

        implied_sum = yes_micro + no_micro
        dislocation = implied_sum - 1.0
        if abs(dislocation) < self.min_dislocation:
            return None

        # If sum > 1, both sides are too expensive. Prefer selling the richer side.
        # If sum < 1, both sides are cheap. Prefer buying the cheaper side.
        if dislocation > 0:
            if yes_micro >= no_micro:
                outcome, action, ref_price = "YES", "SELL", yes_mid
            else:
                outcome, action, ref_price = "NO", "SELL", no_mid
        else:
            if yes_micro <= no_micro:
                outcome, action, ref_price = "YES", "BUY", yes_mid
            else:
                outcome, action, ref_price = "NO", "BUY", no_mid

        expected_edge = max(abs(dislocation) * 0.5, 0.005)
        confidence = min(abs(dislocation) / (self.min_dislocation * 3), 1.0)
        sizing_engine = risk_manager or RiskManager(
            {
                "risk": {"max_daily_loss": 0.05, "max_position_size": 0.1, "circuit_breaker_dd": 0.1},
                "strategies": {"dislocation_arb": {"kelly_fraction": self.kelly_fraction, "min_edge": 0.005}},
            },
            initial_capital=self.reference_capital,
        )
        sizing = sizing_engine.calculate_position_size(
            "dislocation_arb",
            confidence=confidence,
            price=ref_price,
            volatility=max(abs(dislocation), 0.02),
            stop_loss=max(abs(dislocation) * 0.75, 0.02),
            edge=expected_edge,
        )
        if sizing.size <= 0:
            return None

        if action == "BUY":
            if outcome == "YES":
                best_bid = orderbook.yes_bids[0][0]
            else:
                best_bid = orderbook.no_bids[0][0]
            limit_price = min(ref_price * 0.998, best_bid + 0.002)
        else:
            if outcome == "YES":
                best_ask = orderbook.yes_asks[0][0]
            else:
                best_ask = orderbook.no_asks[0][0]
            limit_price = max(ref_price * 1.002, best_ask - 0.002)

        limit_price = min(max(limit_price, 0.01), 0.99)
        return DislocationSignal(
            market_id=market_id,
            outcome=outcome,
            action=action,
            price=round(limit_price, 4),
            confidence=confidence,
            size=round(sizing.size, 2),
            reason=f"yes_micro={yes_micro:.4f} no_micro={no_micro:.4f} implied_sum={implied_sum:.4f}",
            dislocation=dislocation,
            expected_edge=expected_edge,
        )
