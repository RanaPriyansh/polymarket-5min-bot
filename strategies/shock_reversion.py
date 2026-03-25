"""
Shock-and-reversion strategy for short-duration Polymarket markets.

Idea:
- detect outsized short-horizon jumps in logit space
- fade only when the move looks liquidity-driven rather than information-driven
- require microprice disagreement and favorable spread/liquidity
"""

from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd

from market_data import OrderBook, PolymarketData
from risk import RiskManager


@dataclass
class ShockSignal:
    market_id: str
    outcome: str
    action: str
    price: float
    confidence: float
    size: float
    reason: str
    regime: str
    jump_zscore: float
    expected_edge: float


class ShockReversionStrategy:
    def __init__(self, config: dict):
        params = config["strategies"]["shock_reversion"]
        self.jump_zscore_threshold = params["jump_zscore_threshold"]
        self.lookback_points = params["lookback_points"]
        self.min_volume = params["min_volume"]
        self.max_spread_bps = params["max_spread_bps"]
        self.kelly_fraction = params["kelly_fraction"]
        self.reference_capital = params.get("reference_capital", 1000.0)
        self.price_history: Dict[str, pd.DataFrame] = {}

    def update_price(self, market_id: str, price: float, timestamp: float, volume: float = 0):
        if market_id not in self.price_history:
            self.price_history[market_id] = pd.DataFrame(columns=["ts", "price", "volume"])
        df = self.price_history[market_id]
        df.loc[len(df)] = [timestamp, price, volume]
        if len(df) > 1000:
            self.price_history[market_id] = df.iloc[-1000:].reset_index(drop=True)

    @staticmethod
    def _bounded_price(price: float) -> float:
        return min(max(price, 0.01), 0.99)

    def _logit(self, price: float) -> float:
        p = self._bounded_price(price)
        return float(np.log(p / (1 - p)))

    def realized_sigma(self, market_id: str) -> Optional[float]:
        df = self.price_history.get(market_id)
        if df is None or len(df) < max(self.lookback_points, 8):
            return None
        prices = df["price"].clip(0.01, 0.99)
        logits = np.log(prices / (1 - prices))
        rets = logits.diff().dropna()
        if len(rets) < 5:
            return None
        return max(float(rets.tail(self.lookback_points).std()), 0.02)

    def jump_zscore(self, market_id: str, current_price: float) -> Optional[float]:
        df = self.price_history.get(market_id)
        sigma = self.realized_sigma(market_id)
        if df is None or sigma is None or len(df) < 2:
            return None
        prev_price = float(df["price"].iloc[-2])
        jump = self._logit(current_price) - self._logit(prev_price)
        return float(jump / sigma)

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
        return float((spread / mid) * 10000)

    @staticmethod
    def microprice(orderbook: OrderBook, outcome: str = "YES") -> float:
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
        return float((ask_price * bid_size + bid_price * ask_size) / total)

    @staticmethod
    def classify_regime(spread_bps: float, sigma: float, jump_z: float) -> str:
        if abs(jump_z) >= 2.5:
            return "shock"
        if spread_bps > 300:
            return "stressed"
        if sigma > 0.12:
            return "volatile"
        return "calm"

    def generate_signal(
        self,
        market_id: str,
        outcome: str,
        price: float,
        orderbook: OrderBook,
        volume: float,
        risk_manager: Optional[RiskManager] = None,
    ) -> Optional[ShockSignal]:
        if volume < self.min_volume:
            return None

        sigma = self.realized_sigma(market_id)
        jump_z = self.jump_zscore(market_id, price)
        if sigma is None or jump_z is None:
            return None

        spread = self.spread_bps(orderbook, outcome)
        if spread <= 0 or spread > self.max_spread_bps:
            return None

        mid = PolymarketData.mid_price(orderbook, outcome)
        micro = self.microprice(orderbook, outcome)
        if mid <= 0 or micro <= 0:
            return None

        imbalance = PolymarketData.calculate_imbalance(orderbook, outcome)
        regime = self.classify_regime(spread, sigma, jump_z)
        if regime == "stressed":
            return None
        if abs(jump_z) < self.jump_zscore_threshold:
            return None

        # Fade the jump only when the microprice disagrees with the jump direction.
        micro_dislocation = (micro - mid) / mid
        if jump_z > 0:
            action = "SELL" if outcome == "YES" else "BUY"
            if not (micro_dislocation <= 0 and imbalance <= 0):
                return None
        else:
            action = "BUY" if outcome == "YES" else "SELL"
            if not (micro_dislocation >= 0 and imbalance >= 0):
                return None

        expected_edge = max((abs(jump_z) - self.jump_zscore_threshold) * sigma * 0.08, 0.005)
        stop_loss = max(sigma, 0.02)
        confidence = min(abs(jump_z) / (self.jump_zscore_threshold * 2), 1.0)

        sizing_engine = risk_manager or RiskManager(
            {
                "risk": {"max_daily_loss": 0.05, "max_position_size": 0.1, "circuit_breaker_dd": 0.1},
                "strategies": {"shock_reversion": {"kelly_fraction": self.kelly_fraction, "min_edge": 0.005}},
            },
            initial_capital=self.reference_capital,
        )
        sizing = sizing_engine.calculate_position_size(
            "shock_reversion",
            confidence=confidence,
            price=mid,
            volatility=sigma,
            stop_loss=stop_loss,
            edge=expected_edge,
        )
        if sizing.size <= 0:
            return None

        if action == "BUY":
            best_bid = orderbook.yes_bids[0][0] if outcome == "YES" and orderbook.yes_bids else mid
            best_bid = orderbook.no_bids[0][0] if outcome == "NO" and orderbook.no_bids else best_bid
            limit_price = min(mid * 0.998, best_bid + 0.002)
        else:
            best_ask = orderbook.yes_asks[0][0] if outcome == "YES" and orderbook.yes_asks else mid
            best_ask = orderbook.no_asks[0][0] if outcome == "NO" and orderbook.no_asks else best_ask
            limit_price = max(mid * 1.002, best_ask - 0.002)
        limit_price = self._bounded_price(limit_price)

        return ShockSignal(
            market_id=market_id,
            outcome=outcome,
            action=action,
            price=round(limit_price, 4),
            confidence=confidence,
            size=round(sizing.size, 2),
            reason=f"jump_z={jump_z:.2f} sigma={sigma:.3f} imb={imbalance:.2f} micro={(micro-mid)/mid:.2%}",
            regime=regime,
            jump_zscore=jump_z,
            expected_edge=expected_edge,
        )
