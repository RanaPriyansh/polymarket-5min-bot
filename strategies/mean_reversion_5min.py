"""
Mean Reversion Strategy for 5/15-minute Polymarket YES/NO markets.

Upgrades over the original placeholder version:
- Uses logit-space z-scores instead of raw % deviation from EMA
- Estimates realized volatility from recent price history
- Uses microprice + book imbalance as a reversal confirmation filter
- Sizes from capital/risk budgets via RiskManager instead of market-volume proxies
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from market_data import OrderBook, PolymarketData
from risk import RiskManager


@dataclass
class Signal:
    market_id: str
    outcome: str  # "YES" or "NO"
    action: str   # "BUY" or "SELL"
    price: float  # limit price
    confidence: float  # 0-1
    size: float   # units to trade
    reason: str
    expected_edge: float = 0.0
    zscore: float = 0.0


class MeanReversion5Min:
    def __init__(self, config: dict):
        params = config["strategies"]["mean_reversion_5min"]
        self.ema_period = params["ema_period"]
        self.dev_threshold = params["deviation_threshold"]
        self.imbalance_threshold = params["imbalance_threshold"]
        self.kelly_fraction = params["kelly_fraction"]
        self.timeframes = params["timeframes"]
        self.min_volume = params["min_volume"]
        self.zscore_threshold = params.get("zscore_threshold", 1.75)
        self.max_spread_bps = params.get("max_spread_bps", 150)
        self.reference_capital = params.get("reference_capital", 1000.0)
        self.price_history: Dict[str, pd.DataFrame] = {}
        self.volatility_estimate = 0.05

    def update_price(self, market_id: str, price: float, timestamp: float, volume: float = 0):
        """Append new price to history."""
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
        bounded = self._bounded_price(price)
        return float(np.log(bounded / (1 - bounded)))

    def calculate_ema(self, market_id: str) -> Optional[float]:
        df = self.price_history.get(market_id)
        if df is None or len(df) < self.ema_period:
            return None
        ema = df["price"].ewm(span=self.ema_period, adjust=False).mean().iloc[-1]
        return float(ema)

    def calculate_deviation(self, market_id: str, current_price: float) -> Optional[float]:
        ema = self.calculate_ema(market_id)
        if ema is None:
            return None
        return float((current_price - ema) / ema)

    def estimate_realized_volatility(self, market_id: str) -> float:
        df = self.price_history.get(market_id)
        if df is None or len(df) < max(self.ema_period, 10):
            return self.volatility_estimate
        prices = df["price"].clip(0.01, 0.99)
        logits = np.log(prices / (1 - prices))
        returns = logits.diff().dropna()
        if len(returns) < 5:
            return self.volatility_estimate
        sigma = float(returns.tail(50).std())
        return max(sigma, 0.02)

    def calculate_zscore(self, market_id: str, current_price: float) -> Optional[float]:
        df = self.price_history.get(market_id)
        if df is None or len(df) < self.ema_period:
            return None
        prices = df["price"].clip(0.01, 0.99)
        logits = np.log(prices / (1 - prices))
        ema = logits.ewm(span=self.ema_period, adjust=False).mean().iloc[-1]
        sigma = max(float(logits.tail(50).std()), 0.02)
        current_logit = self._logit(current_price)
        return float((current_logit - ema) / sigma)

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

        total_size = bid_size + ask_size
        if total_size <= 0:
            return 0.0
        return float((ask_price * bid_size + bid_price * ask_size) / total_size)

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

    def generate_signal(
        self,
        market_id: str,
        outcome: str,
        price: float,
        orderbook: OrderBook,
        volume: float,
        risk_manager: Optional[RiskManager] = None,
    ) -> Optional[Signal]:
        """Check if we should trade."""
        if volume < self.min_volume:
            return None

        zscore = self.calculate_zscore(market_id, price)
        if zscore is None:
            return None

        spread_bps = self.spread_bps(orderbook, outcome)
        if spread_bps <= 0 or spread_bps > self.max_spread_bps:
            return None

        realized_vol = self.estimate_realized_volatility(market_id)
        imbalance = PolymarketData.calculate_imbalance(orderbook, outcome)
        mid = PolymarketData.mid_price(orderbook, outcome)
        micro = self.microprice(orderbook, outcome)
        if mid <= 0 or micro <= 0:
            return None

        micro_dislocation = float((micro - mid) / mid)

        if abs(zscore) <= self.zscore_threshold:
            return None

        if outcome == "YES":
            action = "BUY" if zscore < 0 else "SELL"
        else:
            action = "SELL" if zscore < 0 else "BUY"

        confidence = min(abs(zscore) / (self.zscore_threshold * 2), 1.0)

        # Reversal filter: only fade when microprice and top-book imbalance support reversion.
        if action == "BUY" and not (imbalance >= self.imbalance_threshold and micro_dislocation >= 0):
            return None
        if action == "SELL" and not (imbalance <= -self.imbalance_threshold and micro_dislocation <= 0):
            return None

        expected_edge = max((abs(zscore) - self.zscore_threshold) * realized_vol * 0.10, 0.005)
        stop_loss = max(realized_vol * 0.75, 0.02)
        if expected_edge <= 0:
            return None

        sizing_engine = risk_manager or RiskManager(
            {
                "risk": {"max_daily_loss": 0.05, "max_position_size": 0.1, "circuit_breaker_dd": 0.1},
                "strategies": {"mean_reversion_5min": {"kelly_fraction": self.kelly_fraction, "min_edge": 0.005}},
            },
            initial_capital=self.reference_capital,
        )
        sizing = sizing_engine.calculate_position_size(
            "mean_reversion_5min",
            confidence=confidence,
            price=mid,
            volatility=realized_vol,
            stop_loss=stop_loss,
            edge=expected_edge,
        )
        if sizing.size <= 0:
            return None

        if action == "BUY":
            if outcome == "YES":
                best_bid = orderbook.yes_bids[0][0] if orderbook.yes_bids else mid
            else:
                best_bid = orderbook.no_bids[0][0] if orderbook.no_bids else mid
            limit_price = min(mid * 0.9975, best_bid + 0.0025)
        else:
            if outcome == "YES":
                best_ask = orderbook.yes_asks[0][0] if orderbook.yes_asks else mid
            else:
                best_ask = orderbook.no_asks[0][0] if orderbook.no_asks else mid
            limit_price = max(mid * 1.0025, best_ask - 0.0025)
        limit_price = self._bounded_price(limit_price)

        return Signal(
            market_id=market_id,
            outcome=outcome,
            action=action,
            price=round(limit_price, 4),
            confidence=confidence,
            size=round(sizing.size, 2),
            reason=(
                f"z={zscore:.2f} | rv={realized_vol:.3f} | imb={imbalance:.2f} | "
                f"micro={micro_dislocation:.2%} | {sizing.reason}"
            ),
            expected_edge=expected_edge,
            zscore=zscore,
        )

    def get_markets_to_monitor(self, all_markets: List[Dict]) -> List[Dict]:
        """Filter markets that match our timeframe criteria."""
        suitable = []
        for m in all_markets:
            if m.get("status") != "ACTIVE":
                continue
            if "YES" not in [t["outcome"] for t in m.get("tokens", [])]:
                continue
            if float(m.get("volume", 0)) < self.min_volume:
                continue
            suitable.append(m)
        return suitable


if __name__ == "__main__":
    import yaml

    cfg = yaml.safe_load(open("config.yaml"))
    strat = MeanReversion5Min(cfg)
    print(f"Mean Reversion strategy initialized. Timeframes: {strat.timeframes}")
