"""
Mean Reversion Strategy for 5/15-minute Polymarket YES/NO markets
- Trades when price deviates >8% from EMA(20)
- Requires order book imbalance to filter momentum vs reversal
- Kelly position sizing
"""

import pandas as pd
import numpy as np
from typing import Dict, Tuple, Optional
from dataclasses import dataclass
from market_data import OrderBook, PolymarketData

@dataclass
class Signal:
    market_id: str
    outcome: str  # "YES" or "NO"
    action: str   # "BUY" or "SELL"
    price: float  # limit price
    confidence: float  # 0-1
    size: float   # units to trade
    reason: str

class MeanReversion5Min:
    def __init__(self, config: dict):
        params = config["strategies"]["mean_reversion_5min"]
        self.ema_period = params["ema_period"]
        self.dev_threshold = params["deviation_threshold"]
        self.imbalance_threshold = params["imbalance_threshold"]
        self.kelly_fraction = params["kelly_fraction"]
        self.timeframes = params["timeframes"]
        self.min_volume = params["min_volume"]
        self.price_history = {}  # market_id -> DataFrame
        self.volatility_estimate = 0.05  # default 5% volatility

    def update_price(self, market_id: str, price: float, timestamp: float, volume: float = 0):
        """Append new price to history."""
        if market_id not in self.price_history:
            self.price_history[market_id] = pd.DataFrame(columns=['ts', 'price', 'volume'])
        df = self.price_history[market_id]
        df.loc[len(df)] = [timestamp, price, volume]
        # Keep last 1000 points
        if len(df) > 1000:
            self.price_history[market_id] = df.iloc[-1000:].reset_index(drop=True)

    def calculate_ema(self, market_id: str) -> Optional[float]:
        df = self.price_history.get(market_id)
        if df is None or len(df) < self.ema_period:
            return None
        ema = df['price'].ewm(span=self.ema_period, adjust=False).mean().iloc[-1]
        return ema

    def calculate_deviation(self, market_id: str, current_price: float) -> Optional[float]:
        ema = self.calculate_ema(market_id)
        if ema is None:
            return None
        return (current_price - ema) / ema

    def generate_signal(self, market_id: str, outcome: str, price: float,
                       orderbook: OrderBook, volume: float) -> Optional[Signal]:
        """Check if we should trade."""
        if volume < self.min_volume:
            return None

        dev = self.calculate_deviation(market_id, price)
        if dev is None:
            return None

        # Get order book imbalance
        imbalance = PolymarketData.calculate_imbalance(orderbook, outcome)

        # Condition: price far from EMA AND imbalance suggests reversal
        if abs(dev) > self.dev_threshold:
            # For mean reversion: buy when price low (dev negative), sell when high (dev positive)
            if outcome == "YES":
                # For YES outcome, price is probability 0-1
                action = "BUY" if dev < 0 else "SELL"
                confidence = min(abs(dev) / self.dev_threshold, 1.0)
            else:  # NO outcome
                # NO outcome behaves inversely: when YES price low, NO price high
                action = "SELL" if dev < 0 else "BUY"
                confidence = min(abs(dev) / self.dev_threshold, 1.0)

            # Imbalance must filter: if we're buying, want buying pressure already present
            # i.e., imbalance should be positive for buy, negative for sell
            if action == "BUY" and imbalance < self.imbalance_threshold:
                return None
            if action == "SELL" and imbalance > -self.imbalance_threshold:
                return None

            # Position size: Kelly with confidence as edge
            edge = confidence * self.volatility_estimate
            if edge == 0:
                return None
            kelly = edge / (self.volatility_estimate ** 2) * self.kelly_fraction
            kelly = max(0.01, min(kelly, 0.25))  # clamp 1-25%

            # Determine size based on current price and available capital (would need portfolio)
            size = kelly * self.min_volume  # placeholder: use volume as proxy

            # Limit price: for buys, use mid - 0.5%; for sells, mid + 0.5%
            ob = orderbook
            mid = PolymarketData.mid_price(ob, outcome)
            if action == "BUY":
                limit_price = mid * 0.995
            else:
                limit_price = mid * 1.005

            return Signal(
                market_id=market_id,
                outcome=outcome,
                action=action,
                price=round(limit_price, 4),
                confidence=confidence,
                size=round(size, 2),
                reason=f"EMA dev {dev:.2%} | imb {imbalance:.2f}"
            )
        return None

    def get_markets_to_monitor(self, all_markets: List[Dict]) -> List[Dict]:
        """Filter markets that match our timeframe criteria."""
        suitable = []
        for m in all_markets:
            # Check if market is active and has YES/NO outcomes
            if m.get("status") != "ACTIVE":
                continue
            if "YES" not in [t["outcome"] for t in m.get("tokens", [])]:
                continue
            # Check volume threshold
            if float(m.get("volume", 0)) < self.min_volume:
                continue
            suitable.append(m)
        return suitable

if __name__ == "__main__":
    import yaml
    cfg = yaml.safe_load(open("config.yaml"))
    strat = MeanReversion5Min(cfg)
    print(f"Mean Reversion strategy initialized. Timeframes: {strat.timeframes}")