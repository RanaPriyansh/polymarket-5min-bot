"""
Mean reversion strategy for binary Polymarket interval markets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from book_quality import BookQuality, assess_book_quality
from market_data import OrderBook, PolymarketData


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


class MeanReversion5Min:
    def __init__(self, config: dict):
        params = config["strategies"]["mean_reversion_5min"]
        filters = config.get("filters", {})
        self.ema_period = params["ema_period"]
        self.ema_period_by_interval = {
            5: params.get("ema_period_5m", params["ema_period"]),
            15: params.get("ema_period_15m", params["ema_period"]),
        }
        self.dev_threshold = params["deviation_threshold"]
        self.imbalance_threshold = params["imbalance_threshold"]
        self.kelly_fraction = params["kelly_fraction"]
        self.timeframes = params["timeframes"]
        self.min_volume = params["min_volume"]
        self.max_book_spread_bps = filters.get("max_book_spread_bps", 250)
        self.min_top_depth = filters.get("min_top_depth", 25)
        self.min_top_notional = filters.get("min_top_notional", 10)
        self.max_depth_ratio = filters.get("max_depth_ratio", 12)
        self.price_history = {}
        self.volatility_estimate = 0.05

    def _ema_period_for_interval(self, interval_minutes: Optional[int]) -> int:
        return self.ema_period_by_interval.get(interval_minutes, self.ema_period)

    def update_price(self, market_id: str, price: float, timestamp: float, volume: float = 0, interval_minutes: Optional[int] = None):
        if market_id not in self.price_history:
            self.price_history[market_id] = pd.DataFrame(columns=["ts", "price", "volume"])
        df = self.price_history[market_id]
        df.loc[len(df)] = [timestamp, price, volume]
        if len(df) > 1000:
            self.price_history[market_id] = df.iloc[-1000:].reset_index(drop=True)

    def calculate_ema(self, market_id: str, interval_minutes: Optional[int] = None) -> Optional[float]:
        df = self.price_history.get(market_id)
        ema_period = self._ema_period_for_interval(interval_minutes)
        if df is None or len(df) < ema_period:
            return None
        return df["price"].ewm(span=ema_period, adjust=False).mean().iloc[-1]

    def calculate_deviation(self, market_id: str, current_price: float, interval_minutes: Optional[int] = None) -> Optional[float]:
        ema = self.calculate_ema(market_id, interval_minutes=interval_minutes)
        if ema is None:
            return None
        return (current_price - ema) / ema

    def assess_book(self, orderbook: OrderBook, outcome: str = "YES") -> BookQuality:
        return assess_book_quality(
            orderbook,
            outcome,
            max_spread_bps=self.max_book_spread_bps,
            min_top_depth=self.min_top_depth,
            min_top_notional=self.min_top_notional,
            max_depth_ratio=self.max_depth_ratio,
        )

    def generate_signal(self, market_id: str, outcome: str, price: float,
                        orderbook: OrderBook, volume: float, interval_minutes: Optional[int] = None) -> Optional[Signal]:
        if volume < self.min_volume:
            return None

        quality = self.assess_book(orderbook, outcome)
        if not quality.is_tradeable:
            return None

        dev = self.calculate_deviation(market_id, price, interval_minutes=interval_minutes)
        if dev is None:
            return None

        imbalance = PolymarketData.calculate_imbalance(orderbook, outcome)
        if abs(dev) <= self.dev_threshold:
            return None

        action = "BUY" if dev < 0 else "SELL"
        confidence = min(abs(dev) / self.dev_threshold, 1.0)

        if action == "BUY" and imbalance < self.imbalance_threshold:
            return None
        if action == "SELL" and imbalance > -self.imbalance_threshold:
            return None

        edge = confidence * self.volatility_estimate
        if edge == 0:
            return None
        kelly = edge / (self.volatility_estimate ** 2) * self.kelly_fraction
        kelly = max(0.01, min(kelly, 0.25))
        size = kelly * self.min_volume
        mid = PolymarketData.mid_price(orderbook, outcome)
        limit_price = mid * 0.995 if action == "BUY" else mid * 1.005

        return Signal(
            market_id=market_id,
            outcome=outcome,
            action=action,
            price=round(limit_price, 4),
            confidence=confidence,
            size=round(size, 2),
            reason=(
                f"EMA dev {dev:.2%} | imb {imbalance:.2f} | "
                f"book_spread_bps {quality.spread_bps:.1f}"
            ),
            book_quality=quality.to_dict(),
        )

    def get_markets_to_monitor(self, all_markets: List[Dict]) -> List[Dict]:
        suitable = []
        for market in all_markets:
            if not market.get("active", False):
                continue
            if len(market.get("tokens", [])) < 2:
                continue
            if float(market.get("volume", 0)) < self.min_volume:
                continue
            suitable.append(market)
        return suitable


if __name__ == "__main__":
    import yaml
    cfg = yaml.safe_load(open("config.yaml"))
    strat = MeanReversion5Min(cfg)
    print(f"Mean Reversion strategy initialized. Timeframes: {strat.timeframes}")
