"""
Toxicity-Aware Market Making for 5/15-minute markets
- Provides liquidity when VPIN (volume imbalance) is low (< threshold)
- Steps back when toxic flow detected (VPIN high)
- Dynamically adjusts spread based on volatility
"""

import numpy as np
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from market_data import OrderBook, PolymarketData

@dataclass
class MMQuote:
    market_id: str
    outcome: str
    bid_price: float
    ask_price: float
    bid_size: float
    ask_size: float
    reason: str

class ToxicityMM:
    def __init__(self, config: dict):
        params = config["strategies"]["toxicity_mm"]
        self.vpin_threshold = params["vpin_threshold"]
        self.spread_multiplier = params["spread_multiplier"]
        self.kelly_fraction = params["kelly_fraction"]
        self.timeframes = params["timeframes"]
        self.max_position = params["max_position"]
        self.base_spread_bps = 5  # 5 bps base spread
        self.position_risk_limit = 0.1  # 10% of max position

        # Track positions
        self.positions = {}  # market_id -> dict( YES: size, NO: size, avg_price)
        self.recent_trades = []  # for VPIN calc

    def calculate_vpin(self, orderbook: OrderBook, timeframe_seconds: int = 60) -> float:
        """Volume-Order Imbalance over recent trades."""
        # For simplicity, use order book imbalance as proxy for VPIN
        # In production, would aggregate trade flow
        yes_imb = PolymarketData.calculate_imbalance(orderbook, "YES")
        no_imb = PolymarketData.calculate_imbalance(orderbook, "NO")
        # Average absolute imbalance
        vpin = (abs(yes_imb) + abs(no_imb)) / 2
        return vpin

    def get_optimal_spread(self, volatility_estimate: float, vpin: float) -> float:
        """Dynamic spread: wider when toxic or volatile."""
        base = self.base_spread_bps / 10000.0  # convert bps to decimal
        if vpin > self.vpin_threshold:
            multiplier = self.spread_multiplier * 2  # step back significantly
        else:
            multiplier = 1.0
        spread = base * multiplier * (1 + volatility_estimate * 10)
        return spread

    def generate_quotes(self, market_id: str, orderbook: OrderBook) -> Tuple[Optional[MMQuote], Optional[MMQuote]]:
        """Generate bid/ask quotes for both YES and NO outcomes if conditions are favorable."""
        vpin = self.calculate_vpin(orderbook)
        if vpin > self.vpin_threshold:
            return None, None  # Step back, do not quote

        # Estimate volatility from recent price moves (using mid prices)
        mid_yes = PolymarketData.mid_price(orderbook, "YES")
        mid_no = PolymarketData.mid_price(orderbook, "NO")

        if mid_yes == 0 or mid_no == 0:
            return None, None

        # For a complete MM, we'd quote both sides of both tokens.
        # For MVP, we'll quote the more liquid token (typically YES)
        spread = self.get_optimal_spread(0.02, vpin)  # placeholder volatility 2%

        bid_price = mid_yes * (1 - spread/2)
        ask_price = mid_yes * (1 + spread/2)

        # Size based on kelly: size = capital * fraction / (spread in price units)
        spread_price_units = (ask_price - bid_price)
        if spread_price_units == 0:
            return None, None

        size = (self.kelly_fraction * 1000) / spread_price_units  # $1k risk capital example
        size = min(size, self.max_position)
        size = max(size, 1.0)

        quote = MMQuote(
            market_id=market_id,
            outcome="YES",
            bid_price=round(bid_price, 4),
            ask_price=round(ask_price, 4),
            bid_size=round(size, 2),
            ask_size=round(size, 2),
            reason=f"VPIN={vpin:.2f}|spread={spread:.3%}"
        )
        return quote, None  # yes_quote, no_quote

    def update_position(self, market_id: str, outcome: str, executed_price: float, size: float, is_buy: bool):
        """Update inventory after trade."""
        if market_id not in self.positions:
            self.positions[market_id] = {"YES": {"size": 0, "avg": 0}, "NO": {"size": 0, "avg": 0}}
        pos = self.positions[market_id][outcome]
        if is_buy:
            # Adding to long position
            total_cost = pos["size"] * pos["avg"] + size * executed_price
            pos["size"] += size
            pos["avg"] = total_cost / pos["size"] if pos["size"] > 0 else 0
        else:
            # Reducing position (or shorting — in production handle short separately)
            pos["size"] -= size
            if pos["size"] < 0:
                # Simple handling: negative means short, avg remains
                pass
            else:
                # Reduce average cost
                if pos["size"] == 0:
                    pos["avg"] = 0
                else:
                    total_cost = pos["size"] * pos["avg"] - size * executed_price
                    pos["avg"] = total_cost / pos["size"]

    def inventory_adjust_price(self, mid_price: float, inventory: float, max_inventory: float) -> float:
        """Skew quotes based on inventory (risk aversion)."""
        theta = 0.1  # inventory risk coefficient
        inventory_ratio = inventory / max_inventory if max_inventory > 0 else 0
        adjustment = -theta * inventory_ratio * mid_price
        return mid_price + adjustment

if __name__ == "__main__":
    import yaml
    cfg = yaml.safe_load(open("config.yaml"))
    mm = ToxicityMM(cfg)
    print("Toxicity Market Maker initialized.")