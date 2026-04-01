"""
Toxicity-Aware Market Making for 5/15-minute markets.
Provides liquidity only when the book is structurally sane.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from book_quality import BookQuality, assess_book_quality
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
    book_quality: Dict


class ToxicityMM:
    def __init__(self, config: dict):
        params = config["strategies"]["toxicity_mm"]
        execution_cfg = config.get("execution", {})
        filters = config.get("filters", {})
        self.vpin_threshold = params["vpin_threshold"]
        self.spread_multiplier = params["spread_multiplier"]
        self.kelly_fraction = params["kelly_fraction"]
        self.timeframes = params["timeframes"]
        self.max_position = params["max_position"]
        self.paper_max_notional_usd = float(execution_cfg.get("mm_paper_max_notional_usd", 5.0))
        self.base_spread_bps = 5
        self.position_risk_limit = 0.1
        self.max_book_spread_bps = filters.get("max_book_spread_bps", 250)
        self.min_top_depth = filters.get("min_top_depth", 5)
        self.min_top_notional = filters.get("min_top_notional", 1)
        self.max_depth_ratio = filters.get("max_depth_ratio", 12)
        self.positions = {}
        self.recent_trades = []

    def calculate_vpin(self, orderbook: OrderBook, timeframe_seconds: int = 60) -> float:
        yes_imb = PolymarketData.calculate_imbalance(orderbook, orderbook.outcome_labels[0])
        no_imb = PolymarketData.calculate_imbalance(orderbook, orderbook.outcome_labels[1])
        return (abs(yes_imb) + abs(no_imb)) / 2

    def assess_book(self, orderbook: OrderBook, outcome: str = "YES") -> BookQuality:
        return assess_book_quality(
            orderbook,
            outcome,
            max_spread_bps=self.max_book_spread_bps,
            min_top_depth=self.min_top_depth,
            min_top_notional=self.min_top_notional,
            max_depth_ratio=self.max_depth_ratio,
        )

    def get_optimal_spread(self, volatility_estimate: float, vpin: float, quality: BookQuality) -> float:
        base = self.base_spread_bps / 10000.0
        multiplier = self.spread_multiplier if vpin <= self.vpin_threshold else self.spread_multiplier * 2
        spread_penalty = max(1.0, quality.spread_bps / max(self.max_book_spread_bps, 1.0))
        return base * multiplier * spread_penalty * (1 + volatility_estimate * 10)

    def generate_quotes(self, market_id: str, orderbook: OrderBook) -> Tuple[Optional[MMQuote], Optional[MMQuote], BookQuality]:
        primary_outcome = orderbook.outcome_labels[0]
        quality = self.assess_book(orderbook, primary_outcome)
        if not quality.is_tradeable:
            return None, None, quality

        vpin = self.calculate_vpin(orderbook)
        if vpin > self.vpin_threshold:
            quality.reasons.append("high_vpin")
            quality.is_tradeable = False
            return None, None, quality

        mid_yes = PolymarketData.mid_price(orderbook, primary_outcome)
        mid_no = PolymarketData.mid_price(orderbook, orderbook.outcome_labels[1])
        if mid_yes == 0 or mid_no == 0:
            quality.reasons.append("missing_mid")
            quality.is_tradeable = False
            return None, None, quality

        spread = self.get_optimal_spread(0.02, vpin, quality)
        bid_price = mid_yes * (1 - spread / 2)
        ask_price = mid_yes * (1 + spread / 2)
        spread_price_units = ask_price - bid_price
        if spread_price_units <= 0:
            quality.reasons.append("non_positive_quote_spread")
            quality.is_tradeable = False
            return None, None, quality

        size = (self.kelly_fraction * 1000) / spread_price_units
        size = min(size, self.max_position, self.paper_max_notional_usd / max(mid_yes, 1e-9))
        size = max(size, 1.0)
        quote = MMQuote(
            market_id=market_id,
            outcome=primary_outcome,
            bid_price=round(bid_price, 4),
            ask_price=round(ask_price, 4),
            bid_size=round(size, 2),
            ask_size=round(size, 2),
            reason=f"VPIN={vpin:.2f}|book_spread_bps={quality.spread_bps:.1f}|quote_spread={spread:.3%}",
            book_quality=quality.to_dict(),
        )
        return quote, None, quality

    def update_position(self, market_id: str, outcome: str, executed_price: float, size: float, is_buy: bool):
        if market_id not in self.positions:
            self.positions[market_id] = {}
        pos = self.positions[market_id].setdefault(outcome, {"size": 0, "avg": 0})
        if is_buy:
            total_cost = pos["size"] * pos["avg"] + size * executed_price
            pos["size"] += size
            pos["avg"] = total_cost / pos["size"] if pos["size"] > 0 else 0
        else:
            pos["size"] -= size
            if pos["size"] <= 0:
                pos["avg"] = 0

    def inventory_adjust_price(self, mid_price: float, inventory: float, max_inventory: float) -> float:
        theta = 0.1
        inventory_ratio = inventory / max_inventory if max_inventory > 0 else 0
        adjustment = -theta * inventory_ratio * mid_price
        return mid_price + adjustment


if __name__ == "__main__":
    import yaml
    cfg = yaml.safe_load(open("config.yaml"))
    mm = ToxicityMM(cfg)
    print("Toxicity Market Maker initialized.")
