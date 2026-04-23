"""
Toxicity-Aware Market Making for 5/15-minute markets.
Provides liquidity only when the book is structurally sane.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from book_quality import BookQuality
from market_context import MarketContext
from market_data import OrderBook, PolymarketData
from tradeability_policy import assess_tradeability, tradeability_policy


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
        self.config = config
        params = config["strategies"]["toxicity_mm"]
        execution_cfg = config.get("execution", {})
        self.vpin_threshold = params["vpin_threshold"]
        self.spread_multiplier = params["spread_multiplier"]
        self.kelly_fraction = params["kelly_fraction"]
        self.timeframes = params["timeframes"]
        self.max_position = params["max_position"]
        self.paper_max_notional_usd = float(execution_cfg.get("mm_paper_max_notional_usd", 5.0))
        self.base_spread_bps = 5
        self.position_risk_limit = 0.1
        self.positions = {}
        self.recent_trades = []

    def calculate_vpin(self, orderbook: OrderBook, timeframe_seconds: int = 60) -> float:
        yes_imb = PolymarketData.calculate_imbalance(orderbook, orderbook.outcome_labels[0])
        # Guard: single-outcome market has no second label
        no_label = orderbook.outcome_labels[1] if len(orderbook.outcome_labels) > 1 else orderbook.outcome_labels[0]
        no_imb = PolymarketData.calculate_imbalance(orderbook, no_label)
        return (abs(yes_imb) + abs(no_imb)) / 2

    def assess_book(self, orderbook: OrderBook, outcome: str = "YES") -> BookQuality:
        return assess_tradeability(self.config, "toxicity_mm", orderbook, outcome)

    def get_optimal_spread(self, volatility_estimate: float, vpin: float, quality: BookQuality) -> float:
        base = self.base_spread_bps / 10000.0
        multiplier = self.spread_multiplier if vpin <= self.vpin_threshold else self.spread_multiplier * 2
        policy = tradeability_policy(self.config, "toxicity_mm")
        spread_penalty = max(1.0, quality.spread_bps / max(policy.max_spread_bps, 1.0))
        return base * multiplier * spread_penalty * (1 + volatility_estimate * 10)

    def generate_quotes(
        self,
        market_id: str,
        orderbook: OrderBook,
        *,
        preferred_outcome: str | None = None,
        context: MarketContext | None = None,
    ) -> Tuple[Optional[MMQuote], Optional[MMQuote], BookQuality]:
        # Pick outcome: use preferred_outcome if provided and valid, else default to outcome_labels[0]
        if preferred_outcome is not None and preferred_outcome in orderbook.outcome_labels:
            primary_outcome = preferred_outcome
        else:
            primary_outcome = orderbook.outcome_labels[0]
        quality = self.assess_book(orderbook, primary_outcome)
        inventory = self._inventory_for(market_id, primary_outcome)
        if context is not None and context.seconds_to_expiry < 30.0:
            return self._endgame_quote(market_id, primary_outcome, orderbook, quality, inventory)

        if not quality.is_tradeable:
            return None, None, quality

        vpin = self.calculate_vpin(orderbook)
        if vpin > self.vpin_threshold:
            quality.reasons.append("high_vpin")
            quality.is_tradeable = False
            return None, None, quality

        mid_yes = PolymarketData.mid_price(orderbook, primary_outcome)
        if mid_yes == 0:
            quality.reasons.append("missing_mid")
            quality.is_tradeable = False
            return None, None, quality

        spread = self.get_optimal_spread(0.02, vpin, quality)
        reason_parts = [f"VPIN={vpin:.2f}", f"book_spread_bps={quality.spread_bps:.1f}"]
        if context is not None:
            tte_spread_mult = self._tte_spread_multiplier(context.seconds_to_expiry)
            spread *= tte_spread_mult
            reason_parts.append(f"tte={context.seconds_to_expiry:.1f}s")
            reason_parts.append(f"tte_spread_mult={tte_spread_mult:.1f}")
        bid_price = mid_yes * (1 - spread / 2)
        ask_price = mid_yes * (1 + spread / 2)

        if context is not None and inventory != 0:
            bid_price = self.inventory_adjust_price(bid_price, inventory, self.max_position)
            ask_price = self.inventory_adjust_price(ask_price, -inventory, self.max_position)
            reason_parts.append(f"inventory_skew={inventory:.2f}")

        if context is not None:
            bid_price = self._clamp_price(bid_price)
            ask_price = self._clamp_price(ask_price)
        spread_price_units = ask_price - bid_price
        if spread_price_units <= 0:
            quality.reasons.append("non_positive_quote_spread")
            quality.is_tradeable = False
            return None, None, quality

        size = (self.kelly_fraction * 1000) / spread_price_units
        size = min(size, self.max_position, self.paper_max_notional_usd / max(mid_yes, 1e-9))
        size = max(size, 1.0)
        bid_size = size
        ask_size = size
        if context is not None:
            tte_size_mult = max(0.0, 0.4 + 0.6 * float(context.tte_pct))
            bid_size = max(bid_size * tte_size_mult, 0.01)
            ask_size = max(ask_size * tte_size_mult, 0.01)
            reason_parts.append(f"tte_size_mult={tte_size_mult:.2f}")
            if context.imbalance_yes > 0.6:
                bid_size *= 0.75
                ask_size *= 1.25
                reason_parts.append("imbalance_fade=ask_heavier")
            elif context.imbalance_yes < -0.6:
                bid_size *= 1.25
                ask_size *= 0.75
                reason_parts.append("imbalance_fade=bid_heavier")
        reason_parts.append(f"quote_spread={spread:.3%}")
        quote = MMQuote(
            market_id=market_id,
            outcome=primary_outcome,
            bid_price=round(bid_price, 4),
            ask_price=round(ask_price, 4),
            bid_size=round(bid_size, 2),
            ask_size=round(ask_size, 2),
            reason="|".join(reason_parts),
            book_quality=quality.to_dict(),
        )
        return quote, None, quality

    def _inventory_for(self, market_id: str, outcome: str) -> float:
        try:
            pos = self.positions.get(market_id, {}).get(outcome, {})
            return float(pos.get("size", 0.0) or 0.0)
        except (AttributeError, TypeError, ValueError):
            return 0.0

    def _tte_spread_multiplier(self, seconds_to_expiry: float) -> float:
        tte = float(seconds_to_expiry)
        if tte > 180.0:
            return 1.0
        if tte >= 60.0:
            return 1.3
        if tte >= 30.0:
            return 1.8
        return 3.0

    def _clamp_price(self, price: float) -> float:
        return max(0.01, min(float(price), 0.99))

    def _endgame_quote(
        self,
        market_id: str,
        outcome: str,
        orderbook: OrderBook,
        quality: BookQuality,
        inventory: float,
    ) -> Tuple[Optional[MMQuote], Optional[MMQuote], BookQuality]:
        tick = 0.01
        if inventory == 0:
            quality.reasons.append("endgame_no_new_orders")
            return None, None, quality

        best_bid = PolymarketData.best_bid(orderbook, outcome)
        best_ask = PolymarketData.best_ask(orderbook, outcome)

        flatten_size = round(abs(inventory), 2)
        if inventory > 0:
            if best_bid <= 0:
                quality.reasons.append("endgame_missing_touch")
                quality.is_tradeable = False
                return None, None, quality
            # Long inventory must flatten by selling with a marketable price.  A sell
            # limit at or below the best bid crosses the book instead of resting.
            aggressive_ask = self._clamp_price(best_bid - tick)
            quote = MMQuote(
                market_id=market_id,
                outcome=outcome,
                bid_price=0.0,
                ask_price=round(aggressive_ask, 4),
                bid_size=0.0,
                ask_size=flatten_size,
                reason=f"endgame_flatten|inventory={inventory:.2f}|side=sell",
                book_quality=quality.to_dict(),
            )
        else:
            if best_ask <= 0:
                quality.reasons.append("endgame_missing_touch")
                quality.is_tradeable = False
                return None, None, quality
            # Short inventory must flatten by buying with a marketable price.  A buy
            # limit at or above the best ask crosses the book instead of resting.
            aggressive_bid = self._clamp_price(best_ask + tick)
            quote = MMQuote(
                market_id=market_id,
                outcome=outcome,
                bid_price=round(aggressive_bid, 4),
                ask_price=0.0,
                bid_size=flatten_size,
                ask_size=0.0,
                reason=f"endgame_flatten|inventory={inventory:.2f}|side=buy",
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
