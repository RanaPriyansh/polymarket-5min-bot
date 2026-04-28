from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from market_data import OrderBook


@dataclass(frozen=True)
class ComplementDislocation:
    strategy_family: str
    strategy_variant: str
    slot_id: str
    market_slug: str
    direction: str
    yes_price: float
    no_price: float
    combined_price: float
    edge_after_fees: float
    executable_size: float
    action: str = "scanner_only"

    def to_event(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["event_type"] = "scanner.opportunity"
        return payload


class ComplementDislocationScanner:
    strategy_family = "complement_dislocation"

    def __init__(self, config: dict):
        params = config.get("strategies", {}).get("complement_dislocation", {}) or {}
        self.safety_margin = float(params.get("safety_margin", 0.01))
        self.fee_buffer = float(params.get("fee_buffer", 0.0))

    def evaluate(self, market: dict[str, Any], orderbook: OrderBook) -> list[ComplementDislocation]:
        opportunities: list[ComplementDislocation] = []
        if not orderbook.yes_asks or not orderbook.no_asks or not orderbook.yes_bids or not orderbook.no_bids:
            return opportunities

        yes_ask, yes_ask_size = orderbook.yes_asks[0]
        no_ask, no_ask_size = orderbook.no_asks[0]
        ask_sum = yes_ask + no_ask
        buy_edge = 1.0 - ask_sum - self.fee_buffer - self.safety_margin
        if buy_edge > 0:
            opportunities.append(
                ComplementDislocation(
                    strategy_family=self.strategy_family,
                    strategy_variant="yes_no_ask_sum_lt_one",
                    slot_id=market.get("slot_id", ""),
                    market_slug=market.get("slug", ""),
                    direction="buy_yes_buy_no",
                    yes_price=yes_ask,
                    no_price=no_ask,
                    combined_price=ask_sum,
                    edge_after_fees=buy_edge,
                    executable_size=min(yes_ask_size, no_ask_size),
                )
            )

        yes_bid, yes_bid_size = orderbook.yes_bids[0]
        no_bid, no_bid_size = orderbook.no_bids[0]
        bid_sum = yes_bid + no_bid
        sell_edge = bid_sum - 1.0 - self.fee_buffer - self.safety_margin
        if sell_edge > 0:
            opportunities.append(
                ComplementDislocation(
                    strategy_family=self.strategy_family,
                    strategy_variant="yes_no_bid_sum_gt_one",
                    slot_id=market.get("slot_id", ""),
                    market_slug=market.get("slug", ""),
                    direction="sell_yes_sell_no_if_supported",
                    yes_price=yes_bid,
                    no_price=no_bid,
                    combined_price=bid_sum,
                    edge_after_fees=sell_edge,
                    executable_size=min(yes_bid_size, no_bid_size),
                )
            )
        return opportunities
