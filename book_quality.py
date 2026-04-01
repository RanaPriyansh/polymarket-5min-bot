from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple

from market_data import OrderBook


@dataclass
class BookQuality:
    outcome: str
    best_bid: float
    best_ask: float
    mid_price: float
    spread: float
    spread_bps: float
    top_depth: float
    top_notional: float
    depth_ratio: float
    is_tradeable: bool
    reasons: List[str]

    def to_dict(self) -> Dict:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        return payload


def _levels(ob: OrderBook, outcome: str) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
    if outcome == "YES":
        return ob.yes_bids, ob.yes_asks
    return ob.no_bids, ob.no_asks


def assess_book_quality(ob: OrderBook, outcome: str = "YES", *, max_spread_bps: float = 250.0,
                        min_top_depth: float = 25.0, min_top_notional: float = 10.0,
                        max_depth_ratio: float = 12.0) -> BookQuality:
    bids, asks = _levels(ob, outcome)
    best_bid = bids[0][0] if bids else 0.0
    best_ask = asks[0][0] if asks else 0.0
    bid_size = bids[0][1] if bids else 0.0
    ask_size = asks[0][1] if asks else 0.0
    reasons: List[str] = []

    if best_bid <= 0 or best_ask <= 0:
        reasons.append("missing_side")
        return BookQuality(
            outcome=outcome,
            best_bid=best_bid,
            best_ask=best_ask,
            mid_price=0.0,
            spread=0.0,
            spread_bps=0.0,
            top_depth=bid_size + ask_size,
            top_notional=0.0,
            depth_ratio=0.0,
            is_tradeable=False,
            reasons=reasons,
        )

    if best_ask <= best_bid:
        reasons.append("crossed_book")

    mid = (best_bid + best_ask) / 2.0
    spread = max(best_ask - best_bid, 0.0)
    spread_bps = (spread / mid) * 10000.0 if mid > 0 else 0.0
    top_depth = bid_size + ask_size
    top_notional = top_depth * mid
    min_depth = min(bid_size, ask_size)
    max_depth = max(bid_size, ask_size)
    depth_ratio = (max_depth / min_depth) if min_depth > 0 else float("inf")

    if spread_bps > max_spread_bps:
        reasons.append(f"wide_spread>{max_spread_bps}")
    if top_depth < min_top_depth:
        reasons.append(f"thin_depth<{min_top_depth}")
    if top_notional < min_top_notional:
        reasons.append(f"thin_notional<{min_top_notional}")
    if depth_ratio > max_depth_ratio:
        reasons.append(f"imbalanced_depth>{max_depth_ratio}")

    return BookQuality(
        outcome=outcome,
        best_bid=best_bid,
        best_ask=best_ask,
        mid_price=mid,
        spread=spread,
        spread_bps=spread_bps,
        top_depth=top_depth,
        top_notional=top_notional,
        depth_ratio=depth_ratio,
        is_tradeable=not reasons,
        reasons=reasons,
    )
