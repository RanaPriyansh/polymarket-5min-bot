"""MarketContext: human-grade per-market per-loop snapshot.

Bundles every variable a human trader would have on the screen for a 5m/15m
crypto up/down market: TTE, slot age, mid prices, book imbalance, spot price,
spot move since slot start, and mid-momentum.

Pure data layer — strategies consume it but no strategy wires yet (Task 2).
"""
from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional, Tuple

from market_data import OrderBook, PolymarketData

logger = logging.getLogger(__name__)

# Mid-history cap for diagnostics / momentum window.
MID_HISTORY_MAXLEN = 30
MOMENTUM_WINDOW = 10  # last N mids used for slope calc


@dataclass
class MarketContext:
    """Snapshot of a single market at a single loop tick."""
    market_id: str
    slot_id: str
    asset: str
    interval_minutes: int
    outcome_labels: List[str]

    now_ts: float
    end_ts: float
    seconds_to_expiry: float
    slot_age_seconds: float
    tte_pct: float
    tte_bucket: str

    mid_price_yes: float
    mid_price_no: float
    best_bid_yes: float
    best_ask_yes: float
    book_spread_bps: float
    top_depth_yes: float
    top_depth_no: float
    imbalance_yes: float

    last_trade_price: float
    spot_price: Optional[float]
    spot_move_pct_window: float
    momentum_score: float

    recent_mid_history: List[float] = field(default_factory=list)


def tte_bucket_from_seconds(seconds_to_expiry: float) -> str:
    """Bucket by time-to-expiry.

    Thresholds per plan spec:
      early   > 180s
      mid     60-180s   (inclusive of 180, inclusive of 60)
      late    30-60s    (inclusive of 30, exclusive of 60)
      endgame < 30s
    """
    s = max(0.0, float(seconds_to_expiry))
    if s > 180.0:
        return "early"
    if s > 60.0:
        return "mid"
    if s > 30.0:
        return "late"
    return "endgame"


def _top_depth(levels: List[Tuple[float, float]], n: int = 5) -> float:
    return float(sum(size for _, size in levels[:n]))


def _compute_spread_bps(best_bid: float, best_ask: float) -> float:
    if best_bid <= 0 or best_ask <= 0:
        return 0.0
    mid = (best_bid + best_ask) / 2.0
    if mid <= 0:
        return 0.0
    return ((best_ask - best_bid) / mid) * 10_000.0


def _linear_slope_score(samples: List[float]) -> float:
    """Signed slope of the last samples, normalized roughly to [-1, 1].

    Uses least-squares slope over integer time index (0..n-1); then scales by
    a soft factor so a consistent +0.01-per-step uptrend in a 0..1 probability
    market returns a clearly positive but bounded value.

    Returns 0.0 if fewer than 2 samples.
    """
    n = len(samples)
    if n < 2:
        return 0.0
    # classic closed-form OLS slope, x = 0..n-1
    x_mean = (n - 1) / 2.0
    y_mean = sum(samples) / n
    num = 0.0
    den = 0.0
    for i, y in enumerate(samples):
        dx = i - x_mean
        num += dx * (y - y_mean)
        den += dx * dx
    if den <= 0:
        return 0.0
    slope = num / den  # change in mid-price per step
    # Normalize: a probability mid moves in [0,1]; 0.01 per step is a strong
    # trend, so multiply by 100 and clamp.
    score = slope * 100.0
    if score > 1.0:
        score = 1.0
    elif score < -1.0:
        score = -1.0
    return float(score)


def _resolve_asset_and_interval(market: dict, orderbook: OrderBook) -> Tuple[str, int, str]:
    """Derive (asset, interval_minutes, slot_id) from market dict, falling back
    to parsing the slug (same rules as market_data.PolymarketData.parse_slug).
    """
    asset = str(market.get("asset") or "") or None
    interval_minutes = market.get("interval_minutes")
    slot_id = market.get("slot_id") or orderbook.slot_id or ""
    slug = market.get("slug") or orderbook.market_slug or ""

    if not asset or not interval_minutes:
        parsed_asset, parsed_interval, parsed_slot_ts = PolymarketData.parse_slug(slug)
        asset = asset or parsed_asset or ""
        interval_minutes = interval_minutes or parsed_interval or 0
        if not slot_id and parsed_asset and parsed_interval and parsed_slot_ts:
            slot_id = f"{parsed_asset}:{parsed_interval}:{parsed_slot_ts}"

    try:
        interval_minutes_int = int(interval_minutes)
    except (TypeError, ValueError):
        interval_minutes_int = 0

    return str(asset or ""), interval_minutes_int, str(slot_id or "")


def build_market_context(
    market: dict,
    orderbook: OrderBook,
    now_ts: float,
    spot_provider: Callable[[str], Optional[float]],
    mid_history_store: Dict[Tuple[str, str], Deque[float]],
    slot_spot_anchor_store: Dict[Tuple[str, str], float],
) -> MarketContext:
    """Assemble a MarketContext from market + orderbook + live spot.

    Mutates mid_history_store (appends current mid_yes, capped to 30 via deque
    maxlen) and slot_spot_anchor_store (writes first-seen spot for (asset,
    slot_id) and never overwrites).
    """
    asset, interval_minutes, slot_id = _resolve_asset_and_interval(market, orderbook)
    window_seconds = float(interval_minutes * 60) if interval_minutes > 0 else 0.0

    end_ts = float(market.get("end_ts") or orderbook.end_ts or 0.0)
    raw_seconds_to_expiry = end_ts - float(now_ts)
    seconds_to_expiry = max(0.0, raw_seconds_to_expiry)

    if window_seconds > 0.0:
        slot_age_seconds = max(0.0, window_seconds - seconds_to_expiry)
        tte_pct = max(0.0, min(1.0, seconds_to_expiry / window_seconds))
    else:
        slot_age_seconds = 0.0
        tte_pct = 0.0

    tte_bucket = tte_bucket_from_seconds(seconds_to_expiry)

    # Prices / book features (use helpers on PolymarketData for consistency).
    outcome_labels = list(orderbook.outcome_labels)
    yes_label = outcome_labels[0] if outcome_labels else "YES"
    no_label = outcome_labels[1] if len(outcome_labels) > 1 else "NO"

    best_bid_yes = PolymarketData.best_bid(orderbook, yes_label)
    best_ask_yes = PolymarketData.best_ask(orderbook, yes_label)
    mid_price_yes = PolymarketData.mid_price(orderbook, yes_label)
    mid_price_no = PolymarketData.mid_price(orderbook, no_label)
    imbalance_yes = PolymarketData.calculate_imbalance(orderbook, yes_label)

    book_spread_bps = _compute_spread_bps(best_bid_yes, best_ask_yes)
    top_depth_yes = _top_depth(orderbook.yes_bids) + _top_depth(orderbook.yes_asks)
    top_depth_no = _top_depth(orderbook.no_bids) + _top_depth(orderbook.no_asks)

    last_trade_price = float(market.get("last_trade_price") or mid_price_yes or 0.0)

    # Spot
    spot_price: Optional[float] = None
    try:
        if asset:
            spot_price = spot_provider(asset)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("spot_provider raised for asset=%s: %s", asset, exc)
        spot_price = None

    anchor_key = (asset, slot_id)
    if spot_price is not None and anchor_key not in slot_spot_anchor_store:
        slot_spot_anchor_store[anchor_key] = float(spot_price)

    spot_move_pct_window = 0.0
    anchor = slot_spot_anchor_store.get(anchor_key)
    if anchor is not None and anchor > 0.0 and spot_price is not None:
        spot_move_pct_window = ((float(spot_price) - anchor) / anchor) * 100.0

    # Mid history / momentum
    mid_key = (orderbook.market_id, yes_label)
    history = mid_history_store.get(mid_key)
    if history is None:
        history = deque(maxlen=MID_HISTORY_MAXLEN)
        mid_history_store[mid_key] = history
    if mid_price_yes > 0.0:
        history.append(float(mid_price_yes))

    recent = list(history)
    momentum_samples = recent[-MOMENTUM_WINDOW:]
    momentum_score = _linear_slope_score(momentum_samples)

    return MarketContext(
        market_id=str(market.get("id") or orderbook.market_id or ""),
        slot_id=slot_id,
        asset=asset,
        interval_minutes=interval_minutes,
        outcome_labels=outcome_labels,
        now_ts=float(now_ts),
        end_ts=end_ts,
        seconds_to_expiry=seconds_to_expiry,
        slot_age_seconds=slot_age_seconds,
        tte_pct=tte_pct,
        tte_bucket=tte_bucket,
        mid_price_yes=mid_price_yes,
        mid_price_no=mid_price_no,
        best_bid_yes=best_bid_yes,
        best_ask_yes=best_ask_yes,
        book_spread_bps=book_spread_bps,
        top_depth_yes=top_depth_yes,
        top_depth_no=top_depth_no,
        imbalance_yes=imbalance_yes,
        last_trade_price=last_trade_price,
        spot_price=spot_price,
        spot_move_pct_window=spot_move_pct_window,
        momentum_score=momentum_score,
        recent_mid_history=recent,
    )
