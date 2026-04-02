from __future__ import annotations

from collections import defaultdict
from typing import Any


def build_exposure_snapshot(projection) -> dict[str, Any]:
    by_strategy: dict[str, dict[str, float]] = defaultdict(lambda: _bucket())
    by_market: dict[str, dict[str, float]] = defaultdict(lambda: _bucket())
    by_slot: dict[str, dict[str, float]] = defaultdict(lambda: _bucket())
    by_asset: dict[str, dict[str, float]] = defaultdict(lambda: _bucket())
    by_interval: dict[str, dict[str, float]] = defaultdict(lambda: _bucket())

    gross_position_exposure = 0.0
    gross_open_order_exposure = 0.0
    reserved_buy_order_notional = 0.0
    pending_settlement_exposure = 0.0
    open_position_count = 0
    pending_settlement_count = len(projection.pending_slots)

    for position in projection.positions.values():
        quantity = float(position.get("quantity", 0.0))
        if abs(quantity) <= 1e-9:
            continue
        open_position_count += 1
        slot_id = position.get("slot_id")
        market_id = position.get("market_id")
        strategy = position.get("strategy_family", "unknown")
        exposure = _position_exposure(quantity, float(position.get("average_price", 0.0)))
        gross_position_exposure += exposure
        by_strategy[strategy]["position_exposure"] += exposure
        if market_id:
            by_market[market_id]["position_exposure"] += exposure
        if slot_id:
            by_slot[slot_id]["position_exposure"] += exposure
            asset, interval = _parse_slot_id(slot_id)
            if asset:
                by_asset[asset]["position_exposure"] += exposure
            if interval:
                by_interval[interval]["position_exposure"] += exposure
            if slot_id in projection.pending_slots:
                pending_settlement_exposure += exposure

    for order_id in projection.open_orders:
        order = projection.orders.get(order_id)
        if not order:
            continue
        remaining_qty = float(order.get("remaining_qty", order.get("remaining_size", 0.0)))
        if remaining_qty <= 1e-9:
            continue
        price = float(order.get("price", 0.0))
        order_exposure = remaining_qty * price
        gross_open_order_exposure += order_exposure
        strategy = order.get("strategy_family", "unknown")
        market_id = order.get("market_id")
        slot_id = order.get("slot_id")
        by_strategy[strategy]["open_order_exposure"] += order_exposure
        if market_id:
            by_market[market_id]["open_order_exposure"] += order_exposure
        if slot_id:
            by_slot[slot_id]["open_order_exposure"] += order_exposure
            asset, interval = _parse_slot_id(slot_id)
            if asset:
                by_asset[asset]["open_order_exposure"] += order_exposure
            if interval:
                by_interval[interval]["open_order_exposure"] += order_exposure
        if str(order.get("side", "")).upper() == "BUY":
            reserved_buy_order_notional += order_exposure

    return {
        "open_position_count": open_position_count,
        "open_order_count": len(projection.open_orders),
        "gross_position_exposure": round(gross_position_exposure, 6),
        "gross_open_order_exposure": round(gross_open_order_exposure, 6),
        "reserved_buy_order_notional": round(reserved_buy_order_notional, 6),
        "pending_settlement_exposure": round(pending_settlement_exposure, 6),
        "pending_settlement_count": pending_settlement_count,
        "total_gross_exposure": round(gross_position_exposure + gross_open_order_exposure, 6),
        "by_strategy_family": _finalize(by_strategy),
        "by_market_id": _finalize(by_market),
        "by_slot_id": _finalize(by_slot),
        "by_asset": _finalize(by_asset),
        "by_interval": _finalize(by_interval),
    }


def _position_exposure(quantity: float, average_price: float) -> float:
    if quantity >= 0:
        return abs(quantity) * average_price
    return abs(quantity) * max(0.0, 1.0 - average_price)


def _parse_slot_id(slot_id: str | None) -> tuple[str | None, str | None]:
    if not slot_id:
        return None, None
    parts = str(slot_id).split(":")
    if len(parts) < 2:
        return None, None
    asset = parts[0]
    interval = parts[1]
    return asset, interval


def _bucket() -> dict[str, float]:
    return {"position_exposure": 0.0, "open_order_exposure": 0.0, "total_exposure": 0.0}


def _finalize(values: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    finalized: dict[str, dict[str, float]] = {}
    for key, bucket in values.items():
        total = float(bucket["position_exposure"] + bucket["open_order_exposure"])
        finalized[key] = {
            "position_exposure": round(float(bucket["position_exposure"]), 6),
            "open_order_exposure": round(float(bucket["open_order_exposure"]), 6),
            "total_exposure": round(total, 6),
        }
    return finalized
