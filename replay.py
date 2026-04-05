from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from exposure import build_exposure_snapshot
from ledger import LedgerEvent


@dataclass
class ReplayProjection:
    orders: dict[str, dict[str, Any]] = field(default_factory=dict)
    open_orders: set[str] = field(default_factory=set)
    pending_slots: dict[str, dict[str, Any]] = field(default_factory=dict)
    settled_slots: dict[str, dict[str, Any]] = field(default_factory=dict)
    positions: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    realized_pnl_total: float = 0.0
    resolved_trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    latest_settlement: dict[str, Any] | None = None
    realized_pnl_timeline: list[dict[str, float]] = field(default_factory=list)
    exposure: dict[str, Any] = field(default_factory=dict)


ORDER_TERMINAL_STATUSES = {"cancelled", "rejected", "expired", "filled"}


def replay_ledger(events: list[LedgerEvent]) -> ReplayProjection:
    projection = ReplayProjection()
    for event in events:
        if event.stream == "order":
            _apply_order_event(projection, event)
        elif event.stream == "market_slot":
            _apply_slot_event(projection, event)
    projection.exposure = build_exposure_snapshot(projection)
    return projection


def realized_pnl_for_day(events: list[LedgerEvent], *, day_start_ts: float, day_end_ts: float) -> float:
    if day_end_ts <= day_start_ts:
        return 0.0
    prior_projection = replay_ledger([event for event in events if event.event_ts < day_start_ts])
    window_projection = replay_ledger([event for event in events if event.event_ts <= day_end_ts])
    return float(window_projection.realized_pnl_total - prior_projection.realized_pnl_total)


def utc_day_bounds(now_ts: float) -> tuple[float, float]:
    dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(day=start.day)  # placeholder for type stability
    return start.timestamp(), start.timestamp() + 86400.0


def _apply_order_event(projection: ReplayProjection, event: LedgerEvent) -> None:
    order_id = event.aggregate_id
    state = projection.orders.setdefault(
        order_id,
        {
            "order_id": order_id,
            "status": "unknown",
            "size": 0.0,
            "filled_qty": 0.0,
            "remaining_qty": 0.0,
            "average_fill_price": 0.0,
        },
    )

    if event.event_type == "order_created":
        state.update(event.payload)
        state["status"] = "created"
        state["size"] = float(event.payload.get("size", 0.0))
        state["filled_qty"] = 0.0
        state["remaining_qty"] = state["size"]
        state["average_fill_price"] = 0.0
        projection.open_orders.add(order_id)
        return

    if event.event_type == "order_acknowledged":
        state.update(event.payload)
        state["status"] = event.payload.get("status", "open")
        projection.open_orders.add(order_id)
        return

    if event.event_type in {"order_cancelled", "order_rejected", "order_expired", "order_filled"}:
        state.update(event.payload)
        state["status"] = event.event_type.replace("order_", "")
        projection.open_orders.discard(order_id)
        return

    if event.event_type == "fill_observed":
        state["last_fill_observation"] = dict(event.payload)
        return

    if event.event_type == "fill_applied":
        fill_payload = dict(event.payload)
        state.update(fill_payload)
        state["filled_qty"] = float(fill_payload.get("filled_qty", state.get("filled_qty", 0.0)))
        state["remaining_qty"] = float(fill_payload.get("remaining_qty", 0.0))
        state["average_fill_price"] = float(fill_payload.get("average_fill_price", state.get("average_fill_price", 0.0)))
        state["status"] = "filled" if state["remaining_qty"] <= 1e-9 else "partially_filled"
        state["last_fill"] = fill_payload
        if state["status"] == "filled":
            projection.open_orders.discard(order_id)
        else:
            projection.open_orders.add(order_id)
        realized = _apply_position_fill(projection, fill_payload)
        _record_realized_event(projection, event, realized)


def _apply_slot_event(projection: ReplayProjection, event: LedgerEvent) -> None:
    slot_id = event.aggregate_id
    if event.event_type == "slot_resolution_pending":
        projection.pending_slots[slot_id] = dict(event.payload)
        return

    if event.event_type == "slot_settled":
        projection.pending_slots.pop(slot_id, None)
        settled_payload = dict(event.payload)
        projection.settled_slots[slot_id] = settled_payload
        projection.latest_settlement = {"slot_id": slot_id, **settled_payload}

        market_id = settled_payload.get("market_id")

        # Settle positions that still have non-zero quantity at this point.
        _settle_positions_for_slot(projection, slot_id, settled_payload, event)

        # Remove ALL positions for this market after settlement.
        # This ensures open_positions counts reflect only truly open (unresolved)
        # positions and not stale zero-quantity entries.
        for k in list(projection.positions.keys()):
            if projection.positions[k].get("market_id") == market_id:
                del projection.positions[k]


def _apply_position_fill(projection: ReplayProjection, payload: dict[str, Any]) -> float:
    key = (
        payload.get("strategy_family", "unknown"),
        payload["market_id"],
        payload["outcome"],
    )
    position = projection.positions.setdefault(
        key,
        {
            "slot_id": payload.get("slot_id"),
            "market_id": payload["market_id"],
            "outcome": payload["outcome"],
            "strategy_family": payload.get("strategy_family", "unknown"),
            "quantity": 0.0,
            "average_price": 0.0,
            "realized_pnl": 0.0,
        },
    )

    signed_size = float(payload["fill_size"])
    if payload["side"].upper() == "SELL":
        signed_size *= -1.0

    realized = _apply_signed_fill(position, signed_size=signed_size, price=float(payload["fill_price"]))
    position["realized_pnl"] += realized
    projection.realized_pnl_total += realized
    return realized


def _settle_positions_for_slot(
    projection: ReplayProjection,
    slot_id: str,
    settled_payload: dict[str, Any],
    event: LedgerEvent,
) -> None:
    market_id = settled_payload.get("market_id")
    winning_outcome = settled_payload.get("winning_outcome")
    settled_ts = settled_payload.get("settled_ts")

    for position in projection.positions.values():
        if position.get("market_id") != market_id:
            continue
        quantity = float(position.get("quantity", 0.0))
        if abs(quantity) <= 1e-9:
            continue

        payout = 1.0 if position["outcome"] == winning_outcome else 0.0
        close_signed_size = -quantity
        realized = _apply_signed_fill(position, signed_size=close_signed_size, price=payout)
        position["realized_pnl"] += realized
        position["settled_slot_id"] = slot_id
        position["settled_ts"] = settled_ts
        position["winning_outcome"] = winning_outcome
        projection.realized_pnl_total += realized
        projection.resolved_trade_count += 1
        if realized >= 0:
            projection.win_count += 1
        else:
            projection.loss_count += 1
        _record_realized_event(projection, event, realized)


def _apply_signed_fill(position: dict[str, Any], *, signed_size: float, price: float) -> float:
    current_qty = float(position.get("quantity", 0.0))
    current_avg = float(position.get("average_price", 0.0))
    realized = 0.0

    if current_qty == 0.0 or current_qty * signed_size > 0:
        new_qty = current_qty + signed_size
        total_cost = (current_avg * abs(current_qty)) + (price * abs(signed_size))
        position["quantity"] = new_qty
        position["average_price"] = total_cost / abs(new_qty) if abs(new_qty) > 1e-9 else 0.0
        return realized

    close_size = min(abs(current_qty), abs(signed_size))
    if current_qty > 0:
        realized = (price - current_avg) * close_size
    else:
        realized = (current_avg - price) * close_size

    remaining_qty = current_qty + signed_size
    if abs(remaining_qty) <= 1e-9:
        position["quantity"] = 0.0
        position["average_price"] = 0.0
    elif current_qty * remaining_qty < 0:
        position["quantity"] = remaining_qty
        position["average_price"] = price
    else:
        position["quantity"] = remaining_qty
    return realized


def _record_realized_event(projection: ReplayProjection, event: LedgerEvent, realized_delta: float) -> None:
    if abs(realized_delta) <= 1e-9:
        return
    projection.realized_pnl_timeline.append(
        {
            "event_ts": float(event.event_ts),
            "recorded_ts": float(event.recorded_ts),
            "realized_delta": float(realized_delta),
            "realized_total": float(projection.realized_pnl_total),
        }
    )
