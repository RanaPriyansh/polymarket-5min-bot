from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

from ledger import LedgerEvent


@dataclass(frozen=True)
class OrderBookSnapshot:
    timestamp: float
    best_bid: float
    best_ask: float


@dataclass(frozen=True)
class FillPolicy:
    min_rest_seconds: float = 1.0
    max_fill_fraction_per_snapshot: float = 0.25
    allow_same_snapshot_fill: bool = False


class ConservativeFillEngine:
    def __init__(self, policy: FillPolicy | None = None):
        self.policy = policy or FillPolicy()

    def observe_fill(
        self,
        order: dict[str, Any],
        snapshot: OrderBookSnapshot,
        *,
        event_id: str | None = None,
        sequence_num: int = 0,
        run_id: str = "paper-run",
        correlation_id: str | None = None,
        causation_id: str | None = None,
        schema_version: int = 1,
    ) -> LedgerEvent | None:
        if not self._is_fill_eligible(order, snapshot):
            return None

        fill_price = snapshot.best_ask if order["side"].upper() == "BUY" else snapshot.best_bid
        remaining_qty = max(0.0, float(order["size"]) - float(order.get("filled_qty", 0.0)))
        max_fill_size = float(order["size"]) * self.policy.max_fill_fraction_per_snapshot
        fill_size = min(remaining_qty, max_fill_size)
        if fill_size <= 1e-9:
            return None

        event_id = event_id or f"evt-{uuid.uuid4().hex}"
        return LedgerEvent(
            event_id=event_id,
            stream="order",
            aggregate_id=order["order_id"],
            sequence_num=sequence_num,
            event_type="fill_observed",
            event_ts=snapshot.timestamp,
            recorded_ts=snapshot.timestamp,
            run_id=run_id,
            idempotency_key=(
                f"fill_obs:{order['order_id']}:{self._format_decimal(fill_price)}:"
                f"{self._format_decimal(fill_size)}:{self._format_decimal(snapshot.timestamp)}"
            ),
            causation_id=causation_id,
            correlation_id=correlation_id,
            schema_version=schema_version,
            payload={
                "market_id": order["market_id"],
                "slot_id": order.get("slot_id"),
                "outcome": order["outcome"],
                "side": order["side"].upper(),
                "strategy_family": order.get("strategy_family", "unknown"),
                "fill_price": float(fill_price),
                "fill_size": float(fill_size),
                "observed_ts": float(snapshot.timestamp),
                "best_bid": float(snapshot.best_bid),
                "best_ask": float(snapshot.best_ask),
            },
        )

    def apply_fill(
        self,
        order: dict[str, Any],
        observed_event: LedgerEvent | None,
        *,
        event_id: str | None = None,
        sequence_num: int = 0,
        run_id: str = "paper-run",
        correlation_id: str | None = None,
        schema_version: int = 1,
    ) -> LedgerEvent | None:
        if observed_event is None or observed_event.event_type != "fill_observed":
            return None

        prior_filled = float(order.get("filled_qty", 0.0))
        fill_size = min(float(observed_event.payload["fill_size"]), max(0.0, float(order["size"]) - prior_filled))
        if fill_size <= 1e-9:
            return None

        fill_price = float(observed_event.payload["fill_price"])
        new_filled = prior_filled + fill_size
        remaining_qty = max(0.0, float(order["size"]) - new_filled)
        previous_notional = prior_filled * float(order.get("average_fill_price", 0.0))
        average_fill_price = (previous_notional + (fill_size * fill_price)) / new_filled
        status = "filled" if remaining_qty <= 1e-9 else "partially_filled"

        # Compute time-to-expiry from slot_id pattern "asset:interval_minutes:slot_start_ts"
        slot_id = order.get("slot_id")
        fill_ts = float(observed_event.event_ts)
        time_to_expiry_seconds = None
        tte_bucket = None
        if slot_id:
            parts = slot_id.split(":")
            if len(parts) == 3:
                try:
                    interval_minutes = int(parts[1])
                    slot_start_ts = float(parts[2])
                    slot_end_ts = slot_start_ts + interval_minutes * 60
                    time_to_expiry_seconds = max(0.0, round(slot_end_ts - fill_ts, 1))
                    tte_bucket = (
                        "<60s" if time_to_expiry_seconds < 60 else
                        "60-120s" if time_to_expiry_seconds < 120 else
                        "120-300s" if time_to_expiry_seconds < 300 else
                        ">300s"
                    )
                except (ValueError, IndexError):
                    pass

        event_id = event_id or f"evt-{uuid.uuid4().hex}"
        return LedgerEvent(
            event_id=event_id,
            stream="order",
            aggregate_id=order["order_id"],
            sequence_num=sequence_num,
            event_type="fill_applied",
            event_ts=observed_event.event_ts,
            recorded_ts=observed_event.recorded_ts,
            run_id=run_id,
            idempotency_key=f"fill_apply:{order['order_id']}:{observed_event.event_id}",
            causation_id=observed_event.event_id,
            correlation_id=correlation_id,
            schema_version=schema_version,
            payload={
                "market_id": order["market_id"],
                "slot_id": slot_id,
                "outcome": order["outcome"],
                "side": order["side"].upper(),
                "strategy_family": order.get("strategy_family", "unknown"),
                "fill_price": fill_price,
                "fill_size": fill_size,
                "filled_qty": new_filled,
                "remaining_qty": remaining_qty,
                "average_fill_price": average_fill_price,
                "status": status,
                "observed_event_id": observed_event.event_id,
                "time_to_expiry_seconds": time_to_expiry_seconds,
                "tte_bucket": tte_bucket,
            },
        )

    def _is_fill_eligible(self, order: dict[str, Any], snapshot: OrderBookSnapshot) -> bool:
        status = order.get("status", "unknown")
        if status not in {"open", "partially_filled", "acknowledged"}:
            return False

        order_created_ts = float(order.get("created_ts", order.get("timestamp", 0.0)))
        market_end_ts = order.get("market_end_ts")
        if market_end_ts is not None and snapshot.timestamp > float(market_end_ts):
            return False
        if not self.policy.allow_same_snapshot_fill and snapshot.timestamp <= order_created_ts:
            return False
        if snapshot.timestamp - order_created_ts < self.policy.min_rest_seconds:
            return False

        side = order["side"].upper()
        limit_price = float(order["price"])
        if side == "BUY":
            return snapshot.best_ask > 0 and limit_price >= float(snapshot.best_ask)
        if side == "SELL":
            return snapshot.best_bid > 0 and limit_price <= float(snapshot.best_bid)
        return False

    @staticmethod
    def _format_decimal(value: float) -> str:
        text = f"{value:.6f}".rstrip("0").rstrip(".")
        return text or "0"
