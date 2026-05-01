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
    last_trade_price: float | None = None
    last_trade_size: float = 0.0
    level_consumed_size: float = 0.0
    queue_ahead_shares: float = 0.0
    book_age_ms: float = 0.0
    external_spot_age_ms: float = 0.0
    ws_lag_ms: float = 0.0
    market_metadata_incomplete: bool = False


@dataclass(frozen=True)
class FillPolicy:
    min_rest_seconds: float = 1.0
    max_fill_fraction_per_snapshot: float = 0.25
    allow_same_snapshot_fill: bool = False
    max_book_age_ms: float = 1000.0
    max_external_spot_age_ms: float = 1000.0
    max_ws_lag_ms: float = 1000.0
    cancel_latency_ms: float = 250.0
    taker_fee_bps: float = 0.0


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
        fill_reason = self._maker_fill_reason(order, snapshot)
        if fill_reason is None:
            return None

        fill_price = float(order["price"])
        remaining_qty = max(0.0, float(order["size"]) - float(order.get("filled_qty", 0.0)))
        max_fill_size = float(order["size"]) * self.policy.max_fill_fraction_per_snapshot
        queue_ahead = self._queue_ahead(order, snapshot)
        visible_consumed = max(float(snapshot.level_consumed_size), float(snapshot.last_trade_size))
        queue_fill_probability = self._queue_fill_probability(queue_ahead, visible_consumed)
        if fill_reason == "trade_through":
            fill_size = min(remaining_qty, max_fill_size)
        else:
            available_after_queue = max(0.0, visible_consumed - queue_ahead)
            fill_size = min(remaining_qty, max_fill_size, available_after_queue)
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
                "maker_or_taker": "maker",
                "fill_reason": fill_reason,
                "paper_fill_reason": fill_reason,
                "paper_fill_confidence": self._maker_fill_confidence(fill_reason, queue_fill_probability),
                "simulator_assumption": "maker_requires_trade_through_or_queue_evidence",
                "queue_ahead_shares": queue_ahead,
                "queue_fill_probability": queue_fill_probability,
                "maker_fill_confidence": self._maker_fill_confidence(fill_reason, queue_fill_probability),
                "fees_estimated": 0.0,
                "slippage": 0.0,
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
                "maker_or_taker": observed_event.payload.get("maker_or_taker", "maker"),
                "fill_reason": observed_event.payload.get("fill_reason", "unknown"),
                "paper_fill_reason": observed_event.payload.get("paper_fill_reason", observed_event.payload.get("fill_reason", "unknown")),
                "paper_fill_confidence": observed_event.payload.get("paper_fill_confidence"),
                "simulator_assumption": observed_event.payload.get("simulator_assumption"),
                "queue_ahead_shares": observed_event.payload.get("queue_ahead_shares"),
                "queue_fill_probability": observed_event.payload.get("queue_fill_probability"),
                "fees_estimated": observed_event.payload.get("fees_estimated", 0.0),
                "slippage": observed_event.payload.get("slippage", 0.0),
            },
        )

    def walk_taker_book(
        self,
        order: dict[str, Any],
        levels: list[tuple[float, float]],
        *,
        snapshot_ts: float,
        book_age_ms: float = 0.0,
        external_spot_age_ms: float = 0.0,
        ws_lag_ms: float = 0.0,
        market_metadata_incomplete: bool = False,
        fees_enabled: bool | None = None,
        fee_bps: float | None = None,
    ) -> dict[str, Any]:
        stale_reason = self._stale_reason(
            book_age_ms=book_age_ms,
            external_spot_age_ms=external_spot_age_ms,
            ws_lag_ms=ws_lag_ms,
            market_metadata_incomplete=market_metadata_incomplete,
        )
        if stale_reason:
            return {"filled": False, "reason": stale_reason, "fill_reason": stale_reason}

        status = order.get("status", "unknown")
        if status not in {"open", "partially_filled", "acknowledged"}:
            return {"filled": False, "reason": "not_open", "fill_reason": "not_open"}

        side = order["side"].upper()
        limit_price = float(order["price"])
        remaining_qty = max(0.0, float(order["size"]) - float(order.get("filled_qty", 0.0)))
        if remaining_qty <= 1e-9:
            return {"filled": False, "reason": "no_remaining_qty", "fill_reason": "no_remaining_qty"}

        filled_qty = 0.0
        notional = 0.0
        best_executable = levels[0][0] if levels else 0.0
        for price, available in levels:
            price = float(price)
            available = float(available)
            executable = (side == "BUY" and price <= limit_price) or (side == "SELL" and price >= limit_price)
            if not executable:
                break
            take = min(remaining_qty - filled_qty, available)
            if take <= 1e-9:
                continue
            filled_qty += take
            notional += take * price
            if filled_qty >= remaining_qty - 1e-9:
                break

        if filled_qty <= 1e-9:
            return {"filled": False, "reason": "no_executable_depth", "fill_reason": "no_executable_depth"}

        average_price = notional / filled_qty
        slippage = abs(average_price - best_executable) if best_executable > 0 else 0.0
        resolved_fee_bps = self.policy.taker_fee_bps if fee_bps is None else float(fee_bps)
        fees_estimated = notional * (resolved_fee_bps / 10000.0) if fees_enabled or resolved_fee_bps > 0 else 0.0
        fill_reason = "taker_book_walk_full" if filled_qty >= remaining_qty - 1e-9 else "taker_book_walk_partial"
        return {
            "filled": True,
            "fill_price": average_price,
            "fill_size": filled_qty,
            "fill_ts": snapshot_ts,
            "fill_reason": fill_reason,
            "paper_fill_reason": fill_reason,
            "paper_fill_confidence": 0.85 if fill_reason.endswith("full") else 0.65,
            "simulator_assumption": "taker_walks_visible_book_depth",
            "fees_estimated": fees_estimated,
            "slippage": slippage,
            "notional": notional,
        }

    def _maker_fill_reason(self, order: dict[str, Any], snapshot: OrderBookSnapshot) -> str | None:
        status = order.get("status", "unknown")
        if status not in {"open", "partially_filled", "acknowledged", "cancel_pending"}:
            return None

        order_created_ts = float(order.get("created_ts", order.get("timestamp", 0.0)))
        market_end_ts = order.get("market_end_ts")
        if market_end_ts is not None and snapshot.timestamp > float(market_end_ts):
            return None
        if not self.policy.allow_same_snapshot_fill and snapshot.timestamp <= order_created_ts:
            return None
        if snapshot.timestamp - order_created_ts < self.policy.min_rest_seconds:
            return None
        if self._snapshot_stale_reason(snapshot):
            return None

        if status == "cancel_pending":
            cancel_effective_at = float(order.get("cancel_effective_at", 0.0))
            if cancel_effective_at and snapshot.timestamp >= cancel_effective_at:
                return None

        side = order["side"].upper()
        limit_price = float(order["price"])
        last_trade = snapshot.last_trade_price
        if last_trade is not None:
            if side == "BUY" and float(last_trade) < limit_price:
                return "trade_through"
            if side == "SELL" and float(last_trade) > limit_price:
                return "trade_through"
            if abs(float(last_trade) - limit_price) <= 1e-9 and self._queue_consumed(order, snapshot):
                return "queue_exhausted_at_price"

        if snapshot.level_consumed_size > 0 and self._queue_consumed(order, snapshot):
            return "level_consumed_after_order"
        return None

    def _snapshot_stale_reason(self, snapshot: OrderBookSnapshot) -> str | None:
        return self._stale_reason(
            book_age_ms=snapshot.book_age_ms,
            external_spot_age_ms=snapshot.external_spot_age_ms,
            ws_lag_ms=snapshot.ws_lag_ms,
            market_metadata_incomplete=snapshot.market_metadata_incomplete,
        )

    def _stale_reason(
        self,
        *,
        book_age_ms: float,
        external_spot_age_ms: float,
        ws_lag_ms: float,
        market_metadata_incomplete: bool,
    ) -> str | None:
        if market_metadata_incomplete:
            return "metadata_incomplete"
        if book_age_ms > self.policy.max_book_age_ms:
            return "stale_book"
        if external_spot_age_ms > self.policy.max_external_spot_age_ms:
            return "stale_external_spot"
        if ws_lag_ms > self.policy.max_ws_lag_ms:
            return "stale_ws"
        return None

    @staticmethod
    def _queue_ahead(order: dict[str, Any], snapshot: OrderBookSnapshot) -> float:
        queue_ahead = order.get("queue_ahead_shares")
        if queue_ahead is None:
            queue_ahead = snapshot.queue_ahead_shares
        if queue_ahead is None:
            queue_ahead = 0.0
        return max(0.0, float(queue_ahead))

    def _queue_consumed(self, order: dict[str, Any], snapshot: OrderBookSnapshot) -> bool:
        visible_consumed = max(float(snapshot.level_consumed_size), float(snapshot.last_trade_size))
        return visible_consumed > self._queue_ahead(order, snapshot)

    @staticmethod
    def _queue_fill_probability(queue_ahead: float, visible_consumed: float) -> float:
        if queue_ahead <= 0:
            return 1.0 if visible_consumed > 0 else 0.0
        return max(0.0, min(1.0, visible_consumed / queue_ahead))

    @staticmethod
    def _maker_fill_confidence(fill_reason: str, queue_fill_probability: float) -> float:
        if fill_reason == "trade_through":
            return 0.9
        return max(0.0, min(0.8, queue_fill_probability * 0.8))

    @staticmethod
    def _format_decimal(value: float) -> str:
        text = f"{value:.6f}".rstrip("0").rstrip(".")
        return text or "0"
