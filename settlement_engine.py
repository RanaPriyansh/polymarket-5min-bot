from __future__ import annotations

import uuid

from ledger import LedgerEvent


class SettlementEngine:
    def pending_event(
        self,
        *,
        event_id: str | None = None,
        slot_id: str,
        market_id: str,
        run_id: str,
        sequence_num: int,
        recorded_ts: float,
        next_poll_ts: float,
        market_slug: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        first_pending_ts: float | None = None,
        delay_seconds: float | None = None,
        deferred: bool = False,
        schema_version: int = 1,
    ) -> LedgerEvent:
        event_id = event_id or f"evt-{uuid.uuid4().hex}"
        return LedgerEvent(
            event_id=event_id,
            stream="market_slot",
            aggregate_id=slot_id,
            sequence_num=sequence_num,
            event_type="slot_resolution_pending",
            event_ts=recorded_ts,
            recorded_ts=recorded_ts,
            run_id=run_id,
            idempotency_key=f"pending:{slot_id}:{_format_ts(next_poll_ts)}",
            causation_id=causation_id,
            correlation_id=correlation_id,
            schema_version=schema_version,
            payload={
                "market_id": market_id,
                "market_slug": market_slug,
                "next_poll_ts": next_poll_ts,
                "first_pending_ts": recorded_ts if first_pending_ts is None else first_pending_ts,
                "delay_seconds": delay_seconds,
                "deferred": deferred,
            },
        )

    def settled_event(
        self,
        *,
        event_id: str | None = None,
        slot_id: str,
        market_id: str,
        market_slug: str | None,
        winning_outcome: str,
        settled_ts: float,
        run_id: str,
        sequence_num: int,
        correlation_id: str | None = None,
        causation_id: str | None = None,
        schema_version: int = 1,
    ) -> LedgerEvent:
        event_id = event_id or f"evt-{uuid.uuid4().hex}"
        return LedgerEvent(
            event_id=event_id,
            stream="market_slot",
            aggregate_id=slot_id,
            sequence_num=sequence_num,
            event_type="slot_settled",
            event_ts=settled_ts,
            recorded_ts=settled_ts,
            run_id=run_id,
            idempotency_key=f"settled:{slot_id}:{winning_outcome}:{_format_ts(settled_ts)}",
            causation_id=causation_id,
            correlation_id=correlation_id,
            schema_version=schema_version,
            payload={
                "market_id": market_id,
                "market_slug": market_slug,
                "winning_outcome": winning_outcome,
                "settled_ts": settled_ts,
            },
        )


def _format_ts(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.6f}".rstrip("0").rstrip(".")
