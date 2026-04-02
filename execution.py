"""
Execution layer for Polymarket orders.
Handles paper/live placement, cancellation, paper position lifecycle, and settlement accounting.
"""

from __future__ import annotations

import copy
import logging
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp

from ledger import LedgerEvent, SQLiteLedger
from paper_exchange import ConservativeFillEngine, FillPolicy, OrderBookSnapshot
from replay import replay_ledger
from settlement_engine import SettlementEngine
from exposure import build_exposure_snapshot

logger = logging.getLogger(__name__)


@dataclass
class FamilyPosition:
    quantity: float = 0.0
    average_price: float = 0.0


@dataclass
class SlotState:
    slot_id: str
    market_id: str
    market_slug: str
    strategy_family: str
    outcome: str
    quantity: float
    average_price: float
    opened_ts: float
    updated_ts: float
    end_ts: float
    status: str = "open"
    close_reason: str = ""


class PolymarketExecutor:
    def __init__(
        self,
        config: dict,
        market_data: "PolymarketData",
        mode: str = "paper",
        *,
        run_id: str | None = None,
        ledger: SQLiteLedger | None = None,
    ):
        execution_cfg = config.get("execution", {})
        self.clob_url = config["polymarket"]["clob_api_url"]
        self.wallet_address = config["polymarket"].get("wallet_address", "paper-wallet")
        self.private_key = config["polymarket"].get("private_key", "paper-key")
        self.session = None
        self.md = market_data
        self.mode = mode
        self.run_id = run_id or execution_cfg.get("run_id") or f"{mode}-runtime"
        self.paper_bankroll = float(execution_cfg.get("paper_starting_bankroll", 500.0))
        self.resolution_initial_poll_seconds = int(execution_cfg.get("resolution_initial_poll_seconds", 10))
        self.resolution_poll_cap_seconds = int(execution_cfg.get("resolution_poll_cap_seconds", 300))
        fill_policy_cfg = execution_cfg.get("fill_policy", {})
        self.fill_engine = ConservativeFillEngine(
            FillPolicy(
                min_rest_seconds=float(fill_policy_cfg.get("min_rest_seconds", 1.0)),
                max_fill_fraction_per_snapshot=float(fill_policy_cfg.get("max_fill_fraction_per_snapshot", 0.25)),
                allow_same_snapshot_fill=bool(fill_policy_cfg.get("allow_same_snapshot_fill", False)),
            )
        )
        self.settlement_engine = SettlementEngine()
        self.orders: Dict[str, Dict] = {}
        self.positions: Dict[Tuple[str, str, str], FamilyPosition] = {}
        self.family_metrics: Dict[str, Dict] = {}
        self.realized_pnl_total = 0.0
        self.market_registry: Dict[str, Dict] = {}
        self.signal_slots: Dict[str, SlotState] = {}
        self.pending_resolution: Dict[str, Dict] = {}
        self.resolved_trade_count = 0
        self.win_count = 0
        self.loss_count = 0
        self.latest_settlement: Optional[Dict] = None
        ledger_path = execution_cfg.get("ledger_db_path")
        self.ledger = ledger or (SQLiteLedger(Path(ledger_path)) if ledger_path else None)
        self._sequence_counters: Dict[Tuple[str, str], int] = {}
        if self.ledger is not None:
            self._restore_from_ledger()

    async def __aenter__(self):
        if self.mode == "live":
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            self.session = None

    def _next_sequence(self, stream: str, aggregate_id: str) -> int:
        key = (stream, aggregate_id)
        next_value = self._sequence_counters.get(key, 0) + 1
        self._sequence_counters[key] = next_value
        return next_value

    def _track_sequence(self, event: LedgerEvent) -> None:
        key = (event.stream, event.aggregate_id)
        self._sequence_counters[key] = max(self._sequence_counters.get(key, 0), int(event.sequence_num))

    def _persist_events(self, *events: LedgerEvent | None) -> list[LedgerEvent]:
        materialized = [event for event in events if event is not None]
        if self.ledger is None:
            return materialized

        appended: list[LedgerEvent] = []
        for event in materialized:
            self._track_sequence(event)
            try:
                self.ledger.append_event(event)
                appended.append(event)
            except sqlite3.IntegrityError:
                logger.info("Ignoring duplicate ledger event idempotency_key=%s", event.idempotency_key)
        if materialized:
            self._restore_from_ledger()
        return appended

    def _restore_from_ledger(self) -> None:
        if self.ledger is None:
            return
        events = self.ledger.list_events(run_id=self.run_id)
        projection = replay_ledger(events)
        self._sequence_counters = {}
        for event in events:
            self._track_sequence(event)
        self.orders = {}
        for order_id, state in projection.orders.items():
            order_state = dict(state)
            size = float(order_state.get("size", 0.0))
            filled_qty = float(order_state.get("filled_qty", 0.0))
            order_state.setdefault("remaining_size", max(0.0, size - filled_qty))
            order_state.setdefault("remaining_qty", max(0.0, size - filled_qty))
            order_state.setdefault("timestamp", order_state.get("created_ts", order_state.get("event_ts", 0.0)))
            self.orders[order_id] = order_state
        self.positions = {
            key: FamilyPosition(
                quantity=float(position.get("quantity", 0.0)),
                average_price=float(position.get("average_price", 0.0)),
            )
            for key, position in projection.positions.items()
            if abs(float(position.get("quantity", 0.0))) > 1e-9
        }
        self.pending_resolution = {
            slot_id: {
                "slot_id": slot_id,
                "first_pending_ts": payload.get("first_pending_ts", payload.get("next_poll_ts", 0.0)),
                "next_poll_ts": payload.get("next_poll_ts", 0.0),
                "delay_seconds": payload.get("delay_seconds", self.resolution_initial_poll_seconds),
                "deferred": bool(payload.get("deferred", False)),
            }
            for slot_id, payload in projection.pending_slots.items()
        }
        self.realized_pnl_total = float(projection.realized_pnl_total)
        self.resolved_trade_count = int(projection.resolved_trade_count)
        self.win_count = int(projection.win_count)
        self.loss_count = int(projection.loss_count)
        settled = sorted(
            projection.settled_slots.items(),
            key=lambda item: float(item[1].get("settled_ts", 0.0)),
        )
        self.latest_settlement = None
        if projection.latest_settlement is not None:
            self.latest_settlement = {
                "event_type": "market.settled",
                **projection.latest_settlement,
            }
        elif settled:
            slot_id, payload = settled[-1]
            self.latest_settlement = {
                "event_type": "market.settled",
                "slot_id": slot_id,
                **payload,
            }
        self._rebuild_signal_slots_from_orders()

    def _rebuild_signal_slots_from_orders(self) -> None:
        self.signal_slots = {}
        for (family, market_id, outcome), position in self.positions.items():
            if family != "mean_reversion_5min" or abs(position.quantity) <= 1e-9:
                continue
            matching_order = None
            for order in self.orders.values():
                if order.get("market_id") == market_id and order.get("outcome") == outcome and order.get("strategy_family") == family:
                    matching_order = order
                    break
            slot_id = None if matching_order is None else matching_order.get("slot_id")
            market = self.market_registry.get(slot_id) if slot_id else None
            if matching_order is None and market is None:
                continue
            if slot_id is None and market is not None:
                slot_id = market.get("slot_id")
            market_slug = (matching_order or {}).get("market_slug") or (market or {}).get("slug", "")
            end_ts = float((matching_order or {}).get("market_end_ts", (market or {}).get("end_ts", 0.0)))
            opened_ts = float((matching_order or {}).get("created_ts", (matching_order or {}).get("timestamp", 0.0)))
            updated_ts = float((matching_order or {}).get("filled_ts", opened_ts))
            if slot_id:
                self.signal_slots[slot_id] = SlotState(
                    slot_id=slot_id,
                    market_id=market_id,
                    market_slug=market_slug,
                    strategy_family=family,
                    outcome=outcome,
                    quantity=position.quantity,
                    average_price=position.average_price,
                    opened_ts=opened_ts,
                    updated_ts=updated_ts,
                    end_ts=end_ts,
                )

    def _order_created_event(self, order: Dict) -> LedgerEvent:
        return LedgerEvent(
            event_id=f"evt-{uuid.uuid4().hex}",
            stream="order",
            aggregate_id=order["order_id"],
            sequence_num=self._next_sequence("order", order["order_id"]),
            event_type="order_created",
            event_ts=float(order.get("timestamp", time.time())),
            recorded_ts=float(order.get("timestamp", time.time())),
            run_id=self.run_id,
            idempotency_key=f"order_created:{order['order_id']}",
            causation_id=None,
            correlation_id=order["order_id"],
            schema_version=1,
            payload={
                "market_id": order["market_id"],
                "slot_id": order.get("slot_id"),
                "market_slug": order.get("market_slug"),
                "outcome": order["outcome"],
                "side": order["side"],
                "size": float(order["size"]),
                "price": float(order["price"]),
                "post_only": bool(order.get("post_only", True)),
                "strategy_family": order.get("strategy_family", "unknown"),
                "order_kind": order.get("order_kind", "signal"),
                "created_ts": float(order.get("timestamp", time.time())),
                "market_end_ts": order.get("market_end_ts"),
                "wallet": order.get("wallet", self.wallet_address),
            },
        )

    def _order_acknowledged_event(self, order: Dict) -> LedgerEvent:
        return LedgerEvent(
            event_id=f"evt-{uuid.uuid4().hex}",
            stream="order",
            aggregate_id=order["order_id"],
            sequence_num=self._next_sequence("order", order["order_id"]),
            event_type="order_acknowledged",
            event_ts=float(order.get("timestamp", time.time())),
            recorded_ts=float(order.get("timestamp", time.time())),
            run_id=self.run_id,
            idempotency_key=f"order_ack:{order['order_id']}",
            causation_id=None,
            correlation_id=order["order_id"],
            schema_version=1,
            payload={
                "status": order.get("status", "open"),
                "remaining_qty": float(order.get("remaining_size", order.get("size", 0.0))),
                "filled_qty": float(order.get("filled_qty", 0.0)),
                "average_fill_price": float(order.get("average_fill_price", 0.0)),
            },
        )

    def _order_cancelled_event(self, order: Dict) -> LedgerEvent:
        cancelled_ts = time.time()
        return LedgerEvent(
            event_id=f"evt-{uuid.uuid4().hex}",
            stream="order",
            aggregate_id=order["order_id"],
            sequence_num=self._next_sequence("order", order["order_id"]),
            event_type="order_cancelled",
            event_ts=cancelled_ts,
            recorded_ts=cancelled_ts,
            run_id=self.run_id,
            idempotency_key=f"order_cancelled:{order['order_id']}",
            causation_id=None,
            correlation_id=order["order_id"],
            schema_version=1,
            payload={
                "status": "cancelled",
                "remaining_qty": float(order.get("remaining_size", 0.0)),
                "filled_qty": float(order.get("filled_qty", 0.0)),
            },
        )

    def _direct_fill_observed_event(
        self,
        order: Dict,
        *,
        fill_price: float,
        fill_size: float,
        fill_ts: float,
    ) -> LedgerEvent:
        best_bid = fill_price if order["side"].upper() == "SELL" else 0.0
        best_ask = fill_price if order["side"].upper() == "BUY" else 0.0
        return LedgerEvent(
            event_id=f"evt-{uuid.uuid4().hex}",
            stream="order",
            aggregate_id=order["order_id"],
            sequence_num=self._next_sequence("order", order["order_id"]),
            event_type="fill_observed",
            event_ts=fill_ts,
            recorded_ts=fill_ts,
            run_id=self.run_id,
            idempotency_key=(
                f"fill_obs:{order['order_id']}:{self.fill_engine._format_decimal(fill_price)}:"
                f"{self.fill_engine._format_decimal(fill_size)}:{self.fill_engine._format_decimal(fill_ts)}"
            ),
            causation_id=None,
            correlation_id=order["order_id"],
            schema_version=1,
            payload={
                "market_id": order["market_id"],
                "slot_id": order.get("slot_id"),
                "outcome": order["outcome"],
                "side": order["side"].upper(),
                "strategy_family": order.get("strategy_family", "unknown"),
                "fill_price": float(fill_price),
                "fill_size": float(fill_size),
                "observed_ts": float(fill_ts),
                "best_bid": float(best_bid),
                "best_ask": float(best_ask),
            },
        )

    def _risk_snapshot_event(self, risk_report: Dict, *, snapshot_ts: float) -> LedgerEvent:
        return LedgerEvent(
            event_id=f"evt-{uuid.uuid4().hex}",
            stream="risk",
            aggregate_id=self.run_id,
            sequence_num=self._next_sequence("risk", self.run_id),
            event_type="risk_snapshot_recorded",
            event_ts=float(snapshot_ts),
            recorded_ts=float(snapshot_ts),
            run_id=self.run_id,
            idempotency_key=f"risk:{self.run_id}:{self.fill_engine._format_decimal(snapshot_ts)}",
            causation_id=None,
            correlation_id=self.run_id,
            schema_version=1,
            payload=dict(risk_report),
        )

    def record_risk_snapshot(self, risk_report: Dict, *, snapshot_ts: float | None = None) -> None:
        if self.ledger is None:
            return
        ts = float(snapshot_ts if snapshot_ts is not None else time.time())
        self._persist_events(self._risk_snapshot_event(risk_report, snapshot_ts=ts))

    def _ensure_family_metrics(self, strategy_family: str) -> Dict:
        metrics = self.family_metrics.setdefault(
            strategy_family,
            {
                "quotes_submitted": 0,
                "orders_resting": 0,
                "orders_filled": 0,
                "cancellations": 0,
                "realized_pnl": 0.0,
                "markets_seen": 0,
                "toxic_book_skips": 0,
            },
        )
        return metrics

    def note_market_seen(self, strategy_family: str) -> None:
        self._ensure_family_metrics(strategy_family)["markets_seen"] += 1

    def note_toxic_book_skip(self, strategy_family: str) -> None:
        self._ensure_family_metrics(strategy_family)["toxic_book_skips"] += 1

    def register_market(self, market: Dict) -> None:
        self.market_registry[market["slot_id"]] = dict(market)

    def _market_for_order(self, order: Dict) -> Optional[Dict]:
        slot_id = order.get("slot_id")
        if slot_id:
            return self.market_registry.get(slot_id)
        for market in self.market_registry.values():
            if market["id"] == order["market_id"]:
                return market
        return None

    async def place_order(
        self,
        market_id: str,
        outcome: str,
        side: str,
        size: float,
        price: float,
        post_only: bool = True,
        strategy_family: str = "unknown",
        order_kind: str = "signal",
        market: Optional[Dict] = None,
    ) -> Optional[str]:
        if size <= 0 or price < 0 or price > 1:
            logger.error("Invalid order parameters: size=%s price=%s", size, price)
            return None

        if market:
            self.register_market(market)

        family_metrics = self._ensure_family_metrics(strategy_family)
        order_id = f"{self.mode}-{uuid.uuid4().hex[:12]}"
        order = {
            "order_id": order_id,
            "market_id": market_id,
            "outcome": outcome,
            "side": side.upper(),
            "size": float(size),
            "remaining_size": float(size),
            "price": float(round(price, 4)),
            "post_only": post_only,
            "wallet": self.wallet_address,
            "timestamp": time.time(),
            "strategy_family": strategy_family,
            "order_kind": order_kind,
            "status": "open",
        }
        if market:
            order["market_slug"] = market["slug"]
            order["slot_id"] = market["slot_id"]
            order["market_end_ts"] = market["end_ts"]

        if self.mode == "live":
            token_id = market["token_ids"][outcome] if market else self._get_token_id(market_id, outcome)
            payload = {
                "market_id": market_id,
                "token_id": token_id,
                "side": side.lower(),
                "size": str(size),
                "price": str(round(price, 4)),
                "type": "LIMIT",
                "post_only": post_only,
                "wallet": self.wallet_address,
            }
            try:
                async with self.session.post(f"{self.clob_url}/order", json=payload) as resp:
                    if resp.status != 200:
                        logger.error("Order failed: %s - %s", resp.status, await resp.text())
                        return None
                    data = await resp.json()
                    order_id = data.get("order_id", order_id)
                    order["order_id"] = order_id
            except Exception as exc:
                logger.exception("Exception placing order: %s", exc)
                return None

        self.orders[order_id] = order
        family_metrics["orders_resting"] += 1
        if order_kind == "quote":
            family_metrics["quotes_submitted"] += 1
        self._persist_events(
            self._order_created_event(order),
            self._order_acknowledged_event(order),
        )
        logger.info("Order accepted: %s %s %s@%s %s family=%s", order_id, side, size, price, outcome, strategy_family)
        return order_id

    async def cancel_order(self, order_id: str):
        order = self.orders.get(order_id)
        if not order or order.get("status") != "open":
            return False

        if self.mode == "live":
            try:
                async with self.session.delete(f"{self.clob_url}/order/{order_id}") as resp:
                    if resp.status != 200:
                        return False
            except Exception as exc:
                logger.error("Cancel error: %s", exc)
                return False

        order["status"] = "cancelled"
        metrics = self._ensure_family_metrics(order["strategy_family"])
        metrics["orders_resting"] = max(0, metrics["orders_resting"] - 1)
        metrics["cancellations"] += 1
        self._persist_events(self._order_cancelled_event(order))
        logger.info("Order cancelled: %s", order_id)
        return True

    async def cancel_family_market(self, market_id: str, strategy_family: str):
        to_cancel = [
            oid for oid, order in self.orders.items()
            if order["market_id"] == market_id and order["strategy_family"] == strategy_family and order["status"] == "open"
        ]
        for oid in to_cancel:
            await self.cancel_order(oid)
        return len(to_cancel)

    async def cancel_market_orders(self, market_id: str) -> int:
        to_cancel = [oid for oid, order in self.orders.items() if order["market_id"] == market_id and order["status"] == "open"]
        for oid in to_cancel:
            await self.cancel_order(oid)
        return len(to_cancel)

    def fill_order(
        self,
        order_id: str,
        *,
        fill_price: Optional[float] = None,
        fill_size: Optional[float] = None,
        fill_ts: Optional[float] = None,
        observed_event: LedgerEvent | None = None,
    ) -> Dict:
        order = self.orders.get(order_id)
        if not order or order.get("status") not in {"open", "partially_filled", "acknowledged"}:
            return {"filled": False, "reason": "not_open"}

        order_before = copy.deepcopy(order)
        remaining_size = float(order.get("remaining_size", order.get("remaining_qty", order["size"])))
        executed_size = float(fill_size if fill_size is not None else remaining_size)
        executed_price = float(fill_price if fill_price is not None else order["price"])
        executed_ts = float(fill_ts if fill_ts is not None else time.time())
        realized_before = self.realized_pnl_total
        realized_delta = self._apply_fill(
            strategy_family=order["strategy_family"],
            market_id=order["market_id"],
            outcome=order["outcome"],
            side=order["side"],
            size=executed_size,
            price=executed_price,
        )
        filled_before = float(order.get("filled_qty", 0.0))
        new_filled_qty = filled_before + executed_size
        order["filled_qty"] = new_filled_qty
        order["remaining_size"] = max(0.0, remaining_size - executed_size)
        order["remaining_qty"] = order["remaining_size"]
        order["filled_ts"] = executed_ts
        order["fill_price"] = executed_price
        previous_notional = filled_before * float(order.get("average_fill_price", 0.0))
        order["average_fill_price"] = (previous_notional + (executed_size * executed_price)) / new_filled_qty if new_filled_qty > 0 else 0.0
        order["status"] = "filled" if order["remaining_size"] <= 1e-9 else "partially_filled"
        if order["status"] == "filled":
            metrics = self._ensure_family_metrics(order["strategy_family"])
            metrics["orders_resting"] = max(0, metrics["orders_resting"] - 1)
            metrics["orders_filled"] += 1

        if observed_event is None:
            observed_event = self._direct_fill_observed_event(
                order_before,
                fill_price=executed_price,
                fill_size=executed_size,
                fill_ts=executed_ts,
            )
        applied_event = self.fill_engine.apply_fill(
            order_before,
            observed_event,
            sequence_num=self._next_sequence("order", order_id),
            run_id=self.run_id,
            correlation_id=order_id,
        )
        self._persist_events(observed_event, applied_event)
        if self.ledger is not None:
            realized_delta = self.realized_pnl_total - realized_before
            order = self.orders.get(order_id, order)

        logger.info("Order filled: %s family=%s realized_delta=%.4f", order_id, order["strategy_family"], realized_delta)
        return {
            "filled": True,
            "order_id": order_id,
            "strategy_family": order["strategy_family"],
            "market_id": order["market_id"],
            "market_slug": order.get("market_slug"),
            "slot_id": order.get("slot_id"),
            "outcome": order["outcome"],
            "side": order["side"],
            "realized_pnl_delta": realized_delta,
            "fill_price": executed_price,
            "size": executed_size,
        }

    def evaluate_market_orders(self, market_id: str, orderbook: "OrderBook"):
        fills = []
        for order_id, order in list(self.orders.items()):
            if order["market_id"] != market_id or order.get("status") not in {"open", "partially_filled", "acknowledged"}:
                continue
            snapshot = OrderBookSnapshot(
                timestamp=float(orderbook.timestamp),
                best_bid=float(self.md.best_bid(orderbook, order["outcome"])),
                best_ask=float(self.md.best_ask(orderbook, order["outcome"])),
            )
            observed = self.fill_engine.observe_fill(
                order,
                snapshot,
                sequence_num=self._next_sequence("order", order_id),
                run_id=self.run_id,
                correlation_id=order_id,
            )
            if observed is None:
                continue
            fills.append(
                self.fill_order(
                    order_id,
                    fill_price=float(observed.payload["fill_price"]),
                    fill_size=float(observed.payload["fill_size"]),
                    fill_ts=float(observed.payload["observed_ts"]),
                    observed_event=observed,
                )
            )
        return fills

    def _apply_fill(self, *, strategy_family: str, market_id: str, outcome: str, side: str, size: float, price: float) -> float:
        position = self.positions.setdefault((strategy_family, market_id, outcome), FamilyPosition())
        signed_size = size if side == "BUY" else -size
        realized = 0.0

        if position.quantity == 0 or position.quantity * signed_size > 0:
            new_qty = position.quantity + signed_size
            total_cost = (position.average_price * abs(position.quantity)) + (price * abs(signed_size))
            position.quantity = new_qty
            position.average_price = total_cost / abs(new_qty) if new_qty != 0 else 0.0
        else:
            close_size = min(abs(position.quantity), abs(signed_size))
            if position.quantity > 0:
                realized = (price - position.average_price) * close_size
            else:
                realized = (position.average_price - price) * close_size
            remaining_qty = position.quantity + signed_size
            if remaining_qty == 0:
                position.quantity = 0.0
                position.average_price = 0.0
            elif position.quantity * remaining_qty < 0:
                position.quantity = remaining_qty
                position.average_price = price
            else:
                position.quantity = remaining_qty
        metrics = self._ensure_family_metrics(strategy_family)
        metrics["realized_pnl"] += realized
        self.realized_pnl_total += realized
        return realized

    def _signal_slot_quantity(self, slot: SlotState) -> float:
        position = self.positions.get((slot.strategy_family, slot.market_id, slot.outcome))
        return position.quantity if position else 0.0

    async def close_signal_slot(self, slot_id: str, orderbook: "OrderBook", reason: str) -> List[Dict]:
        slot = self.signal_slots.get(slot_id)
        if not slot or slot.status != "open":
            return []
        quantity = self._signal_slot_quantity(slot)
        if abs(quantity) <= 1e-9:
            slot.status = "closed"
            slot.close_reason = reason
            del self.signal_slots[slot_id]
            return []

        side = "SELL" if quantity > 0 else "BUY"
        fill_price = self.md.best_bid(orderbook, slot.outcome) if side == "SELL" else self.md.best_ask(orderbook, slot.outcome)
        if fill_price <= 0:
            return []
        market = self.market_registry.get(slot_id)
        order_id = await self.place_order(
            slot.market_id,
            slot.outcome,
            side,
            abs(quantity),
            fill_price,
            post_only=False,
            strategy_family=slot.strategy_family,
            order_kind="signal_close",
            market=market,
        )
        if not order_id:
            return []
        fill = self.fill_order(order_id, fill_price=fill_price, fill_ts=orderbook.timestamp)
        slot.status = "closed"
        slot.updated_ts = orderbook.timestamp
        slot.close_reason = reason
        del self.signal_slots[slot_id]
        return [
            {
                "event_type": "position.closed",
                "slot_id": slot_id,
                "market_id": slot.market_id,
                "market_slug": slot.market_slug,
                "strategy_family": slot.strategy_family,
                "outcome": slot.outcome,
                "size": abs(quantity),
                "close_side": side,
                "fill_price": fill_price,
                "realized_pnl_delta": fill.get("realized_pnl_delta", 0.0),
                "reason": reason,
            },
            {
                **fill,
                "event_type": "order.filled",
                "reason": reason,
            },
        ]

    async def execute_signal_trade(self, market: Dict, orderbook: "OrderBook", signal) -> Dict:
        self.register_market(market)
        slot_id = market["slot_id"]
        events: List[Dict] = []

        existing_slot = self.signal_slots.get(slot_id)
        if existing_slot and existing_slot.status == "open":
            quantity = self._signal_slot_quantity(existing_slot)
            same_direction = (
                existing_slot.outcome == signal.outcome
                and ((quantity > 0 and signal.action == "BUY") or (quantity < 0 and signal.action == "SELL"))
            )
            if same_direction:
                return {"opened": False, "reason": "existing_same_direction", "events": []}
            events.extend(await self.close_signal_slot(slot_id, orderbook, reason="signal_reversal"))

        fill_price = self.md.best_ask(orderbook, signal.outcome) if signal.action == "BUY" else self.md.best_bid(orderbook, signal.outcome)
        if fill_price <= 0:
            return {"opened": False, "reason": "missing_fill_price", "events": []}

        order_id = await self.place_order(
            market["id"],
            signal.outcome,
            signal.action,
            signal.size,
            fill_price,
            post_only=False,
            strategy_family="mean_reversion_5min",
            order_kind="signal",
            market=market,
        )
        if not order_id:
            return {"opened": False, "reason": "order_rejected", "events": events}
        fill = self.fill_order(order_id, fill_price=fill_price, fill_ts=orderbook.timestamp)
        position = self.positions[( "mean_reversion_5min", market["id"], signal.outcome)]
        slot_state = SlotState(
            slot_id=slot_id,
            market_id=market["id"],
            market_slug=market["slug"],
            strategy_family="mean_reversion_5min",
            outcome=signal.outcome,
            quantity=position.quantity,
            average_price=position.average_price,
            opened_ts=orderbook.timestamp,
            updated_ts=orderbook.timestamp,
            end_ts=market["end_ts"],
        )
        self.signal_slots[slot_id] = slot_state
        events.extend([
            {
                "event_type": "order.opened",
                "slot_id": slot_id,
                "market_id": market["id"],
                "market_slug": market["slug"],
                "strategy_family": "mean_reversion_5min",
                "outcome": signal.outcome,
                "side": signal.action,
                "size": signal.size,
                "price": fill_price,
                "reason": signal.reason,
            },
            {
                **fill,
                "event_type": "order.filled",
                "reason": signal.reason,
            },
        ])
        return {
            "opened": True,
            "order_id": order_id,
            "events": events,
            "slot_state": asdict(slot_state),
        }

    def _market_has_open_exposure(self, market_id: str) -> bool:
        return any(position.quantity != 0 for (_, candidate_market_id, _), position in self.positions.items() if candidate_market_id == market_id)

    def has_strategy_market_exposure(self, strategy_family: str, market_id: str) -> bool:
        return any(
            position.quantity != 0
            for (family, candidate_market_id, _), position in self.positions.items()
            if family == strategy_family and candidate_market_id == market_id
        )

    def _update_pending_resolution(self, slot_id: str, now_ts: float) -> Dict:
        state = self.pending_resolution.get(slot_id)
        if state is None:
            state = {
                "slot_id": slot_id,
                "first_pending_ts": now_ts,
                "next_poll_ts": now_ts,
                "delay_seconds": self.resolution_initial_poll_seconds,
                "deferred": False,
            }
            self.pending_resolution[slot_id] = state
            return state

        elapsed = now_ts - state["first_pending_ts"]
        if elapsed >= self.resolution_poll_cap_seconds:
            state["deferred"] = True
            state["next_poll_ts"] = now_ts + self.resolution_initial_poll_seconds
            return state

        state["delay_seconds"] = min(max(state["delay_seconds"] * 2, self.resolution_initial_poll_seconds), 60)
        state["next_poll_ts"] = now_ts + state["delay_seconds"]
        return state

    def _settle_market_positions(self, market: Dict, winning_outcome: str, settled_ts: float) -> List[Dict]:
        settlement_events: List[Dict] = []
        for (family, market_id, outcome), position in list(self.positions.items()):
            if market_id != market["id"] or position.quantity == 0:
                continue
            quantity_before = position.quantity
            average_price = position.average_price
            payout = 1.0 if outcome == winning_outcome else 0.0
            side = "SELL" if quantity_before > 0 else "BUY"
            realized = self._apply_fill(
                strategy_family=family,
                market_id=market_id,
                outcome=outcome,
                side=side,
                size=abs(quantity_before),
                price=payout,
            )
            self.resolved_trade_count += 1
            if realized >= 0:
                self.win_count += 1
            else:
                self.loss_count += 1
            event = {
                "event_type": "market.settled",
                "slot_id": market["slot_id"],
                "market_id": market_id,
                "market_slug": market["slug"],
                "strategy_family": family,
                "outcome": outcome,
                "winning_outcome": winning_outcome,
                "quantity": quantity_before,
                "average_price": average_price,
                "payout": payout,
                "realized_pnl_delta": realized,
                "settled_ts": settled_ts,
            }
            settlement_events.append(event)
            self.latest_settlement = event

        if market["slot_id"] in self.signal_slots:
            self.signal_slots[market["slot_id"]].status = "settled"
            del self.signal_slots[market["slot_id"]]
        return settlement_events

    async def process_pending_resolutions(self, now_ts: Optional[float] = None) -> List[Dict]:
        now_ts = now_ts or time.time()
        events: List[Dict] = []
        for slot_id, market in list(self.market_registry.items()):
            if market["end_ts"] > now_ts:
                continue

            await self.cancel_market_orders(market["id"])
            if not self._market_has_open_exposure(market["id"]):
                self.pending_resolution.pop(slot_id, None)
                continue

            state = self.pending_resolution.get(slot_id)
            if state is None:
                state = self._update_pending_resolution(slot_id, now_ts)
                self._persist_events(
                    self.settlement_engine.pending_event(
                        slot_id=slot_id,
                        market_id=market["id"],
                        market_slug=market["slug"],
                        run_id=self.run_id,
                        sequence_num=self._next_sequence("market_slot", slot_id),
                        recorded_ts=now_ts,
                        next_poll_ts=state["next_poll_ts"],
                        first_pending_ts=state["first_pending_ts"],
                        delay_seconds=state["delay_seconds"],
                        deferred=state["deferred"],
                        correlation_id=slot_id,
                    )
                )
                events.append({
                    "event_type": "market.pending_resolution",
                    "slot_id": slot_id,
                    "market_id": market["id"],
                    "market_slug": market["slug"],
                    "next_poll_ts": state["next_poll_ts"],
                    "deferred": state["deferred"],
                })

            if now_ts < state["next_poll_ts"]:
                continue

            try:
                refreshed_market = await self.md.get_market_by_slug(market["slug"])
                self.register_market(refreshed_market)
            except Exception as exc:
                logger.warning("Failed to refresh market %s for resolution: %s", market["slug"], exc)
                state = self._update_pending_resolution(slot_id, now_ts)
                continue

            winning_outcome = self.md.get_winning_outcome(refreshed_market)
            if not refreshed_market.get("closed") or winning_outcome is None:
                state = self._update_pending_resolution(slot_id, now_ts)
                self._persist_events(
                    self.settlement_engine.pending_event(
                        slot_id=slot_id,
                        market_id=refreshed_market["id"],
                        market_slug=refreshed_market["slug"],
                        run_id=self.run_id,
                        sequence_num=self._next_sequence("market_slot", slot_id),
                        recorded_ts=now_ts,
                        next_poll_ts=state["next_poll_ts"],
                        first_pending_ts=state["first_pending_ts"],
                        delay_seconds=state["delay_seconds"],
                        deferred=state["deferred"],
                        correlation_id=slot_id,
                    )
                )
                events.append({
                    "event_type": "market.pending_resolution",
                    "slot_id": slot_id,
                    "market_id": refreshed_market["id"],
                    "market_slug": refreshed_market["slug"],
                    "next_poll_ts": state["next_poll_ts"],
                    "deferred": state["deferred"],
                })
                continue

            self.pending_resolution.pop(slot_id, None)
            events.extend(self._settle_market_positions(refreshed_market, winning_outcome, now_ts))
            self._persist_events(
                self.settlement_engine.settled_event(
                    slot_id=slot_id,
                    market_id=refreshed_market["id"],
                    market_slug=refreshed_market["slug"],
                    winning_outcome=winning_outcome,
                    settled_ts=now_ts,
                    run_id=self.run_id,
                    sequence_num=self._next_sequence("market_slot", slot_id),
                    correlation_id=slot_id,
                )
            )
        return events

    def get_family_metrics(self) -> Dict[str, Dict]:
        if self.ledger is None:
            return {
                family: {
                    **metrics,
                    "realized_pnl": round(metrics["realized_pnl"], 6),
                }
                for family, metrics in self.family_metrics.items()
            }
        return self.get_replay_family_metrics()

    def get_replay_family_metrics(self) -> Dict[str, Dict]:
        metrics: Dict[str, Dict] = {
            family: {
                **values,
                "realized_pnl": round(values["realized_pnl"], 6),
            }
            for family, values in self.family_metrics.items()
        }
        projection = self.get_replay_projection()
        for order in projection.orders.values():
            family = order.get("strategy_family", "unknown")
            entry = metrics.setdefault(
                family,
                {
                    "quotes_submitted": 0,
                    "orders_resting": 0,
                    "orders_filled": 0,
                    "cancellations": 0,
                    "realized_pnl": 0.0,
                    "markets_seen": 0,
                    "toxic_book_skips": 0,
                },
            )
            if order.get("order_kind") == "quote":
                entry["quotes_submitted"] += 1
            if order.get("status") in {"open", "partially_filled", "acknowledged"}:
                entry["orders_resting"] += 1
            if order.get("status") == "filled":
                entry["orders_filled"] += 1
            if order.get("status") == "cancelled":
                entry["cancellations"] += 1
        family_realized: Dict[str, float] = {}
        for (family, _market_id, _outcome), position in projection.positions.items():
            family_realized[family] = family_realized.get(family, 0.0) + float(position.get("realized_pnl", 0.0))
        for family, realized in family_realized.items():
            metrics.setdefault(
                family,
                {
                    "quotes_submitted": 0,
                    "orders_resting": 0,
                    "orders_filled": 0,
                    "cancellations": 0,
                    "realized_pnl": 0.0,
                    "markets_seen": 0,
                    "toxic_book_skips": 0,
                },
            )["realized_pnl"] = round(realized, 6)
        return metrics

    def get_realized_pnl_total(self) -> float:
        return self.realized_pnl_total

    def get_ledger_events(self) -> list[LedgerEvent]:
        if self.ledger is None:
            return []
        return self.ledger.list_events(run_id=self.run_id)

    def get_replay_projection(self):
        if self.ledger is not None:
            return replay_ledger(self.get_ledger_events())
        projection = replay_ledger([])
        projection.orders = {order_id: dict(order) for order_id, order in self.orders.items()}
        projection.open_orders = {order_id for order_id, order in self.orders.items() if order.get("status") in {"open", "partially_filled", "acknowledged"}}
        projection.pending_slots = {slot_id: dict(state) for slot_id, state in self.pending_resolution.items()}
        projection.realized_pnl_total = float(self.realized_pnl_total)
        projection.resolved_trade_count = int(self.resolved_trade_count)
        projection.win_count = int(self.win_count)
        projection.loss_count = int(self.loss_count)
        projection.latest_settlement = None if self.latest_settlement is None else dict(self.latest_settlement)
        for key, position in self.positions.items():
            projection.positions[key] = {
                "slot_id": None,
                "market_id": key[1],
                "outcome": key[2],
                "strategy_family": key[0],
                "quantity": float(position.quantity),
                "average_price": float(position.average_price),
                "realized_pnl": 0.0,
            }
        projection.exposure = {
            "open_position_count": len([position for position in self.positions.values() if abs(position.quantity) > 1e-9]),
            "open_order_count": len(projection.open_orders),
            "gross_position_exposure": 0.0,
            "gross_open_order_exposure": 0.0,
            "reserved_buy_order_notional": 0.0,
            "pending_settlement_exposure": 0.0,
            "pending_settlement_count": len(projection.pending_slots),
            "total_gross_exposure": 0.0,
            "by_strategy_family": {},
            "by_market_id": {},
            "by_slot_id": {},
            "by_asset": {},
            "by_interval": {},
        }
        return projection

    def _position_mark_payload(self, position: Dict, orderbook) -> Dict:
        quantity = float(position.get("quantity", 0.0))
        average_price = float(position.get("average_price", 0.0))
        outcome = position.get("outcome")
        if abs(quantity) <= 1e-9 or orderbook is None:
            return {
                "mark_price": None,
                "mark_source": "unavailable",
                "mark_ts": None,
                "unrealized_pnl": 0.0,
            }

        if quantity > 0:
            direct = float(self.md.best_bid(orderbook, outcome))
            if direct > 0:
                return {
                    "mark_price": direct,
                    "mark_source": "best_bid",
                    "mark_ts": float(orderbook.timestamp),
                    "unrealized_pnl": round(quantity * (direct - average_price), 6),
                }
        else:
            direct = float(self.md.best_ask(orderbook, outcome))
            if direct > 0:
                return {
                    "mark_price": direct,
                    "mark_source": "best_ask",
                    "mark_ts": float(orderbook.timestamp),
                    "unrealized_pnl": round(quantity * (direct - average_price), 6),
                }

        labels = getattr(orderbook, "outcome_labels", ("YES", "NO"))
        alt_outcome = labels[1] if outcome == labels[0] else labels[0]
        if quantity > 0:
            alt_ask = float(self.md.best_ask(orderbook, alt_outcome))
            if alt_ask > 0:
                mark_price = max(0.0, min(1.0, 1.0 - alt_ask))
                return {
                    "mark_price": mark_price,
                    "mark_source": "complement_ask",
                    "mark_ts": float(orderbook.timestamp),
                    "unrealized_pnl": round(quantity * (mark_price - average_price), 6),
                }
        else:
            alt_bid = float(self.md.best_bid(orderbook, alt_outcome))
            if alt_bid > 0:
                mark_price = max(0.0, min(1.0, 1.0 - alt_bid))
                return {
                    "mark_price": mark_price,
                    "mark_source": "complement_bid",
                    "mark_ts": float(orderbook.timestamp),
                    "unrealized_pnl": round(quantity * (mark_price - average_price), 6),
                }

        mid = float(self.md.mid_price(orderbook, outcome))
        if mid > 0:
            return {
                "mark_price": mid,
                "mark_source": "mid_price",
                "mark_ts": float(orderbook.timestamp),
                "unrealized_pnl": round(quantity * (mid - average_price), 6),
            }

        return {
            "mark_price": None,
            "mark_source": "unavailable",
            "mark_ts": None,
            "unrealized_pnl": 0.0,
        }

    def get_runtime_snapshot(self, now_ts: Optional[float] = None, orderbooks_by_market: Optional[Dict[str, object]] = None) -> Dict:
        now_ts = now_ts or time.time()
        projection = self.get_replay_projection()
        orderbooks_by_market = orderbooks_by_market or {}
        marks_by_position: Dict[Tuple[str, str, str], Dict] = {}
        open_positions = []
        for (family, market_id, outcome), position in projection.positions.items():
            if abs(float(position.get("quantity", 0.0))) <= 1e-9:
                continue
            mark_payload = self._position_mark_payload(position, orderbooks_by_market.get(market_id))
            marks_by_position[(family, market_id, outcome)] = mark_payload
            open_positions.append(
                {
                    "strategy_family": family,
                    "market_id": market_id,
                    "outcome": outcome,
                    "quantity": position.get("quantity", 0.0),
                    "average_price": position.get("average_price", 0.0),
                    "slot_id": position.get("slot_id"),
                    "realized_pnl": position.get("realized_pnl", 0.0),
                    **mark_payload,
                }
            )
        active_slots = [
            {
                "slot_id": market["slot_id"],
                "market_id": market["id"],
                "market_slug": market["slug"],
                "asset": market.get("asset"),
                "interval_minutes": market.get("interval_minutes"),
                "end_ts": market["end_ts"],
            }
            for market in self.market_registry.values()
            if market["end_ts"] > now_ts
        ]
        projection.exposure = build_exposure_snapshot(projection, marks_by_position=marks_by_position)
        pending_slots = [
            {
                "slot_id": slot_id,
                "market_slug": self.market_registry.get(slot_id, {}).get("slug") or state.get("market_slug"),
                "next_poll_ts": state.get("next_poll_ts"),
                "deferred": bool(state.get("deferred", False)),
            }
            for slot_id, state in projection.pending_slots.items()
        ]
        resolved_trade_count = int(projection.resolved_trade_count)
        win_count = int(projection.win_count)
        loss_count = int(projection.loss_count)
        return {
            "open_position_count": len(open_positions),
            "open_positions": open_positions,
            "resolved_trade_count": resolved_trade_count,
            "win_count": win_count,
            "loss_count": loss_count,
            "win_rate": (win_count / resolved_trade_count) if resolved_trade_count else 0.0,
            "active_slots": active_slots,
            "pending_resolution_slots": pending_slots,
            "latest_settlement": projection.latest_settlement or self.latest_settlement,
            "realized_pnl_total": float(projection.realized_pnl_total),
            "unrealized_pnl_total": float(projection.exposure.get("unrealized_pnl_total", 0.0)),
            "marked_position_count": int(projection.exposure.get("marked_position_count", 0)),
            "unmarked_position_count": int(projection.exposure.get("unmarked_position_count", 0)),
            "exposure": projection.exposure,
            "open_order_count": len(projection.open_orders),
        }

    def _get_token_id(self, market_id: str, outcome: str) -> str:
        for market in self.market_registry.values():
            if market["id"] == market_id:
                return market["token_ids"][outcome]
        raise NotImplementedError("Need token ID mapping from market data for live mode")

    async def refresh_positions(self):
        if self.mode != "live":
            positions = {}
            for (family, market_id, outcome), position in self.positions.items():
                if position.quantity == 0:
                    continue
                slot_id = None
                market_slug = None
                for market in self.market_registry.values():
                    if market["id"] == market_id:
                        slot_id = market["slot_id"]
                        market_slug = market["slug"]
                        break
                positions[f"{family}:{market_id}:{outcome}"] = {
                    "quantity": position.quantity,
                    "average_price": position.average_price,
                    "slot_id": slot_id,
                    "market_slug": market_slug,
                }
            return positions
        try:
            async with self.session.get(f"{self.clob_url}/account?wallet={self.wallet_address}") as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as exc:
            logger.error("Failed to refresh positions: %s", exc)
        return None


if __name__ == "__main__":
    print("Execution module. Use as library.")
