"""
Execution layer for Polymarket orders.
Handles paper/live placement, cancellation, paper position lifecycle, and settlement accounting.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Tuple
import logging

import aiohttp

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
    def __init__(self, config: dict, market_data: "PolymarketData", mode: str = "paper"):
        execution_cfg = config.get("execution", {})
        self.clob_url = config["polymarket"]["clob_api_url"]
        self.wallet_address = config["polymarket"].get("wallet_address", "paper-wallet")
        self.private_key = config["polymarket"].get("private_key", "paper-key")
        self.session = None
        self.md = market_data
        self.mode = mode
        self.paper_bankroll = float(execution_cfg.get("paper_starting_bankroll", 500.0))
        self.resolution_initial_poll_seconds = int(execution_cfg.get("resolution_initial_poll_seconds", 10))
        self.resolution_poll_cap_seconds = int(execution_cfg.get("resolution_poll_cap_seconds", 300))
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

    async def __aenter__(self):
        if self.mode == "live":
            self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            self.session = None

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
            token_id = market["token_ids"].get(outcome) if market else self._get_token_id(market_id, outcome)
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
    ) -> Dict:
        order = self.orders.get(order_id)
        if not order or order.get("status") != "open":
            return {"filled": False, "reason": "not_open"}

        executed_size = float(fill_size if fill_size is not None else order["remaining_size"])
        executed_price = float(fill_price if fill_price is not None else order["price"])
        realized_delta = self._apply_fill(
            strategy_family=order["strategy_family"],
            market_id=order["market_id"],
            outcome=order["outcome"],
            side=order["side"],
            size=executed_size,
            price=executed_price,
        )
        order["remaining_size"] = max(0.0, order["remaining_size"] - executed_size)
        order["filled_ts"] = fill_ts or time.time()
        order["fill_price"] = executed_price
        if order["remaining_size"] <= 1e-9:
            order["status"] = "filled"
            metrics = self._ensure_family_metrics(order["strategy_family"])
            metrics["orders_resting"] = max(0, metrics["orders_resting"] - 1)
            metrics["orders_filled"] += 1
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
            if order["market_id"] != market_id or order["status"] != "open":
                continue
            best_bid = self.md.best_bid(orderbook, order["outcome"])
            best_ask = self.md.best_ask(orderbook, order["outcome"])
            should_fill = (
                order["side"] == "BUY" and best_ask > 0 and order["price"] >= best_ask
            ) or (
                order["side"] == "SELL" and best_bid > 0 and order["price"] <= best_bid
            )
            if should_fill:
                market_price = best_ask if order["side"] == "BUY" else best_bid
                fills.append(self.fill_order(order_id, fill_price=market_price, fill_ts=orderbook.timestamp))
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
        return events

    def get_family_metrics(self) -> Dict[str, Dict]:
        return {
            family: {
                **metrics,
                "realized_pnl": round(metrics["realized_pnl"], 6),
            }
            for family, metrics in self.family_metrics.items()
        }

    def get_realized_pnl_total(self) -> float:
        return self.realized_pnl_total

    def get_runtime_snapshot(self, now_ts: Optional[float] = None) -> Dict:
        now_ts = now_ts or time.time()
        open_positions = [
            {
                "strategy_family": family,
                "market_id": market_id,
                "outcome": outcome,
                "quantity": position.quantity,
                "average_price": position.average_price,
            }
            for (family, market_id, outcome), position in self.positions.items()
            if position.quantity != 0
        ]
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
        pending_slots = [
            {
                "slot_id": slot_id,
                "market_slug": self.market_registry.get(slot_id, {}).get("slug"),
                "next_poll_ts": state["next_poll_ts"],
                "deferred": state["deferred"],
            }
            for slot_id, state in self.pending_resolution.items()
        ]
        return {
            "open_position_count": len(open_positions),
            "open_positions": open_positions,
            "resolved_trade_count": self.resolved_trade_count,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "win_rate": (self.win_count / self.resolved_trade_count) if self.resolved_trade_count else 0.0,
            "active_slots": active_slots,
            "pending_resolution_slots": pending_slots,
            "latest_settlement": self.latest_settlement,
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
