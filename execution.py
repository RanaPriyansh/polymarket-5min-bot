"""
Execution layer for Polymarket orders.
Handles paper/live placement, cancellation, per-strategy counters, and realized PnL accounting.
"""

from __future__ import annotations

import aiohttp
import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


@dataclass
class FamilyPosition:
    quantity: float = 0.0
    average_price: float = 0.0


class PolymarketExecutor:
    def __init__(self, config: dict, market_data: 'PolymarketData', mode: str = "paper"):
        self.clob_url = config["polymarket"]["clob_api_url"]
        self.wallet_address = config["polymarket"].get("wallet_address", "paper-wallet")
        self.private_key = config["polymarket"].get("private_key", "paper-key")
        self.session = None
        self.md = market_data
        self.mode = mode
        self.orders: Dict[str, Dict] = {}
        self.positions: Dict[Tuple[str, str, str], FamilyPosition] = {}
        self.family_metrics: Dict[str, Dict] = {}
        self.realized_pnl_total = 0.0

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

    async def place_order(self, market_id: str, outcome: str, side: str, size: float, price: float,
                          post_only: bool = True, strategy_family: str = "unknown",
                          order_kind: str = "signal") -> Optional[str]:
        if size <= 0 or price <= 0 or price > 1:
            logger.error("Invalid order parameters: size=%s price=%s", size, price)
            return None

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

        if self.mode == "live":
            payload = {
                "market_id": market_id,
                "token_id": self._get_token_id(market_id, outcome),
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

    def fill_order(self, order_id: str, *, fill_price: Optional[float] = None,
                   fill_size: Optional[float] = None, fill_ts: Optional[float] = None) -> Dict:
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
            "realized_pnl_delta": realized_delta,
            "fill_price": executed_price,
            "size": executed_size,
        }

    def evaluate_market_orders(self, market_id: str, orderbook: 'OrderBook'):
        fills = []
        for order_id, order in list(self.orders.items()):
            if order["market_id"] != market_id or order["status"] != "open":
                continue
            if order["outcome"] == "YES":
                best_bid = orderbook.yes_bids[0][0] if orderbook.yes_bids else 0.0
                best_ask = orderbook.yes_asks[0][0] if orderbook.yes_asks else 0.0
            else:
                best_bid = orderbook.no_bids[0][0] if orderbook.no_bids else 0.0
                best_ask = orderbook.no_asks[0][0] if orderbook.no_asks else 0.0
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

    def _get_token_id(self, market_id: str, outcome: str) -> str:
        raise NotImplementedError("Need token ID mapping from market data for live mode")

    async def refresh_positions(self):
        if self.mode != "live":
            return {
                f"{family}:{market}:{outcome}": {
                    "quantity": position.quantity,
                    "average_price": position.average_price,
                }
                for (family, market, outcome), position in self.positions.items()
                if position.quantity != 0
            }
        try:
            async with self.session.get(f"{self.clob_url}/account?wallet={self.wallet_address}") as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as exc:
            logger.error("Failed to refresh positions: %s", exc)
        return None


if __name__ == "__main__":
    print("Execution module. Use as library.")
