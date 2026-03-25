"""
Execution layer for Polymarket orders.

Execution modes:
- PaperBroker: simulated broker with resting-order support, simple crossing logic,
  inventory tracking, cash/equity accounting, and order processing on new books.
- LiveBroker: guarded scaffold that fails fast until real credentials + token mappings exist.
"""

import time
from dataclasses import dataclass
from typing import Dict, Optional

import aiohttp
import logging

from market_data import PolymarketData

logger = logging.getLogger(__name__)


@dataclass
class OrderRecord:
    order_id: str
    market_id: str
    outcome: str
    side: str
    size: float
    price: float
    status: str
    timestamp: float
    mode: str
    filled_size: float = 0.0
    remaining_size: float = 0.0
    average_fill_price: float = 0.0


class BaseBroker:
    mode = "base"

    def __init__(self, config: dict, market_data: "PolymarketData"):
        self.config = config
        self.md = market_data
        self.orders: Dict[str, OrderRecord] = {}
        self.positions: Dict[str, Dict[str, float]] = {}
        self.session = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return None

    async def place_order(self, market_id: str, outcome: str, side: str, size: float, price: float, post_only: bool = True):
        raise NotImplementedError

    async def cancel_order(self, order_id: str):
        raise NotImplementedError

    async def cancel_all_market(self, market_id: str):
        to_cancel = [oid for oid, order in self.orders.items() if order.market_id == market_id]
        for order_id in to_cancel:
            await self.cancel_order(order_id)

    async def refresh_positions(self):
        return self.positions

    def _ensure_market_position(self, market_id: str):
        if market_id not in self.positions:
            self.positions[market_id] = {"YES": 0.0, "NO": 0.0}
        return self.positions[market_id]


class PaperBroker(BaseBroker):
    mode = "paper"

    def __init__(self, config: dict, market_data: "PolymarketData"):
        super().__init__(config, market_data)
        self.initial_capital = float(config.get("paper", {}).get("initial_capital", 1000.0))
        self.cash = self.initial_capital
        self.realized_pnl = 0.0
        self.open_orders: Dict[str, OrderRecord] = {}
        self.last_marks: Dict[str, Dict[str, float]] = {}

    def _next_order_id(self) -> str:
        return f"paper-{int(time.time() * 1000)}-{len(self.orders) + 1}"

    def _crosses_touch(self, order: OrderRecord, orderbook) -> bool:
        if order.outcome == "YES":
            best_bid = orderbook.yes_bids[0][0] if orderbook.yes_bids else 0.0
            best_ask = orderbook.yes_asks[0][0] if orderbook.yes_asks else 0.0
        else:
            best_bid = orderbook.no_bids[0][0] if orderbook.no_bids else 0.0
            best_ask = orderbook.no_asks[0][0] if orderbook.no_asks else 0.0

        if order.side.upper() == "BUY":
            return best_ask > 0 and order.price >= best_ask
        return best_bid > 0 and order.price <= best_bid

    def _mark_price(self, orderbook, outcome: str) -> float:
        return PolymarketData.mid_price(orderbook, outcome)

    def _touch_price(self, orderbook, outcome: str, side: str) -> float:
        side = side.upper()
        if outcome == "YES":
            best_bid = orderbook.yes_bids[0][0] if orderbook.yes_bids else 0.0
            best_ask = orderbook.yes_asks[0][0] if orderbook.yes_asks else 0.0
        else:
            best_bid = orderbook.no_bids[0][0] if orderbook.no_bids else 0.0
            best_ask = orderbook.no_asks[0][0] if orderbook.no_asks else 0.0
        if side == "BUY":
            return best_ask if best_ask > 0 else self._mark_price(orderbook, outcome)
        return best_bid if best_bid > 0 else self._mark_price(orderbook, outcome)

    def _apply_fill(self, order: OrderRecord, fill_price: float, fill_size: float):
        signed_size = fill_size if order.side.upper() == "BUY" else -fill_size
        market_pos = self._ensure_market_position(order.market_id)
        market_pos[order.outcome] = market_pos.get(order.outcome, 0.0) + signed_size

        cash_delta = fill_size * fill_price
        if order.side.upper() == "BUY":
            self.cash -= cash_delta
        else:
            self.cash += cash_delta

        order.filled_size += fill_size
        order.remaining_size = max(order.size - order.filled_size, 0.0)
        order.average_fill_price = fill_price if order.average_fill_price == 0 else (
            ((order.average_fill_price * (order.filled_size - fill_size)) + (fill_price * fill_size)) / order.filled_size
        )
        order.status = "filled" if order.remaining_size <= 0 else "partial"
        logger.info("PAPER FILL %s %s %s@%s %s", order.market_id, order.side, fill_size, fill_price, order.outcome)

    async def place_order(self, market_id: str, outcome: str, side: str, size: float, price: float, post_only: bool = True):
        if size <= 0 or not (0 < price < 1):
            logger.error("Paper order rejected: invalid params size=%s price=%s", size, price)
            return None

        order_id = self._next_order_id()
        order = OrderRecord(
            order_id=order_id,
            market_id=market_id,
            outcome=outcome,
            side=side.upper(),
            size=float(size),
            price=float(price),
            status="open",
            timestamp=time.time(),
            mode=self.mode,
            filled_size=0.0,
            remaining_size=float(size),
            average_fill_price=0.0,
        )
        self.orders[order_id] = order

        current_book = self.md.orderbooks.get(market_id) if self.md is not None else None
        if current_book is not None and self._crosses_touch(order, current_book):
            fill_price = self._touch_price(current_book, outcome, order.side)
            self._apply_fill(order, fill_price, order.size)
        elif not post_only and current_book is not None:
            fill_price = self._touch_price(current_book, outcome, order.side)
            self._apply_fill(order, fill_price, order.size)
        else:
            order.status = "open"
            self.open_orders[order_id] = order
            logger.info("PAPER RESTING ORDER %s %s %s@%s %s", market_id, side, size, price, outcome)

        return order_id

    async def process_orderbook(self, market_id: str, orderbook):
        self.last_marks[market_id] = {
            "YES": PolymarketData.mid_price(orderbook, "YES"),
            "NO": PolymarketData.mid_price(orderbook, "NO"),
        }
        for order_id, order in list(self.open_orders.items()):
            if order.market_id != market_id or order.status not in {"open", "partial"}:
                continue
            if self._crosses_touch(order, orderbook):
                fill_price = self._touch_price(orderbook, order.outcome, order.side)
                self._apply_fill(order, fill_price, order.remaining_size)
                if order.status == "filled":
                    self.open_orders.pop(order_id, None)

    async def cancel_order(self, order_id: str):
        order = self.orders.get(order_id)
        if not order:
            return False
        if order.status == "filled":
            return False
        order.status = "cancelled"
        self.open_orders.pop(order_id, None)
        return True

    def equity(self) -> float:
        mark_value = 0.0
        for market_id, outcomes in self.positions.items():
            marks = self.last_marks.get(market_id, {})
            for outcome, size in outcomes.items():
                mark = marks.get(outcome, 0.0)
                mark_value += size * mark
        return self.cash + mark_value

    def total_pnl(self) -> float:
        return self.equity() - self.initial_capital

    async def refresh_positions(self):
        return {
            "positions": self.positions,
            "cash": round(self.cash, 2),
            "equity": round(self.equity(), 2),
            "open_orders": len(self.open_orders),
            "realized_pnl": round(self.realized_pnl, 2),
            "total_pnl": round(self.total_pnl(), 2),
        }


class LiveBroker(BaseBroker):
    mode = "live"

    def __init__(self, config: dict, market_data: "PolymarketData"):
        super().__init__(config, market_data)
        self.clob_url = config["polymarket"]["clob_api_url"]
        self.wallet_address = config["polymarket"].get("wallet_address", "")
        self.private_key = config["polymarket"].get("private_key", "")
        self.token_mappings = config["polymarket"].get("token_mappings", {})
        self._validate_live_config()

    def _validate_live_config(self):
        if not self.wallet_address or self.wallet_address == "YOUR_WALLET_ADDRESS":
            raise ValueError("Live mode requires a real wallet_address in config.yaml")
        if not self.private_key or self.private_key == "YOUR_PRIVATE_KEY":
            raise ValueError("Live mode requires a real private_key in config.yaml")
        if not self.token_mappings:
            raise ValueError("Live mode requires polymarket.token_mappings in config.yaml")

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session is not None:
            await self.session.close()

    def sign_order(self, order_params: dict) -> str:
        raise NotImplementedError("Proper signing required for live trading")

    def _get_token_id(self, market_id: str, outcome: str) -> str:
        key = f"{market_id}:{outcome}"
        token_id = self.token_mappings.get(key)
        if not token_id:
            raise KeyError(f"Missing token mapping for {key}")
        return token_id

    async def place_order(self, market_id: str, outcome: str, side: str, size: float, price: float, post_only: bool = True):
        if size <= 0 or price <= 0 or price >= 1:
            logger.error("Invalid live order parameters: size=%s price=%s", size, price)
            return None

        token_id = self._get_token_id(market_id, outcome)
        order = {
            "market_id": market_id,
            "token_id": token_id,
            "side": side.lower(),
            "size": str(size),
            "price": str(round(price, 4)),
            "type": "LIMIT",
            "post_only": post_only,
            "wallet": self.wallet_address,
        }

        self.sign_order(order)
        return None

    async def cancel_order(self, order_id: str):
        if self.session is None:
            return False
        try:
            async with self.session.delete(f"{self.clob_url}/order/{order_id}") as resp:
                return resp.status == 200
        except Exception as exc:
            logger.error("Cancel error: %s", exc)
            return False

    async def refresh_positions(self):
        if self.session is None:
            return self.positions
        try:
            async with self.session.get(f"{self.clob_url}/account?wallet={self.wallet_address}") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.positions = data.get("positions", {})
        except Exception as exc:
            logger.error("Failed to refresh positions: %s", exc)
        return self.positions


def create_broker(mode: str, config: dict, market_data: "PolymarketData") -> BaseBroker:
    if mode == "paper":
        return PaperBroker(config, market_data)
    if mode == "live":
        return LiveBroker(config, market_data)
    raise ValueError(f"Unsupported broker mode: {mode}")


if __name__ == "__main__":
    print("Execution module. Import create_broker() or broker classes.")
