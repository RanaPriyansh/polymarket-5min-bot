"""
Execution layer for Polymarket orders
Handles order placement, cancellation, and position tracking
"""

import asyncio
import aiohttp
import json
import time
from typing import Dict, Optional, List
from web3 import Web3
import logging
from market_data import OrderBook

logger = logging.getLogger(__name__)

class PolymarketExecutor:
    def __init__(self, config: dict, market_data: 'PolymarketData'):
        self.clob_url = config["polymarket"]["clob_api_url"]
        self.wallet_address = config["polymarket"]["wallet_address"]
        self.private_key = config["polymarket"]["private_key"]
        self.session = None
        self.md = market_data
        self.orders = {}  # order_id -> order details
        self.positions = {}  # market_id -> {YES: size, NO: size}
        self.running = False
        # Connect to Polygon for signing (assuming Polygon chain)
        # self.w3 = Web3(Web3.HTTPProvider("https://polygon-rpc.com"))

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.close()

    def sign_order(self, order_params: dict) -> str:
        """Sign an order using wallet private key (EIP-712 signature)."""
        # Simplified: In production, need proper EIP-712 signing with Polymarket's spec
        # For MVP, we'll use API key auth if configured, or skip signature for paper
        # return "0x..." signature
        raise NotImplementedError("Proper signing required for live trading")

    async def place_order(self, market_id: str, outcome: str, side: str, size: float, price: float, post_only: bool = True) -> Optional[str]:
        """Place a limit order on Polymarket CLOB."""
        # Validate inputs
        if size <= 0 or price <= 0 or price > 1:
            logger.error(f"Invalid order parameters: size={size}, price={price}")
            return None

        order = {
            "market_id": market_id,
            "token_id": self._get_token_id(market_id, outcome),  # need to map
            "side": side.lower(),  # "BUY" or "SELL"
            "size": str(size),
            "price": str(round(price, 4)),
            "type": "LIMIT",
            "post_only": post_only,
            "wallet": self.wallet_address
        }

        # In a real implementation, we'd sign the order and include signature
        # order["signature"] = self.sign_order(order)

        try:
            async with self.session.post(f"{self.clob_url}/order", json=order) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    order_id = data.get("order_id")
                    self.orders[order_id] = {
                        "market_id": market_id,
                        "outcome": outcome,
                        "side": side,
                        "size": size,
                        "price": price,
                        "timestamp": time.time()
                    }
                    logger.info(f"Order placed: {order_id} {side} {size}@{price} {outcome}")
                    return order_id
                else:
                    logger.error(f"Order failed: {resp.status} - {await resp.text()}")
                    return None
        except Exception as e:
            logger.exception(f"Exception placing order: {e}")
            return None

    async def cancel_order(self, order_id: str):
        """Cancel an existing order."""
        try:
            async with self.session.delete(f"{self.clob_url}/order/{order_id}") as resp:
                if resp.status == 200:
                    self.orders.pop(order_id, None)
                    logger.info(f"Order cancelled: {order_id}")
                    return True
        except Exception as e:
            logger.error(f"Cancel error: {e}")
        return False

    async def cancel_all_market(self, market_id: str):
        """Cancel all orders for a given market."""
        to_cancel = [oid for oid, o in self.orders.items() if o["market_id"] == market_id]
        for oid in to_cancel:
            await self.cancel_order(oid)

    def _get_token_id(self, market_id: str, outcome: str) -> str:
        """Map market + outcome to token ID. In production, we'd query market data."""
        # For MVP, we'll require token_id to be provided via config or cache
        # Store mapping from previous market fetch
        raise NotImplementedError("Need token ID mapping from market data")

    async def refresh_positions(self):
        """Poll account balance and positions."""
        # GET /account endpoint
        try:
            async with self.session.get(f"{self.clob_url}/account?wallet={self.wallet_address}") as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.positions = data.get("positions", {})
                    return self.positions
        except Exception as e:
            logger.error(f"Failed to refresh positions: {e}")
        return None

if __name__ == "__main__":
    print("Execution module. Use as library.")