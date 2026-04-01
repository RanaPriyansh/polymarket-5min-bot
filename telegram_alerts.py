#!/usr/bin/env python3
"""Minimal Telegram alert sender for Polymarket bot events."""
import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Optional

import aiohttp
import click
import yaml

logger = logging.getLogger(__name__)

TELEGRAM_BASE = "https://api.telegram.org/bot{token}/sendMessage"

EVENT_EMOJIS = {
    "entry": "&#x1F7E2;",
    "exit": "&#x1F534;",
    "pnl_update": "&#x1F4CA;",
    "circuit_breaker": "&#x1F6A8;",
    "error": "&#x274C;",
    "info": "&#x2139;&#xFE0F;",
    "signal": "&#x1F4E1;",
}

class TelegramNotifier:
    def __init__(self, config: dict):
        telegram_cfg = config.get("telegram", {}) or {}
        self.enabled = bool(telegram_cfg.get("enabled"))
        self.bot_token = telegram_cfg.get("bot_token", os.getenv("TELEGRAM_BOT_TOKEN", ""))
        self.chat_id = telegram_cfg.get("chat_id", os.getenv("TELEGRAM_CHAT_ID", ""))
        self._session: Optional[aiohttp.ClientSession] = None
        if self.enabled and (not self.bot_token or not self.chat_id):
            logger.warning("Telegram enabled but token or chat_id missing — disabling alerts")
            self.enabled = False
        elif self.enabled:
            logger.info(f"Telegram alerts active (chat: {self.chat_id})")

    async def _send(self, text: str, parse_mode: str = "HTML") -> bool:
        if not self.enabled or not self.bot_token or not self.chat_id:
            return False
        url = TELEGRAM_BASE.format(token=self.bot_token)
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True}
        try:
            if self._session is None:
                self._session = aiohttp.ClientSession()
            async with self._session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(f"Telegram send failed {resp.status}: {body}")
                    return False
                return True
        except Exception as e:
            logger.error(f"Telegram send error: {e}")
            return False

    def _ts(self) -> str:
        return datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    async def startup(self, mode: str, strategies: list):
        s = ", ".join(strategies) if strategies else "none"
        return await self._send(f"&#x1F916; <b>Polymarket Bot Online</b>\n{self._ts()}\nMode: {mode.upper()}\nStrategies: {s}")

    async def signal(self, strategy: str, slug: str, action: str, price: float, confidence: float):
        return await self._send(f"&#x1F4E1; <b>Signal — {strategy}</b>\n{self._ts()} | {slug}\n{action} @ {price:.4f}\nConfidence: {confidence:.0%}")

    async def circuit_breaker(self, reason: str, detail: str):
        return await self._send(f"&#x1F6A8; <b>Circuit Breaker</b>\n{self._ts()}\nReason: {reason}\n{detail}")

    async def error(self, message: str, context: str = ""):
        msg = f"&#x274C; <b>Error</b>\n{self._ts()} | {message}"
        if context:
            msg += f"\nContext: {context}"
        return await self._send(msg)

    async def close(self):
        if self._session:
            await self._session.close()
            self._session = None

def load_notifier(config_path: str = "config.yaml") -> TelegramNotifier:
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    return TelegramNotifier(cfg)
