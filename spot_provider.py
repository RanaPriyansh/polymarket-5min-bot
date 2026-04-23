"""SpotProvider: lightweight Binance public-REST spot price cache.

Design:
- Background daemon thread polls every `fetch_interval_seconds` (default 2.0)
  for BTC, ETH, SOL, XRP spot prices.
- Caches last-known values, returns them via `get(asset)`.
- Fail-soft: any HTTP / parse error keeps the prior value and logs at DEBUG.
- For tests, pass `requests_session` (duck-typed; must expose `.get(url,
  timeout=...)` returning an object with `.json()`). Also `prime(asset, value)`
  to inject directly without network.

We avoid a hard dependency on the `requests` library — the default session is
a tiny stdlib-based adapter over `urllib.request`. Tests always inject their
own fake session, so this path is only hit in production.
"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price?symbol={symbol}"

# Canonical mapping. Stored lowercase → Binance uppercase symbol.
SYMBOL_MAP: Dict[str, str] = {
    "btc": "BTCUSDT",
    "eth": "ETHUSDT",
    "sol": "SOLUSDT",
    "xrp": "XRPUSDT",
}


class _UrllibResponse:
    """Tiny requests.Response-ish wrapper around urllib."""
    def __init__(self, body: bytes, status: int):
        self._body = body
        self.status_code = status

    def json(self) -> Any:
        return json.loads(self._body.decode("utf-8"))

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _UrllibSession:
    """Minimal drop-in for a requests.Session to avoid hard dep."""
    def get(self, url: str, timeout: float = 5.0) -> _UrllibResponse:
        with urllib.request.urlopen(url, timeout=timeout) as resp:  # nosec - fixed Binance URL
            body = resp.read()
            status = getattr(resp, "status", 200)
            return _UrllibResponse(body, status)


class SpotProvider:
    """Thread-safe last-known spot price cache for crypto assets."""

    def __init__(
        self,
        requests_session: Optional[Any] = None,
        fetch_interval_seconds: float = 2.0,
        timeout_seconds: float = 3.0,
        assets: Optional[list[str]] = None,
    ) -> None:
        self._session = requests_session if requests_session is not None else _UrllibSession()
        self._fetch_interval = float(fetch_interval_seconds)
        self._timeout = float(timeout_seconds)
        self._assets = [a.lower() for a in (assets or list(SYMBOL_MAP.keys()))]
        self._cache: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    # -------- public API --------

    def get(self, asset: str) -> Optional[float]:
        if not asset:
            return None
        key = asset.lower()
        with self._lock:
            return self._cache.get(key)

    def prime(self, asset: str, value: float) -> None:
        """Inject a cached value directly (tests + bootstrap)."""
        if not asset:
            return
        key = asset.lower()
        with self._lock:
            self._cache[key] = float(value)

    def fetch_once(self, asset: str) -> None:
        """Fetch a single asset, fail-soft. Public for tests + manual refresh."""
        key = asset.lower()
        symbol = SYMBOL_MAP.get(key)
        if not symbol:
            return
        url = BINANCE_TICKER_URL.format(symbol=symbol)
        try:
            resp = self._session.get(url, timeout=self._timeout)
            # raise_for_status optional; guard if present
            raise_for_status = getattr(resp, "raise_for_status", None)
            if callable(raise_for_status):
                raise_for_status()
            payload = resp.json()
            price_str = payload.get("price") if isinstance(payload, dict) else None
            if price_str is None:
                logger.debug("spot_provider: no 'price' in payload for %s: %r", symbol, payload)
                return
            price = float(price_str)
        except (urllib.error.URLError, OSError, ValueError, RuntimeError, TypeError, KeyError) as exc:
            logger.debug("spot_provider: fetch failed for %s: %s", symbol, exc)
            return
        except Exception as exc:  # noqa: BLE001 - fail-soft catch-all
            logger.debug("spot_provider: unexpected error for %s: %s", symbol, exc)
            return
        with self._lock:
            self._cache[key] = price

    def start(self) -> None:
        """Spawn the background poll thread (idempotent)."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="spot-provider",
            daemon=True,
        )
        self._thread.start()

    def stop(self, join_timeout: float = 2.0) -> None:
        """Signal the thread to stop and wait briefly."""
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=join_timeout)
        self._thread = None

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    # -------- internals --------

    def _run(self) -> None:
        while not self._stop_event.is_set():
            for asset in self._assets:
                if self._stop_event.is_set():
                    break
                self.fetch_once(asset)
            # sleep with early-wake on stop
            self._stop_event.wait(self._fetch_interval)
