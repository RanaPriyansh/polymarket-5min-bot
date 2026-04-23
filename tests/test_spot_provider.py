"""Tests for spot_provider module.

No real HTTP calls — all interactions mocked via injected session.
"""
from __future__ import annotations

import unittest

from spot_provider import SpotProvider, SYMBOL_MAP


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeSession:
    """Duck-typed requests.Session-ish."""
    def __init__(self):
        self.calls = []
        self.next_response = None
        self.raise_on_get = None

    def get(self, url, timeout=None):
        self.calls.append((url, timeout))
        if self.raise_on_get is not None:
            raise self.raise_on_get
        return self.next_response


class SpotProviderTests(unittest.TestCase):
    def test_symbol_map_uppercase(self):
        self.assertEqual(SYMBOL_MAP["btc"], "BTCUSDT")
        self.assertEqual(SYMBOL_MAP["eth"], "ETHUSDT")
        self.assertEqual(SYMBOL_MAP["sol"], "SOLUSDT")
        self.assertEqual(SYMBOL_MAP["xrp"], "XRPUSDT")

    def test_prime_returns_value(self):
        sp = SpotProvider(requests_session=_FakeSession())
        sp.prime("btc", 70000.0)
        self.assertEqual(sp.get("btc"), 70000.0)

    def test_prime_is_case_insensitive(self):
        sp = SpotProvider(requests_session=_FakeSession())
        sp.prime("BTC", 65000.0)
        self.assertEqual(sp.get("btc"), 65000.0)
        self.assertEqual(sp.get("BTC"), 65000.0)

    def test_get_unknown_asset_returns_none(self):
        sp = SpotProvider(requests_session=_FakeSession())
        sp.prime("btc", 70000.0)
        self.assertIsNone(sp.get("doge"))

    def test_get_before_start_returns_none(self):
        sp = SpotProvider(requests_session=_FakeSession())
        # no prime, no start — cache is empty
        self.assertIsNone(sp.get("btc"))
        self.assertIsNone(sp.get("eth"))

    def test_fetch_once_populates_cache(self):
        session = _FakeSession()
        session.next_response = _FakeResponse({"symbol": "BTCUSDT", "price": "71234.5"})
        sp = SpotProvider(requests_session=session)
        sp.fetch_once("btc")
        self.assertAlmostEqual(sp.get("btc"), 71234.5, places=2)
        self.assertEqual(len(session.calls), 1)
        self.assertIn("BTCUSDT", session.calls[0][0])

    def test_fail_soft_on_http_error_keeps_prior_value(self):
        session = _FakeSession()
        sp = SpotProvider(requests_session=session)
        sp.prime("btc", 70000.0)

        # Now inject a failing session call
        session.raise_on_get = RuntimeError("boom network")
        # fetch_once should not raise
        sp.fetch_once("btc")
        # prior primed value still cached
        self.assertEqual(sp.get("btc"), 70000.0)

    def test_fail_soft_on_bad_payload(self):
        session = _FakeSession()
        sp = SpotProvider(requests_session=session)
        sp.prime("eth", 3500.0)

        session.next_response = _FakeResponse({"unexpected": "shape"})
        sp.fetch_once("eth")
        self.assertEqual(sp.get("eth"), 3500.0)

    def test_start_stop_does_not_crash(self):
        """Smoke test: start + stop the daemon thread quickly without network."""
        session = _FakeSession()
        session.next_response = _FakeResponse({"symbol": "BTCUSDT", "price": "100.0"})
        sp = SpotProvider(requests_session=session, fetch_interval_seconds=0.01)
        sp.start()
        try:
            # give the thread a few loop iterations
            import time
            time.sleep(0.05)
        finally:
            sp.stop()
        # join completes cleanly
        self.assertFalse(sp.is_running())


if __name__ == "__main__":
    unittest.main()
