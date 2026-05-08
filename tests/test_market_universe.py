import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from market_universe import OfficialPolymarketCliAdapter, normalize_cli_market, select_universe_markets


class MarketUniverseTests(unittest.TestCase):
    def test_normalize_cli_market_keeps_runtime_fields(self):
        raw = {
            "id": "123",
            "question": "Will Bitcoin close above $100k today?",
            "conditionId": "0xabc",
            "endDate": "2026-03-29T00:10:00Z",
            "outcomes": json.dumps(["Yes", "No"]),
            "clobTokenIds": json.dumps(["yes-token", "no-token"]),
            "bestBid": "0.47",
            "bestAsk": "0.49",
            "lastTradePrice": "0.48",
            "volumeNum": "25000",
            "liquidityNum": "12000",
            "tags": ["Crypto", "Bitcoin"],
        }

        market = normalize_cli_market(raw)

        self.assertEqual(market["id"], "123")
        self.assertEqual(market["conditionId"], "0xabc")
        self.assertEqual(market["yes_token_id"], "yes-token")
        self.assertEqual(market["no_token_id"], "no-token")
        self.assertEqual(market["price"], 0.48)
        self.assertEqual(market["bestBid"], 0.47)
        self.assertEqual(market["bestAsk"], 0.49)
        self.assertEqual(market["volume"], 25000.0)
        self.assertEqual(market["liquidity"], 12000.0)

    def test_strict_crypto_5m_15m_only_keeps_crypto_inside_15m(self):
        now = datetime.now(timezone.utc)
        markets = [
            {
                "id": "crypto-keep",
                "question": "Will Bitcoin pump?",
                "endDate": (now + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
                "volume": 50000,
                "liquidity": 20000,
                "tags": ["Crypto"],
            },
            {
                "id": "crypto-drop-long",
                "question": "Will Ethereum pump?",
                "endDate": (now + timedelta(minutes=45)).isoformat().replace("+00:00", "Z"),
                "volume": 50000,
                "liquidity": 20000,
                "tags": ["Crypto"],
            },
            {
                "id": "generic-drop",
                "question": "Will Team A win?",
                "endDate": (now + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
                "volume": 50000,
                "liquidity": 20000,
                "tags": ["Sports"],
            },
        ]

        selected = select_universe_markets(markets, {"mode": "strict_crypto_5m_15m", "min_liquidity": 1000})

        self.assertEqual([market["id"] for market in selected], ["crypto-keep"])

    def test_short_horizon_crypto_keeps_crypto_inside_custom_horizon(self):
        now = datetime.now(timezone.utc)
        markets = [
            {
                "id": "btc-30m",
                "question": "Will Bitcoin close green?",
                "endDate": (now + timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
                "volume": 50000,
                "liquidity": 12000,
                "tags": ["Crypto"],
            },
            {
                "id": "btc-3h",
                "question": "Will Bitcoin hit ATH?",
                "endDate": (now + timedelta(hours=3)).isoformat().replace("+00:00", "Z"),
                "volume": 50000,
                "liquidity": 12000,
                "tags": ["Crypto"],
            },
        ]

        selected = select_universe_markets(markets, {"mode": "short_horizon_crypto", "max_minutes": 60, "min_liquidity": 1000})

        self.assertEqual([market["id"] for market in selected], ["btc-30m"])

    def test_all_liquid_keeps_any_liquid_market(self):
        now = datetime.now(timezone.utc)
        markets = [
            {
                "id": "politics-1",
                "question": "Will candidate X win?",
                "endDate": (now + timedelta(days=2)).isoformat().replace("+00:00", "Z"),
                "volume": 70000,
                "liquidity": 25000,
                "tags": ["Politics"],
            },
            {
                "id": "illiquid-1",
                "question": "Will candidate Y win?",
                "endDate": (now + timedelta(days=2)).isoformat().replace("+00:00", "Z"),
                "volume": 50,
                "liquidity": 20,
                "tags": ["Politics"],
            },
        ]

        selected = select_universe_markets(markets, {"mode": "all_liquid", "min_liquidity": 1000})

        self.assertEqual([market["id"] for market in selected], ["politics-1"])

    def test_cli_adapter_fetch_orderbook_populates_yes_and_no_sides(self):
        adapter = OfficialPolymarketCliAdapter({})
        market = {"id": "m1", "yes_token_id": "yes-token", "no_token_id": "no-token"}

        yes_book = {"bids": [{"price": "0.45", "size": "10"}], "asks": [{"price": "0.47", "size": "12"}], "timestamp": "1000"}
        no_book = {"bids": [{"price": "0.53", "size": "8"}], "asks": [{"price": "0.55", "size": "9"}], "timestamp": "1100"}

        with patch.object(adapter, "_run_json", side_effect=[yes_book, no_book]):
            orderbook = adapter.fetch_orderbook(market, "YES")

        self.assertEqual(orderbook["yes_bids"][0], (0.45, 10.0))
        self.assertEqual(orderbook["yes_asks"][0], (0.47, 12.0))
        self.assertEqual(orderbook["no_bids"][0], (0.53, 8.0))
        self.assertEqual(orderbook["no_asks"][0], (0.55, 9.0))


if __name__ == "__main__":
    unittest.main()
