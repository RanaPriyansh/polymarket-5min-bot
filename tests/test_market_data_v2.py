import asyncio
import unittest

from execution import PolymarketExecutor
from market_data import PolymarketData


class MarketDataV2Tests(unittest.TestCase):
    def test_clob_v2_url_resolution_prefers_read_only_environment(self):
        md = PolymarketData({
            "polymarket": {
                "clob_api_version": "v2",
                "clob_environment": "read_only",
                "clob_api_url": "https://clob.polymarket.com",
                "clob_urls": {
                    "production": "https://clob.polymarket.com",
                    "read_only": "https://clob-v2.polymarket.com",
                },
                "gamma_api_url": "https://gamma-api.polymarket.com",
            }
        })
        self.assertEqual(md.clob_url, "https://clob-v2.polymarket.com")

    def test_book_metadata_preserves_v2_fields(self):
        payload = {
            "market": "0xcondition",
            "asset_id": "token-1",
            "timestamp": "1234567890000",
            "hash": "book-hash",
            "bids": [{"price": "0.45", "size": "100"}],
            "asks": [{"price": "0.46", "size": "150"}],
            "min_order_size": "1",
            "tick_size": "0.01",
            "neg_risk": False,
            "fees_enabled": True,
            "fee_schedule": {"maker_bps": 0, "taker_bps": 10},
            "last_trade_price": "0.45",
        }
        metadata = PolymarketData._book_metadata(payload, "token-1")
        self.assertEqual(metadata["token_id"], "token-1")
        self.assertEqual(metadata["condition_id"], "0xcondition")
        self.assertEqual(metadata["min_order_size"], 1.0)
        self.assertEqual(metadata["tick_size"], 0.01)
        self.assertEqual(metadata["hash"], "book-hash")
        self.assertFalse(metadata["neg_risk"])
        self.assertTrue(metadata["fees_enabled"])
        self.assertEqual(metadata["fee_schedule"]["taker_bps"], 10)

    def test_server_time_normalization(self):
        self.assertEqual(PolymarketData._normalize_server_time("123"), 123.0)
        self.assertEqual(PolymarketData._normalize_server_time({"timestamp": "456"}), 456.0)

    def test_executor_live_mode_is_hard_blocked(self):
        executor = PolymarketExecutor(
            {
                "polymarket": {"clob_api_url": "https://clob.polymarket.com"},
                "execution": {},
            },
            market_data=None,
            mode="live",
        )
        with self.assertRaises(RuntimeError):
            asyncio.run(executor.__aenter__())


if __name__ == "__main__":
    unittest.main()
