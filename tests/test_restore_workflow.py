import unittest

import pandas as pd

from backtest_engine import Backtester
from execution import PolymarketExecutor
from market_data import OrderBook, PolymarketData
from strategies.mean_reversion_5min import Signal


class FakeMarketData:
    def __init__(self, market):
        self.market = market
        self.resolved_market = dict(market)

    def best_bid(self, orderbook, outcome):
        return PolymarketData.best_bid(orderbook, outcome)

    def best_ask(self, orderbook, outcome):
        return PolymarketData.best_ask(orderbook, outcome)

    async def get_market_by_slug(self, slug):
        return dict(self.resolved_market)

    def get_winning_outcome(self, market):
        return PolymarketData.get_winning_outcome(market)


class RestoreWorkflowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            "polymarket": {
                "clob_api_url": "https://clob.polymarket.com",
                "gamma_api_url": "https://gamma-api.polymarket.com",
                "wallet_address": "paper-wallet",
                "private_key": "paper-key",
                "assets": ["btc", "eth", "sol", "xrp"],
                "intervals": [5, 15],
            },
            "execution": {
                "paper_starting_bankroll": 500,
                "resolution_initial_poll_seconds": 10,
                "resolution_poll_cap_seconds": 300,
            },
            "strategies": {
                "mean_reversion_5min": {
                    "deviation_threshold": 0.05,
                    "ema_period": 3,
                    "imbalance_threshold": 0.0,
                    "kelly_fraction": 0.1,
                    "min_volume": 0,
                    "timeframes": ["5m", "15m"],
                },
                "toxicity_mm": {
                    "vpin_threshold": 0.45,
                    "spread_multiplier": 1.5,
                    "kelly_fraction": 0.2,
                    "timeframes": ["5m", "15m"],
                    "max_position": 1000,
                },
                "active": ["mean_reversion_5min", "toxicity_mm"],
            },
            "filters": {
                "max_book_spread_bps": 250,
                "min_top_depth": 1,
                "min_top_notional": 0,
                "max_depth_ratio": 100,
            },
            "risk": {
                "circuit_breaker_dd": 0.1,
                "max_daily_loss": 0.05,
                "max_position_size": 0.1,
                "max_risk_per_trade_usd": 10,
            },
        }
        self.market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "slot_id": "btc:5:100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": 130.0,
            "token_ids": {"Up": "up-token", "Down": "down-token"},
            "outcomes": ["Up", "Down"],
            "outcome_prices": [0.52, 0.48],
            "volume": 100000.0,
            "liquidity": 25000.0,
            "active": True,
            "closed": False,
            "accepting_orders": True,
            "enable_order_book": True,
        }

    def _orderbook(self, *, up_bid, up_ask, down_bid, down_ask, ts):
        return OrderBook(
            market_id="m1",
            yes_asks=[(up_ask, 100)],
            yes_bids=[(up_bid, 100)],
            no_asks=[(down_ask, 100)],
            no_bids=[(down_bid, 100)],
            timestamp=ts,
            sequence=int(ts),
            outcome_labels=("Up", "Down"),
            market_slug=self.market["slug"],
            slot_id=self.market["slot_id"],
            end_ts=self.market["end_ts"],
            token_ids=self.market["token_ids"],
        )

    def test_normalize_market_payload_from_slug_shape(self):
        md = PolymarketData(self.config)
        payload = {
            "id": "1799581",
            "question": "Bitcoin Up or Down - March 31, 10:50PM-10:55PM ET",
            "conditionId": "0xabc",
            "slug": "btc-updown-5m-1775011800",
            "endDate": "2026-04-01T02:55:00Z",
            "active": True,
            "closed": False,
            "acceptingOrders": True,
            "enableOrderBook": True,
            "clobTokenIds": "[\"up-token\", \"down-token\"]",
            "outcomes": "[\"Up\", \"Down\"]",
            "outcomePrices": "[\"0.545\", \"0.455\"]",
        }
        market = md._normalize_market_payload(payload)
        self.assertEqual(market["asset"], "btc")
        self.assertEqual(market["interval_minutes"], 5)
        self.assertEqual(market["slot_id"], "btc:5:1775011800")
        self.assertEqual(market["token_ids"]["Up"], "up-token")
        self.assertEqual(market["outcomes"], ["Up", "Down"])

    async def test_executor_signal_reversal_and_settlement(self):
        fake_md = FakeMarketData(self.market)
        executor = PolymarketExecutor(self.config, fake_md, mode="paper")
        await executor.__aenter__()
        try:
            open_book = self._orderbook(up_bid=0.48, up_ask=0.50, down_bid=0.50, down_ask=0.52, ts=101.0)
            first_signal = Signal(
                market_id="m1",
                outcome="Up",
                action="BUY",
                price=0.50,
                confidence=0.9,
                size=10,
                reason="initial entry",
                book_quality={},
            )
            open_result = await executor.execute_signal_trade(self.market, open_book, first_signal)
            self.assertTrue(open_result["opened"])
            self.assertEqual(executor.get_runtime_snapshot()["open_position_count"], 1)

            reverse_book = self._orderbook(up_bid=0.46, up_ask=0.48, down_bid=0.52, down_ask=0.54, ts=110.0)
            reverse_signal = Signal(
                market_id="m1",
                outcome="Up",
                action="SELL",
                price=0.46,
                confidence=0.95,
                size=10,
                reason="reversal",
                book_quality={},
            )
            reverse_result = await executor.execute_signal_trade(self.market, reverse_book, reverse_signal)
            event_types = [event["event_type"] for event in reverse_result["events"]]
            self.assertIn("position.closed", event_types)
            self.assertGreaterEqual(event_types.count("order.filled"), 2)

            fake_md.resolved_market.update({
                "closed": True,
                "outcomes": ["Up", "Down"],
                "outcome_prices": [0.0, 1.0],
            })
            settlement_events = await executor.process_pending_resolutions(now_ts=131.0)
            self.assertTrue(any(event["event_type"] == "market.pending_resolution" for event in settlement_events) or settlement_events)
            self.assertTrue(any(event["event_type"] == "market.settled" for event in settlement_events))
            snapshot = executor.get_runtime_snapshot(now_ts=132.0)
            self.assertEqual(snapshot["open_position_count"], 0)
            self.assertEqual(snapshot["resolved_trade_count"], 1)
        finally:
            await executor.__aexit__(None, None, None)

    async def test_pending_resolution_backoff_marks_deferred(self):
        fake_md = FakeMarketData(self.market)
        executor = PolymarketExecutor(self.config, fake_md, mode="paper")
        await executor.__aenter__()
        try:
            book = self._orderbook(up_bid=0.48, up_ask=0.50, down_bid=0.50, down_ask=0.52, ts=101.0)
            signal = Signal(
                market_id="m1",
                outcome="Up",
                action="BUY",
                price=0.50,
                confidence=0.9,
                size=10,
                reason="initial entry",
                book_quality={},
            )
            await executor.execute_signal_trade(self.market, book, signal)
            fake_md.resolved_market.update({"closed": False, "outcome_prices": [0.5, 0.5]})
            await executor.process_pending_resolutions(now_ts=131.0)
            await executor.process_pending_resolutions(now_ts=450.0)
            pending = executor.get_runtime_snapshot(now_ts=451.0)["pending_resolution_slots"]
            self.assertEqual(len(pending), 1)
            self.assertTrue(pending[0]["deferred"])
        finally:
            await executor.__aexit__(None, None, None)

    async def test_runtime_snapshot_tracks_only_active_slots(self):
        fake_md = FakeMarketData(self.market)
        executor = PolymarketExecutor(self.config, fake_md, mode="paper")
        await executor.__aenter__()
        try:
            for interval_minutes in (5, 15):
                for asset in ("btc", "eth", "sol", "xrp"):
                    market = dict(self.market)
                    market["slot_id"] = f"{asset}:{interval_minutes}:100"
                    market["slug"] = f"{asset}-updown-{interval_minutes}m-100"
                    market["asset"] = asset
                    market["interval_minutes"] = interval_minutes
                    market["end_ts"] = 200.0
                    executor.register_market(market)
            expired = dict(self.market)
            expired["slot_id"] = "btc:5:old"
            expired["slug"] = "btc-updown-5m-0"
            expired["end_ts"] = 10.0
            executor.register_market(expired)
            snapshot = executor.get_runtime_snapshot(now_ts=100.0)
            self.assertEqual(len(snapshot["active_slots"]), 8)
            self.assertTrue(all(slot["end_ts"] > 100.0 for slot in snapshot["active_slots"]))
        finally:
            await executor.__aexit__(None, None, None)

    def test_backtester_uses_resolved_fixture_and_exits(self):
        cfg = dict(self.config)
        cfg["strategies"] = dict(self.config["strategies"])
        cfg["strategies"]["mean_reversion_5min"] = dict(self.config["strategies"]["mean_reversion_5min"])
        cfg["strategies"]["mean_reversion_5min"]["deviation_threshold"] = 0.02
        bt = Backtester(cfg, initial_capital=500.0)
        rows = []
        prices = [0.50] * 5 + [0.40] * 4 + [0.53] * 4
        for idx, price in enumerate(prices):
            rows.append({
                "timestamp": pd.Timestamp("2026-04-01T00:00:00Z") + pd.Timedelta(minutes=idx),
                "market_id": "m1",
                "outcome": "Up",
                "best_bid": price - 0.001,
                "best_ask": price + 0.001,
                "bid_size": 100,
                "ask_size": 100,
                "mid_price": price,
                "volume": 1000,
                "resolved_outcome": "Up",
            })
        df = pd.DataFrame(rows)
        result = bt.simulate_mean_reversion(df, "m1", outcome="Up")
        self.assertGreaterEqual(result.total_trades, 1)
        self.assertTrue(all(trade.reason in {"ema_recross", "timeout", "resolved_outcome"} for trade in result.trades))


if __name__ == "__main__":
    unittest.main()
