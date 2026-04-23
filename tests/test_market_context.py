"""Tests for market_context module.

Full TDD coverage for MarketContext dataclass + build_market_context builder.
"""
from __future__ import annotations

import unittest
from collections import deque
from typing import Dict, Tuple

from market_context import (
    MarketContext,
    build_market_context,
    tte_bucket_from_seconds,
)
from market_data import OrderBook


def _make_orderbook(
    *,
    market_id: str = "m1",
    yes_bid: float = 0.49,
    yes_ask: float = 0.51,
    no_bid: float = 0.49,
    no_ask: float = 0.51,
    yes_bid_size: float = 100.0,
    yes_ask_size: float = 100.0,
    no_bid_size: float = 100.0,
    no_ask_size: float = 100.0,
    end_ts: float = 0.0,
    slot_id: str = "btc:5:1776981900",
    market_slug: str = "btc-updown-5m-1776981900",
    timestamp: float = 1776981700.0,
) -> OrderBook:
    return OrderBook(
        market_id=market_id,
        yes_asks=[(yes_ask, yes_ask_size)],
        yes_bids=[(yes_bid, yes_bid_size)],
        no_asks=[(no_ask, no_ask_size)],
        no_bids=[(no_bid, no_bid_size)],
        timestamp=timestamp,
        sequence=int(timestamp),
        outcome_labels=("Up", "Down"),
        market_slug=market_slug,
        slot_id=slot_id,
        end_ts=end_ts,
        token_ids={"Up": "tok_up", "Down": "tok_down"},
    )


def _make_market(
    *,
    market_id: str = "m1",
    slug: str = "btc-updown-5m-1776981900",
    asset: str = "btc",
    interval_minutes: int = 5,
    slot_start_ts: int = 1776981900,
    end_ts: float = 1776982200.0,
) -> dict:
    slot_id = f"{asset}:{interval_minutes}:{slot_start_ts}"
    return {
        "id": market_id,
        "slug": slug,
        "asset": asset,
        "interval_minutes": interval_minutes,
        "slot_start_ts": slot_start_ts,
        "slot_id": slot_id,
        "end_ts": end_ts,
        "outcomes": ["Up", "Down"],
        "last_trade_price": 0.50,
    }


class MarketContextFieldsTests(unittest.TestCase):
    def test_context_fields_populated(self):
        market = _make_market(end_ts=1776982200.0)
        ob = _make_orderbook(end_ts=1776982200.0)
        now_ts = 1776982100.0  # 100s before expiry, slot is 300s long => age 200s, tte_pct = 1/3
        mid_store: Dict[Tuple[str, str], deque] = {}
        anchor_store: Dict[Tuple[str, str], float] = {}

        ctx = build_market_context(
            market=market,
            orderbook=ob,
            now_ts=now_ts,
            spot_provider=lambda asset: 70000.0,
            mid_history_store=mid_store,
            slot_spot_anchor_store=anchor_store,
        )

        self.assertIsInstance(ctx, MarketContext)
        self.assertEqual(ctx.market_id, "m1")
        self.assertEqual(ctx.slot_id, "btc:5:1776981900")
        self.assertEqual(ctx.asset, "btc")
        self.assertEqual(ctx.interval_minutes, 5)
        self.assertEqual(ctx.outcome_labels, ["Up", "Down"])
        self.assertEqual(ctx.now_ts, now_ts)
        self.assertEqual(ctx.end_ts, 1776982200.0)
        self.assertAlmostEqual(ctx.seconds_to_expiry, 100.0)
        self.assertAlmostEqual(ctx.slot_age_seconds, 200.0)
        self.assertAlmostEqual(ctx.tte_pct, 100.0 / 300.0, places=5)
        self.assertEqual(ctx.tte_bucket, "mid")  # 60 < 100 <= 180
        self.assertAlmostEqual(ctx.mid_price_yes, 0.50, places=5)
        self.assertAlmostEqual(ctx.mid_price_no, 0.50, places=5)
        self.assertAlmostEqual(ctx.best_bid_yes, 0.49, places=5)
        self.assertAlmostEqual(ctx.best_ask_yes, 0.51, places=5)
        self.assertGreater(ctx.book_spread_bps, 0.0)
        self.assertEqual(ctx.top_depth_yes, 200.0)  # 100 bid + 100 ask
        self.assertEqual(ctx.top_depth_no, 200.0)
        self.assertAlmostEqual(ctx.imbalance_yes, 0.0, places=5)
        self.assertAlmostEqual(ctx.last_trade_price, 0.50, places=5)
        self.assertEqual(ctx.spot_price, 70000.0)
        self.assertAlmostEqual(ctx.spot_move_pct_window, 0.0, places=5)
        self.assertEqual(ctx.momentum_score, 0.0)  # first sample
        self.assertEqual(len(ctx.recent_mid_history), 1)

    def test_tte_bucket_transitions(self):
        # Mandatory spec cases: 200→early, 150→mid, 90→mid, 45→late, 15→endgame
        self.assertEqual(tte_bucket_from_seconds(200.0), "early")
        self.assertEqual(tte_bucket_from_seconds(150.0), "mid")
        self.assertEqual(tte_bucket_from_seconds(90.0), "mid")
        self.assertEqual(tte_bucket_from_seconds(45.0), "late")
        self.assertEqual(tte_bucket_from_seconds(15.0), "endgame")
        # Edge cases around boundaries (> rule)
        self.assertEqual(tte_bucket_from_seconds(180.0), "mid")   # not > 180
        self.assertEqual(tte_bucket_from_seconds(60.0), "late")   # not > 60
        self.assertEqual(tte_bucket_from_seconds(30.0), "endgame")  # not > 30
        self.assertEqual(tte_bucket_from_seconds(0.0), "endgame")

    def test_tte_pct_clamped_past(self):
        market = _make_market(end_ts=1776982000.0)
        ob = _make_orderbook(end_ts=1776982000.0)
        # now is 100s AFTER expiry
        ctx = build_market_context(
            market=market,
            orderbook=ob,
            now_ts=1776982100.0,
            spot_provider=lambda a: None,
            mid_history_store={},
            slot_spot_anchor_store={},
        )
        self.assertEqual(ctx.tte_pct, 0.0)
        self.assertEqual(ctx.seconds_to_expiry, 0.0)
        self.assertEqual(ctx.tte_bucket, "endgame")

    def test_tte_pct_clamped_future(self):
        market = _make_market(end_ts=1776983000.0)
        ob = _make_orderbook(end_ts=1776983000.0)
        # now is WAY before start — more than full window
        ctx = build_market_context(
            market=market,
            orderbook=ob,
            now_ts=1776981000.0,  # 2000s before expiry, window is 300s
            spot_provider=lambda a: None,
            mid_history_store={},
            slot_spot_anchor_store={},
        )
        self.assertEqual(ctx.tte_pct, 1.0)
        self.assertEqual(ctx.slot_age_seconds, 0.0)
        self.assertEqual(ctx.tte_bucket, "early")


class MomentumTests(unittest.TestCase):
    def _build_with_mid(self, mid_yes: float, now_ts: float, mid_store, anchor_store):
        """Helper: build a context with a specified mid_yes by adjusting bid/ask."""
        market = _make_market(end_ts=1776982200.0)
        ob = _make_orderbook(
            yes_bid=mid_yes - 0.005,
            yes_ask=mid_yes + 0.005,
            no_bid=1.0 - mid_yes - 0.005,
            no_ask=1.0 - mid_yes + 0.005,
            end_ts=1776982200.0,
        )
        return build_market_context(
            market=market,
            orderbook=ob,
            now_ts=now_ts,
            spot_provider=lambda a: 70000.0,
            mid_history_store=mid_store,
            slot_spot_anchor_store=anchor_store,
        )

    def test_momentum_positive_when_trending_up(self):
        mid_store: Dict[Tuple[str, str], deque] = {}
        anchor_store: Dict[Tuple[str, str], float] = {}
        ctx = None
        for i, m in enumerate([0.50, 0.51, 0.52, 0.53, 0.54]):
            ctx = self._build_with_mid(m, now_ts=1776982100.0 + i, mid_store=mid_store, anchor_store=anchor_store)
        self.assertIsNotNone(ctx)
        self.assertGreater(ctx.momentum_score, 0.0)

    def test_momentum_negative_when_trending_down(self):
        mid_store: Dict[Tuple[str, str], deque] = {}
        anchor_store: Dict[Tuple[str, str], float] = {}
        ctx = None
        for i, m in enumerate([0.54, 0.53, 0.52, 0.51, 0.50]):
            ctx = self._build_with_mid(m, now_ts=1776982100.0 + i, mid_store=mid_store, anchor_store=anchor_store)
        self.assertIsNotNone(ctx)
        self.assertLess(ctx.momentum_score, 0.0)

    def test_recent_mid_history_capped_at_30(self):
        mid_store: Dict[Tuple[str, str], deque] = {}
        anchor_store: Dict[Tuple[str, str], float] = {}
        ctx = None
        for i in range(50):
            ctx = self._build_with_mid(0.50 + (i % 5) * 0.001,
                                       now_ts=1776982100.0 + i,
                                       mid_store=mid_store,
                                       anchor_store=anchor_store)
        self.assertIsNotNone(ctx)
        self.assertLessEqual(len(ctx.recent_mid_history), 30)


class SpotAnchorTests(unittest.TestCase):
    def test_spot_anchor_not_overwritten(self):
        market = _make_market()
        ob = _make_orderbook()
        mid_store: Dict[Tuple[str, str], deque] = {}
        anchor_store: Dict[Tuple[str, str], float] = {}

        # First call anchors at 70000
        ctx1 = build_market_context(
            market=market,
            orderbook=ob,
            now_ts=1776982100.0,
            spot_provider=lambda a: 70000.0,
            mid_history_store=mid_store,
            slot_spot_anchor_store=anchor_store,
        )
        # Second call with different spot — anchor must not change
        ctx2 = build_market_context(
            market=market,
            orderbook=ob,
            now_ts=1776982101.0,
            spot_provider=lambda a: 70700.0,  # +1%
            mid_history_store=mid_store,
            slot_spot_anchor_store=anchor_store,
        )

        self.assertAlmostEqual(ctx1.spot_move_pct_window, 0.0, places=5)
        # 70700 vs anchor 70000 = +1.0%
        self.assertAlmostEqual(ctx2.spot_move_pct_window, 1.0, places=3)
        # anchor_store should contain exactly one entry keyed by (asset, slot_id)
        self.assertEqual(len(anchor_store), 1)
        self.assertAlmostEqual(list(anchor_store.values())[0], 70000.0)

    def test_spot_move_pct_zero_when_no_spot(self):
        market = _make_market()
        ob = _make_orderbook()
        ctx = build_market_context(
            market=market,
            orderbook=ob,
            now_ts=1776982100.0,
            spot_provider=lambda a: None,  # no spot
            mid_history_store={},
            slot_spot_anchor_store={},
        )
        self.assertEqual(ctx.spot_move_pct_window, 0.0)
        self.assertIsNone(ctx.spot_price)


if __name__ == "__main__":
    unittest.main()
