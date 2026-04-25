import copy
import unittest

from cli import _risk_reduce_toxicity_quote, _toxicity_mm_quote_refresh_decision
from market_context import MarketContext
from market_data import OrderBook
from strategies.toxicity_mm import ToxicityMM


BASE_CFG = {
    "polymarket": {},
    "execution": {"mm_paper_max_notional_usd": 6.0},
    "filters": {
        "max_book_spread_bps": 500,
        "min_top_depth": 2,
        "min_top_notional": 0.5,
        "max_depth_ratio": 12,
    },
    "strategies": {
        "toxicity_mm": {
            "vpin_threshold": 0.5,
            "spread_multiplier": 1.5,
            "kelly_fraction": 0.2,
            "timeframes": ["5m"],
            "max_position": 1000,
        }
    },
}


def make_orderbook(*, yes_bid=0.49, yes_ask=0.51, yes_bid_size=20.0, yes_ask_size=20.0):
    return OrderBook(
        market_id="m1",
        yes_bids=[(yes_bid, yes_bid_size)],
        yes_asks=[(yes_ask, yes_ask_size)],
        no_bids=[(0.49, 20.0)],
        no_asks=[(0.51, 20.0)],
        timestamp=1000.0,
        sequence=1,
        outcome_labels=("Up", "Down"),
        market_slug="btc-updown-5m-900",
        slot_id="btc:5:900",
        end_ts=1200.0,
    )


def make_context(*, seconds_to_expiry, tte_pct=None, imbalance_yes=0.0, best_bid_yes=0.49, best_ask_yes=0.51):
    if tte_pct is None:
        tte_pct = max(0.0, min(1.0, seconds_to_expiry / 300.0))
    return MarketContext(
        market_id="m1",
        slot_id="btc:5:900",
        asset="btc",
        interval_minutes=5,
        outcome_labels=["Up", "Down"],
        now_ts=1200.0 - seconds_to_expiry,
        end_ts=1200.0,
        seconds_to_expiry=float(seconds_to_expiry),
        slot_age_seconds=300.0 - float(seconds_to_expiry),
        tte_pct=float(tte_pct),
        tte_bucket="test",
        mid_price_yes=(best_bid_yes + best_ask_yes) / 2.0,
        mid_price_no=0.5,
        best_bid_yes=best_bid_yes,
        best_ask_yes=best_ask_yes,
        book_spread_bps=((best_ask_yes - best_bid_yes) / ((best_bid_yes + best_ask_yes) / 2.0)) * 10000,
        top_depth_yes=40.0,
        top_depth_no=40.0,
        imbalance_yes=float(imbalance_yes),
        last_trade_price=0.5,
        spot_price=100000.0,
        spot_move_pct_window=0.0,
        momentum_score=0.0,
        recent_mid_history=[0.5],
    )


class ToxicityMMTTETest(unittest.TestCase):
    def setUp(self):
        self.mm = ToxicityMM(copy.deepcopy(BASE_CFG))
        self.orderbook = make_orderbook()

    def test_tte_above_180_and_tte_120_produce_different_spreads(self):
        early, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181))
        late, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=120))

        self.assertIsNotNone(early)
        self.assertIsNotNone(late)
        early_spread = early.ask_price - early.bid_price
        late_spread = late.ask_price - late.bid_price
        self.assertGreater(late_spread, early_spread)

    def test_no_fresh_non_risk_reducing_quotes_under_90s_strict_mode(self):
        quote, other, quality = self.mm.generate_quotes(
            "m1",
            self.orderbook,
            preferred_outcome="Up",
            context=make_context(seconds_to_expiry=89),
        )

        self.assertIsNone(quote)
        self.assertIsNone(other)
        self.assertIn("no_fresh_toxicity_quotes_under_tte", quality.reasons)

    def test_strict_mode_flatten_still_allowed_under_90s(self):
        self.mm.positions = {"m1": {"Up": {"size": 4.0, "avg": 0.48}}}

        quote, other, quality = self.mm.generate_quotes(
            "m1",
            self.orderbook,
            preferred_outcome="Up",
            context=make_context(seconds_to_expiry=89),
        )

        self.assertIsNotNone(quote)
        self.assertIsNone(other)
        self.assertEqual(quote.bid_size, 0.0)
        self.assertEqual(quote.ask_size, 4.0)
        self.assertIn("endgame_flatten", quote.reason)
        self.assertTrue(quality.is_tradeable)

    def test_spread_widens_more_as_tte_falls_under_strict_mode(self):
        early, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181))
        mid, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=120))

        self.assertIsNotNone(early)
        self.assertIsNotNone(mid)
        self.assertGreater(mid.ask_price - mid.bid_price, early.ask_price - early.bid_price)

    def test_adverse_spot_veto_skips_up_bid_when_spot_falls(self):
        quote, other, quality = self.mm.generate_quotes(
            "m1",
            self.orderbook,
            preferred_outcome="Up",
            context=make_context(seconds_to_expiry=181, tte_pct=0.9, imbalance_yes=0.0),
        )
        self.assertIsNotNone(quote)

        adverse_context = make_context(seconds_to_expiry=181, tte_pct=0.9, imbalance_yes=0.0)
        adverse_context.spot_move_pct_window = -0.35
        adverse_context.momentum_score = -0.7
        quote, other, quality = self.mm.generate_quotes(
            "m1",
            self.orderbook,
            preferred_outcome="Up",
            context=adverse_context,
        )

        self.assertIsNotNone(quote)
        self.assertIsNone(other)
        self.assertEqual(quote.bid_size, 0.0)
        self.assertGreater(quote.ask_size, 0.0)
        self.assertIn("spot_momentum_adverse_veto=bid", quote.reason)

    def test_size_decreases_under_adverse_conditions_without_full_veto(self):
        neutral, _, _ = self.mm.generate_quotes(
            "m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181, tte_pct=0.9)
        )
        adverse_context = make_context(seconds_to_expiry=181, tte_pct=0.9, imbalance_yes=0.7)
        adverse_context.spot_move_pct_window = -0.10
        adverse_context.momentum_score = -0.2
        adverse, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=adverse_context)

        self.assertIsNotNone(neutral)
        self.assertIsNotNone(adverse)
        self.assertLess(adverse.bid_size, neutral.bid_size)
        self.assertGreaterEqual(adverse.ask_size, neutral.ask_size)
        self.assertIn("bid_adverse_size_mult", adverse.reason)

    def test_quote_refresh_decision_keeps_fresh_quote_with_small_price_move(self):
        quote, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181))
        existing_orders = {
            "bid": {"order_id": "bid1", "side": "BUY", "price": quote.bid_price + 0.001, "size": quote.bid_size, "timestamp": 100.0},
            "ask": {"order_id": "ask1", "side": "SELL", "price": quote.ask_price - 0.001, "size": quote.ask_size, "timestamp": 100.0},
        }

        decision = _toxicity_mm_quote_refresh_decision(
            quote,
            existing_orders=existing_orders,
            now_ts=103.0,
            price_threshold=0.005,
            ttl_seconds=10.0,
        )

        self.assertFalse(decision["refresh"])
        self.assertEqual(decision["reason"], "quote_reuse_within_threshold")

    def test_quote_refresh_decision_reprices_when_stale_or_moved(self):
        quote, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181))
        existing_orders = {
            "bid": {"order_id": "bid1", "side": "BUY", "price": quote.bid_price - 0.02, "size": quote.bid_size, "timestamp": 100.0},
            "ask": {"order_id": "ask1", "side": "SELL", "price": quote.ask_price, "size": quote.ask_size, "timestamp": 100.0},
        }

        moved = _toxicity_mm_quote_refresh_decision(
            quote,
            existing_orders=existing_orders,
            now_ts=103.0,
            price_threshold=0.005,
            ttl_seconds=10.0,
        )
        stale = _toxicity_mm_quote_refresh_decision(
            quote,
            existing_orders=existing_orders,
            now_ts=111.0,
            price_threshold=0.05,
            ttl_seconds=10.0,
        )

        self.assertTrue(moved["refresh"])
        self.assertEqual(moved["reason"], "quote_reprice_threshold")
        self.assertTrue(stale["refresh"])
        self.assertEqual(stale["reason"], "quote_ttl_expired")

    def test_quote_refresh_decision_reprices_when_size_reduced(self):
        quote, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181))
        existing_orders = {
            "bid": {"order_id": "bid1", "side": "BUY", "price": quote.bid_price, "size": quote.bid_size * 2, "timestamp": 100.0},
            "ask": {"order_id": "ask1", "side": "SELL", "price": quote.ask_price, "size": quote.ask_size, "timestamp": 100.0},
        }

        decision = _toxicity_mm_quote_refresh_decision(
            quote,
            existing_orders=existing_orders,
            now_ts=103.0,
            price_threshold=0.05,
            ttl_seconds=10.0,
            size_reduction_threshold=0.10,
        )

        self.assertTrue(decision["refresh"])
        self.assertEqual(decision["reason"], "quote_size_reduced")
        self.assertGreaterEqual(decision["size_reduction"], 0.10)

    def test_quote_refresh_decision_reprices_when_extra_side_would_remain(self):
        quote, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181))
        quote = quote.__class__(
            market_id=quote.market_id,
            outcome=quote.outcome,
            bid_price=0.0,
            ask_price=quote.ask_price,
            bid_size=0.0,
            ask_size=quote.ask_size,
            reason=f"{quote.reason}|risk_reduce_only",
            book_quality=quote.book_quality,
        )
        existing_orders = {
            "bid": {"order_id": "bid1", "side": "BUY", "price": 0.49, "size": 2.0, "timestamp": 100.0},
            "ask": {"order_id": "ask1", "side": "SELL", "price": quote.ask_price, "size": quote.ask_size, "timestamp": 100.0},
        }

        decision = _toxicity_mm_quote_refresh_decision(
            quote,
            existing_orders=existing_orders,
            now_ts=103.0,
            price_threshold=0.05,
            ttl_seconds=10.0,
        )

        self.assertTrue(decision["refresh"])
        self.assertEqual(decision["reason"], "extra_quote_side")
        self.assertEqual(decision["extra_sides"], ["bid"])

    def test_tte_under_30_produces_no_new_quote_without_inventory(self):
        quote, other, quality = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=29))

        self.assertIsNone(quote)
        self.assertIsNone(other)
        self.assertIn("endgame_no_new_orders", quality.reasons)

    def test_tte_under_30_produces_flatten_quote_only_with_inventory(self):
        self.mm.positions = {"m1": {"Up": {"size": 4.0, "avg": 0.48}}}

        quote, other, quality = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=29))

        self.assertIsNotNone(quote)
        self.assertIsNone(other)
        self.assertEqual(quote.bid_size, 0.0)
        self.assertEqual(quote.ask_size, 4.0)
        self.assertLessEqual(quote.ask_price, 0.49)
        self.assertIn("endgame_flatten", quote.reason)
        self.assertTrue(quality.is_tradeable)

    def test_tte_under_30_short_inventory_flatten_bid_crosses_best_ask(self):
        self.mm.positions = {"m1": {"Up": {"size": -3.0, "avg": 0.52}}}

        quote, other, quality = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=29))

        self.assertIsNotNone(quote)
        self.assertIsNone(other)
        self.assertEqual(quote.bid_size, 3.0)
        self.assertEqual(quote.ask_size, 0.0)
        self.assertGreaterEqual(quote.bid_price, 0.51)
        self.assertIn("endgame_flatten", quote.reason)
        self.assertTrue(quality.is_tradeable)

    def test_inventory_skew_biases_the_right_side(self):
        neutral, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181))
        self.mm.positions = {"m1": {"Up": {"size": 100.0, "avg": 0.49}}}

        skewed, _, _ = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=make_context(seconds_to_expiry=181))

        self.assertIsNotNone(neutral)
        self.assertIsNotNone(skewed)
        self.assertLess(skewed.bid_price, neutral.bid_price)
        self.assertGreater(skewed.ask_price, neutral.ask_price)

    def test_no_context_exact_legacy_behavior(self):
        first, _, first_quality = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up")
        self.mm.positions = {"m1": {"Up": {"size": 100.0, "avg": 0.49}}}
        second, _, second_quality = self.mm.generate_quotes("m1", self.orderbook, preferred_outcome="Up", context=None)

        self.assertEqual(first, second)
        self.assertEqual(first_quality.to_dict(), second_quality.to_dict())

    def test_no_context_does_not_clamp_legacy_edge_prices(self):
        cfg = copy.deepcopy(BASE_CFG)
        cfg["filters"]["max_book_spread_bps"] = 20_000
        cfg["filters"]["min_top_notional"] = 0.01
        mm = ToxicityMM(cfg)
        edge_book = make_orderbook(yes_bid=0.0005, yes_ask=0.0015, yes_bid_size=20.0, yes_ask_size=20.0)

        quote, other, quality = mm.generate_quotes("m1", edge_book, preferred_outcome="Up", context=None)

        self.assertIsNotNone(quote)
        self.assertIsNone(other)
        self.assertTrue(quality.is_tradeable)
        self.assertLess(quote.bid_price, 0.01)
        self.assertLess(quote.ask_price, 0.01)

    def test_endgame_flatten_bypasses_quality_rejection_when_inventory_and_touch_exist(self):
        self.mm.positions = {"m1": {"Up": {"size": 4.0, "avg": 0.48}}}
        imbalanced_book = make_orderbook(yes_bid_size=200.0, yes_ask_size=1.0)

        quote, other, quality = self.mm.generate_quotes(
            "m1",
            imbalanced_book,
            preferred_outcome="Up",
            context=make_context(seconds_to_expiry=29),
        )

        self.assertIsNotNone(quote)
        self.assertIsNone(other)
        self.assertIn("imbalanced_depth", "|".join(quality.reasons))
        self.assertEqual(quote.ask_size, 4.0)
        self.assertIn("endgame_flatten", quote.reason)

    def test_endgame_without_inventory_still_emits_no_quote_when_quality_rejected(self):
        imbalanced_book = make_orderbook(yes_bid_size=200.0, yes_ask_size=1.0)

        quote, other, quality = self.mm.generate_quotes(
            "m1",
            imbalanced_book,
            preferred_outcome="Up",
            context=make_context(seconds_to_expiry=29),
        )

        self.assertIsNone(quote)
        self.assertIsNone(other)
        self.assertIn("endgame_no_new_orders", quality.reasons)

    def test_existing_long_inventory_replacement_quote_is_ask_only_and_capped(self):
        self.mm.positions = {"m1": {"Up": {"size": 4.0, "avg": 0.49}}}
        quote, _, _ = self.mm.generate_quotes(
            "m1",
            self.orderbook,
            preferred_outcome="Up",
            context=make_context(seconds_to_expiry=91),
        )

        reduced = _risk_reduce_toxicity_quote(quote, inventory=4.0)

        self.assertIsNotNone(reduced)
        self.assertEqual(reduced.bid_size, 0.0)
        self.assertGreater(reduced.ask_size, 0.0)
        self.assertLessEqual(reduced.ask_size, 4.0)
        self.assertIn("inventory_skew", reduced.reason)
        self.assertIn("risk_reduce_only", reduced.reason)

    def test_existing_short_inventory_replacement_quote_is_bid_only_and_capped(self):
        self.mm.positions = {"m1": {"Up": {"size": -3.0, "avg": 0.52}}}
        quote, _, _ = self.mm.generate_quotes(
            "m1",
            self.orderbook,
            preferred_outcome="Up",
            context=make_context(seconds_to_expiry=91),
        )

        reduced = _risk_reduce_toxicity_quote(quote, inventory=-3.0)

        self.assertIsNotNone(reduced)
        self.assertGreater(reduced.bid_size, 0.0)
        self.assertLessEqual(reduced.bid_size, 3.0)
        self.assertEqual(reduced.ask_size, 0.0)
        self.assertIn("inventory_skew", reduced.reason)
        self.assertIn("risk_reduce_only", reduced.reason)

    def test_endgame_long_flatten_uses_bid_touch_even_when_ask_missing(self):
        self.mm.positions = {"m1": {"Up": {"size": 2.0, "avg": 0.48}}}
        bid_only_book = OrderBook(
            market_id="m1",
            yes_bids=[(0.49, 20.0)],
            yes_asks=[],
            no_bids=[(0.49, 20.0)],
            no_asks=[(0.51, 20.0)],
            timestamp=1000.0,
            sequence=1,
            outcome_labels=("Up", "Down"),
        )

        quote, other, quality = self.mm.generate_quotes(
            "m1",
            bid_only_book,
            preferred_outcome="Up",
            context=make_context(seconds_to_expiry=29),
        )

        self.assertIsNotNone(quote)
        self.assertIsNone(other)
        self.assertIn("missing_side", quality.reasons)
        self.assertEqual(quote.ask_size, 2.0)
        self.assertLessEqual(quote.ask_price, 0.49)


if __name__ == "__main__":
    unittest.main()
