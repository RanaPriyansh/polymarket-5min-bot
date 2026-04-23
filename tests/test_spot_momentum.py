import unittest

from market_context import MarketContext
from strategies.spot_momentum import SpotMomentum


class SpotMomentumTests(unittest.TestCase):
    def _cfg(self):
        return {
            "strategies": {
                "spot_momentum": {
                    "min_abs_spot_move_pct": 0.20,
                    "min_tte_pct": 0.2,
                    "max_tte_pct": 0.7,
                    "max_entry_price": 0.70,
                    "base_notional_usd": 2.0,
                    "move_notional_multiplier": 4.0,
                    "max_notional_usd": 6.0,
                }
            }
        }

    def _ctx(self, **overrides):
        data = dict(
            market_id="m1",
            slot_id="btc:5:100",
            asset="btc",
            interval_minutes=5,
            outcome_labels=["Up", "Down"],
            now_ts=200.0,
            end_ts=400.0,
            seconds_to_expiry=200.0,
            slot_age_seconds=100.0,
            tte_pct=200.0 / 300.0,
            tte_bucket="early",
            mid_price_yes=0.60,
            mid_price_no=0.40,
            best_bid_yes=0.59,
            best_ask_yes=0.61,
            book_spread_bps=333.0,
            top_depth_yes=10.0,
            top_depth_no=10.0,
            imbalance_yes=0.0,
            last_trade_price=0.60,
            spot_price=100.0,
            spot_move_pct_window=0.30,
            momentum_score=0.2,
            recent_mid_history=[0.58, 0.59, 0.60],
        )
        data.update(overrides)
        return MarketContext(**data)

    def test_positive_spot_move_mid_window_buys_up_with_sized_notional(self):
        strategy = SpotMomentum(self._cfg())
        ctx = self._ctx(spot_move_pct_window=0.30, mid_price_yes=0.60, seconds_to_expiry=200.0, tte_pct=200.0 / 300.0)

        signal = strategy.generate_signal(ctx)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "BUY")
        self.assertEqual(signal.outcome, "Up")
        expected_notional = 2.0 + 4.0 * 0.30
        self.assertAlmostEqual(signal.size, expected_notional / 0.60)
        self.assertAlmostEqual(signal.price, 0.60)

    def test_positive_spot_move_does_not_buy_when_up_already_priced_in(self):
        strategy = SpotMomentum(self._cfg())
        ctx = self._ctx(spot_move_pct_window=0.30, mid_price_yes=0.85)

        self.assertIsNone(strategy.generate_signal(ctx))

    def test_negative_spot_move_buys_down_when_down_underpriced(self):
        strategy = SpotMomentum(self._cfg())
        ctx = self._ctx(spot_move_pct_window=-0.30, mid_price_yes=0.40, mid_price_no=0.60)

        signal = strategy.generate_signal(ctx)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.action, "BUY")
        self.assertEqual(signal.outcome, "Down")
        expected_notional = 2.0 + 4.0 * 0.30
        self.assertAlmostEqual(signal.size, expected_notional / 0.60)
        self.assertAlmostEqual(signal.price, 0.60)

    def test_ignores_15m_markets(self):
        strategy = SpotMomentum(self._cfg())
        ctx = self._ctx(interval_minutes=15, spot_move_pct_window=0.50)

        self.assertIsNone(strategy.generate_signal(ctx))

    def test_one_fire_per_slot_after_mark_fired(self):
        strategy = SpotMomentum(self._cfg())
        ctx = self._ctx(spot_move_pct_window=0.30)

        first = strategy.generate_signal(ctx)
        self.assertIsNotNone(first)
        strategy.mark_fired(ctx.slot_id)
        self.assertIsNone(strategy.generate_signal(ctx))

    def test_config_lists_spot_momentum_as_candidate_not_active(self):
        import yaml
        from pathlib import Path

        cfg = yaml.safe_load((Path(__file__).resolve().parents[1] / "config.yaml").read_text(encoding="utf-8"))
        self.assertIn("spot_momentum", cfg["strategies"]["candidates"])
        self.assertNotIn("spot_momentum", cfg["strategies"]["active"])


if __name__ == "__main__":
    unittest.main()
