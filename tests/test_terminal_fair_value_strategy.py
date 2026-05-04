import time
import unittest

from cli import _terminal_fair_value_signal_from_decisions
from external_spot import SpotSnapshot
from market_data import OrderBook
from strategies.complement_dislocation import ComplementDislocationScanner
from strategies.lag_scanner import LagScanner
from strategies.terminal_fair_value import ScannerDecision, TerminalFairValueScanner


class TerminalFairValueStrategyTests(unittest.TestCase):
    def setUp(self):
        self.now = time.time()
        self.market = {
            "id": "m1",
            "slug": "btc-updown-5m-100",
            "slot_id": "btc:5:100",
            "asset": "btc",
            "interval_minutes": 5,
            "end_ts": self.now + 30,
            "question": "Bitcoin Up or Down open price $100.00",
        }
        self.orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.60, 20)],
            yes_bids=[(0.56, 20)],
            no_asks=[(0.45, 20)],
            no_bids=[(0.40, 20)],
            timestamp=self.now,
            sequence=1,
            outcome_labels=("Up", "Down"),
            market_slug=self.market["slug"],
            slot_id=self.market["slot_id"],
            end_ts=self.market["end_ts"],
            token_ids={"Up": "up-token", "Down": "down-token"},
            condition_id="condition-1",
            tick_size=0.01,
            min_order_size=1.0,
            book_age_ms=10.0,
        )

    def test_terminal_scanner_emits_scanner_only_decision_with_fair_and_edge(self):
        scanner = TerminalFairValueScanner({
            "strategies": {
                "terminal_fair_value": {
                    "annualized_vol": 0.8,
                    "min_scanner_edge": 0.01,
                    "max_book_age_ms": 500,
                    "max_spot_age_ms": 300,
                }
            }
        })
        decisions = scanner.evaluate(
            self.market,
            self.orderbook,
            SpotSnapshot(asset="btc", price=101.0, timestamp=time.time(), source="unit-test"),
            now_ts=self.now,
            run_id="run-1",
            experiment_id="exp-1",
        )
        self.assertTrue(decisions)
        decision = decisions[0]
        self.assertEqual(decision.strategy_family, "terminal_fair_value")
        self.assertEqual(decision.action, "scanner_only")
        self.assertGreater(decision.model_fair, decision.best_ask)
        self.assertGreater(decision.model_edge, 0)

    def test_terminal_scanner_vetoes_uncertain_strike(self):
        scanner = TerminalFairValueScanner({"strategies": {"terminal_fair_value": {}}})
        market = dict(self.market)
        market["question"] = "Bitcoin Up or Down"
        decisions = scanner.evaluate(
            market,
            self.orderbook,
            SpotSnapshot(asset="btc", price=101.0, timestamp=time.time(), source="unit-test"),
            now_ts=self.now,
        )
        self.assertEqual(decisions, [])

    def test_lag_scanner_detects_fair_probability_lag(self):
        scanner = LagScanner({"strategies": {"lag_scanner": {"latency_penalty": 0.01}}})
        opportunity = scanner.evaluate(
            market=self.market,
            outcome="Up",
            previous_fair=0.50,
            current_fair=0.66,
            previous_market_probability=0.50,
            current_best_ask=0.54,
            current_ask_size=10.0,
            spot_move_ts=100.0,
            book_update_ts=100.2,
        )
        self.assertIsNotNone(opportunity)
        self.assertGreater(opportunity.edge_after_latency, 0.0)

    def test_complement_scanner_detects_yes_no_ask_sum(self):
        scanner = ComplementDislocationScanner({"strategies": {"complement_dislocation": {"safety_margin": 0.01}}})
        orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.48, 5)],
            yes_bids=[(0.47, 5)],
            no_asks=[(0.49, 6)],
            no_bids=[(0.48, 6)],
            timestamp=self.now,
            sequence=1,
        )
        opportunities = scanner.evaluate(self.market, orderbook)
        self.assertEqual(len(opportunities), 1)
        self.assertEqual(opportunities[0].direction, "buy_yes_buy_no")
    def test_terminal_active_signal_selects_best_edge_and_caps_notional(self):
        cfg = {
            "strategies": {
                "terminal_fair_value": {
                    "min_active_edge": 0.10,
                    "base_notional_usd": 1.0,
                    "edge_notional_multiplier": 10.0,
                    "max_notional_usd": 2.0,
                    "max_entry_price": 0.90,
                }
            }
        }
        decisions = [
            self._decision(outcome="Up", best_ask=0.62, model_fair=0.74, model_edge=0.12),
            self._decision(outcome="Down", best_ask=0.41, model_fair=0.59, model_edge=0.18),
        ]

        signal = _terminal_fair_value_signal_from_decisions(cfg, self.market, decisions)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.outcome, "Down")
        self.assertEqual(signal.action, "BUY")
        self.assertAlmostEqual(signal.price, 0.41)
        self.assertAlmostEqual(signal.size, round(2.0 / 0.41, 4))
        self.assertIn("terminal_fair_value", signal.reason)

    def test_terminal_active_signal_rejects_small_edge_or_expensive_entry(self):
        cfg = {"strategies": {"terminal_fair_value": {"min_active_edge": 0.10, "max_entry_price": 0.80}}}

        self.assertIsNone(
            _terminal_fair_value_signal_from_decisions(
                cfg,
                self.market,
                [self._decision(outcome="Up", best_ask=0.50, model_fair=0.57, model_edge=0.07)],
            )
        )
        self.assertIsNone(
            _terminal_fair_value_signal_from_decisions(
                cfg,
                self.market,
                [self._decision(outcome="Up", best_ask=0.91, model_fair=1.00, model_edge=0.09)],
            )
        )

    def _decision(self, **overrides):
        data = dict(
            experiment_id=None,
            run_id=None,
            strategy_family="terminal_fair_value",
            strategy_variant="gbm",
            slot_id=self.market["slot_id"],
            market_slug=self.market["slug"],
            condition_id="condition-1",
            token_id="token",
            outcome="Up",
            asset="btc",
            timeframe="5m",
            side="BUY",
            order_type="scanner",
            maker_or_taker="taker_candidate",
            price=0.60,
            size=0.0,
            best_bid=0.56,
            best_ask=0.60,
            mid=0.58,
            spread=0.04,
            book_timestamp=self.now,
            book_age_ms=10.0,
            external_spot_source="unit-test",
            external_spot_price=101.0,
            external_spot_timestamp=self.now,
            external_spot_age_ms=10.0,
            strike_or_open_price=100.0,
            time_to_expiry_s=30.0,
            model_fair=0.75,
            model_edge=0.15,
            entry_reason="terminal_fair_value edge=0.1500 fair=0.7500 ask=0.6000",
            vetoes_triggered=[],
            action="scanner_only",
        )
        data.update(overrides)
        return ScannerDecision(**data)


if __name__ == "__main__":
    unittest.main()
