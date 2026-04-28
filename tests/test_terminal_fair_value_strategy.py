import time
import unittest

from external_spot import SpotSnapshot
from market_data import OrderBook
from strategies.complement_dislocation import ComplementDislocationScanner
from strategies.lag_scanner import LagScanner
from strategies.terminal_fair_value import TerminalFairValueScanner


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


if __name__ == "__main__":
    unittest.main()
