import tempfile
import unittest
from pathlib import Path

from execution import PolymarketExecutor
from market_data import OrderBook, PolymarketData
from risk import RiskManager


class FakeMarketData:
    def best_bid(self, orderbook, outcome):
        return PolymarketData.best_bid(orderbook, outcome)

    def best_ask(self, orderbook, outcome):
        return PolymarketData.best_ask(orderbook, outcome)

    def mid_price(self, orderbook, outcome):
        return PolymarketData.mid_price(orderbook, outcome)


class MTMRiskTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "polymarket": {
                "clob_api_url": "https://clob.polymarket.com",
                "wallet_address": "paper-wallet",
                "private_key": "paper-key",
            },
            "execution": {
                "paper_starting_bankroll": 500,
                "ledger_db_path": None,
            },
            "strategies": {
                "mean_reversion_5min": {"kelly_fraction": 0.1},
                "toxicity_mm": {"kelly_fraction": 0.2},
            },
            "risk": {
                "circuit_breaker_dd": 0.1,
                "max_daily_loss": 0.05,
                "max_position_size": 0.1,
                "max_risk_per_trade_usd": 10,
            },
        }
        self.executor = PolymarketExecutor(self.config, FakeMarketData(), mode="paper", run_id="mtm-test")
        self.rm = RiskManager(self.config, initial_capital=500.0)

    def test_long_position_marks_to_best_bid(self):
        projection = self.executor.get_replay_projection()
        projection.positions[("toxicity_mm", "m1", "Up")] = {
            "slot_id": "btc:5:100",
            "market_id": "m1",
            "outcome": "Up",
            "strategy_family": "toxicity_mm",
            "quantity": 10.0,
            "average_price": 0.40,
            "realized_pnl": 0.0,
        }
        self.executor.get_replay_projection = lambda: projection
        orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.46, 10)],
            yes_bids=[(0.44, 10)],
            no_asks=[(0.58, 10)],
            no_bids=[(0.54, 10)],
            timestamp=200.0,
            sequence=1,
            outcome_labels=("Up", "Down"),
        )
        snapshot = self.executor.get_runtime_snapshot(now_ts=200.0, orderbooks_by_market={"m1": orderbook})
        position = snapshot["open_positions"][0]
        self.assertEqual(position["mark_source"], "best_bid")
        self.assertAlmostEqual(position["mark_price"], 0.44)
        self.assertAlmostEqual(position["unrealized_pnl"], 0.4)
        self.assertAlmostEqual(snapshot["unrealized_pnl_total"], 0.4)

    def test_short_position_marks_to_best_ask(self):
        projection = self.executor.get_replay_projection()
        projection.positions[("toxicity_mm", "m1", "Up")] = {
            "slot_id": "btc:5:100",
            "market_id": "m1",
            "outcome": "Up",
            "strategy_family": "toxicity_mm",
            "quantity": -10.0,
            "average_price": 0.60,
            "realized_pnl": 0.0,
        }
        self.executor.get_replay_projection = lambda: projection
        orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.55, 10)],
            yes_bids=[(0.53, 10)],
            no_asks=[(0.49, 10)],
            no_bids=[(0.45, 10)],
            timestamp=200.0,
            sequence=1,
            outcome_labels=("Up", "Down"),
        )
        snapshot = self.executor.get_runtime_snapshot(now_ts=200.0, orderbooks_by_market={"m1": orderbook})
        position = snapshot["open_positions"][0]
        self.assertEqual(position["mark_source"], "best_ask")
        self.assertAlmostEqual(position["mark_price"], 0.55)
        self.assertAlmostEqual(position["unrealized_pnl"], 0.5)

    def test_risk_report_uses_unrealized_pnl_in_capital(self):
        report = self.rm.get_risk_report(
            executor_snapshot={
                "open_position_count": 1,
                "open_order_count": 0,
                "realized_pnl_total": 5.0,
                "unrealized_pnl_total": 2.5,
                "exposure": {
                    "gross_position_exposure": 4.0,
                    "gross_open_order_exposure": 0.0,
                    "reserved_buy_order_notional": 0.0,
                    "pending_settlement_exposure": 0.0,
                    "pending_settlement_count": 0,
                    "total_gross_exposure": 4.0,
                    "marked_position_count": 1,
                    "unmarked_position_count": 0,
                    "by_strategy_family": {},
                    "by_market_id": {},
                    "by_asset": {},
                    "by_interval": {},
                },
            },
            ledger_events=[],
            now_ts=200.0,
        )
        self.assertAlmostEqual(report["realized_capital"], 505.0)
        self.assertAlmostEqual(report["capital"], 507.5)
        self.assertAlmostEqual(report["mark_to_market_capital"], 507.5)
        self.assertAlmostEqual(report["unrealized_pnl_total"], 2.5)


if __name__ == "__main__":
    unittest.main()
