import tempfile
import unittest
from pathlib import Path

from execution import PolymarketExecutor
from market_data import OrderBook, PolymarketData
from risk import RiskManager
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


class RiskReportingTests(unittest.IsolatedAsyncioTestCase):
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
        self.orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.50, 100)],
            yes_bids=[(0.48, 100)],
            no_asks=[(0.52, 100)],
            no_bids=[(0.50, 100)],
            timestamp=101.0,
            sequence=101,
            outcome_labels=("Up", "Down"),
            market_slug=self.market["slug"],
            slot_id=self.market["slot_id"],
            end_ts=self.market["end_ts"],
            token_ids=self.market["token_ids"],
        )

    async def test_risk_report_is_replay_derived_and_restart_stable(self):
        fake_md = FakeMarketData(self.market)
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = dict(self.config)
            cfg["execution"] = dict(self.config["execution"])
            cfg["execution"]["ledger_db_path"] = str(Path(tmpdir) / "ledger.db")
            executor = PolymarketExecutor(cfg, fake_md, mode="paper", run_id="risk-run")
            await executor.__aenter__()
            try:
                signal = Signal(
                    market_id="m1",
                    outcome="Up",
                    action="BUY",
                    price=0.50,
                    confidence=0.9,
                    size=10,
                    reason="risk-entry",
                    book_quality={},
                )
                await executor.execute_signal_trade(self.market, self.orderbook, signal)
                fake_md.resolved_market.update({"closed": True, "outcome_prices": [0.0, 1.0]})
                await executor.process_pending_resolutions(now_ts=160.0)
                snapshot_before = executor.get_runtime_snapshot(now_ts=161.0)
                rm = RiskManager(cfg, initial_capital=500.0)
                report_before = rm.get_risk_report(
                    executor_snapshot=snapshot_before,
                    ledger_events=executor.get_ledger_events(),
                    now_ts=161.0,
                )
            finally:
                await executor.__aexit__(None, None, None)

            restored = PolymarketExecutor(cfg, fake_md, mode="paper", run_id="risk-run")
            restored.register_market(self.market)
            snapshot_after = restored.get_runtime_snapshot(now_ts=161.0)
            rm_restored = RiskManager(cfg, initial_capital=500.0)
            report_after = rm_restored.get_risk_report(
                executor_snapshot=snapshot_after,
                ledger_events=restored.get_ledger_events(),
                now_ts=161.0,
            )

            self.assertEqual(report_before, report_after)
            self.assertAlmostEqual(report_after["capital"], 495.0)
            self.assertAlmostEqual(report_after["realized_pnl_total"], -5.0)
            self.assertEqual(report_after["positions"], 0)
            self.assertEqual(report_after["pending_settlement_count"], 0)

    async def test_circuit_breaker_uses_replay_derived_report(self):
        fake_md = FakeMarketData(self.market)
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = dict(self.config)
            cfg["risk"] = dict(self.config["risk"])
            cfg["risk"]["max_daily_loss"] = 0.005
            cfg["execution"] = dict(self.config["execution"])
            cfg["execution"]["ledger_db_path"] = str(Path(tmpdir) / "ledger.db")
            executor = PolymarketExecutor(cfg, fake_md, mode="paper", run_id="risk-breaker")
            await executor.__aenter__()
            try:
                signal = Signal(
                    market_id="m1",
                    outcome="Up",
                    action="BUY",
                    price=0.50,
                    confidence=0.9,
                    size=10,
                    reason="breaker-entry",
                    book_quality={},
                )
                await executor.execute_signal_trade(self.market, self.orderbook, signal)
                fake_md.resolved_market.update({"closed": True, "outcome_prices": [0.0, 1.0]})
                await executor.process_pending_resolutions(now_ts=160.0)
            finally:
                await executor.__aexit__(None, None, None)

            restored = PolymarketExecutor(cfg, fake_md, mode="paper", run_id="risk-breaker")
            restored.register_market(self.market)
            snapshot = restored.get_runtime_snapshot(now_ts=161.0)
            rm = RiskManager(cfg, initial_capital=500.0)
            report = rm.get_risk_report(
                executor_snapshot=snapshot,
                ledger_events=restored.get_ledger_events(),
                now_ts=161.0,
            )
            self.assertTrue(rm.check_circuit_breakers(report))
            self.assertLess(report["daily_pnl"], 0)

    def test_risk_report_surfaces_exposure_fields(self):
        rm = RiskManager(self.config, initial_capital=500.0)
        report = rm.get_risk_report(
            executor_snapshot={
                "open_position_count": 1,
                "open_order_count": 2,
                "realized_pnl_total": 0.0,
                "exposure": {
                    "gross_position_exposure": 4.0,
                    "gross_open_order_exposure": 3.0,
                    "reserved_buy_order_notional": 3.0,
                    "pending_settlement_exposure": 1.5,
                    "pending_settlement_count": 1,
                    "total_gross_exposure": 7.0,
                    "by_strategy_family": {"toxicity_mm": {"total_exposure": 7.0}},
                    "by_market_id": {"m1": {"total_exposure": 7.0}},
                    "by_asset": {"btc": {"total_exposure": 7.0}},
                    "by_interval": {"5": {"total_exposure": 7.0}},
                },
            },
            ledger_events=[],
            now_ts=161.0,
        )
        self.assertEqual(report["gross_position_exposure"], 4.0)
        self.assertEqual(report["gross_open_order_exposure"], 3.0)
        self.assertEqual(report["reserved_buy_order_notional"], 3.0)
        self.assertEqual(report["pending_settlement_exposure"], 1.5)
        self.assertEqual(report["total_gross_exposure"], 7.0)


if __name__ == "__main__":
    unittest.main()
