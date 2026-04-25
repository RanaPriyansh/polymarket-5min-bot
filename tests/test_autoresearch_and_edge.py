import json
import tempfile
import unittest
from pathlib import Path

from execution import PolymarketExecutor
from market_data import OrderBook
from research.loop import ResearchLoop
from research.polymarket import PolymarketRuntimeResearchAdapter
from runtime_telemetry import RuntimeTelemetry
from strategies.time_decay import TimeDecay


class FakeMarketData:
    def best_bid(self, orderbook, outcome):
        if outcome == orderbook.outcome_labels[0]:
            return orderbook.yes_bids[0][0]
        return orderbook.no_bids[0][0]

    def best_ask(self, orderbook, outcome):
        if outcome == orderbook.outcome_labels[0]:
            return orderbook.yes_asks[0][0]
        return orderbook.no_asks[0][0]


class AutoresearchAndEdgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_research_loop_writes_latest_and_dedupes_timestamped_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            telemetry = RuntimeTelemetry(tmpdir)
            telemetry.update_status(
                run_id="paper-test",
                phase="running",
                mode="paper",
                loop_count=9,
                fetched_markets=8,
                processed_markets=5,
                open_position_count=1,
                risk={"open_order_count": 2},
            )
            telemetry.write_strategy_metrics(
                {
                    "toxicity_mm": {
                        "quotes_submitted": 20,
                        "orders_filled": 2,
                        "markets_seen": 16,
                        "toxic_book_skips": 4,
                        "realized_pnl": 0.5,
                    }
                }
            )
            telemetry.append_market_sample({"market_id": "m1", "book_reasons": ["thin_depth<2"]})
            loop = ResearchLoop(Path(tmpdir) / "research")
            adapter = PolymarketRuntimeResearchAdapter(tmpdir)

            first = loop.run_cycle(adapter)
            second = loop.run_cycle(adapter)

            artifact_dir = Path(tmpdir) / "research"
            self.assertTrue((artifact_dir / "latest.json").exists())
            self.assertTrue((artifact_dir / "latest.md").exists())
            timestamped_json = sorted(
                path for path in artifact_dir.glob("*.json") if path.name not in {"latest.json", "family_scoreboard.json", "bucket_scoreboard.json"}
            )
            self.assertEqual(len(timestamped_json), 1)
            latest_payload = json.loads((artifact_dir / "latest.json").read_text(encoding="utf-8"))
            self.assertEqual(latest_payload["source"], "live-runtime-artifacts")
            self.assertTrue(latest_payload["next_actions"])
            self.assertIsNotNone(first.top_recommendation)
            self.assertEqual(second.summary, first.summary)

    async def test_execute_signal_trade_preserves_strategy_family(self):
        cfg = {
            "polymarket": {
                "clob_api_url": "https://clob.polymarket.com",
                "wallet_address": "paper-wallet",
                "private_key": "paper-key",
            },
            "execution": {},
        }
        executor = PolymarketExecutor(cfg, FakeMarketData(), mode="paper")
        await executor.__aenter__()
        try:
            market = {
                "id": "m1",
                "slug": "btc-updown-5m-100",
                "slot_id": "btc:5:100",
                "end_ts": 300.0,
            }
            orderbook = OrderBook(
                market_id="m1",
                yes_asks=[(0.61, 10)],
                yes_bids=[(0.60, 10)],
                no_asks=[(0.41, 10)],
                no_bids=[(0.40, 10)],
                timestamp=100.0,
                sequence=1,
                outcome_labels=("Up", "Down"),
            )
            signal = type(
                "Signal",
                (),
                {
                    "outcome": "Up",
                    "action": "BUY",
                    "size": 2.0,
                    "reason": "breakout",
                },
            )()
            result = await executor.execute_signal_trade(market, orderbook, signal, strategy_family="opening_range")
            self.assertTrue(result["opened"])
            families = {event.get("strategy_family") for event in result["events"] if event.get("strategy_family")}
            self.assertEqual(families, {"opening_range"})
        finally:
            await executor.__aexit__(None, None, None)

    async def test_time_decay_can_pick_second_outcome(self):
        cfg = {
            "strategies": {
                "time_decay": {
                    "min_seconds_left": 10,
                    "max_seconds_left": 60,
                    "min_price": 0.55,
                }
            },
            "filters": {
                "max_book_spread_bps": 1000,
                "min_top_depth": 1,
                "min_top_notional": 0,
                "max_depth_ratio": 100,
            },
        }
        strategy = TimeDecay(cfg)
        market = {
            "end_ts": 120.0,
            "outcomes": ["Up", "Down"],
        }
        orderbook = OrderBook(
            market_id="m1",
            yes_asks=[(0.53, 10)],
            yes_bids=[(0.52, 10)],
            no_asks=[(0.64, 10)],
            no_bids=[(0.62, 10)],
            timestamp=100.0,
            sequence=1,
            outcome_labels=("Up", "Down"),
        )
        signal = strategy.generate_signal("m1", market, orderbook, current_time=90.0)
        self.assertIsNotNone(signal)
        self.assertEqual(signal.outcome, "Down")


if __name__ == "__main__":
    unittest.main()
