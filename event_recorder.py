"""
Event-level market recorder.

This is the data moat builder.
It records per-market feature rows that can later be used for:
- regime studies
- shock/exhaustion labels
- complementary YES/NO dislocation analysis
- resolver/source-aware latency studies
- terminal-minute stale-truth studies
- execution quality studies
"""

import csv
from dataclasses import dataclass, asdict
from pathlib import Path

from market_data import OrderBook, PolymarketData


@dataclass
class MarketEvent:
    timestamp: float
    market_id: str
    regime: str
    source_family: str
    resolver: str
    resolver_confidence: float
    seconds_to_resolution: float
    volume: float
    yes_best_bid: float
    yes_best_ask: float
    no_best_bid: float
    no_best_ask: float
    yes_mid: float
    no_mid: float
    yes_spread_bps: float
    no_spread_bps: float
    yes_microprice: float
    no_microprice: float
    yes_imbalance: float
    no_imbalance: float
    yes_no_sum: float
    dislocation: float
    sequence: int
    active_signal_family: str


class EventRecorder:
    def __init__(self, output_path: Path):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._header_written = self.output_path.exists() and self.output_path.stat().st_size > 0

    @staticmethod
    def _spread_bps(orderbook: OrderBook, outcome: str) -> float:
        mid = PolymarketData.mid_price(orderbook, outcome)
        if mid <= 0:
            return 0.0
        if outcome == "YES":
            if not orderbook.yes_bids or not orderbook.yes_asks:
                return 0.0
            spread = orderbook.yes_asks[0][0] - orderbook.yes_bids[0][0]
        else:
            if not orderbook.no_bids or not orderbook.no_asks:
                return 0.0
            spread = orderbook.no_asks[0][0] - orderbook.no_bids[0][0]
        return (spread / mid) * 10000

    @staticmethod
    def _microprice(orderbook: OrderBook, outcome: str) -> float:
        if outcome == "YES":
            if not orderbook.yes_bids or not orderbook.yes_asks:
                return 0.0
            bid_price, bid_size = orderbook.yes_bids[0]
            ask_price, ask_size = orderbook.yes_asks[0]
        else:
            if not orderbook.no_bids or not orderbook.no_asks:
                return 0.0
            bid_price, bid_size = orderbook.no_bids[0]
            ask_price, ask_size = orderbook.no_asks[0]
        total = bid_size + ask_size
        if total <= 0:
            return 0.0
        return (ask_price * bid_size + bid_price * ask_size) / total

    @staticmethod
    def build_event(
        orderbook: OrderBook,
        volume: float,
        regime: str,
        resolver_info=None,
        seconds_to_resolution: float = -1.0,
        active_signal_family: str = "none",
    ) -> MarketEvent:
        yes_mid = PolymarketData.mid_price(orderbook, "YES")
        no_mid = PolymarketData.mid_price(orderbook, "NO")
        yes_imbalance = PolymarketData.calculate_imbalance(orderbook, "YES")
        no_imbalance = PolymarketData.calculate_imbalance(orderbook, "NO")
        yes_no_sum = yes_mid + no_mid
        dislocation = yes_no_sum - 1.0 if yes_mid and no_mid else 0.0
        source_family = getattr(resolver_info, "source_family", "unknown")
        resolver = getattr(resolver_info, "resolver", "unknown")
        resolver_confidence = float(getattr(resolver_info, "confidence", 0.0))
        return MarketEvent(
            timestamp=orderbook.timestamp,
            market_id=orderbook.market_id,
            regime=regime,
            source_family=source_family,
            resolver=resolver,
            resolver_confidence=resolver_confidence,
            seconds_to_resolution=float(seconds_to_resolution),
            volume=float(volume),
            yes_best_bid=orderbook.yes_bids[0][0] if orderbook.yes_bids else 0.0,
            yes_best_ask=orderbook.yes_asks[0][0] if orderbook.yes_asks else 0.0,
            no_best_bid=orderbook.no_bids[0][0] if orderbook.no_bids else 0.0,
            no_best_ask=orderbook.no_asks[0][0] if orderbook.no_asks else 0.0,
            yes_mid=yes_mid,
            no_mid=no_mid,
            yes_spread_bps=EventRecorder._spread_bps(orderbook, "YES"),
            no_spread_bps=EventRecorder._spread_bps(orderbook, "NO"),
            yes_microprice=EventRecorder._microprice(orderbook, "YES"),
            no_microprice=EventRecorder._microprice(orderbook, "NO"),
            yes_imbalance=yes_imbalance,
            no_imbalance=no_imbalance,
            yes_no_sum=yes_no_sum,
            dislocation=dislocation,
            sequence=orderbook.sequence,
            active_signal_family=active_signal_family,
        )

    def append(self, event: MarketEvent):
        row = asdict(event)
        with self.output_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            if not self._header_written:
                writer.writeheader()
                self._header_written = True
            writer.writerow(row)

    def record(
        self,
        orderbook: OrderBook,
        volume: float,
        regime: str,
        resolver_info=None,
        seconds_to_resolution: float = -1.0,
        active_signal_family: str = "none",
    ) -> MarketEvent:
        event = self.build_event(
            orderbook,
            volume,
            regime,
            resolver_info=resolver_info,
            seconds_to_resolution=seconds_to_resolution,
            active_signal_family=active_signal_family,
        )
        self.append(event)
        return event
