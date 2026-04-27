from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from external_spot import SpotSnapshot
from market_data import OrderBook, PolymarketData
from market_rules import MarketRule, parse_market_rule
from models.terminal_probability import bachelier_terminal_up_probability, gbm_terminal_up_probability


@dataclass(frozen=True)
class ScannerDecision:
    experiment_id: str | None
    run_id: str | None
    strategy_family: str
    strategy_variant: str
    slot_id: str
    market_slug: str
    condition_id: str | None
    token_id: str | None
    outcome: str
    asset: str | None
    timeframe: str | None
    side: str
    order_type: str
    maker_or_taker: str
    price: float
    size: float
    best_bid: float
    best_ask: float
    mid: float
    spread: float
    book_timestamp: float
    book_age_ms: float
    external_spot_source: str
    external_spot_price: float
    external_spot_timestamp: float
    external_spot_age_ms: float
    strike_or_open_price: float
    time_to_expiry_s: float
    model_fair: float
    model_edge: float
    entry_reason: str
    vetoes_triggered: list[str]
    action: str

    def to_event(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["event_type"] = "scanner.opportunity"
        return payload


class TerminalFairValueScanner:
    strategy_family = "terminal_fair_value"

    def __init__(self, config: dict):
        params = config.get("strategies", {}).get("terminal_fair_value", {}) or {}
        self.max_tte_seconds = float(params.get("max_tte_seconds", 60.0))
        self.max_active_tte_seconds = float(params.get("max_active_tte_seconds", 120.0))
        self.max_book_age_ms = float(params.get("max_book_age_ms", 500.0))
        self.max_spot_age_ms = float(params.get("max_spot_age_ms", 300.0))
        self.max_spread = float(params.get("max_spread", 0.06))
        self.min_scanner_edge = float(params.get("min_scanner_edge", 0.05))
        self.min_taker_edge = float(params.get("min_taker_edge", 0.08))
        self.min_maker_edge = float(params.get("min_maker_edge", 0.04))
        self.annualized_vol = float(params.get("annualized_vol", 0.8))
        self.annualized_drift = float(params.get("annualized_drift", 0.0))
        self.model = str(params.get("model", "gbm"))

    def evaluate(
        self,
        market: dict[str, Any],
        orderbook: OrderBook,
        spot: SpotSnapshot,
        *,
        now_ts: float,
        run_id: str | None = None,
        experiment_id: str | None = None,
        adverse_selection_veto: bool = False,
    ) -> list[ScannerDecision]:
        rule = parse_market_rule(market)
        vetoes = self._vetoes(rule, orderbook, spot, now_ts, adverse_selection_veto=adverse_selection_veto)
        if vetoes:
            return []

        assert rule.expiry_ts is not None
        assert rule.strike_or_open_price is not None
        tte = max(0.0, rule.expiry_ts - now_ts)
        p_up = self._fair_probability(spot.price, rule.strike_or_open_price, tte)
        labels = orderbook.outcome_labels
        fair_by_outcome = {labels[0]: p_up, labels[1]: 1.0 - p_up}
        decisions: list[ScannerDecision] = []
        for outcome in labels:
            best_bid = PolymarketData.best_bid(orderbook, outcome)
            best_ask = PolymarketData.best_ask(orderbook, outcome)
            if best_ask <= 0:
                continue
            spread = max(0.0, best_ask - best_bid)
            if spread > self.max_spread and fair_by_outcome[outcome] <= 0.95:
                continue
            edge = fair_by_outcome[outcome] - best_ask
            if edge < self.min_scanner_edge:
                continue
            token_id = orderbook.token_ids.get(outcome)
            decisions.append(
                ScannerDecision(
                    experiment_id=experiment_id,
                    run_id=run_id,
                    strategy_family=self.strategy_family,
                    strategy_variant=self.model,
                    slot_id=market.get("slot_id", ""),
                    market_slug=market.get("slug", ""),
                    condition_id=getattr(orderbook, "condition_id", None),
                    token_id=token_id,
                    outcome=outcome,
                    asset=rule.asset,
                    timeframe=f"{rule.timeframe_minutes}m" if rule.timeframe_minutes else None,
                    side="BUY",
                    order_type="scanner",
                    maker_or_taker="taker_candidate",
                    price=best_ask,
                    size=0.0,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    mid=(best_bid + best_ask) / 2.0 if best_bid and best_ask else 0.0,
                    spread=spread,
                    book_timestamp=orderbook.timestamp,
                    book_age_ms=float(getattr(orderbook, "book_age_ms", 0.0) or 0.0),
                    external_spot_source=spot.source,
                    external_spot_price=spot.price,
                    external_spot_timestamp=spot.timestamp,
                    external_spot_age_ms=spot.age_ms,
                    strike_or_open_price=rule.strike_or_open_price,
                    time_to_expiry_s=tte,
                    model_fair=fair_by_outcome[outcome],
                    model_edge=edge,
                    entry_reason=f"terminal_fair_value edge={edge:.4f} fair={fair_by_outcome[outcome]:.4f} ask={best_ask:.4f}",
                    vetoes_triggered=[],
                    action="scanner_only",
                )
            )
        return decisions

    def _fair_probability(self, spot: float, strike: float, tte: float) -> float:
        if self.model == "bachelier":
            expected_std = max(1e-9, spot * self.annualized_vol * (tte / (365.0 * 24.0 * 60.0 * 60.0)) ** 0.5)
            return bachelier_terminal_up_probability(
                spot=spot,
                strike=strike,
                drift_to_expiry=0.0,
                expected_price_std_to_expiry=expected_std,
            )
        return gbm_terminal_up_probability(
            spot=spot,
            strike=strike,
            seconds_to_expiry=tte,
            annualized_vol=self.annualized_vol,
            annualized_drift=self.annualized_drift,
        )

    def _vetoes(
        self,
        rule: MarketRule,
        orderbook: OrderBook,
        spot: SpotSnapshot,
        now_ts: float,
        *,
        adverse_selection_veto: bool,
    ) -> list[str]:
        vetoes: list[str] = []
        if rule.uncertain:
            vetoes.append(rule.reason)
        if rule.timeframe_minutes != 5:
            vetoes.append("not_5m_crypto")
        if rule.expiry_ts is not None:
            tte = rule.expiry_ts - now_ts
            if tte > self.max_tte_seconds:
                vetoes.append("tte_gt_60s")
            if tte > self.max_active_tte_seconds:
                vetoes.append("tte_gt_120s")
        if float(getattr(orderbook, "book_age_ms", 0.0) or 0.0) > self.max_book_age_ms:
            vetoes.append("stale_book")
        if spot.age_ms > self.max_spot_age_ms:
            vetoes.append("stale_spot")
        if getattr(orderbook, "market_metadata_incomplete", False):
            vetoes.append("market_metadata_incomplete")
        if adverse_selection_veto:
            vetoes.append("adverse_selection_veto")
        return vetoes
