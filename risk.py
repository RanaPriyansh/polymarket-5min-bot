"""
Risk Manager: Kelly sizing plus ledger-derived risk reporting.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np

from replay import realized_pnl_for_day

logger = logging.getLogger(__name__)


@dataclass
class PositionSizing:
    size: float
    confidence: float
    kelly_fraction: float
    max_loss: float
    reason: str


class RiskManager:
    def __init__(self, config: dict, initial_capital: float = 1000.0):
        self.config = config["risk"]
        self.strategy_params = config["strategies"]
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.peak_capital = initial_capital
        self.daily_pnl = 0.0
        self.max_drawdown = 0.0
        self.positions = {}
        self.max_risk_per_trade_usd = float(self.config.get("max_risk_per_trade_usd", 10.0))

    def kelly_size(self, edge: float, volatility: float, fraction: float = 0.25) -> float:
        if volatility <= 0:
            return 0.0
        kelly = edge / (volatility ** 2) * fraction
        return max(0.01, min(kelly, 0.25))

    def calculate_position_size(self, strategy: str, confidence: float, price: float, volatility: float) -> PositionSizing:
        params = self.strategy_params.get(strategy, {})
        kelly_frac = params.get("kelly_fraction", 0.25)
        edge = confidence * volatility
        f = self.kelly_size(edge, volatility, kelly_frac)

        target_dollar = self.current_capital * f
        max_dollar = self.current_capital * self.config.get("max_position_size", 0.1)
        max_dollar = min(max_dollar, self.max_risk_per_trade_usd)
        target_dollar = min(target_dollar, max_dollar)
        size = target_dollar / price if price > 0 else 0

        return PositionSizing(
            size=round(size, 2),
            confidence=confidence,
            kelly_fraction=f,
            max_loss=target_dollar * volatility,
            reason=f"Kelly(f={f:.2%}, edge={edge:.2%})",
        )

    def cap_requested_size(self, price: float, requested_size: float) -> float:
        if price <= 0 or requested_size <= 0:
            return 0.0
        max_position_dollar = self.current_capital * self.config.get("max_position_size", 0.1)
        capped_dollar = min(max_position_dollar, self.max_risk_per_trade_usd)
        capped_units = capped_dollar / price
        return round(min(requested_size, capped_units), 2)

    def check_circuit_breakers(self, risk_report: Optional[Dict] = None) -> bool:
        report = risk_report or self.get_risk_report()
        capital = float(report.get("capital", self.initial_capital))
        peak = float(report.get("peak", max(self.initial_capital, capital)))
        daily_pnl = float(report.get("daily_pnl", 0.0))
        dd = float(report.get("max_drawdown", 0.0))
        if peak > 0 and dd <= 0:
            dd = max(0.0, (peak - capital) / peak)

        if daily_pnl < 0 and abs(daily_pnl) / self.initial_capital > self.config.get("max_daily_loss", 0.05):
            logger.warning("Daily loss limit hit: %.2f", daily_pnl)
            return True
        if dd > self.config.get("circuit_breaker_dd", 0.1):
            logger.warning("Drawdown limit hit: %.2f%%", dd * 100)
            return True
        return False

    def update_capital(self, pnl: float):
        self.current_capital += pnl
        self.daily_pnl += pnl
        if self.current_capital > self.peak_capital:
            self.peak_capital = self.current_capital
        dd = (self.peak_capital - self.current_capital) / self.peak_capital if self.peak_capital > 0 else 0.0
        self.max_drawdown = max(self.max_drawdown, dd)

    def reset_daily_limits(self):
        self.daily_pnl = 0.0

    def get_risk_report(
        self,
        *,
        executor_snapshot: Optional[Dict] = None,
        ledger_events: Optional[list] = None,
        now_ts: Optional[float] = None,
    ) -> Dict:
        if executor_snapshot is None:
            return {
                "capital": self.current_capital,
                "peak": self.peak_capital,
                "daily_pnl": self.daily_pnl,
                "max_drawdown": self.max_drawdown,
                "positions": len(self.positions),
            }

        now_ts = float(now_ts if now_ts is not None else 0.0)
        realized_total = float(executor_snapshot.get("realized_pnl_total", 0.0))
        unrealized_total = float(executor_snapshot.get("unrealized_pnl_total", 0.0) or executor_snapshot.get("exposure", {}).get("unrealized_pnl_total", 0.0))
        realized_capital = float(self.initial_capital + realized_total)
        capital = float(realized_capital + unrealized_total)
        daily_pnl = realized_total
        if ledger_events and now_ts > 0:
            day_start_ts = now_ts - (now_ts % 86400)
            day_end_ts = day_start_ts + 86400
            daily_pnl = float(realized_pnl_for_day(ledger_events, day_start_ts=day_start_ts, day_end_ts=day_end_ts))

        peak = float(max(self.initial_capital, capital))
        max_drawdown = 0.0
        if ledger_events:
            for event in ledger_events:
                if getattr(event, "stream", None) != "risk":
                    continue
                if getattr(event, "event_type", None) != "risk_snapshot_recorded":
                    continue
                payload = getattr(event, "payload", {}) or {}
                historical_capital = float(payload.get("capital", payload.get("mark_to_market_capital", self.initial_capital)))
                peak = max(peak, historical_capital)
        max_drawdown = max(0.0, (peak - capital) / peak) if peak > 0 else 0.0

        exposure = executor_snapshot.get("exposure", {}) or {}
        return {
            "capital": round(capital, 6),
            "realized_capital": round(realized_capital, 6),
            "mark_to_market_capital": round(capital, 6),
            "peak": round(max(peak, capital), 6),
            "daily_pnl": round(daily_pnl, 6),
            "max_drawdown": round(max_drawdown, 6),
            "positions": int(executor_snapshot.get("open_position_count", 0)),
            "realized_pnl_total": round(realized_total, 6),
            "unrealized_pnl_total": round(unrealized_total, 6),
            "marked_position_count": int(exposure.get("marked_position_count", 0)),
            "unmarked_position_count": int(exposure.get("unmarked_position_count", 0)),
            "open_order_count": int(executor_snapshot.get("open_order_count", 0)),
            "gross_position_exposure": round(float(exposure.get("gross_position_exposure", 0.0)), 6),
            "gross_open_order_exposure": round(float(exposure.get("gross_open_order_exposure", 0.0)), 6),
            "reserved_buy_order_notional": round(float(exposure.get("reserved_buy_order_notional", 0.0)), 6),
            "pending_settlement_exposure": round(float(exposure.get("pending_settlement_exposure", 0.0)), 6),
            "pending_settlement_count": int(exposure.get("pending_settlement_count", 0)),
            "total_gross_exposure": round(float(exposure.get("total_gross_exposure", 0.0)), 6),
            "exposure_by_strategy_family": exposure.get("by_strategy_family", {}),
            "exposure_by_market_id": exposure.get("by_market_id", {}),
            "exposure_by_asset": exposure.get("by_asset", {}),
            "exposure_by_interval": exposure.get("by_interval", {}),
        }


if __name__ == "__main__":
    import yaml

    cfg = yaml.safe_load(open("config.yaml"))
    rm = RiskManager(cfg, initial_capital=1000.0)
    sizing = rm.calculate_position_size("mean_reversion_5min", confidence=0.8, price=0.5, volatility=0.05)
    print(f"Suggested size: {sizing.size} units (${sizing.max_loss:.2f} max loss) - {sizing.reason}")
