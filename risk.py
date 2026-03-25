"""
Risk Manager: Kelly Criterion, drawdown guards, position sizing
"""

import numpy as np
from typing import Dict, Optional
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class PositionSizing:
    size: float  # units to trade
    confidence: float
    kelly_fraction: float
    max_loss: float
    target_notional: float
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
        self.positions = {}  # market_id -> size and entry

    def kelly_size(self, edge: float, volatility: float, fraction: float = 0.25) -> float:
        """
        Kelly Criterion: f* = p - q / b where edge = p - 0.5? Actually:
        For binary bets: f = (win_rate * avg_win - loss_rate * avg_loss) / (avg_win * avg_loss)
        Simplified: f = edge / variance if edge is expected return per unit risk
        """
        if volatility <= 0:
            return 0.0
        kelly = edge / (volatility ** 2) * fraction
        return max(0.01, min(kelly, 0.25))  # clamp 1%-25%

    def calculate_position_size(
        self,
        strategy: str,
        confidence: float,
        price: float,
        volatility: float,
        stop_loss: Optional[float] = None,
        edge: Optional[float] = None,
    ) -> PositionSizing:
        """Calculate position size from capital/risk budgets instead of venue volume proxies."""
        params = self.strategy_params.get(strategy, {})
        kelly_frac = params.get("kelly_fraction", 0.25)
        min_edge = params.get("min_edge", 0.005)
        effective_volatility = max(volatility, 0.01)
        expected_edge = edge if edge is not None else max(confidence * effective_volatility * 0.5, min_edge)
        f = self.kelly_size(expected_edge, effective_volatility, kelly_frac)

        # Size in dollar terms
        target_dollar = self.current_capital * f
        # But also enforce strategy max position size
        max_dollar = self.current_capital * self.config.get("max_position_size", 0.1)
        target_dollar = min(target_dollar, max_dollar)

        risk_budget = self.current_capital * self.config.get("max_daily_loss", 0.05) * 0.25
        stop_loss = max(stop_loss or effective_volatility, 0.01)

        # Contracts are bounded by both notional and worst-case stop loss.
        size_from_notional = target_dollar / price if price > 0 else 0
        size_from_risk = risk_budget / stop_loss if stop_loss > 0 else 0
        size = min(size_from_notional, size_from_risk)

        target_notional = size * price
        max_loss = size * stop_loss

        return PositionSizing(
            size=round(size, 2),
            confidence=confidence,
            kelly_fraction=f,
            max_loss=round(max_loss, 2),
            target_notional=round(target_notional, 2),
            reason=f"Kelly(f={f:.2%}, edge={expected_edge:.2%}, stop={stop_loss:.2%})"
        )

    def check_circuit_breakers(self) -> bool:
        """Return True if trading should be halted."""
        # Daily loss limit
        if self.daily_pnl < 0 and abs(self.daily_pnl) / self.initial_capital > self.config.get("max_daily_loss", 0.05):
            logger.warning(f"Daily loss limit hit: {self.daily_pnl:.2f}")
            return True
        # Max drawdown
        current = self.peak_capital + self.daily_pnl
        dd = (self.peak_capital - current) / self.peak_capital if self.peak_capital > 0 else 0
        if dd > self.config.get("circuit_breaker_dd", 0.1):
            logger.warning(f"Drawdown limit hit: {dd:.2%}")
            return True
        return False

    def update_capital(self, pnl: float):
        """Update capital after a trade."""
        self.current_capital += pnl
        self.daily_pnl += pnl
        if self.current_capital > self.peak_capital:
            self.peak_capital = self.current_capital
        # Drawdown
        dd = (self.peak_capital - self.current_capital) / self.peak_capital
        self.max_drawdown = max(self.max_drawdown, dd)

    def sync_equity(self, equity: float):
        """Synchronize risk state to broker-marked equity during paper/live trading."""
        self.current_capital = equity
        self.daily_pnl = equity - self.initial_capital
        if equity > self.peak_capital:
            self.peak_capital = equity
        dd = (self.peak_capital - equity) / self.peak_capital if self.peak_capital > 0 else 0
        self.max_drawdown = max(self.max_drawdown, dd)

    def reset_daily_limits(self):
        """Call at UTC 00:00."""
        self.daily_pnl = 0.0

    def get_risk_report(self) -> Dict:
        return {
            "capital": self.current_capital,
            "peak": self.peak_capital,
            "daily_pnl": self.daily_pnl,
            "max_drawdown": self.max_drawdown,
            "positions": len(self.positions)
        }

if __name__ == "__main__":
    import yaml
    cfg = yaml.safe_load(open("config.yaml"))
    rm = RiskManager(cfg, initial_capital=1000.0)
    sizing = rm.calculate_position_size("mean_reversion_5min", confidence=0.8, price=0.5, volatility=0.05)
    print(f"Suggested size: {sizing.size} units (${sizing.max_loss:.2f} max loss) - {sizing.reason}")