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
        self.max_risk_per_trade_usd = float(self.config.get("max_risk_per_trade_usd", 10.0))

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

    def calculate_position_size(self, strategy: str, confidence: float, price: float, volatility: float) -> PositionSizing:
        """Calculate position size based on Kelly plus risk limits."""
        params = self.strategy_params.get(strategy, {})
        kelly_frac = params.get("kelly_fraction", 0.25)
        edge = confidence * volatility  # rough approximation
        f = self.kelly_size(edge, volatility, kelly_frac)

        # Size in dollar terms
        target_dollar = self.current_capital * f
        # But also enforce strategy max position size
        max_dollar = self.current_capital * self.config.get("max_position_size", 0.1)
        max_dollar = min(max_dollar, self.max_risk_per_trade_usd)
        target_dollar = min(target_dollar, max_dollar)

        # Convert price to units: units = dollar / price (for YES/NO, price is probability)
        size = target_dollar / price if price > 0 else 0

        return PositionSizing(
            size=round(size, 2),
            confidence=confidence,
            kelly_fraction=f,
            max_loss=target_dollar * volatility,
            reason=f"Kelly(f={f:.2%}, edge={edge:.2%})"
        )

    def cap_requested_size(self, price: float, requested_size: float) -> float:
        """Cap externally generated sizes to paper-trading limits."""
        if price <= 0 or requested_size <= 0:
            return 0.0
        max_position_dollar = self.current_capital * self.config.get("max_position_size", 0.1)
        capped_dollar = min(max_position_dollar, self.max_risk_per_trade_usd)
        capped_units = capped_dollar / price
        return round(min(requested_size, capped_units), 2)

    def check_circuit_breakers(self) -> bool:
        """Return True if trading should be halted."""
        # Daily loss limit
        if self.daily_pnl < 0 and abs(self.daily_pnl) / self.initial_capital > self.config.get("max_daily_loss", 0.05):
            logger.warning(f"Daily loss limit hit: {self.daily_pnl:.2f}")
            return True
        # Max drawdown
        current = self.current_capital
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
