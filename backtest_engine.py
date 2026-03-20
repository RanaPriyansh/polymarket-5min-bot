"""
Backtesting Engine for 5/15-minute strategies
Replays historical order book snapshots and simulates trades
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from dataclasses import dataclass, field
from market_data import OrderBook
from strategies.mean_reversion_5min import MeanReversion5Min
from risk import RiskManager
import matplotlib.pyplot as plt
import logging

logger = logging.getLogger(__name__)

@dataclass
class Trade:
    market_id: str
    outcome: str
    entry_time: float
    exit_time: float
    side: str  # "BUY"/"SELL"
    entry_price: float
    exit_price: float
    size: float
    pnl: float
    reason: str

@dataclass
class BacktestResult:
    total_trades: int
    win_rate: float
    total_pnl: float
    sharpe: float
    max_dd: float
    trades: List[Trade] = field(default_factory=list)
    equity_curve: List[float] = field(default_factory=list)

class Backtester:
    def __init__(self, config: dict, initial_capital: float = 1000.0):
        self.config = config
        self.rm = RiskManager(config, initial_capital)
        self.strategies = {
            "mean_reversion": MeanReversion5Min(config)
        }
        # Results
        self.trades = []
        self.equity = [initial_capital]
        self.current_capital = initial_capital

    def load_historical_orderbooks(self, csv_path: str) -> pd.DataFrame:
        """Load historical order book snapshots from CSV."""
        # Expected columns: timestamp, market_id, outcome, best_bid, best_ask, bid_size, ask_size, mid_price, volume
        df = pd.read_csv(csv_path, parse_dates=['timestamp'])
        df = df.sort_values('timestamp')
        return df

    def simulate_mean_reversion(self, df: pd.DataFrame, market_id: str, outcome: str = "YES") -> BacktestResult:
        """Run mean reversion backtest on a single market."""
        strat = self.strategies["mean_reversion"]
        df_market = df[(df['market_id']==market_id) & (df['outcome']==outcome)].copy()
        if len(df_market) < strat.ema_period + 10:
            logger.warning(f"Insufficient data for {market_id}-{outcome}")
            return BacktestResult(0, 0, 0, 0, 0)

        # Simulate
        position = None  # None, "LONG", "SHORT"
        entry_price = 0
        entry_time = None
        trades = []

        for idx, row in df_market.iterrows():
            price = row['mid_price']
            ts = row['timestamp'].timestamp()
            volume = row.get('volume', 0)

            # Update strategy with price
            strat.update_price(market_id, price, ts, volume)

            # Build a fake OrderBook object from the row
            ob = OrderBook(
                market_id=market_id,
                yes_asks=[(row['best_ask'], row.get('ask_size', 0))],
                yes_bids=[(row['best_bid'], row.get('bid_size', 0))],
                no_asks=[], no_bids=[],
                timestamp=ts,
                sequence=0
            )

            signal = strat.generate_signal(market_id, outcome, price, ob, volume)
            if signal and position is None:
                # Enter position
                position = signal.action
                entry_price = signal.price
                entry_time = ts
                logger.info(f"ENTRY {ts}: {signal.action} {size}@{signal.price} (conf {signal.confidence:.2f})")
            elif position and signal is None:
                # Check exit: mean reversion target hit or timeout
                # For simplicity: exit after 10 minutes or if price crosses EMA
                if ts - entry_time > 600:  # 10 minute max hold
                    exit_price = price
                    pnl = self._calculate_pnl(position, entry_price, exit_price, signal.size if 'size' in locals() else 0)
                    trades.append(Trade(
                        market_id=market_id,
                        outcome=outcome,
                        entry_time=entry_time,
                        exit_time=ts,
                        side=position,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        size=signal.size if 'size' in locals() else 0,
                        pnl=pnl,
                        reason="timeout"
                    ))
                    position = None
                    self.current_capital += pnl
                    self.equity.append(self.current_capital)

        # Close any open position at end
        if position:
            exit_price = df_market.iloc[-1]['mid_price']
            pnl = self._calculate_pnl(position, entry_price, exit_price, signal.size if 'size' in locals() else 0)
            trades.append(Trade(
                market_id=market_id,
                outcome=outcome,
                entry_time=entry_time,
                exit_time=df_market.iloc[-1]['timestamp'].timestamp(),
                side=position,
                entry_price=entry_price,
                exit_price=exit_price,
                size=signal.size if 'size' in locals() else 0,
                pnl=pnl,
                reason="end_of_data"
            ))
            self.current_capital += pnl
            self.equity.append(self.current_capital)

        # Compute stats
        if not trades:
            return BacktestResult(0, 0, 0, 0, 0)

        pnls = [t.pnl for t in trades]
        win_rate = sum(1 for p in pnls if p > 0) / len(pnls)
        total_pnl = sum(pnls)
        # Sharpe: assume 0% rf, daily-ish (but intraday) — approximate
        returns = pd.Series(pnls) / self.initial_capital
        sharpe = returns.mean() / returns.std() * np.sqrt(365*24*60/5) if returns.std() > 0 else 0
        # Max drawdown
        equity_curve = pd.Series([self.initial_capital] + [self.initial_capital + sum(pnls[:i+1]) for i in range(len(pnls))])
        running_max = equity_curve.cummax()
        dd = (equity_curve - running_max) / running_max
        max_dd = dd.min()

        return BacktestResult(
            total_trades=len(trades),
            win_rate=win_rate,
            total_pnl=total_pnl,
            sharpe=sharpe,
            max_dd=max_dd,
            trades=trades,
            equity_curve=list(equity_curve)
        )

    def _calculate_pnl(self, side: str, entry: float, exit: float, size: float) -> float:
        """Calculate PnL for a YES/NO trade."""
        if side == "BUY":
            # For YES, buying at entry and selling at exit: (exit - entry) * size
            # For NO, the price is 1 - probability; we have to think in terms of token value
            # For MVP we assume we're always trading YES tokens
            return (exit - entry) * size
        else:  # SELL
            return (entry - exit) * size

if __name__ == "__main__":
    import yaml, glob
    cfg = yaml.safe_load(open("config.yaml"))
    bt = Backtester(cfg, initial_capital=1000.0)

    # Find a sample CSV or create dummy
    csv_files = glob.glob("data/*.csv")
    if csv_files:
        df = bt.load_historical_orderbooks(csv_files[0])
        result = bt.simulate_mean_reversion(df, market_id="example-market", outcome="YES")
        print(f"Backtest: {result.total_trades} trades, WR {result.win_rate:.1%}, PnL ${result.total_pnl:.2f}, Sharpe {result.sharpe:.2f}, MaxDD {result.max_dd:.1%}")
    else:
        print("No data CSVs found in data/ directory. Run collection first.")