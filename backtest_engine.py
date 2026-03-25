"""
Backtesting Engine for 5/15-minute strategies
Replays historical order book snapshots and simulates trades
"""

import logging
from dataclasses import dataclass, field
from typing import List

import numpy as np
import pandas as pd

from market_data import OrderBook
from risk import RiskManager
from strategies.mean_reversion_5min import MeanReversion5Min

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
        self.initial_capital = initial_capital
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
        required_columns = {
            'timestamp', 'market_id', 'outcome', 'best_bid', 'best_ask', 'bid_size', 'ask_size', 'mid_price', 'volume'
        }
        missing_columns = required_columns.difference(df.columns)
        if missing_columns:
            raise ValueError(f"Historical data missing required columns: {sorted(missing_columns)}")

        numeric_columns = ['best_bid', 'best_ask', 'bid_size', 'ask_size', 'mid_price', 'volume']
        for column in numeric_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce')

        if df['timestamp'].isna().any():
            raise ValueError("Historical data contains invalid timestamps")
        if df[numeric_columns].isna().any().any():
            raise ValueError("Historical data contains non-numeric values in numeric columns")
        if (df[['bid_size', 'ask_size', 'volume']] < 0).any().any():
            raise ValueError("Historical data contains negative sizes or volume")
        if ((df[['best_bid', 'best_ask', 'mid_price']] < 0) | (df[['best_bid', 'best_ask', 'mid_price']] > 1)).any().any():
            raise ValueError("Historical prices must stay within the 0-1 Polymarket probability range")
        if (df['best_bid'] > df['best_ask']).any():
            raise ValueError("Historical data contains crossed books where best_bid > best_ask")

        df = df.sort_values('timestamp').reset_index(drop=True)
        return df

    def simulate_mean_reversion(self, df: pd.DataFrame, market_id: str, outcome: str = "YES") -> BacktestResult:
        """Run mean reversion backtest on a single market."""
        strat = MeanReversion5Min(self.config)
        local_rm = RiskManager(self.config, self.initial_capital)
        df_market = df[(df['market_id']==market_id) & (df['outcome']==outcome)].copy()
        if len(df_market) < strat.ema_period + 10:
            logger.warning(f"Insufficient data for {market_id}-{outcome}")
            return BacktestResult(0, 0, 0, 0, 0)

        # Simulate
        position = None  # None, "LONG", "SHORT"
        entry_price = 0
        entry_time = None
        entry_size = 0.0
        trades = []
        current_capital = self.initial_capital
        equity_curve = [self.initial_capital]

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

            signal = strat.generate_signal(market_id, outcome, price, ob, volume, risk_manager=local_rm)
            if signal and position is None:
                # Enter position
                position = signal.action
                entry_price = signal.price
                entry_time = ts
                entry_size = signal.size
                logger.info(f"ENTRY {ts}: {signal.action} {entry_size}@{signal.price} (conf {signal.confidence:.2f})")
            elif position:
                # Enforce a fixed holding horizon even if a fresh signal persists.
                if ts - entry_time > 600:  # 10 minute max hold
                    exit_price = price
                    pnl = self._calculate_pnl(position, entry_price, exit_price, entry_size)
                    trades.append(Trade(
                        market_id=market_id,
                        outcome=outcome,
                        entry_time=entry_time,
                        exit_time=ts,
                        side=position,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        size=entry_size,
                        pnl=pnl,
                        reason="timeout"
                    ))
                    position = None
                    entry_size = 0.0
                    current_capital += pnl
                    local_rm.update_capital(pnl)
                    equity_curve.append(current_capital)

        # Close any open position at end
        if position:
            exit_price = df_market.iloc[-1]['mid_price']
            pnl = self._calculate_pnl(position, entry_price, exit_price, entry_size)
            trades.append(Trade(
                market_id=market_id,
                outcome=outcome,
                entry_time=entry_time,
                exit_time=df_market.iloc[-1]['timestamp'].timestamp(),
                side=position,
                entry_price=entry_price,
                exit_price=exit_price,
                size=entry_size,
                pnl=pnl,
                reason="end_of_data"
            ))
            current_capital += pnl
            local_rm.update_capital(pnl)
            equity_curve.append(current_capital)

        self.trades.extend(trades)
        self.current_capital = current_capital
        self.equity = equity_curve

        # Compute stats
        if not trades:
            return BacktestResult(0, 0, 0, 0, 0)

        pnls = [t.pnl for t in trades]
        win_rate = sum(1 for p in pnls if p > 0) / len(pnls)
        total_pnl = sum(pnls)
        # Sharpe: assume 0% rf, daily-ish (but intraday) — approximate
        returns = pd.Series(pnls) / self.initial_capital
        returns_std = returns.std()
        sharpe = returns.mean() / returns_std * np.sqrt(365*24*60/5) if returns_std and returns_std > 0 else 0
        # Max drawdown
        equity_series = pd.Series(equity_curve)
        running_max = equity_series.cummax()
        dd = (equity_series - running_max) / running_max
        max_dd = dd.min()

        return BacktestResult(
            total_trades=len(trades),
            win_rate=win_rate,
            total_pnl=total_pnl,
            sharpe=sharpe,
            max_dd=max_dd,
            trades=trades,
            equity_curve=list(equity_series)
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