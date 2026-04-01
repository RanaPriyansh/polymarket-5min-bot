"""
Backtesting engine for strict interval market snapshots.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging

import numpy as np
import pandas as pd

from market_data import OrderBook
from strategies.mean_reversion_5min import MeanReversion5Min

logger = logging.getLogger(__name__)


@dataclass
class Trade:
    market_id: str
    outcome: str
    entry_time: float
    exit_time: float
    side: str
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
        self.strategy = MeanReversion5Min(config)

    def load_historical_orderbooks(self, csv_path: str) -> pd.DataFrame:
        df = pd.read_csv(csv_path)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", errors="ignore")
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df.sort_values("timestamp")

    def simulate_mean_reversion(
        self,
        df: pd.DataFrame,
        market_id: str,
        outcome: Optional[str] = None,
        resolution_outcomes: Optional[Dict[str, str]] = None,
    ) -> BacktestResult:
        df_market = df[df["market_id"] == market_id].copy()
        if df_market.empty:
            return BacktestResult(0, 0, 0, 0, 0)

        primary_outcome = outcome or str(df_market["outcome"].iloc[0])
        df_market = df_market[df_market["outcome"] == primary_outcome].copy()
        if len(df_market) < self.strategy.ema_period + 2:
            logger.warning("Insufficient data for %s-%s", market_id, primary_outcome)
            return BacktestResult(0, 0, 0, 0, 0)

        resolved_outcome = None
        interval_minutes = None
        if "interval_minutes" in df_market.columns:
            values = [int(value) for value in df_market["interval_minutes"].dropna().unique()]
            interval_minutes = values[0] if values else None
        if resolution_outcomes and market_id in resolution_outcomes:
            resolved_outcome = resolution_outcomes[market_id]
        elif "resolved_outcome" in df_market.columns:
            values = [value for value in df_market["resolved_outcome"].dropna().unique() if value]
            resolved_outcome = values[0] if values else None

        position = None
        trades: List[Trade] = []
        capital = self.initial_capital
        equity_curve = [capital]

        for _, row in df_market.iterrows():
            ts = row["timestamp"].timestamp() if hasattr(row["timestamp"], "timestamp") else float(row["timestamp"])
            price = float(row["mid_price"])
            volume = float(row.get("volume", 0))
            self.strategy.update_price(market_id, price, ts, volume, interval_minutes=interval_minutes)
            ema = self.strategy.calculate_ema(market_id, interval_minutes=interval_minutes)

            ob = OrderBook(
                market_id=market_id,
                yes_asks=[(float(row["best_ask"]), float(row.get("ask_size", 0) or 0))],
                yes_bids=[(float(row["best_bid"]), float(row.get("bid_size", 0) or 0))],
                no_asks=[(max(0.0, 1 - float(row["best_bid"])), float(row.get("ask_size", 0) or 0))],
                no_bids=[(max(0.0, 1 - float(row["best_ask"])), float(row.get("bid_size", 0) or 0))],
                timestamp=ts,
                sequence=int(ts),
                outcome_labels=(primary_outcome, "Opposite"),
            )

            if position:
                should_exit = False
                reason = "timeout"
                if ema is not None:
                    if position["side"] == "BUY" and price >= ema:
                        should_exit = True
                        reason = "ema_recross"
                    elif position["side"] == "SELL" and price <= ema:
                        should_exit = True
                        reason = "ema_recross"
                if not should_exit and ts - position["entry_time"] >= 600:
                    should_exit = True
                    reason = "timeout"
                if should_exit:
                    exit_price = float(row["best_bid"]) if position["side"] == "BUY" else float(row["best_ask"])
                    pnl = self._calculate_pnl(position["side"], position["entry_price"], exit_price, position["size"])
                    trades.append(Trade(
                        market_id=market_id,
                        outcome=primary_outcome,
                        entry_time=position["entry_time"],
                        exit_time=ts,
                        side=position["side"],
                        entry_price=position["entry_price"],
                        exit_price=exit_price,
                        size=position["size"],
                        pnl=pnl,
                        reason=reason,
                    ))
                    capital += pnl
                    equity_curve.append(capital)
                    position = None

            if position is None:
                signal = self.strategy.generate_signal(
                    market_id,
                    primary_outcome,
                    price,
                    ob,
                    volume,
                    interval_minutes=interval_minutes,
                )
                if signal:
                    position = {
                        "side": signal.action,
                        "entry_price": float(row["best_ask"]) if signal.action == "BUY" else float(row["best_bid"]),
                        "entry_time": ts,
                        "size": signal.size,
                    }

        if position:
            if resolved_outcome:
                exit_price = 1.0 if resolved_outcome == primary_outcome else 0.0
                reason = "resolved_outcome"
            else:
                exit_price = float(df_market.iloc[-1]["mid_price"])
                reason = "end_of_data"
            pnl = self._calculate_pnl(position["side"], position["entry_price"], exit_price, position["size"])
            trades.append(Trade(
                market_id=market_id,
                outcome=primary_outcome,
                entry_time=position["entry_time"],
                exit_time=df_market.iloc[-1]["timestamp"].timestamp() if hasattr(df_market.iloc[-1]["timestamp"], "timestamp") else float(df_market.iloc[-1]["timestamp"]),
                side=position["side"],
                entry_price=position["entry_price"],
                exit_price=exit_price,
                size=position["size"],
                pnl=pnl,
                reason=reason,
            ))
            capital += pnl
            equity_curve.append(capital)

        if not trades:
            return BacktestResult(0, 0, 0, 0, 0)

        pnls = [trade.pnl for trade in trades]
        returns = pd.Series(pnls) / self.initial_capital
        sharpe = returns.mean() / returns.std() * np.sqrt(len(returns)) if len(returns) > 1 and returns.std() > 0 else 0.0
        equity_series = pd.Series(equity_curve)
        running_max = equity_series.cummax()
        max_dd = ((equity_series - running_max) / running_max).min()
        win_rate = sum(1 for pnl in pnls if pnl > 0) / len(pnls)
        return BacktestResult(
            total_trades=len(trades),
            win_rate=win_rate,
            total_pnl=sum(pnls),
            sharpe=sharpe,
            max_dd=max_dd,
            trades=trades,
            equity_curve=list(equity_curve),
        )

    @staticmethod
    def _calculate_pnl(side: str, entry: float, exit: float, size: float) -> float:
        if side == "BUY":
            return (exit - entry) * size
        return (entry - exit) * size


if __name__ == "__main__":
    import glob
    import yaml

    cfg = yaml.safe_load(open("config.yaml", encoding="utf-8"))
    bt = Backtester(cfg, initial_capital=float(cfg.get("execution", {}).get("paper_starting_bankroll", 500.0)))
    csv_files = glob.glob("data/*.csv")
    if csv_files:
        df = bt.load_historical_orderbooks(csv_files[0])
        result = bt.simulate_mean_reversion(df, market_id=str(df["market_id"].iloc[0]))
        print(f"Backtest: {result.total_trades} trades, WR {result.win_rate:.1%}, PnL ${result.total_pnl:.2f}, Sharpe {result.sharpe:.2f}, MaxDD {result.max_dd:.1%}")
    else:
        print("No data CSVs found in data/ directory. Run collection first.")
