# Polymarket 5/15-Minute Bot

**Fast, automated trading on ultra-short-duration prediction markets.**

## Edges Implemented

1. **Mean Reversion (EMA + Order Book Imbalance)**
   - 5/15-minute markets exhibit mean-reverting behavior due to market maker inventory cycling.
   - Enters when price deviates >8% from EMA(20) AND order book imbalance supports reversal.
   - Target: 60-65% win rate with Kelly sizing.

2. **Toxicity-Aware Market Making**
   - Provides liquidity when VPIN (volume-order imbalance) is low (<0.6).
   - Steps back when toxic flow detected to avoid adverse selection.
   - Dynamic spread adjustment based on volatility and inventory.

3. **Longshot Bias Exploitation** *(planned)*
   - Systematically short overpriced low-probability events (1-5%) that retail traders overbuy.
   - High win rate, but requires careful hedging.

4. **Resolution Information Edge** *(future)*
   - For markets resolving within minutes, integrates external feeds to know outcome first.

## Project Structure

```
polymarket-5min-bot/
├── strategies/
│   ├── mean_reversion_5min.py  # core mean reversion logic
│   └── toxicity_mm.py         # market making with VPIN filter
├── market_data.py             # Polymarket CLOB + Gamma API wrapper
├── execution.py               # order placement, signing, position tracking
├── risk.py                    # Kelly criterion, circuit breakers
├── backtest_engine.py         # historical replay and metrics
├── cli.py                     # command-line interface
├── config.yaml                # configuration (strategies, risk limits)
├── Dockerfile                 # containerization
└── scripts/
    └── collector.py           # order book data collection for backtesting
```

## Quick Start

### 1. Install dependencies

```bash
cd /root/obsidian-hermes-vault/projects/polymarket-5min-bot
pip install -r requirements.txt
```

### 2. Configure

Copy `.env.example` to `.env` and fill in your Polymarket wallet address and private key (for live trading). For paper trading, leave blank.

Edit `config.yaml` to adjust:
- `min_volume` (default $10k)
- EMA period, deviation threshold
- Kelly fraction (default 25%)
- Max daily loss (5%) and drawdown breaker (10%)

### 3. Collect historical data (optional but recommended)

```bash
python scripts/collector.py
# Let it run for a few hours to build a CSV of order book snapshots
# Data saved to data/collection_YYYYMMDD_HHMMSS.csv
```

### 4. Backtest

```bash
python cli.py backtest --data data/your_collection.csv
```

This will simulate the mean reversion strategy and print stats: win rate, Sharpe, max drawdown, total PnL.

### 5. Run Paper Trading

```bash
python cli.py run --mode paper
```

The bot will:
- Scan Polymarket every minute for 5/15-minute active markets
- Fetch order books for YES tokens
- Generate signals from active strategies
- Print signals to console (no real orders placed)

### 6. Go Live

```bash
python cli.py run --mode live
```

**Requirements:**
- Wallet with sufficient USDC balance on Polygon
- Private key configured in `.env` (test with small amount first!)
- Understand the risks: prediction markets are volatile and bots can lose money fast.

### 7. Deploy via Docker (recommended for VPS)

```bash
docker build -t pm5minbot .
docker run -d --name pmbot --restart unless-stopped --env-file .env -v $(pwd)/data:/app/data -v $(pwd)/logs:/app/logs pm5minbot
```

## Strategy Parameters

Tweak in `config.yaml`:

**Mean Reversion:**
- `ema_period`: 20 (short-term trend)
- `deviation_threshold`: 0.08 (8% deviation triggers)
- `imbalance_threshold`: 0.3 (order book imbalance must confirm)

**Market Making:**
- `vpin_threshold`: 0.6 (toxicity filter)
- `spread_multiplier`: 1.5 (widen spreads when volatile/toxic)
- `kelly_fraction`: 0.2 (conservative)

## Risk Management

- **Kelly sizing** prevents overbetting
- **Daily loss limit** (default 5%) halts trading
- **Max drawdown** (default 10%) circuit breaker
- **Position limits** per strategy (10% of capital)

## Telegram Alerts

Configure bot token and chat ID in `.env` or `config.yaml` to get real-time notifications for:
- Entries and exits
- PnL updates
- Circuit breaker events
- Errors

## Notes

- This bot is for **5 and 15-minute** markets only. It will filter out longer-duration markets.
- Works best on high-volume markets ($10k+ liquidity) to avoid slippage and manipulation.
- Expect 1-3 trades per market per day; not a high-frequency scalper.
- Backtest thoroughly before risking real capital.
- Polymarket API may have rate limits; the bot is polite (1 request/sec).

## Legal & Compliance

- You are responsible for your own trading decisions and compliance with local laws.
- This software is provided as-is, no warranty.
- Never trade with money you cannot afford to lose.

## Development

Logs in `logs/`. Metrics in `data/`. Live metrics endpoint coming soon.

Happy hunting! 🎯