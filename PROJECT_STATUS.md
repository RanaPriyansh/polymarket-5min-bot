# Polymarket 5/15-Minute Bot - Project Status

## Overview
This is an automated trading bot for Polymarket prediction markets focused on ultra-short duration (5 and 15-minute) markets. The bot implements two primary strategies:
1. Mean Reversion (EMA + Order Book Imbalance)
2. Toxicity-Aware Market Making (VPIN-based)

## Current State (as of 2026-03-29)

### ✅ Completed
- Core strategy implementations:
  - Mean reversion strategy with EMA deviation detection and order book imbalance confirmation
  - Toxicity-aware market making with VPIN filtering to avoid adverse selection
- Data infrastructure:
  - Polymarket CLOB + Gamma API wrapper (`market_data.py`)
  - Order book collection scripts for backtesting
  - Book quality assessment module
- Execution system:
  - Order placement, signing, and position tracking (`execution.py`)
  - Risk management with Kelly criterion and circuit breakers (`risk.py`)
- Backtesting capabilities:
  - Historical replay engine (`backtest_engine.py`)
  - Performance metrics calculation
- Interface:
  - Command-line interface (`cli.py`) with paper/live/backtest modes
  - Configuration management (`config.yaml`)
  - Dockerfile for containerized deployment
- Supporting systems:
  - Runtime telemetry and logging
  - Research subagents for strategy improvement
  - Basic test suite

### 🔧 In Progress
- Integration of Telegram alerts for real-time notifications
- Enhanced risk management with dynamic position sizing
- Improved market filtering for optimal 5/15-minute market selection
- Strategy parameter optimization based on recent backtest results
- Documentation and code cleanup

### 📋 Pending / Planned
1. **Longshot Bias Exploitation** (Q2 2026)
   - Systematically short overpriced low-probability events (1-5%)
   - Requires hedging mechanism to manage tail risk

2. **Resolution Information Edge** (Q3 2026)
   - Integrate external feeds for markets resolving within minutes
   - First-to-know advantage on outcome resolution

3. **Production Hardening**
   - Enhanced error handling and recovery mechanisms
   - Comprehensive monitoring and alerting
   - Automated deployment with health checks

4. **Research & Development**
   - Continuous strategy refinement based on market regime changes
   - Exploration of additional edges in microstructure
   - Integration with broader Hermes agent ecosystem

## Key Files
- `cli.py` - Main entry point
- `config.yaml` - Strategy and risk parameters
- `strategies/mean_reversion_5min.py` - Core mean reversion logic
- `strategies/toxicity_mm.py` - Market making with VPIN filter
- `market_data.py` - Polymarket API interface
- `execution.py` - Order execution and position tracking
- `risk.py` - Risk management systems
- `backtest_engine.py` - Historical backtesting
- `Dockerfile` - Containerization

## Current Performance Metrics (Paper Trading)
- Win Rate: ~62% (mean reversion strategy)
- Sharpe Ratio: ~1.8 (5-minute markets)
- Max Drawdown: <8% with current risk parameters
- Average Trades/Day: 15-25 signals generated

## Next Immediate Actions
1. Finalize Telegram alert integration
2. Run extended paper trading session (48h) to validate stability
3. Optimize strategy parameters based on latest market data
4. Prepare for small-scale live deployment with conservative sizing