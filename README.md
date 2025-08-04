# 1minArbitrageTools

**1minArbitrageTools** is a modular and scalable research and trading framework for identifying and acting on arbitrage opportunities in cryptocurrency markets. It collects, stores, analyzes, and optionally executes trades based on 1-minute and sub-minute OHLCV data and order books from 15+ crypto exchanges.

---

## 📈 Features

- ✨ Collect 1-minute OHLCV data across 15+ centralized exchanges using CCXT.
- ⏱ Scrape real-time order book data via WebSocket (e.g., Kraken).
- 🔎 Analyze arbitrage opportunities (downside, upside, basket, futures, cash & carry).
- 📊 Export validated signals into Excel with volume-weighted return models.
- ⚖️ Trade using live entry/exit bots on configurable strategies.
- 🖳️ Uses PostgreSQL for efficient high-volume candle and order book storage.

---

## 🗂️ Repository Overview

```
/
├── Data Collection
│   ├── binance_1min.py, kucoin_1min.py, ...     # 1-minute REST-based scrapers
│   ├── binance.py, kucoin.py, ...              # Exchange wrappers
│
├── Strategy & Analysis
│   ├── chartStrategyProcessorDB.py             # Main arbitrage processor based on stored postgresql data
│   ├── chartStrategyProcessorValidator.py      # Validator for above tool
│   ├── MarketsAnalyzerDB*.py                   # Analyzes a list of opportunity markets and compares against a list of liquid markets for arbitrage opportunities
│   ├── dropCalculatorDB.py, pumpCalculatorDB.py# Reports dump/pump statistics on a given market
│   ├── chartAnalysisProcessor.py               # Excel arbitrage performance breakdown
│   └── fileListGenerator.py                    # File list generator for batch processing
│
├── Live Bots
│   ├── buy_close_bot_kraken.py                 # Kraken Buyer (Closing Arb)
│   ├── sell_close_bot_kraken.py                # Kraken Seller (Closing Arb)
│   ├── buy_entry_bot_kraken.py                 # Kraken Buyer (Opening Arb)
│   ├── sell_entry_bot_kraken.py                # Kraken Seller (Opening Arb)
│   ├── buyer_order_id_checker.py               # Buy-side order validation
│   ├── seller_order_id_checker.py              # Sell-side order validation
│   ├── PricePusher1.py                         # Push liquid prices to database/logic
│   ├── market_settings.py                      # Config for trading bot such as profit target, competition amount, closing discount/premium
│   └── orderbookPusher.py                      # Real-time order book collector
│
├── Utilities
│   ├── prune.py, countRows.py, SQLlookup.py    # Maintenance + query helpers
│   ├── utcconvert.py                           # UTC timestamp handling
│   ├── ccxt_supported_exchanges.py             # Lists supported exchanges
│
├── Deployment
│   ├── deployBots.sh                           # Starts scraping bots in separate terminals
│   ├── deployResearchBots.sh                   # Starts analysis bots
│
├── Outputs
│   ├── *.csv                                   # Arbitrage records from chartProcessor
│   ├── *.xlsx                                  # Excel exports from analysisProcessor
```

---

## 🔢 Strategy Types

- **Downside Arbitrage**: Buy assets when they dip relative to fair value
- **Upside Arbitrage**: Sell when high deviations occur
- **Cash & Carry**: Spot vs. Futures mispricing
- **Perps vs Perps**: Futures cross-exchange decay/gap trading
- **Basket Strategy**: Relative divergence across correlated markets

---

## 📊 Output Format

- **CSV** files: Candlestick-level arbitrage metrics by timestamp
- **XLSX** files: Monthly return breakdowns, frequency analysis, volume-weighted impact
- **TradingView PineScript Labels** (via `pinescript.py`): Visual overlays

---

## 👩‍💻 Author
**Jameel Bsata**  
---

