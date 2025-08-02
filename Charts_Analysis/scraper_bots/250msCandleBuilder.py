import asyncio
import ccxt.pro as ccxtpro
import pandas as pd
from datetime import datetime, timedelta

# Store trades here
trades = []

# Function to handle trades
async def track_trades():
    exchange = ccxtpro.binance()
    symbol = 'BTC/USDT'
    await exchange.load_markets()

    print(f"Tracking real-time trades for {symbol}...")

    end_time = datetime.utcnow() + timedelta(minutes=3)

    try:
        while datetime.utcnow() < end_time:
            trade = await exchange.watch_trades(symbol)
            for t in trade:
                trades.append({
                    'timestamp': pd.to_datetime(t['timestamp'], unit='ms'),
                    'price': t['price'],
                    'amount': t['amount']
                })
                print(f"{t['timestamp']} | Price: {t['price']} | Amount: {t['amount']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await exchange.close()
        generate_candles()

# Function to generate 250ms candles from the trade data
def generate_candles():
    df = pd.DataFrame(trades)
    if df.empty:
        print("No trades collected.")
        return

    df.set_index('timestamp', inplace=True)
    df.sort_index(inplace=True)

    print("\nGenerated 250ms OHLCV Candles:\n")

    ohlcv = df['price'].resample('250ms').ohlc()
    ohlcv['volume'] = df['amount'].resample('250ms').sum()
    ohlcv.dropna(inplace=True)

    print(ohlcv)

# Run the event loop
asyncio.run(track_trades())
