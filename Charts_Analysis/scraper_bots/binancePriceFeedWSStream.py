import asyncio
import ccxt.pro
import pandas as pd
from datetime import datetime, timedelta

# Global list to store trades
trades = []

# Function to convert trades to 1-min OHLCV candles
def convert_to_ohlcv(trade_data):
    df = pd.DataFrame(trade_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    ohlcv = df['price'].resample('1Min').ohlc()
    ohlcv['volume'] = df['amount'].resample('1Min').sum()
    return ohlcv

async def main():
    exchange = ccxt.pro.binance()
    symbol = 'BTC/USDT'
    print(f"Tracking real-time trades for {symbol}...")

    end_time = datetime.utcnow() + timedelta(minutes=3)

    try:
        while datetime.utcnow() < end_time:
            trade = await exchange.watch_trades(symbol)
            for t in trade:
                print(f"Trade: Price={t['price']}, Amount={t['amount']}, Side={t['side']}, Time={exchange.iso8601(t['timestamp'])}")
                trades.append({
                    'timestamp': t['timestamp'],
                    'price': t['price'],
                    'amount': t['amount'],
                    'side': t['side']
                })

    except Exception as e:
        print(f"Error: {e}")
    finally:
        await exchange.close()

        print("\nCreating OHLCV candles from trade data...")
        candles = convert_to_ohlcv(trades)
        print(candles)

if __name__ == '__main__':
    asyncio.run(main())
