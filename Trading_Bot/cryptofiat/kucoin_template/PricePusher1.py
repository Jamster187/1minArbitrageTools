import ccxt.pro as ccxtpro
import ccxt
import psycopg2
from psycopg2.extras import execute_values
import time
import requests
from datetime import datetime, timedelta
from config import *
from dbHelpers import *
from market_settings import *

# Database configuration
DBHOST = KUCOIN_DB_HOST
DBNAME = KUCOIN_DB_NAME
DBUSER = KUCOIN_DB_USER
DBPASS = KUCOIN_DB_PASSWORD

# Connect to PostgreSQL database
def connect_db():
    conn = psycopg2.connect(
        host=DBHOST,
        database=DBNAME,
        user=DBUSER,
        password=DBPASS
    )
    return conn

# Create table if it does not exist
def create_table(conn, TABLE_NAME):
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            base_asset VARCHAR(255),
            price NUMERIC,
            time NUMERIC
        )
        """)
        conn.commit()

# Reconnect helper
async def reconnect_on_timeout(exchange, symbol):
    while True:
        try:
            print("Attempting to reconnect to WebSocket...")
            await exchange.close()  # Close the previous connection
            await exchange.sleep(5000)  # Wait for 5 seconds before retrying
            return await exchange.watch_ticker(symbol)  # Attempt to reconnect
        except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
            print("Reconnection failed. Retrying...", e)
            await exchange.sleep(5000)  # Retry after 5 seconds
        except Exception as e:
            print("An unexpected error occurred during reconnection:", e)
            await exchange.sleep(5000)  # Retry after 5 seconds


# Main function for WebSocket price fetching
async def main(base_asset, liquid_quote_asset, price_pusher_1_sleep_time):
    # Connect to database
    conn = connect_db()
    TABLE_NAME = f"{base_asset}_{liquid_quote_asset}_price"
    create_table(conn, TABLE_NAME)

    # Define the trading symbol directly
    symbol = f"{base_asset}/{liquid_quote_asset}"

    try:
        while True:
            start_time = time.time()
            try:
                # Subscribe to the ticker WebSocket directly without loading markets
                ticker = await bitget_arbitrage1_websocket.watch_ticker(symbol)

                # Get the latest price
                current_rate = ticker['last']

                # Purge and repopulate table with the new price
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {TABLE_NAME}")
                    cur.execute(f"INSERT INTO {TABLE_NAME} VALUES ('{base_asset}', {current_rate}, {time.time()})")
                    conn.commit()

                print(f"{base_asset} Current Price: {current_rate} {liquid_quote_asset}")

                # Sleep for the configured time before next fetch
                await bitget_arbitrage1_websocket.sleep(price_pusher_1_sleep_time * 1000)  # Convert to milliseconds
                end_time = time.time()
                print(end_time-start_time)
            except ccxt.NetworkError as e:  # Use ccxt.NetworkError
                print("Network error:", e)
                ticker = await reconnect_on_timeout(bitget_arbitrage1_websocket, symbol)  # Reconnect on timeout
            except ccxt.RequestTimeout as e:  # Use ccxt.RequestTimeout
                print("Request timeout:", e)
                ticker = await reconnect_on_timeout(bitget_arbitrage1_websocket, symbol)  # Reconnect on timeout
            except ccxt.ExchangeError as e:  # Use ccxt.ExchangeError
                print("Exchange error:", e)
            except Exception as e:
                print("An error occurred:", e)

    except KeyboardInterrupt:
        print("Program interrupted by user")
    finally:
        await bitget_arbitrage1_websocket.close()
        conn.close()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main(price_pusher_1_base_asset,
                     price_pusher_1_liquid_quote_asset,
                     price_pusher_1_sleep_time))
