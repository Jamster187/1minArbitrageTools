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
DBHOST=KUCOIN_DB_HOST
DBNAME=KUCOIN_DB_NAME
DBUSER=KUCOIN_DB_USER
DBPASS=KUCOIN_DB_PASSWORD

def get_price_of_base_asset_vs_liquid_quote_asset(base_asset, liquid_quote_asset, exchange_object):
    ticker = exchange_object.fetch_ticker(f"{base_asset}/{liquid_quote_asset}")
    last_price = ticker['last']
    return last_price

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

def get_price(conn, TABLE_NAME, base_asset):
    price = sqlSelect(f"SELECT price from {TABLE_NAME} WHERE base_asset = '{base_asset}'")
    return price[0]

# Main function
def main(base_asset, liquid_quote_asset, price_pusher_1_sleep_time):
    # Connect to database
    conn = connect_db()
    TABLE_NAME = f"{base_asset}_{liquid_quote_asset}_price"
    create_table(conn, TABLE_NAME)
    try:
        while True:
            try:
                time.start1 = time.time()
                current_rate = get_price_of_base_asset_vs_liquid_quote_asset(base_asset, liquid_quote_asset, bitget_arbitrage1)
                
                # Purge and repopulate table
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {TABLE_NAME}")
                    cur.execute(f"INSERT INTO {TABLE_NAME} VALUES ('{base_asset}', {current_rate}, {time.time()})")
                    conn.commit()
                
                print(f"{base_asset} Current Price: {current_rate} USDT")
                time.end1= time.time()
                print(f"{time.end1-time.start1} seconds")
                time.sleep(price_pusher_1_sleep_time)  # Fetch every 10 seconds
                    
            except ccxt.NetworkError as e:
                print("Network error:", e)
                time.sleep(10)
            except ccxt.ExchangeError as e:
                print("Exchange error:", e)
            except Exception as e:
                print("An error occurred:", e) 
                
    except KeyboardInterrupt:
        print("Program interrupted by user")        
    finally:
        conn.close()

if __name__ == '__main__':
    main(price_pusher_1_base_asset,
         price_pusher_1_liquid_quote_asset,
         price_pusher_1_sleep_time)

