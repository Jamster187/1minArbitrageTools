import ccxt.pro as ccxtpro  # For WebSocket support
import psycopg2
from psycopg2.extras import execute_values
import time
from config import kucoin_arbitrage_new_websocket
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
            side VARCHAR(4),
            price NUMERIC,
            amount NUMERIC,
            time NUMERIC
        )
        """)
        conn.commit()

# Reconnect helper using existing object
async def reconnect_on_timeout(exchange_object, base_asset, target_quote_asset, conn, TABLE_NAME, depth):
    print("Reconnecting to WebSocket...")
    await exchange_object.close()  # Close the existing connection
    await asyncio.sleep(5)  # Wait for 5 seconds before retrying

    # Reinitialize the same WebSocket object from config
    exchange_object = ccxtpro.kucoin()

    # Resume fetching the order book
    await fetch_order_book_websocket(exchange_object, base_asset, target_quote_asset, conn, TABLE_NAME, depth)

# Insert order book into table (this is now being used)
def insert_order_book(conn, order_book, TABLE_NAME):
    with conn.cursor() as cur:
        # Purge the table before inserting new data
        cur.execute(f"DELETE FROM {TABLE_NAME}")
        # Prepare data for bulk insert
        data = []
        for side, orders in order_book.items():
            for order in orders:
                price, amount = order[:2]
                data.append((side, price, amount, time.time()))
        
        # Perform bulk insert
        execute_values(
            cur,
            f"INSERT INTO {TABLE_NAME} (side, price, amount, time) VALUES %s",
            data
        )
        conn.commit()

# Fetch order book from exchange using WebSocket
async def fetch_order_book_websocket(exchange_object, base_asset, target_quote_asset, conn, TABLE_NAME, depth=500):
    while True:
        try:
            # Fetch order book using WebSocket with specified depth
            order_book = await exchange_object.watch_order_book(f"{base_asset}/{target_quote_asset}", limit=depth)
            
            # Insert order book data into the database
            insert_order_book(conn, {'bids': order_book['bids'], 'asks': order_book['asks']}, TABLE_NAME)
            
            print(f"Orderbook for {base_asset}/{target_quote_asset} updated with depth {depth}")

        except ccxt.NetworkError as e:
            print(f"Network error: {e}, reconnecting...")
            await reconnect_on_timeout(exchange_object, base_asset, target_quote_asset, conn, TABLE_NAME, depth)
        except ccxt.RequestTimeout as e:
            print(f"Request timeout: {e}, reconnecting...")
            await reconnect_on_timeout(exchange_object, base_asset, target_quote_asset, conn, TABLE_NAME, depth)
        except ccxt.base.errors.NetworkError as e:
            if "1012" in str(e) or "1013" in str(e):
                print(f"Connection closed by remote server, closing code {str(e)}, reconnecting...")
                await reconnect_on_timeout(exchange_object, base_asset, target_quote_asset, conn, TABLE_NAME, depth)
        except Exception as e:
            print("An unexpected error occurred:", e)

# Main function
async def main(base_asset, target_quote_asset, target_exchange_name_string_for_db, depth=100):
    # Connect to database
    conn = connect_db()
    TABLE_NAME = f"{target_exchange_name_string_for_db}_{base_asset}_{target_quote_asset}_orderbook"
    create_table(conn, TABLE_NAME)

    # Start fetching order book with existing WebSocket object
    try:
        await fetch_order_book_websocket(kucoin_arbitrage_new_websocket, base_asset, target_quote_asset, conn, TABLE_NAME, depth)

    except KeyboardInterrupt:
        print("Program interrupted by user")
    finally:
        conn.close()
        await kucoin_arbitrage_new_websocket.close()

# Run the WebSocket-based bot
if __name__ == '__main__':
    import asyncio
    asyncio.run(main(base_asset, 
                     target_quote_asset, 
                     target_exchange_name_string_for_db))
