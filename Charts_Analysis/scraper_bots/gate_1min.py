import ccxt
import psycopg2
from psycopg2 import sql
import logging
import time
from config import *

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize the exchange
exchange = ccxt.gate({
    'enableRateLimit': True
})

# Connect to PostgreSQL database with retries
def connect_to_db(retries=5, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            conn = psycopg2.connect(
                host=GATE_DB_HOST,
                database=GATE_DB_NAME,
                user=GATE_DB_USER,
                password=GATE_DB_PASSWORD
            )
            return conn
        except psycopg2.Error as e:
            attempt += 1
            logging.error(f"Error connecting to the database: {e}. Retrying {attempt}/{retries}...")
            time.sleep(delay)
    raise Exception("Failed to connect to the database after multiple attempts.")

# Create the target table for a market if it doesn't exist
def create_table_for_market(cursor, table_name):
    try:
        cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                timestamp BIGINT PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT
            )
        """).format(sql.Identifier(table_name)))
    except psycopg2.Error as e:
        logging.error(f"Error creating table {table_name}: {e}")
        raise

# Create a non-temporary table for storing fetched candles
def create_temp_table(cursor, temp_candle_data_table):
    try:
        cursor.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                timestamp BIGINT PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume FLOAT
            )
        """).format(sql.Identifier(temp_candle_data_table)))
    except psycopg2.Error as e:
        logging.error(f"Error creating the table {temp_candle_data_table}: {e}")
        raise

# Fetch OHLCV data from the exchange and store it in the non-temporary table with retries
def fetch_and_store_ohlcv_in_temp(cursor, symbol, temp_candle_data_table, timeframe='1m', max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            logging.info(f"Fetching OHLCV data for {symbol}...")
            candles = exchange.fetch_ohlcv(symbol, timeframe, limit=1000)
            
            # Insert candles into the table
            for candle in candles:
                timestamp, open_price, high, low, close, volume = candle
                cursor.execute(sql.SQL("""
                    INSERT INTO {} (timestamp, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (timestamp) DO NOTHING
                """).format(sql.Identifier(temp_candle_data_table)), (timestamp, open_price, high, low, close, volume))
            
            logging.info(f"Inserted {len(candles)} candles into the table for {symbol}.")
            break  # Exit the retry loop on success

        except ccxt.NetworkError as e:
            retries += 1
            logging.error(f"Network error fetching OHLCV data for {symbol}: {e}. Retrying {retries}/{max_retries}...")
            time.sleep(5)  # Delay before retrying
        except ccxt.ExchangeError as e:
            logging.error(f"Exchange error for {symbol}: {e}. Skipping this market.")
            break  # Skip this market if there's an exchange error
        except psycopg2.Error as e:
            logging.error(f"Database error inserting candles into the table: {e}")
            raise  # Raise exception on database error to handle it in the main function
        except Exception as e:
            logging.error(f"Unexpected error fetching OHLCV data for {symbol}: {e}. Retrying {retries}/{max_retries}...")
            retries += 1
            time.sleep(5)

# Compare and insert missing candles into the target table
def insert_missing_candles(cursor, table_name, temp_candle_data_table):
    try:
        cursor.execute(sql.SQL("""
            INSERT INTO {} (timestamp, open, high, low, close, volume)
            SELECT t.timestamp, t.open, t.high, t.low, t.close, t.volume
            FROM {} t
            LEFT JOIN {} m ON t.timestamp = m.timestamp
            WHERE m.timestamp IS NULL
        """).format(
            sql.Identifier(table_name),             # Target market table
            sql.Identifier(temp_candle_data_table), # Table with fetched candles
            sql.Identifier(table_name)              # Target market table for join
        ))

        logging.info(f"Inserted missing candles into {table_name}.")
        
        # Clear the temporary table after inserting missing data
        cursor.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(temp_candle_data_table)))
        logging.info(f"Cleared table {temp_candle_data_table}.")
        
    except psycopg2.Error as e:
        logging.error(f"Database error during comparison and insertion: {e}")
        raise

# Main function to continuously update tables
def main():
    while True:
        conn = connect_to_db()
        cursor = conn.cursor()

        try:
            # List of markets to update
            markets = exchange.load_markets()

            for symbol in markets:
                # Create table for the market if it doesn't exist
                table_name = f"gate_{symbol.replace('/', '_').lower()}_1m"
                create_table_for_market(cursor, table_name)
                
                # Define the name of the table for this market's fetched data
                temp_candle_data_table = "candle_data_temp"
                
                # Create the temp table for fetched data
                create_temp_table(cursor, temp_candle_data_table)
                
                # Fetch and store candles in the temp table
                fetch_and_store_ohlcv_in_temp(cursor, symbol, temp_candle_data_table)
                
                # Compare and insert missing candles into the target market table
                insert_missing_candles(cursor, table_name, temp_candle_data_table)
                
                # Commit the transaction
                conn.commit()
                

            # Wait for 1 minute before the next iteration
            logging.info("Completed one iteration over all markets. Sleeping for 1 minute...")
            time.sleep(60)

        except Exception as e:
            logging.error(f"Error in main execution: {e}")
        finally:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
