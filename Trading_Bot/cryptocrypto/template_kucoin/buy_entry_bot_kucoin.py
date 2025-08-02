import requests
import ccxt
import time
import psycopg2
from datetime import datetime, timedelta
from config import *
from dbHelpers import *
from market_settings import *
import pandas as pd

conn = psycopg2.connect(host=KUCOIN_DB_HOST, dbname=KUCOIN_DB_NAME, user=KUCOIN_DB_USER, password=KUCOIN_DB_PASSWORD)
cur = conn.cursor()


def get_price_of_crypto_fiat_pair(conn, base_asset, liquid_quote_asset):
    '''
    This is a helper function that gets a price from the corresponding price database constructed using the base_asset and liquid_quote_asset strings
    '''
    TABLE_NAME = f"{base_asset}_{liquid_quote_asset}_price"
    price = sqlSelect(f"SELECT price from {TABLE_NAME} WHERE base_asset = '{base_asset}'")
    timestamp = sqlSelect(f"SELECT time from {TABLE_NAME} WHERE base_asset = '{base_asset}'")
    currentTimestamp = time.time()
    if ( currentTimestamp - float(timestamp[0]) ) > stale_price_timeout_counter:
        return None
    else:
        return price[0]

def get_price_of_crypto_crypto_pair(base_asset, liquid_quote_asset):
    '''
    This is the main price getter function that gets a price from the corresponding price database constructed using the base_asset and liquid_quote_asset strings. 
    If the liquid quote asset is not USDT, USDC or USD - we assume the liquid_quote_asset is another crypto asset (non stablecoin) and construct a price in terms of that other crypto asset.
    '''
    raw_base_asset_price = get_price_of_crypto_fiat_pair(conn, base_asset, 'USDT')
    if raw_base_asset_price != None:
        base_asset_price = float(raw_base_asset_price)
        raw_liquid_quote_asset_price = get_price_of_crypto_fiat_pair(conn, liquid_quote_asset, 'USDT')
        if raw_liquid_quote_asset_price != None:
            liquid_quote_asset_price = float(raw_liquid_quote_asset_price)
            return base_asset_price / liquid_quote_asset_price
    
def place_limit_buy_order(exchange_object, target_price, target_symbol, target_quote_asset_amount):
    '''
    Places limit buy order using an instantiated exchange object, 
    a constructed market symbol string with base_asset and target_quote_asset strings, 
    specified target_quote_asset_amount float for the order size and a target_price float.
    '''
    base_asset_amount = target_quote_asset_amount / target_price
    return exchange_object.create_limit_buy_order(target_symbol, base_asset_amount, target_price)

def fetch_order(exchange_object, order_id, target_symbol, max_retries=3, retry_delay=1):
    '''
    Fetches the order details using an instantiated exchange object, an order id string, and a target_symbol string.
    Retries if an exception occurs until it succeeds or reaches the maximum number of retries.
    
    Parameters:
    - exchange_object: The instantiated exchange object.
    - order_id: The ID of the order to be fetched.
    - target_symbol: The symbol of the target market.
    - max_retries: Maximum number of retries before giving up (default is 5).
    - retry_delay: Delay in seconds between retries (default is 5 seconds).
    '''
    attempt = 0
    while attempt < max_retries:
        try:
            print(f"Fetching order: ID: {order_id}, Symbol: {target_symbol}")
            order_details = exchange_object.fetch_order(order_id, target_symbol)  # Pass the order ID here
            print(f"Order {order_id} fetched successfully.")
            return order_details  # Exit the loop and return the order details if successful
        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} to fetch order {order_id} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)  # Wait before retrying
            else:
                print(f"Failed to fetch order {order_id} after {max_retries} attempts.")
                return None  # Return None or raise an exception if needed

def get_highest_bid_under_specified_value(conn, TABLE_NAME, specified_value, max_allowed_competition_volume):
    '''
    Uses the TABLE_NAME string to connect to an orderbook db table corresponding to the exchange and market pair, 
    grabs the value that is just below the specified value and returns that value for order price calculation,
    considering the max_allowed_competition_volume.
    '''
    timestamp = sqlSelect(f"SELECT MIN(time) as orderbook_time from {TABLE_NAME} WHERE price > 0")
    currentTimestamp = time.time()
    if (currentTimestamp - float(timestamp[0])) > stale_orderbook_timeout_counter:
        return None
    else:    
        with conn.cursor() as cur:
            cur.execute(f"""
            SELECT price, amount
            FROM {TABLE_NAME}
            WHERE side = 'bids' AND price <= %s
            ORDER BY price DESC
            """, (specified_value,))
            bids = cur.fetchall()

            cumulative_volume = 0
            for bid in bids:
                price, amount = bid
                cumulative_volume += amount
                if cumulative_volume > max_allowed_competition_volume:
                    return price

            # If the loop completes without returning, it means all bids were within the allowed volume
            return bids[-1][0] if bids else None


def cancel_order_2(exchange_object, order_id, target_symbol, max_retries=3, retry_delay=1):
    '''
    Cancels an order using an instantiated exchange object, an order id string, and a target_symbol string.
    Retries if an exception occurs until it succeeds or reaches the maximum number of retries.
    
    Parameters:
    - exchange_object: The instantiated exchange object.
    - order_id: The ID of the order to be canceled.
    - target_symbol: The symbol of the target market.
    - max_retries: Maximum number of retries before giving up (default is 5).
    - retry_delay: Delay in seconds between retries (default is 5 seconds).
    '''
    attempt = 0
    while attempt < max_retries:
        try:
            exchange_object.cancel_order(order_id, target_symbol)  # Pass the order ID here
            print(f"Order {order_id} canceled successfully.")
            break  # Exit the loop if the cancellation is successful
        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} to cancel order {order_id} failed: {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)  # Wait before retrying
            else:
                print(f"Failed to cancel order {order_id} after {max_retries} attempts.") 

def cancel_all_orders(exchange_object, target_symbol):
    """Cancels all orders for a given trading symbol on Independent Reserve."""
    orders = exchange_object.fetch_open_orders(target_symbol)
    for order in orders:
        try:
            exchange_object.cancel_order(order['id'], target_symbol)  # Pass the order ID here
        except ccxt.OrderNotFound:
            print(f"Order {order['id']} not found. It might have already been filled or canceled.")
        except Exception as e:
            print(f"Error canceling order {order['id']}:", e)

def fetch_ohlcv(exchange, symbol: str, timeframe: str = '1m', limit: int = 15):
    """
    Fetch OHLCV data using the CCXT library.

    Parameters:
    exchange (ccxt.Exchange): The initialized exchange object from CCXT.
    symbol (str): The trading pair symbol (e.g., 'BTC/USDT').
    timeframe (str): The timeframe for OHLCV data (e.g., '1d', '1h', '1m').
    limit (int): The number of candles to fetch.

    Returns:
    pd.DataFrame: A pandas DataFrame with 'timestamp', 'open', 'high', 'low', 'close', 'volume' columns.
    """
    ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def calculate_atr(data: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculate the Average True Range (ATR) indicator.

    Parameters:
    data (pd.DataFrame): A pandas DataFrame containing 'high', 'low', and 'close' columns.
    period (int): The period for calculating the ATR (default is 14).

    Returns:
    pd.Series: A pandas Series representing the ATR values.
    """
    # Calculate True Range (TR)
    data['high-low'] = data['high'] - data['low']
    data['high-close'] = abs(data['high'] - data['close'].shift(1))
    data['low-close'] = abs(data['low'] - data['close'].shift(1))

    tr = data[['high-low', 'high-close', 'low-close']].max(axis=1)

    # Calculate ATR using a moving average of the True Range
    atr = tr.rolling(window=period).mean()

    # Clean up the temporary columns
    data.drop(['high-low', 'high-close', 'low-close'], axis=1, inplace=True)

    return atr

def return_atr(atr_exchange, atr_target_symbol):
    # Initialize the exchange    
    # Set the trading pair, timeframe, and number of candles to fetch
    timeframe = '1m'
    limit = 15
    # Fetch OHLCV data
    df = fetch_ohlcv(atr_exchange, atr_target_symbol, timeframe, limit)

    # Calculate ATR
    df['ATR'] = calculate_atr(df)

    # Display the dataframe with ATR
    
    return df['ATR'][14]

def min_profitable_discount(min_profitable_discount_list, atr_exchange_object, raw_current_rate):
    atr_value = return_atr(atr_exchange_object, atr_target_symbol)
    min_profitable_discount = None
    for i in range(len(min_profitable_discount_list)):
        if atr_value < ( float(raw_current_rate) - ( (1-min_profitable_discount_list[i]) * float(raw_current_rate) ) ) * max_price_volatility :
            print(( float(raw_current_rate) - ( (1-min_profitable_discount_list[i]) * float(raw_current_rate) ) ) * max_price_volatility)
            min_profitable_discount = min_profitable_discount_list[i]
            break
    if min_profitable_discount != None:
        return min_profitable_discount
    else:
        return None

def main(target_exchange_name_string_for_db, liquid_quote_asset, base_asset, target_quote_asset, min_order_value, min_spot_price_change, min_profitable_discount_list, max_target_quote_asset_to_use, buy_entry_sleep_time, stale_price_timeout_counter,
stale_orderbook_timeout_counter):
    loop_count = 0
    while True:
        start_time1 = time.time()
        target_symbol = f"{base_asset}/{target_quote_asset}"
        enable_operation = sqlSelect(f"SELECT enable_operation FROM Buyer_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'") # THIS COMES FROM OUR DB
        if enable_operation[0] == 1: # if 1 we play, if 0 we dont.
            try:
                entry_order_id = sqlSelect(f"SELECT entry_order_id FROM Buyer_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'") # THIS IS OUR CURRENT PROFIT MAKING BUY ORDER ID

                if entry_order_id[0] != 'None':
                            
                    cancel_order_2(list_of_instantiated_kucoin_objects_1[loop_count], entry_order_id[0], target_symbol)
                    entry_order_id = 'None'
                    
                    # Table: Buyer_{base_asset}_{target_quote_asset} modify database column "entry_order_id" to "None" 
                    sqlCommit(f"UPDATE Buyer_{base_asset}_{target_quote_asset} SET entry_order_id = '{entry_order_id}', entry_order_id_timestamp = {time.time()} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'")                                  
                   
                #Ensures we do not count the profits as part of the budget from the db (negative values)  
                target_quote_asset_spent_budget = sqlSelect(f"SELECT target_quote_asset_spent FROM Buyer_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'") # THIS IS HOW MUCH OF OUR BUYING BUDGET WE HAVE SPENT
                
                if float(target_quote_asset_spent_budget[0]) < 0:
                    sqlCommit(f"UPDATE Buyer_{base_asset}_{target_quote_asset} SET target_quote_asset_spent = 0 WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'") 
                    
                available_funds = max_target_quote_asset_to_use - float(target_quote_asset_spent_budget[0]) # This is how much of the budget we're allowed to put in a profit seeking order     
                raw_current_rate = get_price_of_crypto_crypto_pair(base_asset, liquid_quote_asset)
                min_profitable_discount_2 = min_profitable_discount(min_profitable_discount_list, atr_exchange_object, raw_current_rate)
                
                if raw_current_rate != None:
                    current_rate = float(raw_current_rate) # ONLY SPECIFY A QUOTE ASSET WHEN IT IS A CRYPTO/CRYPTO PAIR 
                    raw_buy_price = get_highest_bid_under_specified_value(conn, f"{target_exchange_name_string_for_db}_{base_asset}_{target_quote_asset}_orderbook", current_rate * (1-min_profitable_discount_2), max_allowed_competition_buy_volume )
                    
                    if raw_buy_price != None:
                        buy_price = float(raw_buy_price) + float(min_spot_price_change) # This is the profit seeking buy price
                        
                        #print(f"Current rate : {round(current_rate, 8)} {target_symbol} Calculated buy price: {round(buy_price, 8)} ({ round(( (current_rate/buy_price) - 1 ) * 100, 5)}% gain), min buy discount: {round(min_profitable_discount * 100, 5)}%")
                                        
                        if current_rate: # Make sure current_rate is not None
                            
                            try:
                                if buy_price and available_funds >= min_order_value:
                                    
                                    buy_order = place_limit_buy_order(list_of_instantiated_kucoin_objects_1[loop_count], buy_price, target_symbol, available_funds)
                                    
                                    if buy_order:
                                        order_id = buy_order['id']
                                        
                                        # Table: Buyer_{base_asset}_{target_quote_asset} modify database column "entry_order_id" to current buy order id                                 
                                        sqlCommit(f"UPDATE Buyer_{base_asset}_{target_quote_asset} SET entry_order_id = '{order_id}', entry_order_id_timestamp = {time.time()} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'")   
                                        
                                        # Table: {target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}_order_ids_checklist insert record into db "entry_order_id, time, status: unchecked"
                                        sqlCommit(f"INSERT INTO {target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}_order_ids_checklist (order_id, timestamp, type, status) VALUES ('{order_id}', {time.time()}, 'entry', 'unchecked')")                                        
                                        
                                        print(f"Current rate : {round(current_rate, 10)} USD Calculated buy price: {round(buy_price, 10)} ({ round(( (current_rate/buy_price) - 1 ) * 100, 3)}% gain), min buy discount: {round(min_profitable_discount_2 * 100, 3)}%") 
                                        print(f"Placed new buy order for {base_asset}_{target_quote_asset} at {buy_price}, ID: {buy_order['id']}")
                            except ccxt.ExchangeError as e:
                                print("Exchange error during buy order:", e)
                                cancel_all_orders(list_of_instantiated_kucoin_objects_1[loop_count], target_symbol)
                            
                            loop_count = (loop_count + 1) % len(list_of_instantiated_kucoin_objects_1) 
                            end_time1 = time.time()
                            print(f"{end_time1-start_time1} seconds")                    
                            time.sleep(buy_entry_sleep_time)  # loop takes 7.2 seconds. Total loop time = time.sleep + 7.2 seconds      
    
            except ccxt.NetworkError as e:
                print("Network error:", e)
                time.sleep(10)
            except ccxt.ExchangeError as e:
                print("Exchange error:", e)
            except Exception as e:
                print("An error occurred:", e)

if __name__ == '__main__':
    main(target_exchange_name_string_for_db,
         liquid_quote_asset,
         base_asset,
         target_quote_asset,
         min_order_value,
         min_spot_price_change,
         min_profitable_discount_list,
         max_target_quote_asset_to_use,
         buy_entry_sleep_time,
         stale_price_timeout_counter,
         stale_orderbook_timeout_counter)
    
