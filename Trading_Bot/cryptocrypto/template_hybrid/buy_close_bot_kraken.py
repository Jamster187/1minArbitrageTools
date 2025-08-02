import requests
import ccxt
import time
import psycopg2
from datetime import datetime, timedelta
from config import *
from dbHelpers import *
from market_settings import *

conn = psycopg2.connect(host=KRAKEN_DB_HOST, dbname=KRAKEN_DB_NAME, user=KRAKEN_DB_USER, password=KRAKEN_DB_PASSWORD)
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

def main(target_exchange_name_string_for_db, liquid_quote_asset, base_asset, target_quote_asset, min_order_value, min_spot_price_change, buy_closing_discount, buy_close_sleep_time, stale_price_timeout_counter):
    loop_count = 0
    while True:
        start_time1 = time.time()
        target_symbol = f"{base_asset}/{target_quote_asset}"
        enable_operation = sqlSelect(f"SELECT enable_operation FROM buyer_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'") # THIS COMES FROM OUR DB
        try:
            close_order_id = sqlSelect(f"SELECT close_order_id FROM buyer_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'") # THIS IS OUR CURRENT CLOSING BUY ORDER ID (FOR THE STUFF DONE BY THE SELLER BOT)
            
            if close_order_id[0] != 'None':
                    
                cancel_order_2(list_of_instantiated_kraken_objects[loop_count], close_order_id[0], target_symbol)
                close_order_id = 'None'
                        
                # Table: Buyer_{base_asset}_{target_quote_asset} modify database column "close_order_id" to "None" 
                sqlCommit(f"UPDATE buyer_{base_asset}_{target_quote_asset} SET close_order_id = '{close_order_id}' WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'") 
                time.sleep(5)
            
            target_quote_asset_balance = sqlSelect(f"SELECT target_quote_asset_balance FROM buyer_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'") # THESE ARE THE FUNDS WE'VE RECEIVED FROM THE SELLER BOT THAT WE NEED TO SPEND BACK
            raw_current_rate = get_price_of_crypto_crypto_pair(base_asset, liquid_quote_asset)
            if raw_current_rate != None:
                current_rate = float(raw_current_rate) # ONLY SPECIFY A QUOTE ASSET WHEN IT IS A CRYPTO/CRYPTO PAIR      
                buy_price = current_rate * (1 - buy_closing_discount)
                                
                print(f"CLOSING: Current rate : {round(current_rate, 8)} {target_quote_asset} Calculated buy2 price: {round(buy_price, 8)} ({ round(( (current_rate/buy_price) - 1 ) * 100, 5)}% gain), min buy discount: {round(buy_closing_discount * 100, 5)}%")
                
                if current_rate: # Make sure current_rate is not None
                    
                    try:
                        if buy_price and target_quote_asset_balance[0] >= min_order_value:
                            buy_order = place_limit_buy_order(list_of_instantiated_kraken_objects[loop_count], buy_price, target_symbol, float(target_quote_asset_balance[0] ) )
                            if buy_order:
                                order_id = buy_order['id']
                                
                                # Table: Buyer_{base_asset}_{target_quote_asset} modify database column "entry_order_id" to current buy order id                                 
                                sqlCommit(f"UPDATE buyer_{base_asset}_{target_quote_asset} SET close_order_id = '{order_id}' WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'")   
                                
                                # Table: {target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}_order_ids_checklist insert record into db "entry_order_id, time, status: unchecked"
                                sqlCommit(f"INSERT INTO {target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}_order_ids_checklist (order_id, timestamp, type, status) VALUES ('{order_id}', {time.time()}, 'exit', 'unchecked')")                                        
                                
                                print(f"Current rate : {round(current_rate, 10)} USD Calculated buy price: {round(buy_price, 10)} ({ round(( (current_rate/buy_price) - 1 ) * 100, 3)}% gain), min buy discount: {round(buy_closing_discount * 100, 3)}%") 
                                print(f"Placed new buy order for {target_symbol} at {buy_price}, ID: {buy_order['id']}")
                    except ccxt.ExchangeError as e:
                        #update db
                        print("Exchange error during buy order:", e)
                        cancel_all_orders(list_of_instantiated_kraken_objects[loop_count], target_symbol)
                        
                    loop_count = (loop_count + 1) % len(list_of_instantiated_kraken_objects)  
                    end_time1 = time.time()
                    print(f"{end_time1-start_time1} seconds")
                    time.sleep(buy_close_sleep_time)  # loop takes 7.2 seconds. Total loop time = time.sleep + 7.2 seconds      

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
         buy_closing_discount, 
         buy_close_sleep_time,
         stale_price_timeout_counter)
    
