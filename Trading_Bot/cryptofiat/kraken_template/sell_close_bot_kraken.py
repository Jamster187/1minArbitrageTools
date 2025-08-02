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

def place_limit_sell_order(exchange_object, target_symbol, base_asset_amount, target_price):
    '''
    Places limit sell order using an instantiated exchange object, 
    a constructed market symbol string with base_asset and target_quote_asset strings, 
    specified base_asset_amount float for the order size and a target_price float.
    '''
    return exchange_object.create_limit_sell_order(target_symbol, base_asset_amount, target_price)

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

def main(target_exchange_name_string_for_db, liquid_quote_asset, base_asset, target_quote_asset, min_order_value, min_spot_price_change, sell_closing_discount, sell_close_sleep_time, stale_price_timeout_counter):
    loop_count = 0
    while True:
        start_time1 = time.time()
        target_symbol = f"{base_asset}/{target_quote_asset}"
        enable_operation = sqlSelect(f"SELECT enable_operation FROM Seller_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'") # THIS COMES FROM OUR DB
        try:
            close_order_id = sqlSelect(f"SELECT close_order_id FROM Seller_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'") # THIS IS OUR CURRENT CLOSING SELL ORDER ID (FOR THE STUFF DONE BY THE BUYER BOT)
                    
            if close_order_id[0] != 'None':
                
                cancel_order_2(list_of_instantiated_kraken_objects[loop_count], close_order_id[0], target_symbol)
                                        
                close_order_id = 'None'
                    
                # Table: Seller_{base_asset}_{target_quote_asset} modify database column "close_order_id" to "None" 
                sqlCommit(f"UPDATE Seller_{base_asset}_{target_quote_asset} SET close_order_id = '{close_order_id}', close_order_id_timestamp = {time.time()} WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'")
                time.sleep(5)
                
            base_asset_balance = sqlSelect(f"SELECT base_asset_balance FROM Seller_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'") # THESE ARE THE COINS WE'VE RECEIVED FROM THE BUYER BOT THAT WE NEED TO SPEND BACK
            raw_current_rate = get_price_of_crypto_fiat_pair(conn, base_asset, liquid_quote_asset)
            if raw_current_rate != None:
                current_rate = float(raw_current_rate) # ONLY SPECIFY A QUOTE ASSET WHEN IT IS A CRYPTO/CRYPTO PAIR      
                sell_price = current_rate * (1 + sell_closing_discount)
                
                print(f"Current rate : {round(current_rate, 8)} USDT Calculated closing sell price: {round(sell_price, 8)} ({round(( (sell_price/current_rate) - 1) * 100, 5)}% premium)") 
                    
                if current_rate: # Make sure current_rate is not None
                        
                    try:
                        
                        if sell_price and (sell_price * float(base_asset_balance[0]) >= min_order_value): # Make sure sell_price is not None
                            sell_order = place_limit_sell_order(list_of_instantiated_kraken_objects[loop_count], target_symbol, (float(base_asset_balance[0]) * 0.99), sell_price) 
                            if sell_order:
                                order_id = sell_order['id'] # THIS IS close_order_id   
                                
                               # Table: Seller_{base_asset}_{target_quote_asset} modify database column "close_order_id" to current sell order id                                 
                                sqlCommit(f"UPDATE Seller_{base_asset}_{target_quote_asset} SET close_order_id = '{order_id}', close_order_id_timestamp = {time.time()} WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'")
                                
                                # Table: {target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}_order_ids_checklist insert record into db "close_order_id, time, status: unchecked"
                                sqlCommit(f"INSERT INTO {target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}_order_ids_checklist (order_id, timestamp, type, status) VALUES ('{order_id}', {time.time()}, 'exit', 'unchecked')")                         
                                                          
                                print(f"Current rate : {round(current_rate, 6)} USDT Calculated sell price: {round(sell_price, 6)} ({round(( (sell_price/current_rate) - 1) * 100, 6)}% premium), min sell premium: {round(sell_closing_discount * 100, 6)}%") 
                                print(f"Placed new sell order for {target_symbol} at {round(sell_price, 6)}, ID: {sell_order['id']}")
                                    
                    except ccxt.ExchangeError as e:
                        print("Exchange error during sell order:", e)
                        cancel_all_orders(list_of_instantiated_kraken_objects[loop_count], target_symbol)
                        
                    loop_count = (loop_count + 1) % len(list_of_instantiated_kraken_objects)
                    end_time1 = time.time()
                    print(f"{end_time1-start_time1} seconds")                    
                    time.sleep(sell_close_sleep_time)  # loop takes 7.2 seconds. Total loop time = time.sleep + 7.2 seconds      
    
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
         sell_closing_discount, 
         sell_close_sleep_time,
         stale_price_timeout_counter)
