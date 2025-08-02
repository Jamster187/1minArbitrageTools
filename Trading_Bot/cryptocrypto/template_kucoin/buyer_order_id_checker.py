import ccxt
import time
from datetime import datetime
from config import *
from dbHelpers import *
from market_settings import *
import psycopg2

conn = psycopg2.connect(host=KUCOIN_DB_HOST, dbname=KUCOIN_DB_NAME, user=KUCOIN_DB_USER, password=KUCOIN_DB_PASSWORD)
cur = conn.cursor()

def seedOrderIDChecklist(target_exchange_name_string_for_db, base_asset, quote_asset):
    sqlCommit(f"CREATE TABLE IF NOT EXISTS {target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}_order_ids_checklist (order_id VARCHAR(255) PRIMARY KEY, timestamp BIGINT, type VARCHAR(255), status VARCHAR(255));")
    sqlCommit(f"CREATE TABLE IF NOT EXISTS {target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}_order_ids_checklist (order_id VARCHAR(255) PRIMARY KEY, timestamp BIGINT, type VARCHAR(255), status VARCHAR(255));")

def purge_complete_orders():
    sqlCommit(f"DELETE FROM {target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}_order_ids_checklist WHERE status = 'complete'")
    print("Purged complete orders.")

def process_entry_buy_order(order_id, base_asset, target_quote_asset, loop_count):
    target_symbol = f"{base_asset}/{target_quote_asset}"
    try:
        order_info = list_of_instantiated_kucoin_objects_1[loop_count].fetch_order(order_id, target_symbol)
    except TypeError as e:
        print(f"An error occurred while processing entry buy order {order_id}, skipping.")
        return
    except ccxt.NetworkError as e:
        print(f"Network error while fetching entry buy order {order_id}: {e}, retrying later.")
        return
    except ccxt.ExchangeError as e:
        print(f"Exchange error while fetching entry buy order {order_id}: {e}, skipping.")
        return
    except Exception as e:
        print(f"Unexpected error while fetching entry buy order {order_id}, skipping.")
        return    

    if order_info['status'] not in ['closed', 'canceled']:
        print(f"Order {order_id} is not closed, skipping.")
        return

    base_asset_acquired = float(order_info.get('filled', 0))
    target_quote_asset_spent = float(order_info.get('cost', 0)) + float(order_info.get('fee', 0)['cost'])

    # Communicate the data to relevant trader databases
    sqlCommit(f"INSERT INTO target_quote_asset_profit_{base_asset}_{target_quote_asset} VALUES ('{order_id}', '{datetime.fromtimestamp(order_info['timestamp'] / 1000)}', -{target_quote_asset_spent})")
    sqlCommit(f"UPDATE Buyer_{base_asset}_{target_quote_asset} SET target_quote_asset_spent = target_quote_asset_spent + {target_quote_asset_spent} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'")
    sqlCommit(f"UPDATE Seller_{base_asset}_{target_quote_asset} SET base_asset_balance = base_asset_balance + {base_asset_acquired} WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'")
    
    sqlCommit(f"UPDATE {target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}_order_ids_checklist SET status = 'complete' WHERE order_id = '{order_id}'")

def process_exit_buy_order(order_id, base_asset, target_quote_asset, loop_count):
    target_symbol = f"{base_asset}/{target_quote_asset}"
    try:
        order_info = list_of_instantiated_kucoin_objects_1[loop_count].fetch_order(order_id, target_symbol)
    except TypeError as e:
        print(f"An error occurred while processing exit buy order {order_id}, skipping.")
        return
    except ccxt.NetworkError as e:
        print(f"Network error while fetching exit buy order {order_id}: {e}, retrying later.")
        return
    except ccxt.ExchangeError as e:
        print(f"Exchange error while fetching exit buy order {order_id}: {e}, skipping.")
        return
    except Exception as e:
        print(f"Unexpected error while fetching exit buy order {order_id}, skipping.")
        return    

    if order_info['status'] not in ['closed', 'canceled']:
        print(f"Order {order_id} is not closed, skipping.")
        return

    base_asset_acquired = float(order_info.get('filled', 0))
    target_quote_asset_spent = float(order_info.get('cost', 0)) + float(order_info.get('fee', 0)['cost'])

    # Communicate the data to relevant trader databases
    sqlCommit(f"INSERT INTO base_asset_profit_{base_asset}_{target_quote_asset} VALUES ('{order_id}', '{datetime.fromtimestamp(order_info['timestamp'] / 1000)}', {base_asset_acquired})")
    sqlCommit(f"UPDATE buyer_{base_asset}_{target_quote_asset} SET target_quote_asset_balance = target_quote_asset_balance - {target_quote_asset_spent} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'")
    sqlCommit(f"UPDATE seller_{base_asset}_{target_quote_asset} SET base_asset_spent = base_asset_spent - {base_asset_acquired} WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'")
    
    sqlCommit(f"UPDATE {target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}_order_ids_checklist SET status = 'complete' WHERE order_id = '{order_id}'")

def check_enable_operation(base_asset, target_quote_asset):
    """Check the number of unchecked orders and update enable_operation as needed."""
    cur.execute(f"SELECT COUNT(*) FROM {target_exchange_name_string_for_db}_buyer_{base_asset}_{target_quote_asset}_order_ids_checklist WHERE status = 'unchecked'")
    unchecked_count = cur.fetchone()[0]

    if unchecked_count >= 3:
        sqlCommit(f"UPDATE buyer_{base_asset}_{target_quote_asset} SET enable_operation = 0")
        print(f"Backlog detected ({unchecked_count} orders), set enable_operation to 0.")
    else:
        sqlCommit(f"UPDATE buyer_{base_asset}_{target_quote_asset} SET enable_operation = 1")
        print(f"Backlog cleared ({unchecked_count} orders), set enable_operation to 1.")

def check_buy_order_ids(base_asset, target_quote_asset, loop_count):
    cur.execute(f"SELECT order_id, type FROM {target_exchange_name_string_for_db}_buyer_{base_asset}_{target_quote_asset}_order_ids_checklist WHERE status = 'unchecked'")
    unchecked_orders = cur.fetchall()

    # Check for backlog
    check_enable_operation(base_asset, target_quote_asset)

    if unchecked_orders:
        for order in unchecked_orders:
            order_id = order[0]
            order_type = order[1]
            try:
                print(f"Checking Order ID {order_id}")
                if order_type == 'entry':
                    process_entry_buy_order(order_id, base_asset, target_quote_asset, loop_count)
                elif order_type == 'exit':
                    process_exit_buy_order(order_id, base_asset, target_quote_asset, loop_count)                    
            except ccxt.NetworkError as e:
                print(f"Network error while fetching order {order_id}: {e}")
                time.sleep(0)
            except ccxt.ExchangeError as e:
                print(f"Exchange error while fetching order {order_id}: {e}")
    else:
        print("No unchecked buy orders found.")


def main(base_asset, target_quote_asset):
    loop_count = 0
    seedOrderIDChecklist(target_exchange_name_string_for_db, base_asset, target_quote_asset)
    while True:
        check_buy_order_ids(base_asset, target_quote_asset, loop_count)
        purge_complete_orders()
        time.sleep(0.1)
        loop_count = (loop_count + 1) % len(list_of_instantiated_kucoin_objects_1) 
        
if __name__ == '__main__':
    main(base_asset, target_quote_asset)
