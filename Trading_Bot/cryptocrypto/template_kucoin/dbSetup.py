import requests
import ccxt
import time
import psycopg2
from datetime import datetime, timedelta
from config import *
from dbHelpers import *
import os
from market_settings import *


conn = psycopg2.connect(host=KUCOIN_DB_HOST, dbname=KUCOIN_DB_NAME, user=KUCOIN_DB_USER, password=KUCOIN_DB_PASSWORD)
cur = conn.cursor()

def seedProfitRecords(base_asset, target_quote_asset):
    sqlCommit(f"\
    CREATE TABLE IF NOT EXISTS target_quote_asset_profit_{base_asset}_{target_quote_asset} (order_id VARCHAR(255) PRIMARY KEY, date TIMESTAMP, cost NUMERIC);\
    CREATE TABLE IF NOT EXISTS base_asset_profit_{base_asset}_{target_quote_asset} (order_id VARCHAR(255) PRIMARY KEY,date TIMESTAMP, amount NUMERIC);\
    INSERT INTO base_asset_profit_{base_asset}_{target_quote_asset} VALUES ('None', '2024-01-01 11:58:01.062', 0);\
    INSERT INTO target_quote_asset_profit_{base_asset}_{target_quote_asset} VALUES ('None', '2024-01-01 11:58:01.062', 0);") 
    
def seedTraderRecords(base_asset, target_quote_asset, target_exchange_name_string_for_db):
    sqlCommit(f"CREATE TABLE IF NOT EXISTS Buyer_{base_asset}_{target_quote_asset} (trading_role VARCHAR(255) PRIMARY KEY, entry_order_id VARCHAR(255), close_order_id VARCHAR(255), target_quote_asset_spent NUMERIC, target_quote_asset_balance NUMERIC, enable_operation NUMERIC, futures_position_size NUMERIC, entry_order_id_timestamp NUMERIC, close_order_id_timestamp NUMERIC);")
    
    sqlCommit(f"CREATE TABLE IF NOT EXISTS Seller_{base_asset}_{target_quote_asset} (trading_role VARCHAR(255) PRIMARY KEY, entry_order_id VARCHAR(255), close_order_id VARCHAR(255), base_asset_spent NUMERIC, base_asset_balance NUMERIC, enable_operation NUMERIC, futures_position_size NUMERIC, entry_order_id_timestamp NUMERIC, close_order_id_timestamp NUMERIC);")
    
    checkBuyerRecord = sqlSelect(f"SELECT trading_role FROM Buyer_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}'")
    checkSellerRecord = sqlSelect(f"SELECT trading_role FROM Seller_{base_asset}_{target_quote_asset} WHERE trading_role = '{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}'")    

    if not checkBuyerRecord:
        
        sqlCommit(f"INSERT INTO Buyer_{base_asset}_{target_quote_asset} VALUES ('{target_exchange_name_string_for_db}_Buyer_{base_asset}_{target_quote_asset}', 'None', 'None', 0, 0, 1, 0, 0, 0);")
    
    if not checkSellerRecord:
        
        sqlCommit(f"INSERT INTO Seller_{base_asset}_{target_quote_asset} VALUES ('{target_exchange_name_string_for_db}_Seller_{base_asset}_{target_quote_asset}', 'None', 'None', 0, 0, 1, 0, 0, 0);")

    
def main(base_asset, target_quote_asset, target_exchange_name_string_for_db):
    seedProfitRecords(base_asset, target_quote_asset)
    seedTraderRecords(base_asset, target_quote_asset, target_exchange_name_string_for_db)
        
if __name__ == "__main__":
    main(base_asset,
         target_quote_asset, 
         target_exchange_name_string_for_db)
    # Close the terminal after the program finishes
    os.system("exit")