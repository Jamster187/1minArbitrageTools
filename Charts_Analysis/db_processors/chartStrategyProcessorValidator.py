import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Configuration Section
opportunity_exchange_string = 'Kraken'
liquid_exchange_string = 'Binance'
base_asset = 'sol'
opportunity_quote_asset = 'eth'
liquid_quote_asset = 'eth'
opportunity_table_name = f'{opportunity_exchange_string.lower()}_{base_asset}_{opportunity_quote_asset}_1m'
liquid_table_name = f'{liquid_exchange_string.lower()}_{base_asset}_{liquid_quote_asset}_1m'

opportunity_database_uri = f"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_{opportunity_exchange_string}"
liquid_database_uri = f"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_{liquid_exchange_string}"

start_datetime = pd.to_datetime('2025-05-01 00:00:00')
end_datetime = pd.to_datetime('2025-07-30 23:10:00')
bin_threshold = -1

# Function to parse and fetch data from the database
def fetch_table_data(uri, table_name, start_datetime, end_datetime):
    engine = create_engine(uri)
    with engine.connect() as conn:
        df = pd.read_sql_table(table_name, conn)
    df = df[df['volume'] > 0]
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df[(df['timestamp'] >= start_datetime) & (df['timestamp'] <= end_datetime)]
    df.sort_values(by='timestamp', inplace=True)
    return df

# Load Data
opportunity_df = fetch_table_data(opportunity_database_uri, opportunity_table_name, start_datetime, end_datetime)
liquid_df = fetch_table_data(liquid_database_uri, liquid_table_name, start_datetime, end_datetime)

# Merge on timestamp
merged_df = pd.merge(opportunity_df, liquid_df, on='timestamp', suffixes=('_opportunity', '_liquid'))

# Calculate difference
merged_df['low_diff_pct'] = ((merged_df['low_opportunity'] - merged_df['low_liquid']) / merged_df['low_liquid']) * 100

# Filter for negative arbitrage opportunity
filtered_df = merged_df[merged_df['low_diff_pct'] <= bin_threshold]

# Select relevant columns for validation
result = filtered_df[['timestamp', 'low_opportunity', 'low_liquid', 'volume_opportunity', 'low_diff_pct']]

# Sort and print
result.sort_values(by='timestamp', inplace=True)
print("Matching arbitrage records (below threshold):")
print(result.to_string(index=False))
