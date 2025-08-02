import pandas as pd
import numpy as np
from sqlalchemy import create_engine

# Configuration Section
opportunity_exchange_string = 'Kraken'
liquid_exchange_string = 'Bitget'
base_asset = 'pengu'
opportunity_quote_asset = 'usd'
liquid_quote_asset = 'usdt'
opportunity_table_name = f'{opportunity_exchange_string.lower()}_{base_asset}_{opportunity_quote_asset}_1m'
liquid_table_name = f'{liquid_exchange_string.lower()}_{base_asset}_{liquid_quote_asset}_1m'

opportunity_database_uri = f"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_{opportunity_exchange_string}"
liquid_database_uri = f"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_{liquid_exchange_string}"

start_datetime = pd.to_datetime('2025-05-01 00:00:00')
end_datetime = pd.to_datetime('2025-08-30 23:10:00')
bin_threshold_upside = 4 # e.g. 0.8% upside threshold

# Fetch function
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

# Calculate arbitrage % using high prices
merged_df['high_diff_pct'] = ((merged_df['high_opportunity'] - merged_df['high_liquid']) / merged_df['high_liquid']) * 100

# Print max value for debugging
print("Max high_diff_pct:", merged_df['high_diff_pct'].max())

# Filter upside arbitrage opportunities
filtered_df = merged_df[merged_df['high_diff_pct'] >= bin_threshold_upside]

# Select relevant columns
result = filtered_df[['timestamp', 'high_opportunity', 'high_liquid', 'volume_opportunity', 'high_diff_pct']]
result.sort_values(by='timestamp', inplace=True)

# Print result
print("Matching upside arbitrage records (high_opportunity > high_liquid):")
print(result.to_string(index=False))
