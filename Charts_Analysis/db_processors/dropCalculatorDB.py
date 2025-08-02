import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def parse_table_name_v2(table_name):
    parts = table_name.split('_')
    if len(parts) == 4:
        exchange = parts[0]
        base_asset = parts[1]
        quote_asset = parts[2]
        timeframe = parts[3]
        return exchange.upper(), base_asset.upper(), quote_asset.upper(), timeframe
    raise ValueError("Table name format is incorrect. Expected format: 'exchange_baseAsset_quoteAsset_timeframe'")

def calculate_percentage_changes_v2(df, ref_df):
    # Merge primary and reference market data on timestamp
    merged_df = pd.merge(df, ref_df, on='timestamp', suffixes=('', '_ref'))

    # Calculate percentage differences relative to the reference market
    merged_df['Low Difference (%)'] = ((merged_df['low'] - merged_df['open_ref']) / merged_df['open_ref']) * 100
    merged_df['High Difference (%)'] = ((merged_df['high'] - merged_df['open_ref']) / merged_df['open_ref']) * 100

    return merged_df

def filter_bad_candles(df, max_diff_pct):
    # Exclude candles where the primary market low drops below the reference market low by max_diff_pct
    df = df[(df['low'] >= df['low_ref'] * (1 - max_diff_pct / 100))]
    return df

def count_occurrences_v2(df, threshold):
    # Count rows where the low difference is below the negative threshold
    return df[df['Low Difference (%)'] <= -threshold]

def bin_results_v2(occurrences, bin_width, threshold):
    # Bin the occurrences based on low percentage changes
    bins = np.arange(-threshold, occurrences['Low Difference (%)'].min() - bin_width, -bin_width)
    binned_data = {f"≤ {bin:.2f}%": occurrences[occurrences['Low Difference (%)'] <= bin].shape[0] for bin in bins}
    return binned_data

def analyze_chart_strategy_v2(database_uri, table_name, reference_database_uri, reference_table_name, start_datetime, end_datetime, threshold, bin_width, max_diff_pct):
    # Parse table names to extract metadata
    exchange, base_asset, quote_asset, timeframe = parse_table_name_v2(table_name)
    ref_exchange, ref_base_asset, ref_quote_asset, ref_timeframe = parse_table_name_v2(reference_table_name)
    
    # Load primary market data
    try:
        engine = create_engine(database_uri)
        with engine.connect() as connection:
            df = pd.read_sql_table(table_name, con=connection)
    except Exception as e:
        print(f"Error loading data from {table_name}: {e}")
        return None

    # Load reference market data
    try:
        ref_engine = create_engine(reference_database_uri)
        with ref_engine.connect() as connection:
            ref_df = pd.read_sql_table(reference_table_name, con=connection)
    except Exception as e:
        print(f"Error loading data from {reference_table_name}: {e}")
        return None

    # Process timestamps (assuming they are in milliseconds)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    ref_df['timestamp'] = pd.to_datetime(ref_df['timestamp'], unit='ms')

    # Filter by date range
    df = df[(df['timestamp'] >= start_datetime) & (df['timestamp'] <= end_datetime)]
    ref_df = ref_df[(ref_df['timestamp'] >= start_datetime) & (ref_df['timestamp'] <= end_datetime)]

    # Ensure the dataframes have necessary columns
    if not {'open', 'high', 'low', 'volume'}.issubset(df.columns):
        print("Error: Required columns are missing from the primary market table.")
        return None
    if not {'open', 'high', 'low', 'volume'}.issubset(ref_df.columns):
        print("Error: Required columns are missing from the reference market table.")
        return None

    # Exclude rows with zero volume
    df = df[df['volume'] > 0]
    ref_df = ref_df[ref_df['volume'] > 0]

    # Calculate percentage changes relative to the reference market
    df = calculate_percentage_changes_v2(df, ref_df)

    # Exclude candles where low drops below the reference market low by max_diff_pct
    df = filter_bad_candles(df, max_diff_pct)

    # Count occurrences of percentage changes
    occurrences = count_occurrences_v2(df, threshold)

    # Bin results
    binned_data = bin_results_v2(occurrences, bin_width, threshold)

    # Output results
    print(f"Primary Market Pair: {base_asset}/{quote_asset} on {exchange}")
    print(f"Reference Market Pair: {ref_base_asset}/{ref_quote_asset} on {ref_exchange}")
    print(f"Timeframe: {timeframe}")
    print(f"Total Days Analyzed: {(df['timestamp'].max() - df['timestamp'].min()).days + 1}")
    print("\nOccurrences of Percentage Drops:")
    for bin_label, count in binned_data.items():
        if count > 0:
            print(f"{bin_label}: {count} occurrences")

    return binned_data


if __name__ == "__main__":
    opportunity_exchange_string = 'Kraken'
    liquid_exchange_string = 'Bitget'
    base_asset = 'pump'
    opportunity_quote_asset = 'usd'
    liquid_quote_asset = 'usdt'
    opportunity_table_name = f'{opportunity_exchange_string.lower()}_{base_asset}_{opportunity_quote_asset}_1m'
    liquid_table_name = f'{liquid_exchange_string.lower()}_{base_asset}_{liquid_quote_asset}_1m'    
    
    
    database_uri = f"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_{liquid_exchange_string}"
    reference_database_uri = f"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_{opportunity_exchange_string}"
    table_name = f"{liquid_exchange_string.lower()}_{base_asset}_{liquid_quote_asset}_1m"
    reference_table_name = f"{opportunity_exchange_string.lower()}_{base_asset}_{opportunity_quote_asset}_1m"
    start_datetime = pd.to_datetime("2025-01-01 00:00:00")
    end_datetime = pd.to_datetime("2026-12-30 23:59:59")
    threshold = 0  # Minimum percentage drop threshold
    bin_width = 0.1  # Bin width for percentage drops
    max_diff_pct = 2  # Maximum allowable low difference percentage

    analyze_chart_strategy_v2(database_uri, table_name, reference_database_uri, reference_table_name, start_datetime, end_datetime, threshold, bin_width, max_diff_pct)
