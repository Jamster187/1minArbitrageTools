import pandas as pd
import numpy as np
import xlsxwriter
from sqlalchemy import create_engine, inspect

# Optional date filters; if both are None, the entire dataset will be analyzed
START_DATE = None
END_DATE = None

# Function to parse table names
def parse_table_name(table_name):
    parts = table_name.split('_')
    if len(parts) == 4:
        exchange = parts[0]
        base_asset = parts[1]
        quote_asset = parts[2]
        timeframe = parts[3]
        return exchange.upper(), base_asset.upper(), quote_asset.upper(), timeframe
    raise ValueError(f"Table name format is incorrect: '{table_name}' - Expected format: 'exchange_baseAsset_quoteAsset_timeframe'")

# Fetch data from a table
def fetch_market_data(engine, table_name, is_liquid=False):
    try:
        print(f"Fetching market data from table: {table_name}")
        with engine.connect() as connection:
            df = pd.read_sql_table(table_name, con=connection)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
            df = df.dropna(subset=['timestamp'])
            if START_DATE and END_DATE:
                df = df[(df['timestamp'] >= START_DATE) & (df['timestamp'] <= END_DATE)]
        print(f"Fetched {len(df)} rows from {table_name}")
        if is_liquid:
            df.rename(columns={'low': 'low_liquid', 'high': 'high_liquid', 'volume': 'volume_liquid'}, inplace=True)
        return df
    except ValueError as e:
        print(f"Error: {e}")
        return pd.DataFrame()

# Calculate synthetic price for base/quote asset
def calculate_synthetic_price(opportunity_engine, liquid_engine, base_asset, quote_asset, timeframe, exchange_name):
    print(f"Calculating synthetic price for {base_asset}/{quote_asset} on {exchange_name}")

    base_to_usdt_table = f"{exchange_name.lower()}_{base_asset.lower()}_usdt_{timeframe.lower()}"
    quote_to_usdt_table = f"{exchange_name.lower()}_{quote_asset.lower()}_usdt_{timeframe.lower()}"

    base_df = fetch_market_data(liquid_engine, base_to_usdt_table)
    quote_df = fetch_market_data(liquid_engine, quote_to_usdt_table)
    
    if base_df.empty or quote_df.empty:
        print(f"Error: Could not fetch synthetic price data for {base_asset}/{quote_asset}. Missing base or quote table.")
        return None

    synthetic_df = pd.merge(base_df[['timestamp', 'close']],
                            quote_df[['timestamp', 'close']],
                            on='timestamp',
                            suffixes=('_base', '_quote')).copy()
    synthetic_df['synthetic_price'] = synthetic_df['close_base'] / synthetic_df['close_quote']
    synthetic_df['low_liquid'] = synthetic_df['synthetic_price']
    synthetic_df['high_liquid'] = synthetic_df['synthetic_price']
    
    return synthetic_df[['timestamp', 'low_liquid', 'high_liquid']].copy()

# Calculate price differences between opportunity and liquid markets
def calculate_differences(opportunity_df, liquid_df):
    print(f"Calculating price differences...")
    merged_df = opportunity_df.merge(liquid_df, on='timestamp', suffixes=('_opportunity', '_liquid')).copy()
    merged_df['Low Difference (%)'] = ((merged_df['low_opportunity'] - merged_df['low_liquid']) / merged_df['low_liquid']) * 100
    merged_df['High Difference (%)'] = ((merged_df['high_opportunity'] - merged_df['high_liquid']) / merged_df['high_liquid']) * 100
    return merged_df.copy()

# Fetch and rename market data with .loc to avoid SettingWithCopyWarning
def fetch_and_rename_market_data(engine, table_name, rename_columns):
    try:
        print(f"Fetching market data from table: {table_name}")
        with engine.connect() as connection:
            df = pd.read_sql_table(table_name, con=connection)
        
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
            df = df.dropna(subset=['timestamp'])

            # Date‐range filtering if both START_DATE and END_DATE are not None
            if START_DATE and END_DATE:
                df = df[(df['timestamp'] >= START_DATE) & (df['timestamp'] <= END_DATE)]

        df = df[df['volume'] > 0]  # Filter out rows where volume is zero
        df.rename(columns=rename_columns, inplace=True)
        
        print(f"Fetched {len(df)} rows from {table_name}")
        return df
    except ValueError as e:
        print(f"Error: {e}")
        return pd.DataFrame()

# Filter the best bins
def filter_best_bins(occurrences):
    if not occurrences:
        return None

    # Filter out tuples that don't have enough elements
    filtered_bins = [item for item in occurrences.items() if len(item[1]) >= 6]

    if not filtered_bins:
        print("No bins with sufficient data for sorting.")
        return None

    # Sort by the highest monthly return percentage (6th element in tuple)
    sorted_bins = sorted(filtered_bins, key=lambda x: x[1][5], reverse=True)
    
    return sorted_bins[0] if sorted_bins else None

# Calculate the number of days in the dataset
def calculate_days_in_dataset(df):
    if df.empty:
        return 0
    first_timestamp = df['timestamp'].min()
    last_timestamp = df['timestamp'].max()
    days_in_dataset = (last_timestamp - first_timestamp).days
    return days_in_dataset if days_in_dataset > 0 else 1

# Analyze opportunities and calculate the necessary metrics
def analyze_opportunities_fixed_bins(opportunity_engine, liquid_engine,
                                     opportunity_table_name, liquid_table_name,
                                     base_asset, quote_asset,
                                     opportunity_exchange_name, liquid_exchange_name,
                                     synthetic_df=None, threshold=0.5, step=0.1,
                                     total_tables=1, current_table=1):
    print(f"Analyzing opportunities between {opportunity_table_name} and {liquid_table_name or 'synthetic price'}")

    # Fetch opportunity data
    rename_opportunity_columns = {'low': 'low_opportunity', 'high': 'high_opportunity', 'close': 'close_opportunity', 'volume': 'volume_opportunity'}
    opportunity_df = fetch_and_rename_market_data(opportunity_engine, opportunity_table_name, rename_opportunity_columns)

    if opportunity_df.empty:
        print(f"Skipping {opportunity_table_name} due to empty opportunity data.")
        return None

    # Fetch liquid data
    if synthetic_df is not None:
        liquid_df = synthetic_df
    else:
        liquid_df = fetch_and_rename_market_data(liquid_engine, liquid_table_name,
                                                 {'low': 'low_liquid', 'high': 'high_liquid'})

    if liquid_df.empty:
        print(f"Skipping {opportunity_table_name} due to empty liquid data.")
        return None

    # Calculate price differences
    merged_df = calculate_differences(opportunity_df, liquid_df)
    merged_df[f'{quote_asset} Volume_opportunity'] = merged_df['volume_opportunity'] * merged_df['low_opportunity']

    # Filter valid rows
    valid_rows = merged_df.loc[
        (merged_df['Low Difference (%)'] <= -threshold) |
        (merged_df['High Difference (%)'] >= threshold)
    ].copy()

    if valid_rows.empty:
        print(f"No valid rows found for {opportunity_table_name}")
        return None

    # Get the number of days in the dataset
    days_in_dataset = calculate_days_in_dataset(valid_rows)

    # Calculate buy opportunities
    buying_opportunities = valid_rows[valid_rows['Low Difference (%)'] <= -threshold].copy()
    buying_opportunities[f'{quote_asset} Volume_opportunity'] = (
        buying_opportunities['volume_opportunity'] * buying_opportunities['close_opportunity']
    )
    buy_occurrences = {}
    min_low_diff = abs(buying_opportunities['Low Difference (%)'].min()) if not buying_opportunities.empty else 0

    if min_low_diff > threshold:
        for t in np.arange(threshold, min_low_diff, step):
            filtered = buying_opportunities[buying_opportunities['Low Difference (%)'] <= -t]
            count = filtered.shape[0]
            if count > 0:
                avg_volume_usd = filtered[f'{quote_asset} Volume_opportunity'].mean()
                median_volume_usd = filtered[f'{quote_asset} Volume_opportunity'].median()

                if avg_volume_usd != 0 and days_in_dataset > 0:
                    total_return = abs(t / 100 * count * avg_volume_usd)
                    monthly_return_percentage = (total_return / avg_volume_usd) * (30 / days_in_dataset) * 100
                    buy_occurrences[f"≥ {t:.1f}%"] = (count, total_return, monthly_return_percentage, avg_volume_usd, median_volume_usd)

    # Calculate sell opportunities
    selling_opportunities = valid_rows[valid_rows['High Difference (%)'] >= threshold].copy()
    sell_occurrences = {}
    max_high_diff = selling_opportunities['High Difference (%)'].max() if not selling_opportunities.empty else 0

    if max_high_diff > threshold:
        # Calculate both base asset and USD-denominated volumes
        selling_opportunities[f'{quote_asset} Volume_opportunity'] = (
            selling_opportunities['volume_opportunity'] * selling_opportunities['close_opportunity']
        )

        for t in np.arange(threshold, max_high_diff + step, step):
            filtered = selling_opportunities[selling_opportunities['High Difference (%)'] >= t]
            count = filtered.shape[0]
            if count > 0:
                avg_volume_base = filtered['volume_opportunity'].mean()
                median_volume_base = filtered['volume_opportunity'].median()
                avg_volume_usd = filtered[f'{quote_asset} Volume_opportunity'].mean()
                median_volume_usd = filtered[f'{quote_asset} Volume_opportunity'].median()

                if avg_volume_usd != 0 and days_in_dataset > 0:
                    total_return = abs(t / 100 * count * avg_volume_usd)
                    monthly_return_percentage = (total_return / avg_volume_usd) * (30 / days_in_dataset) * 100
                    sell_occurrences[f"≥ {t:.1f}%"] = (count, total_return, monthly_return_percentage,
                                                      avg_volume_base, median_volume_base,
                                                      avg_volume_usd, median_volume_usd)

    # Get the best buy and sell bins (highest monthly return)
    best_buy_opportunity = max(buy_occurrences.items(), key=lambda x: x[1][2], default=None)
    best_sell_opportunity = max(sell_occurrences.items(), key=lambda x: x[1][2], default=None)

    if best_buy_opportunity is None and best_sell_opportunity is None:
        print("No valid buy or sell opportunities found.")
        return None

    return {
        'opportunity_exchange': opportunity_exchange_name,
        'liquid_exchange': liquid_exchange_name,
        'market_pair': f"{base_asset}/{quote_asset}",
        'avg_buy_volume': best_buy_opportunity[1][3] if best_buy_opportunity else None,  # USD
        'median_buy_volume': best_buy_opportunity[1][4] if best_buy_opportunity else None,  # USD
        'avg_sell_volume': best_sell_opportunity[1][3] if best_sell_opportunity else None,  # base asset
        'median_sell_volume': best_sell_opportunity[1][4] if best_sell_opportunity else None,  # base asset
        'avg_sell_volume_usd': best_sell_opportunity[1][5] if best_sell_opportunity else None,
        'median_sell_volume_usd': best_sell_opportunity[1][6] if best_sell_opportunity else None,
        'monthly_buy_profit_percentage': best_buy_opportunity[1][2] if best_buy_opportunity else None,
        'monthly_sell_profit_percentage': best_sell_opportunity[1][2] if best_sell_opportunity else None,
        'exchange_quote_asset': quote_asset
    }

# Compare exchanges and find arbitrage opportunities (with batch saving in increments of 4)
def compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe, batch_size=2000):
    excluded_quote_assets = []
    results = []
    batch_counter = 0

    for opportunity_db_uri in opportunity_exchanges:
        print(f"Connecting to opportunity exchange database: {opportunity_db_uri}")
        opportunity_engine = create_engine(opportunity_db_uri)
        opportunity_exchange_name = opportunity_db_uri.split("_")[-1]
        inspector = inspect(opportunity_engine)
        opportunity_tables = inspector.get_table_names()

        total_tables = len(opportunity_tables)
        print(f"Total tables to analyze: {total_tables}")

        for i, opportunity_table in enumerate(opportunity_tables, start=1):
            # Skip tables with a colon (:) in the name
            if ':' in opportunity_table:
                print(f"Skipping table {opportunity_table} due to colon in the name.")
                continue

            try:
                exchange, base_asset, quote_asset, table_timeframe = parse_table_name(opportunity_table)
            except ValueError as ve:
                print(ve)
                continue

            # Skip if base asset is "tap" or other invalid cases
            if base_asset.lower() in ['tap','cate','ace', 'smt', 'gec', 'wsg', 'axl', 'velo', 'degenreborn', 'fire', 'slt', 'hold', 'real', 'pix', 'troll']:
                print(f"Skipping table {opportunity_table} due to base asset being excluded.")
                continue

            if base_asset.lower().endswith('3s') or base_asset.lower().endswith('3l') or quote_asset in excluded_quote_assets or table_timeframe != timeframe:
                print(f"Skipping table {opportunity_table} due to base asset being excluded.")
                continue         

            result = None

            # Iterate through all liquid exchanges in priority order
            for liquid_db_uri in liquid_exchanges:
                liquid_exchange_name = liquid_db_uri.split("_")[-1]
                if liquid_exchange_name == opportunity_exchange_name:
                    continue

                liquid_engine = create_engine(liquid_db_uri)

                # Prioritize USDT, USDC, USD comparisons
                if quote_asset in ['USD', 'USDC']:
                    liquid_table_name = f"{liquid_exchange_name.lower()}_{base_asset.lower()}_usdt_{timeframe.lower()}"
                    result = analyze_opportunities_fixed_bins(
                        opportunity_engine, liquid_engine,
                        opportunity_table, liquid_table_name,
                        base_asset, quote_asset,
                        opportunity_exchange_name, liquid_exchange_name
                    )
                    if not result:
                        liquid_table_name = f"{liquid_exchange_name.lower()}_{base_asset.lower()}_usdc_{timeframe.lower()}"
                        result = analyze_opportunities_fixed_bins(
                            opportunity_engine, liquid_engine,
                            opportunity_table, liquid_table_name,
                            base_asset, quote_asset,
                            opportunity_exchange_name, liquid_exchange_name
                        )
                    if not result:
                        liquid_table_name = f"{liquid_exchange_name.lower()}_{base_asset.lower()}_usd_{timeframe.lower()}"
                        result = analyze_opportunities_fixed_bins(
                            opportunity_engine, liquid_engine,
                            opportunity_table, liquid_table_name,
                            base_asset, quote_asset,
                            opportunity_exchange_name, liquid_exchange_name
                        )

                elif quote_asset == 'USDT':
                    liquid_table_name = f"{liquid_exchange_name.lower()}_{base_asset.lower()}_{quote_asset.lower()}_{timeframe.lower()}"
                    result = analyze_opportunities_fixed_bins(
                        opportunity_engine, liquid_engine,
                        opportunity_table, liquid_table_name,
                        base_asset, quote_asset,
                        opportunity_exchange_name, liquid_exchange_name
                    )
                else:
                    # Synthetic price comparison
                    synthetic_df = calculate_synthetic_price(
                        opportunity_engine, liquid_engine,
                        base_asset, quote_asset, timeframe,
                        liquid_exchange_name
                    )
                    if synthetic_df is not None:
                        result = analyze_opportunities_fixed_bins(
                            opportunity_engine, liquid_engine,
                            opportunity_table, None,
                            base_asset, quote_asset,
                            opportunity_exchange_name, liquid_exchange_name,
                            synthetic_df=synthetic_df
                        )
                    else:
                        print(f"Skipping {opportunity_table}: Synthetic price calculation failed.")
                        continue

                # If a result is found, break out of the liquid exchange loop
                if result:
                    results.append(result)
                    batch_counter += 1
                    print(f"Result found for {opportunity_table} - {result}")

                    # If the batch size is reached, save the results and reset the list
                    if batch_counter % batch_size == 0:
                        save_results_to_excel(results, f"arbitrage_analysis_batch_{batch_counter // batch_size}.xlsx")
                        results.clear()
                    break

            print(f"                                        Processed {i}/{total_tables} tables from the opportunity exchange.")

    # Save any remaining results after the loop finishes
    if results:
        save_results_to_excel(results, f"arbitrage_analysis_final.xlsx")
        print("Final batch saved.")

# Save results to Excel with categorized and sorted tabs
def save_results_to_excel(results, filename="arbitrage_analysis.xlsx"):
    final_df = pd.DataFrame(results)

    # Define the tabs and their filters
    tab_definitions = {
        "USD Buy":  (['usdt', 'usdc', 'usd'], "monthly_buy_profit_percentage"),
        "USD Sell": (['usdt', 'usdc', 'usd'], "monthly_sell_profit_percentage"),
        "BTC Buy":  (['btc'], "monthly_buy_profit_percentage"),
        "BTC Sell": (['btc'], "monthly_sell_profit_percentage"),
        "ETH Buy":  (['eth'], "monthly_buy_profit_percentage"),
        "ETH Sell": (['eth'], "monthly_sell_profit_percentage"),
        "Others Buy": (None, "monthly_buy_profit_percentage"),
        "Others Sell": (None, "monthly_sell_profit_percentage")
    }

    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        for tab_name, (quote_assets, sort_column) in tab_definitions.items():
            if quote_assets:
                filtered_df = final_df[final_df['exchange_quote_asset'].str.lower().isin(quote_assets)]
            else:
                filtered_df = final_df[~final_df['exchange_quote_asset'].str.lower().isin(['usdt','usdc','usd','btc','eth'])]

            sorted_df = filtered_df.sort_values(by=sort_column, ascending=False)

            if not sorted_df.empty:
                sorted_df.to_excel(writer, sheet_name=tab_name, index=False)

    print(f"Results saved to {filename}")


# ------------------------------------------------------------------------------
#  Specify your date range below and run in your IDE
#  If both START_DATE and END_DATE remain None, all data is analyzed.
# ------------------------------------------------------------------------------

START_DATE = pd.to_datetime("2025-07-01 00:00:00")
END_DATE   = pd.to_datetime("2025-09-30 23:59:59")

opportunity_exchanges = [
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Kraken",
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Binance",
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitfinex",
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitstamp",
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bybit",
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Cryptocom",
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Gate",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Kraken"
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Kucoin"
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitget",
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitvavo"
    #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Probit"    
]

liquid_exchanges = [
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Binance",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitget",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Gate",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Coinbase",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_OKX",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Kucoin"
    
    
]

timeframe = '1m'

compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe)
