import pandas as pd
import numpy as np
from sqlalchemy import create_engine

def parse_table_name(table_name):
    parts = table_name.split('_')
    if len(parts) == 4:
        exchange = parts[0]
        base_asset = parts[1]
        quote_asset = parts[2]
        timeframe = parts[3]
        return exchange.upper(), base_asset.upper(), quote_asset.upper(), timeframe
    raise ValueError("Table name format is incorrect. Expected format: 'exchange_baseAsset_quoteAsset_timeframe'")

def determine_timeframe(df):
    df['time_diff'] = df['timestamp'].diff().dropna()
    if df['time_diff'].empty:
        print("Warning: No valid time differences found to determine the timeframe.")
        return "Unknown"
    
    mode_time_diff = df['time_diff'].mode()[0]
    minutes = mode_time_diff.total_seconds() / 60
    if minutes < 60:
        return f"{int(minutes)} min"
    hours = minutes / 60
    if hours < 24:
        return f"{int(hours)} hour"
    days = hours / 24
    return f"{int(days)} day"

def filter_best_bins(occurrences):
    filtered = {}
    for pct_diff, data in occurrences.items():
        numeric_pct_diff = float(pct_diff.split('%')[0])  # Extract the numeric part of the percentage
        count = data[0]
        if count not in filtered or abs(numeric_pct_diff) > abs(filtered[count][0]):
            filtered[count] = (numeric_pct_diff, data)
    # Return sorted by the highest percentage difference
    return [(f"≥ {pct_diff:.1f}%", data) for count, (pct_diff, data) in sorted(filtered.items(), reverse=True)]

def analyze_opportunities_fixed_bins(opportunity_database_uri, liquid_database_uri, opportunity_table_name, liquid_table_name, threshold=0.1, step=0.1, start_datetime=None, end_datetime=None):
    # Parse table names to get exchange, base, quote, and timeframe
    opportunity_exchange, base_asset, quote_asset, table_timeframe = parse_table_name(opportunity_table_name)
    
    # Connect to opportunity database and load opportunity data
    try:
        opportunity_engine = create_engine(opportunity_database_uri)
        with opportunity_engine.connect() as connection:
            opportunity_df = pd.read_sql_table(opportunity_table_name.lower(), con=connection)
    except Exception as e:
        print(f"Error reading {opportunity_table_name}: {e}")
        return None

    # Exclude rows with zero volume in opportunity data
    opportunity_df = opportunity_df[opportunity_df['volume'] > 0]

    # Connect to liquid database and load liquid data
    try:
        liquid_engine = create_engine(liquid_database_uri)
        with liquid_engine.connect() as connection:
            liquid_df = pd.read_sql_table(liquid_table_name.lower(), con=connection)
    except Exception as e:
        print(f"Error reading {liquid_table_name}: {e}")
        return None

    # Exclude rows with zero volume in liquid data
    liquid_df = liquid_df[liquid_df['volume'] > 0]

    # Process timestamps (assuming they are in milliseconds)
    opportunity_df['timestamp'] = pd.to_datetime(opportunity_df['timestamp'], unit='ms')
    liquid_df['timestamp'] = pd.to_datetime(liquid_df['timestamp'], unit='ms')

    # Ensure data is sorted by timestamp
    opportunity_df.sort_values(by='timestamp', inplace=True)
    liquid_df.sort_values(by='timestamp', inplace=True)

    # Filter by date range
    if start_datetime and end_datetime:
        opportunity_df = opportunity_df[(opportunity_df['timestamp'] >= start_datetime) & (opportunity_df['timestamp'] <= end_datetime)]
        liquid_df = liquid_df[(liquid_df['timestamp'] >= start_datetime) & (liquid_df['timestamp'] <= end_datetime)]

    # Determine the timeframe
    timeframe = determine_timeframe(opportunity_df)

    if 'low' not in opportunity_df.columns or 'low' not in liquid_df.columns:
        print(f"Error: The column 'low' does not exist in one of the tables.")
        return None

    # Calculate volumes in quote asset for buying opportunities
    opportunity_df[f'{quote_asset} Volume'] = opportunity_df['volume'] * opportunity_df['low']
    liquid_df[f'{quote_asset} Volume'] = liquid_df['volume'] * liquid_df['low']

    # Merge data on timestamp
    merged_df = pd.merge(opportunity_df, liquid_df, on='timestamp', suffixes=('_opportunity', '_liquid'))

    # Calculate differences
    merged_df['Low Difference (%)'] = ((merged_df['low_opportunity'] - merged_df['low_liquid']) / merged_df['low_liquid']) * 100
    merged_df['High Difference (%)'] = ((merged_df['high_opportunity'] - merged_df['high_liquid']) / merged_df['high_liquid']) * 100
    
    # Filter out rows with NaN values
    valid_rows = merged_df.dropna(subset=['Low Difference (%)', 'High Difference (%)'])
    total_days = (valid_rows['timestamp'].max() - valid_rows['timestamp'].min()).days + 1 if not valid_rows.empty else 0

    # Handle cases where no valid opportunities are found
    if valid_rows.empty:
        print("No valid rows found after filtering.")
        return None

    # Analyze buying opportunities (in quote asset, e.g., USD)
    buying_opportunities = valid_rows[valid_rows['Low Difference (%)'] <= -threshold]
    buy_occurrences = {}

    for t in np.arange(threshold, abs(buying_opportunities['Low Difference (%)'].min()), step):
        count = buying_opportunities[buying_opportunities['Low Difference (%)'] <= -t].shape[0]
        if count > 0:
            avg_volume = buying_opportunities[buying_opportunities['Low Difference (%)'] <= -t][f'{quote_asset} Volume_opportunity'].mean()
            median_volume = buying_opportunities[buying_opportunities['Low Difference (%)'] <= -t][f'{quote_asset} Volume_opportunity'].median()
            total_return = abs(t / 100 * count * avg_volume)
            monthly_return = total_return / 30 if total_days >= 30 else (total_return * (30 / total_days))
            monthly_return_percentage = (monthly_return / avg_volume) * 100 if avg_volume != 0 else 0
            buy_occurrences[f"{-t:.1f}%"] = (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage)

    # Analyze selling opportunities (in base asset, e.g., TAO)
    selling_opportunities = valid_rows[valid_rows['High Difference (%)'] >= threshold]
    sell_occurrences = {}

    for t in np.arange(threshold, selling_opportunities['High Difference (%)'].max() + step, step):
        count = selling_opportunities[selling_opportunities['High Difference (%)'] >= t].shape[0]
        if count > 0:
            avg_volume = selling_opportunities[selling_opportunities['High Difference (%)'] >= t]['volume_opportunity'].mean()  # Using volume of base asset
            median_volume = selling_opportunities[selling_opportunities['High Difference (%)'] >= t]['volume_opportunity'].median()
            total_return = abs(t / 100 * count * avg_volume)
            monthly_return = total_return / 30 if total_days >= 30 else (total_return * (30 / total_days))
            monthly_return_percentage = (monthly_return / avg_volume) * 100 if avg_volume != 0 else 0
            sell_occurrences[f"{t:.1f}%"] = (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage)

    # Filter the best bins
    filtered_buy_occurrences = filter_best_bins(buy_occurrences)
    filtered_sell_occurrences = filter_best_bins(sell_occurrences)

    # Output formatted results
    print(f"Market Pair: {base_asset}/{quote_asset}")
    print(f"Opportunity Exchange: {opportunity_exchange}")
    print(f"Liquid Exchange: {liquid_table_name}")
    print(f"Timeframe: {timeframe}")
    print(f"Total Days Analyzed: {total_days}\n")
    
    print("Buying Opportunities:")
    print(f"- Number of buying opportunities: {buying_opportunities.shape[0]} over {total_days} days")
    if buying_opportunities.shape[0] > 0:
        print(f"- Significant/common % difference values:")
        for pct_diff, (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage) in filtered_buy_occurrences:
            print(f"  {pct_diff}: {count} occurrences, Total Return: {total_return:.2f} USD, Monthly Return: {monthly_return:.2f} USD, "
                  f"Average Volume: {avg_volume:.2f} USD, Median Volume: {median_volume:.2f} USD, Monthly Return %: {monthly_return_percentage:.2f}%")

    print("\nSelling Opportunities:")
    print(f"- Number of selling opportunities: {selling_opportunities.shape[0]} over {total_days} days")
    if selling_opportunities.shape[0] > 0:
        print(f"- Significant/common % difference values:")
        for pct_diff, (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage) in filtered_sell_occurrences:
            print(f"  {pct_diff}: {count} occurrences, Total Return: {total_return:.2f} {base_asset}, Monthly Return: {monthly_return:.2f} {base_asset}, "
                  f"Average Volume: {avg_volume:.2f} {base_asset}, Median Volume: {median_volume:.2f} {base_asset}, "
                  f"Monthly Return %: {monthly_return_percentage:.2f}%")

    return filtered_buy_occurrences, filtered_sell_occurrences



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
#start_datetime = pd.to_datetime('2025-07-10 00:00:00')
end_datetime = pd.to_datetime('2026-09-30 23:10:00')

filtered_buy_occurrences, filtered_sell_occurrences = analyze_opportunities_fixed_bins(
    opportunity_database_uri, 
    liquid_database_uri, 
    opportunity_table_name, 
    liquid_table_name, 
    threshold=0.1, 
    step=0.1, 
    start_datetime=start_datetime, 
    end_datetime=end_datetime
)

