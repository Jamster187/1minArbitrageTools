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


def fetch_eur_usd_conversion_df(engine, table_name="kraken_eur_usd_1m"):
    with engine.connect() as connection:
        df = pd.read_sql_table(table_name, con=connection)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
    df = df.dropna(subset=['timestamp'])
    df = df[['timestamp', 'close']].rename(columns={'close': 'eur_usd_rate'})
    return df


def analyze_opportunities_fixed_bins(opportunity_database_uri, liquid_database_uri,
                                     opportunity_table_name, base_usdt_table_name,
                                     threshold=0.1, step=0.1,
                                     start_datetime=None, end_datetime=None):

    opportunity_exchange, base_asset, quote_asset, table_timeframe = parse_table_name(opportunity_table_name)

    opportunity_engine = create_engine(opportunity_database_uri)
    with opportunity_engine.connect() as connection:
        opportunity_df = pd.read_sql_table(opportunity_table_name.lower(), con=connection)

    liquid_engine = create_engine(liquid_database_uri)
    with liquid_engine.connect() as connection:
        base_usdt_df = pd.read_sql_table(base_usdt_table_name.lower(), con=connection)

    opportunity_df = opportunity_df[opportunity_df['volume'] > 0]
    base_usdt_df = base_usdt_df[base_usdt_df['volume'] > 0]

    opportunity_df['timestamp'] = pd.to_datetime(opportunity_df['timestamp'], unit='ms')
    base_usdt_df['timestamp'] = pd.to_datetime(base_usdt_df['timestamp'], unit='ms')

    opportunity_df.sort_values(by='timestamp', inplace=True)
    base_usdt_df.sort_values(by='timestamp', inplace=True)

    if start_datetime and end_datetime:
        opportunity_df = opportunity_df[(opportunity_df['timestamp'] >= start_datetime) & (opportunity_df['timestamp'] <= end_datetime)]
        base_usdt_df = base_usdt_df[(base_usdt_df['timestamp'] >= start_datetime) & (base_usdt_df['timestamp'] <= end_datetime)]

    kraken_engine = create_engine("postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Kraken")
    eur_usd_df = fetch_eur_usd_conversion_df(kraken_engine)

    # Merge all
    merged_df = pd.merge(opportunity_df, base_usdt_df, on='timestamp', suffixes=('_opportunity', '_usdt'))
    merged_df = pd.merge(merged_df, eur_usd_df, on='timestamp', how='inner')

    # Convert USDT prices to EUR using EUR/USD rate
    merged_df['low_liquid'] = merged_df['low_usdt'] / merged_df['eur_usd_rate']
    merged_df['high_liquid'] = merged_df['high_usdt'] / merged_df['eur_usd_rate']

    merged_df['Low Difference (%)'] = ((merged_df['low_opportunity'] - merged_df['low_liquid']) / merged_df['low_liquid']) * 100
    merged_df['High Difference (%)'] = ((merged_df['high_opportunity'] - merged_df['high_liquid']) / merged_df['high_liquid']) * 100

    merged_df['EUR Volume_opportunity'] = merged_df['volume_opportunity'] * merged_df['low_opportunity']
    merged_df['USDT Volume_opportunity'] = merged_df['EUR Volume_opportunity'] * merged_df['eur_usd_rate']

    valid_rows = merged_df.dropna(subset=['Low Difference (%)', 'High Difference (%)'])
    total_days = (valid_rows['timestamp'].max() - valid_rows['timestamp'].min()).days + 1 if not valid_rows.empty else 0

    if valid_rows.empty:
        print("No valid rows found after filtering.")
        return None

    buying_opportunities = valid_rows[valid_rows['Low Difference (%)'] <= -threshold]
    buy_occurrences = {}

    for t in np.arange(threshold, abs(buying_opportunities['Low Difference (%)'].min()), step):
        filtered = buying_opportunities[buying_opportunities['Low Difference (%)'] <= -t]
        count = filtered.shape[0]
        if count > 0:
            avg_volume = filtered['USDT Volume_opportunity'].mean()
            median_volume = filtered['USDT Volume_opportunity'].median()
            total_return = abs(t / 100 * count * avg_volume)
            monthly_return = total_return / 30 if total_days >= 30 else (total_return * (30 / total_days))
            monthly_return_percentage = (monthly_return / avg_volume) * 100 if avg_volume != 0 else 0
            buy_occurrences[f"{-t:.1f}%"] = (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage)

    selling_opportunities = valid_rows[valid_rows['High Difference (%)'] >= threshold]
    sell_occurrences = {}

    for t in np.arange(threshold, selling_opportunities['High Difference (%)'].max() + step, step):
        filtered = selling_opportunities[selling_opportunities['High Difference (%)'] >= t]
        count = filtered.shape[0]
        if count > 0:
            avg_volume = filtered['volume_opportunity'].mean()
            median_volume = filtered['volume_opportunity'].median()
            total_return = abs(t / 100 * count * avg_volume)
            monthly_return = total_return / 30 if total_days >= 30 else (total_return * (30 / total_days))
            monthly_return_percentage = (monthly_return / avg_volume) * 100 if avg_volume != 0 else 0
            sell_occurrences[f"{t:.1f}%"] = (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage)

    print(f"Buying Opportunities for {base_asset}/{quote_asset} on {opportunity_exchange}")
    for pct_diff, (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage) in buy_occurrences.items():
        print(f"  {pct_diff}: {count} occurrences, Total Return: {total_return:.2f} USDT, Monthly Return: {monthly_return:.2f} USDT, "
              f"Average Volume: {avg_volume:.2f} USDT, Median Volume: {median_volume:.2f} USDT, Monthly Return %: {monthly_return_percentage:.2f}%")

    print(f"\nSelling Opportunities for {base_asset}/{quote_asset} on {opportunity_exchange}")
    for pct_diff, (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage) in sell_occurrences.items():
        print(f"  {pct_diff}: {count} occurrences, Total Return: {total_return:.2f} {base_asset}, Monthly Return: {monthly_return:.2f} {base_asset}, "
              f"Average Volume: {avg_volume:.2f} {base_asset}, Median Volume: {median_volume:.2f} {base_asset}, Monthly Return %: {monthly_return_percentage:.2f}%")

    return buy_occurrences, sell_occurrences


# Example usage
opportunity_exchange_string = 'Bitvavo'
liquid_exchange_string = 'Binance'
base_asset = 'eth'
opportunity_quote_asset = 'eur'
liquid_quote_asset = 'usdt'
opportunity_table_name = f'{opportunity_exchange_string.lower()}_{base_asset}_{opportunity_quote_asset}_1m'
base_usdt_table_name = f'{liquid_exchange_string.lower()}_{base_asset}_{liquid_quote_asset}_1m'

opportunity_database_uri = f"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_{opportunity_exchange_string}"
liquid_database_uri = f"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_{liquid_exchange_string}"

start_datetime = pd.to_datetime('2025-04-16 00:00:00')
end_datetime = pd.to_datetime('2026-09-30 23:10:00')

analyze_opportunities_fixed_bins(
    opportunity_database_uri,
    liquid_database_uri,
    opportunity_table_name,
    base_usdt_table_name,
    threshold=0.1,
    step=0.1,
    start_datetime=start_datetime,
    end_datetime=end_datetime
)
