import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text

# Function to parse table name into exchange, base_asset, quote_asset, timeframe
def parse_table_name(table_name):
    parts = table_name.split('_')
    if len(parts) == 4:
        exchange = parts[0]
        base_asset = parts[1]
        quote_asset = parts[2]
        timeframe = parts[3]
        return exchange.upper(), base_asset.upper(), quote_asset.upper(), timeframe
    raise ValueError("Table name format is incorrect. Expected format: 'exchange_baseAsset_quoteAsset_timeframe'")

# Function to fetch data from a table in a database
def fetch_market_data(engine, table_name):
    try:
        with engine.connect() as connection:
            df = pd.read_sql_table(table_name.lower(), con=connection)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.dropna(subset=['timestamp'])
        return df
    except Exception as e:
        print(f"Error fetching table {table_name}: {e}")
        return pd.DataFrame()

# Function to fetch BTC/ETH reference prices from stablecoin markets
def calculate_btc_eth_reference(liquid_engine, base_asset, stablecoin='USDT'):
    stablecoin_table_name = f"binance_{base_asset.lower()}_{stablecoin.lower()}_1m"
    try:
        df = fetch_market_data(liquid_engine, stablecoin_table_name)
        if df.empty:
            return None
        return df[['timestamp', 'close']].rename(columns={'close': f'{base_asset}_price'})
    except Exception as e:
        print(f"Error fetching {base_asset}/{stablecoin} data: {e}")
        return None

# Function to analyze opportunity vs liquid markets
def analyze_opportunity_vs_liquid(opportunity_engine, liquid_engine, opportunity_table, liquid_table, base_asset, quote_asset, stablecoin_prices=None):
    opportunity_df = fetch_market_data(opportunity_engine, opportunity_table)
    liquid_df = fetch_market_data(liquid_engine, liquid_table)
    
    if opportunity_df.empty or liquid_df.empty:
        print(f"Empty data for {opportunity_table} or {liquid_table}")
        return None

    if stablecoin_prices is not None:
        liquid_df = liquid_df.merge(stablecoin_prices, on='timestamp', how='left')
        liquid_df['adjusted_price'] = liquid_df['close'] * liquid_df[f'{base_asset}_price']
        liquid_df['low'] = liquid_df['adjusted_price']
        liquid_df['high'] = liquid_df['adjusted_price']

    merged_df = opportunity_df.merge(liquid_df[['timestamp', 'low', 'high']], on='timestamp', suffixes=('_opportunity', '_liquid'))
    merged_df['Low Difference (%)'] = ((merged_df['low_opportunity'] - merged_df['low_liquid']) / merged_df['low_liquid']) * 100
    merged_df['High Difference (%)'] = ((merged_df['high_opportunity'] - merged_df['high_liquid']) / merged_df['high_liquid']) * 100
    return merged_df

# Function to determine database URI based on exchange list
def determine_database_uri(exchange, default_uri="postgresql+psycopg2://postgres:!!!@localhost:5432/"):
    return f"{default_uri}{exchange.lower()}"

# Main function to compare exchanges and save results
def compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe, start_datetime, end_datetime, threshold=0.5):
    results = []
    liquid_engine = None
    summary_results = []
    totals_data = {}

    for opportunity_exchange in opportunity_exchanges:
        opportunity_engine = create_engine(determine_database_uri(opportunity_exchange))
        inspector = inspect(opportunity_engine)
        opportunity_tables = inspector.get_table_names()

        for opportunity_table in opportunity_tables:
            try:
                exchange, base_asset, quote_asset, table_timeframe = parse_table_name(opportunity_table)
                if table_timeframe != timeframe:
                    continue

                # Process each liquid exchange
                for liquid_exchange in liquid_exchanges:
                    liquid_engine = create_engine(determine_database_uri(liquid_exchange))
                    liquid_table_name = f"{liquid_exchange.lower()}_{base_asset.lower()}_{quote_asset.lower()}_{timeframe.lower()}"
                    
                    if quote_asset in ['BTC', 'ETH']:
                        stablecoin = 'USDT'
                        stablecoin_prices = calculate_btc_eth_reference(liquid_engine, quote_asset, stablecoin)
                        if stablecoin_prices is not None:
                            merged_data = analyze_opportunity_vs_liquid(opportunity_engine, liquid_engine, opportunity_table, liquid_table_name, base_asset, quote_asset, stablecoin_prices)
                        else:
                            print(f"No stablecoin prices found for {quote_asset}")
                            continue
                    else:
                        merged_data = analyze_opportunity_vs_liquid(opportunity_engine, liquid_engine, opportunity_table, liquid_table_name, base_asset, quote_asset)
                    
                    if merged_data is not None:
                        summary_results.append(merged_data)
                        # Track totals for summary
                        if base_asset not in totals_data:
                            totals_data[base_asset] = {"volume": 0, "notional_value": 0, "price": 0}
                        totals_data[base_asset]["volume"] += merged_data['volume_opportunity'].sum()
                        break  # Use only the first matching liquid exchange
            except Exception as e:
                print(f"Error comparing {opportunity_table} with {liquid_table_name}: {e}")
                continue
    
    return summary_results, totals_data

# Function to save results to Excel workbook with multiple tabs
def save_results_to_excel(results, totals_data, filename="arbitrage_analysis.xlsx"):
    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    
    # Create separate tabs for BTC, ETH, USDT, USDC, USD
    quote_assets = ['BTC', 'ETH', 'USDT', 'USDC', 'USD']
    
    for quote_asset in quote_assets:
        filtered_results = [df for df in results if quote_asset in df['quote_asset'].values]
        if filtered_results:
            combined_df = pd.concat(filtered_results, ignore_index=True)
            combined_df.to_excel(writer, sheet_name=quote_asset)
    
    # Summary tab
    summary_df = pd.concat(results, ignore_index=True)
    summary_df.to_excel(writer, sheet_name="Summary")
    
    # Totals tab
    totals_df = pd.DataFrame.from_dict(totals_data, orient='index')
    totals_df.to_excel(writer, sheet_name="Totals")
    
    writer.save()

# Example usage
opportunity_exchanges = ['Testing_Data_Collection_Kraken']
liquid_exchanges = ['Testing_Data_Collection_Binance','Testing_Data_Collection_Kucoin']
timeframe = '1m'
start_datetime = pd.to_datetime('2024-10-08 02:29:00')
end_datetime = pd.to_datetime('2024-10-21 10:10:00')
threshold = 0.1

summary_results, totals_data = compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe, start_datetime, end_datetime, threshold)
save_results_to_excel(summary_results, totals_data)
