import pandas as pd
import numpy as np
import xlsxwriter
from sqlalchemy import create_engine, inspect

# Optional date filters; if both are None, the entire dataset will be analyzed
START_DATE = pd.to_datetime("2025-04-01 00:00:00")
END_DATE   = pd.to_datetime("2025-06-30 23:59:59")

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
        df = df[df['volume'] > 0]
        if is_liquid:
            df.rename(columns={'low': 'low_liquid', 'high': 'high_liquid', 'close': 'close_liquid', 'volume': 'volume_liquid'}, inplace=True)
        print(f"Fetched {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Calculate bins and monthly profit
def calculate_opportunity_bins(merged_df, quote_asset, threshold=0.5, step=0.1):
    bins = {}
    days_in_dataset = (merged_df['timestamp'].max() - merged_df['timestamp'].min()).days or 1

    for side, direction, col in [("buy", -1, 'low_diff'), ("sell", 1, 'high_diff')]:
        opportunities = merged_df[merged_df[col] * direction >= threshold]
        max_diff = opportunities[col].max() * direction if not opportunities.empty else 0
        for t in np.arange(threshold, max_diff + step, step):
            condition = opportunities[col] * direction >= t
            filtered = opportunities[condition]
            if filtered.empty:
                continue
            if side == 'buy':
                avg_volume = (filtered['volume'] * filtered['close']).mean()
                median_volume = (filtered['volume'] * filtered['close']).median()
            else:
                avg_volume = (filtered['volume_liquid'] * filtered['close_liquid']).mean()
                median_volume = (filtered['volume_liquid'] * filtered['close_liquid']).median()

            total_return = abs(t / 100 * len(filtered) * avg_volume)
            monthly_return = (total_return / avg_volume) * (30 / days_in_dataset) * 100 if avg_volume else 0
            bins[f"{side}_≥{t:.1f}%"] = (len(filtered), total_return, monthly_return, avg_volume, median_volume)
    return bins

# Compare each futures (opportunity) table to each spot (liquid) table
def compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe):
    results = []
    excluded_quote_assets = ['try']
    batch_counter = 0

    for opportunity_db_uri in opportunity_exchanges:
        print(f"Connecting to opportunity exchange database: {opportunity_db_uri}")
        opportunity_engine = create_engine(opportunity_db_uri)
        opportunity_exchange_name = opportunity_db_uri.split("_")[-1]
        inspector = inspect(opportunity_engine)
        opportunity_tables = inspector.get_table_names()

        total_tables = len(opportunity_tables)
        print(f"Total tables to analyze: {total_tables}")
        valid_futures_tables = [t for t in opportunity_tables if ':' in t]
        print(f"Found {len(valid_futures_tables)} futures tables")

        for i, opportunity_table in enumerate(valid_futures_tables, start=1):
            try:
                exchange, base_asset, quote_colon, table_timeframe = parse_table_name(opportunity_table)
                quote_asset = quote_colon.split(":")[0]
            except ValueError as ve:
                print(ve)
                continue

            if quote_asset.lower() in excluded_quote_assets or table_timeframe != timeframe:
                continue

            result = None
            for liquid_db_uri in liquid_exchanges:
                liquid_exchange_name = liquid_db_uri.split("_")[-1]
                liquid_engine = create_engine(liquid_db_uri)
                inspector_liq = inspect(liquid_engine)
                liquid_tables = inspector_liq.get_table_names()

                spot_table_name = f"{liquid_exchange_name.lower()}_{base_asset.lower()}_{quote_asset.lower()}_{table_timeframe.lower()}"
                if spot_table_name not in liquid_tables:
                    continue

                try:
                    print(f"Comparing futures {opportunity_table} to spot {spot_table_name}")
                    df_opp = fetch_market_data(opportunity_engine, opportunity_table)
                    df_liq = fetch_market_data(liquid_engine, spot_table_name, is_liquid=True)

                    merged = pd.merge(df_opp, df_liq, on='timestamp', how='inner')
                    if merged.empty:
                        print(f"No valid rows found for {opportunity_table}")
                        continue

                    merged['low_diff'] = ((merged['low_liquid'] - merged['low']) / merged['low']) * 100
                    merged['high_diff'] = ((merged['high_liquid'] - merged['high']) / merged['high']) * 100

                    bins = calculate_opportunity_bins(merged, quote_asset)
                    best_buy = max((k for k in bins if k.startswith("buy")), key=lambda x: bins[x][2], default=None)
                    best_sell = max((k for k in bins if k.startswith("sell")), key=lambda x: bins[x][2], default=None)

                    if best_buy is None and best_sell is None:
                        continue

                    result = {
                        "opportunity_exchange": opportunity_exchange_name.capitalize(),
                        "liquid_exchange": liquid_exchange_name.capitalize(),
                        "market_pair": f"{base_asset}/{quote_asset}",
                        "avg_buy_volume": bins[best_buy][3] if best_buy else None,
                        "median_buy_volume": bins[best_buy][4] if best_buy else None,
                        "avg_sell_volume": bins[best_sell][3] if best_sell else None,
                        "median_sell_volume": bins[best_sell][4] if best_sell else None,
                        "monthly_buy_profit_percentage": bins[best_buy][2] if best_buy else None,
                        "monthly_sell_profit_percentage": bins[best_sell][2] if best_sell else None,
                        "exchange_quote_asset": quote_asset
                    }
                    print(f"Result found for {opportunity_table} - {result}")
                    results.append(result)
                    batch_counter += 1
                    break
                except Exception as e:
                    print(f"Error comparing {opportunity_table} and {spot_table_name}: {e}")

            print(f"Processed {i}/{len(valid_futures_tables)} tables from the opportunity exchange.")

    print(f"Completed processing. Found {len(results)} opportunities.")
    return results

# Save results to Excel
def save_results_to_excel(results, filename="cash_and_carry_opportunities.xlsx"):
    df = pd.DataFrame(results)
    tab_definitions = {
        "USD Buy":  (["usdt", "usdc", "usd"], "monthly_buy_profit_percentage"),
        "USD Sell": (["usdt", "usdc", "usd"], "monthly_sell_profit_percentage"),
        "BTC Buy":  (["btc"], "monthly_buy_profit_percentage"),
        "BTC Sell": (["btc"], "monthly_sell_profit_percentage"),
        "ETH Buy":  (["eth"], "monthly_buy_profit_percentage"),
        "ETH Sell": (["eth"], "monthly_sell_profit_percentage"),
        "Others Buy": (None, "monthly_buy_profit_percentage"),
        "Others Sell": (None, "monthly_sell_profit_percentage")
    }
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        for tab_name, (quote_assets, sort_col) in tab_definitions.items():
            if quote_assets:
                filtered = df[df['exchange_quote_asset'].str.lower().isin(quote_assets)]
            else:
                filtered = df[~df['exchange_quote_asset'].str.lower().isin(['usdt','usdc','usd','btc','eth'])]
            filtered.sort_values(by=sort_col, ascending=False).to_excel(writer, sheet_name=tab_name, index=False)
    print(f"Saved results to {filename}")

# ----------------------------------------------------------------------------
# Run Comparison with DB URIs and timeframe
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    opportunity_exchanges = [
        "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitget"
    ]

    liquid_exchanges = [
        "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Binance",
        "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitget",
        "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Gate"
    ]

    timeframe = '1m'

    final_results = compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe)
    if final_results:
        save_results_to_excel(final_results)
    else:
        print("No arbitrage opportunities found.")
