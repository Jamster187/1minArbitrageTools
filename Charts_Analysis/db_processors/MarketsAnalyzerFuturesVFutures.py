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
def fetch_market_data(engine, table_name, suffix=''):
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
        if suffix:
            df.rename(columns={
                'low': f'low_{suffix}',
                'high': f'high_{suffix}',
                'close': f'close_{suffix}',
                'volume': f'volume_{suffix}'
            }, inplace=True)
        print(f"Fetched {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        print(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Calculate bins and monthly profit
def calculate_opportunity_bins(merged_df, threshold=0.5, step=0.1):
    bins = {}
    days_in_dataset = (merged_df['timestamp'].max() - merged_df['timestamp'].min()).days or 1

    for side, direction, col, vol_col, close_col in [
        ("buy", -1, 'low_diff', 'volume_base', 'close_base'),
        ("sell", 1, 'high_diff', 'volume_quote', 'close_quote')
    ]:
        opportunities = merged_df[merged_df[col] * direction >= threshold]
        max_diff = opportunities[col].max() * direction if not opportunities.empty else 0
        for t in np.arange(threshold, max_diff + step, step):
            condition = opportunities[col] * direction >= t
            filtered = opportunities[condition]
            if filtered.empty:
                continue
            avg_volume = (filtered[vol_col] * filtered[close_col]).mean()
            median_volume = (filtered[vol_col] * filtered[close_col]).median()
            total_return = abs(t / 100 * len(filtered) * avg_volume)
            monthly_return = (total_return / avg_volume) * (30 / days_in_dataset) * 100 if avg_volume else 0
            bins[f"{side}_≥{t:.1f}%"] = (len(filtered), total_return, monthly_return, avg_volume, median_volume)
    return bins

# Compare each opportunity futures table to each liquid futures table
def compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe):
    results = []
    excluded_quote_assets = ['try']
    batch_counter = 0

    for opp_uri in opportunity_exchanges:
        opp_engine = create_engine(opp_uri)
        opp_name = opp_uri.split("_")[-1]
        print(f"Connecting to opportunity exchange database: {opp_uri}")
        opp_tables = [t for t in inspect(opp_engine).get_table_names() if ':' in t]
        print(f"Found {len(opp_tables)} futures tables in {opp_name}")

        for i, opp_table in enumerate(opp_tables, start=1):
            try:
                exchange, base_asset, quote_colon, table_timeframe = parse_table_name(opp_table)
                quote_asset = quote_colon.split(":")[0]
            except ValueError as ve:
                print(ve)
                continue

            if quote_asset.lower() in excluded_quote_assets or table_timeframe != timeframe:
                continue

            for liq_uri in liquid_exchanges:
                liq_engine = create_engine(liq_uri)
                liq_name = liq_uri.split("_")[-1]
                liq_tables = [t for t in inspect(liq_engine).get_table_names() if ':' in t]
                match_name = f"{liq_name.lower()}_{base_asset.lower()}_{quote_asset.lower()}:{quote_asset.lower()}_{table_timeframe.lower()}"
                if match_name not in liq_tables:
                    continue

                try:
                    print(f"Comparing futures {opp_table} to futures {match_name}")
                    df_opp = fetch_market_data(opp_engine, opp_table, suffix='base')
                    df_liq = fetch_market_data(liq_engine, match_name, suffix='quote')

                    merged = pd.merge(df_opp, df_liq, on='timestamp', how='inner')
                    if merged.empty:
                        print(f"No valid rows found for {opp_table}")
                        continue

                    merged['low_diff'] = ((merged['low_quote'] - merged['low_base']) / merged['low_base']) * 100
                    merged['high_diff'] = ((merged['high_quote'] - merged['high_base']) / merged['high_base']) * 100

                    bins = calculate_opportunity_bins(merged)
                    best_buy = max((k for k in bins if k.startswith("buy")), key=lambda x: bins[x][2], default=None)
                    best_sell = max((k for k in bins if k.startswith("sell")), key=lambda x: bins[x][2], default=None)

                    if best_buy is None and best_sell is None:
                        continue

                    result = {
                        "opportunity_exchange": opp_name.capitalize(),
                        "liquid_exchange": liq_name.capitalize(),
                        "market_pair": f"{base_asset}/{quote_asset}",
                        "avg_buy_volume": bins[best_buy][3] if best_buy else None,
                        "median_buy_volume": bins[best_buy][4] if best_buy else None,
                        "avg_sell_volume": bins[best_sell][3] if best_sell else None,
                        "median_sell_volume": bins[best_sell][4] if best_sell else None,
                        "monthly_buy_profit_percentage": bins[best_buy][2] if best_buy else None,
                        "monthly_sell_profit_percentage": bins[best_sell][2] if best_sell else None,
                        "exchange_quote_asset": quote_asset
                    }
                    print(f"Result found for {opp_table} - {result}")
                    results.append(result)
                    batch_counter += 1
                    break
                except Exception as e:
                    print(f"Error comparing {opp_table} and {match_name}: {e}")

            print(f"Processed {i}/{len(opp_tables)} tables from {opp_name}.")

    print(f"Completed processing. Found {len(results)} opportunities.")
    return results

# Save results to Excel
def save_results_to_excel(results, filename="futures_vs_futures_opportunities.xlsx"):
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
        #"postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitget",
        "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Gate"
    ]

    timeframe = '1m'

    final_results = compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe)
    if final_results:
        save_results_to_excel(final_results)
    else:
        print("No arbitrage opportunities found.")
