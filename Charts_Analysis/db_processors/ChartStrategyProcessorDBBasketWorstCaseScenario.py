import pandas as pd
import numpy as np
from sqlalchemy import create_engine, inspect

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
        numeric_pct_diff = float(pct_diff.split('%')[0])
        count = data[0]
        if count not in filtered or abs(numeric_pct_diff) > abs(filtered[count][0]):
            filtered[count] = (numeric_pct_diff, data)
    return [(f"≥ {pct_diff:.1f}%", data) for count, (pct_diff, data) in sorted(filtered.items(), reverse=True)]

def analyze_opportunities_vs_index(opportunity_database_uri, liquid_database_uris, opportunity_table_name, liquid_table_names, threshold=0.1, step=0.1, start_datetime=None, end_datetime=None):
    import pandas as pd
    opportunity_engine = create_engine(opportunity_database_uri)
    opportunity_df = pd.read_sql_table(opportunity_table_name, con=opportunity_engine)
    opportunity_df = opportunity_df[opportunity_df['volume'] > 0]
    opportunity_df['timestamp'] = pd.to_datetime(opportunity_df['timestamp'], unit='ms')
    opportunity_df.sort_values(by='timestamp', inplace=True)

    if start_datetime and end_datetime:
        opportunity_df = opportunity_df[(opportunity_df['timestamp'] >= start_datetime) & (opportunity_df['timestamp'] <= end_datetime)]

    exchange, base_asset, quote_asset, _ = parse_table_name(opportunity_table_name)
    opportunity_df[f'{quote_asset} Volume'] = opportunity_df['volume'] * opportunity_df['low']
    timeframe = determine_timeframe(opportunity_df)

    for liquid_uri in liquid_database_uris:
        liquid_engine = create_engine(liquid_uri)
        inspector = inspect(liquid_engine)
        liquid_tables = [tbl for tbl in inspector.get_table_names() if tbl in liquid_table_names]

        for liquid_table in liquid_tables:
            try:
                liquid_df = pd.read_sql_table(liquid_table, con=liquid_engine)
                liquid_df = liquid_df[liquid_df['volume'] > 0]
                liquid_df['timestamp'] = pd.to_datetime(liquid_df['timestamp'], unit='ms')
                liquid_df.sort_values(by='timestamp', inplace=True)

                if start_datetime and end_datetime:
                    liquid_df = liquid_df[(liquid_df['timestamp'] >= start_datetime) & (liquid_df['timestamp'] <= end_datetime)]

                liquid_df[f'{quote_asset} Volume'] = liquid_df['volume'] * liquid_df['low']

                merged_df = pd.merge(opportunity_df, liquid_df, on='timestamp', suffixes=('_opportunity', '_liquid'))
                merged_df['Low Difference (%)'] = ((merged_df['low_opportunity'] - merged_df['low_liquid']) / merged_df['low_liquid']) * 100
                merged_df['High Difference (%)'] = ((merged_df['high_opportunity'] - merged_df['high_liquid']) / merged_df['high_liquid']) * 100

                valid_rows = merged_df.dropna(subset=['Low Difference (%)', 'High Difference (%)'])
                total_days = (valid_rows['timestamp'].max() - valid_rows['timestamp'].min()).days + 1 if not valid_rows.empty else 0

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

                selling_opportunities = valid_rows[valid_rows['High Difference (%)'] >= threshold]
                sell_occurrences = {}
                for t in np.arange(threshold, selling_opportunities['High Difference (%)'].max() + step, step):
                    count = selling_opportunities[selling_opportunities['High Difference (%)'] >= t].shape[0]
                    if count > 0:
                        avg_volume = selling_opportunities[selling_opportunities['High Difference (%)'] >= t]['volume_opportunity'].mean()
                        median_volume = selling_opportunities[selling_opportunities['High Difference (%)'] >= t]['volume_opportunity'].median()
                        total_return = abs(t / 100 * count * avg_volume)
                        monthly_return = total_return / 30 if total_days >= 30 else (total_return * (30 / total_days))
                        monthly_return_percentage = (monthly_return / avg_volume) * 100 if avg_volume != 0 else 0
                        sell_occurrences[f"{t:.1f}%"] = (count, total_return, monthly_return, avg_volume, median_volume, monthly_return_percentage)

                filtered_buy_occurrences = filter_best_bins(buy_occurrences)
                filtered_sell_occurrences = filter_best_bins(sell_occurrences)

                print(f"Market Pair: {base_asset}/{quote_asset}")
                print(f"Opportunity Exchange: {exchange}")
                print(f"Liquid Exchange: {liquid_table}")
                print(f"Timeframe: {timeframe}")
                print(f"Total Days Analyzed: {total_days}\n")

                print("Buying Opportunities:")
                print(f"- Number of buying opportunities: {buying_opportunities.shape[0]} over {total_days} days")
                for pct_diff, data in filtered_buy_occurrences:
                    print(f"  {pct_diff}: {data[0]} occurrences, Total Return: {data[1]:.2f} USD, Monthly Return: {data[2]:.2f} USD, "
                          f"Average Volume: {data[3]:.2f} USD, Median Volume: {data[4]:.2f} USD, Monthly Return %: {data[5]:.2f}%")

                print("\nSelling Opportunities:")
                print(f"- Number of selling opportunities: {selling_opportunities.shape[0]} over {total_days} days")
                for pct_diff, data in filtered_sell_occurrences:
                    print(f"  {pct_diff}: {data[0]} occurrences, Total Return: {data[1]:.2f} {base_asset}, Monthly Return: {data[2]:.2f} {base_asset}, "
                          f"Average Volume: {data[3]:.2f} {base_asset}, Median Volume: {data[4]:.2f} {base_asset}, Monthly Return %: {data[5]:.2f}%")

            except Exception as e:
                print(f"Error analyzing liquid table {liquid_table}: {e}")

def run_batch_analysis_filtered(opportunity_database_uri, liquid_database_uris, base_asset, opportunity_quote_asset, liquid_quote_asset, start_datetime=None, end_datetime=None):
    opportunity_engine = create_engine(opportunity_database_uri)
    inspector = inspect(opportunity_engine)
    opportunity_tables = inspector.get_table_names()

    for opportunity_table in opportunity_tables:
        try:
            if f"_{base_asset.lower()}_{opportunity_quote_asset.lower()}_" not in opportunity_table:
                continue

            liquid_tables = []
            for liquid_uri in liquid_database_uris:
                liquid_engine = create_engine(liquid_uri)
                liquid_inspector = inspect(liquid_engine)
                matching_tables = [tbl for tbl in liquid_inspector.get_table_names()
                                   if f"_{base_asset.lower()}_{liquid_quote_asset.lower()}_" in tbl]
                liquid_tables.extend(matching_tables)

            analyze_opportunities_vs_index(
                opportunity_database_uri,
                liquid_database_uris,
                opportunity_table,
                liquid_tables,
                threshold=0.1,
                step=0.1,
                start_datetime=start_datetime,
                end_datetime=end_datetime
            )
        except Exception as e:
            print(f"Error analyzing {opportunity_table}: {e}")

if __name__ == "__main__":
    opportunity_database_uri = "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Binance"
    liquid_database_uris = [
        "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bybit",
        "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitget",
        "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Gate"
    ]
    start_datetime = pd.to_datetime('2025-04-01 00:00:00')
    end_datetime = pd.to_datetime('2025-06-30 23:10:00')

    base_asset = 'sui'
    opportunity_quote_asset = 'usdc:usdc'
    liquid_quote_asset = 'usdc:usdc'

    run_batch_analysis_filtered(
        opportunity_database_uri,
        liquid_database_uris,
        base_asset,
        opportunity_quote_asset,
        liquid_quote_asset,
        start_datetime,
        end_datetime
    )
