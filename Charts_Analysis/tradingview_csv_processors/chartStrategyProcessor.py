import pandas as pd
import numpy as np

def parse_filename(filename):
    parts = filename.replace(',', '').split('_')
    if len(parts) == 3:
        opportunity_exchange = parts[0]
        base_asset = parts[1]
        quote_asset = parts[2].split()[0]
        timeframe = parts[2].split()[1]
        return opportunity_exchange.upper(), base_asset.upper(), quote_asset.upper(), timeframe
    raise ValueError("Filename format is incorrect. Expected format: 'EXCHANGE_BASEASSET_QUOTEASSET, TIMEFRAME.csv'")

def determine_timeframe(df):
    df['time_diff'] = df['time'].diff().dropna()
    mode_time_diff = df['time_diff'].mode()[0]
    minutes = mode_time_diff.total_seconds() / 60
    if minutes < 60:
        return f"{int(minutes)} min"
    hours = minutes / 60
    if hours < 24:
        return f"{int(hours)} hour"
    days = hours / 24
    return f"{int(days)} day"

def filter_best_monthly_return(occurrences):
    filtered = {}
    for pct_diff, data in occurrences.items():
        count = data[0]
        if count not in filtered or data[2] > filtered[count][1][2]:
            filtered[count] = (pct_diff, data)
    return [(pct_diff, data) for count, (pct_diff, data) in sorted(filtered.items(), reverse=True)]

def analyze_opportunities_fixed_bins(file_path, liquid_exchange="Binance", threshold=0.1, step=0.1, start_datetime=None, end_datetime=None):
    filename = file_path.split('/')[-1].split('.')[0]
    opportunity_exchange, base_asset, quote_asset, filename_timeframe = parse_filename(filename)

    df = pd.read_csv(file_path)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    csv_timeframe = determine_timeframe(df)
    
    if start_datetime is not None and end_datetime is not None:
        df = df[(df['time'] >= start_datetime) & (df['time'] <= end_datetime)]

    if 'low' not in df.columns:
        raise KeyError("The column 'low' does not exist in the CSV file.")

    df[f'{quote_asset} Volume'] = df['Volume'] * df['low']
    
    # Exclude rows with NaN in 'Low Difference (%)' or 'High Difference (%)'
    valid_rows = df.dropna(subset=['Low Difference (%)', 'High Difference (%)'])
    total_days = (valid_rows['time'].max() - valid_rows['time'].min()).days + 1

    buying_opportunities = valid_rows[valid_rows['Low Difference (%)'] <= -threshold]
    selling_opportunities = valid_rows[valid_rows['High Difference (%)'] >= threshold]

    buy_count = buying_opportunities.shape[0]
    buy_months = total_days / 30
    buy_avg_volume = buying_opportunities['Volume'].mean()
    buy_avg_quote_volume = buying_opportunities[f'{quote_asset} Volume'].mean()
    buy_opps_per_month = buy_count / buy_months
    buy_75th_volume = np.percentile(buying_opportunities['Volume'], 75)
    buy_75th_quote_volume = np.percentile(buying_opportunities[f'{quote_asset} Volume'], 75)

    buy_occurrences = {}
    for t in np.arange(threshold, abs(buying_opportunities['Low Difference (%)'].min()), step):
        count = buying_opportunities[buying_opportunities['Low Difference (%)'] <= -t].shape[0]
        if count > 0:
            pct_diff = t
            avg_quote_volume = buying_opportunities[buying_opportunities['Low Difference (%)'] <= -t][f'{quote_asset} Volume'].mean()
            total_return_quote = avg_quote_volume * (pct_diff / 100) * count
            monthly_return_quote = total_return_quote / buy_months
            monthly_return_percentage = (monthly_return_quote / avg_quote_volume) * 100 if avg_quote_volume != 0 else 0
            buy_occurrences[f"≥ {-pct_diff:.1f}%"] = (count, total_return_quote, monthly_return_quote, avg_quote_volume, monthly_return_percentage)

    sorted_buy_occurrences = filter_best_monthly_return(buy_occurrences)

    sell_count = selling_opportunities.shape[0]
    sell_months = total_days / 30
    sell_avg_volume = selling_opportunities['Volume'].mean()
    sell_opps_per_month = sell_count / sell_months
    sell_75th_volume = np.percentile(selling_opportunities['Volume'], 75)

    sell_occurrences = {}
    for t in np.arange(threshold, selling_opportunities['High Difference (%)'].max() + step, step):
        count = selling_opportunities[selling_opportunities['High Difference (%)'] >= t].shape[0]
        if count > 0:
            pct_diff = t
            avg_base_volume = selling_opportunities[selling_opportunities['High Difference (%)'] >= t]['Volume'].mean()
            total_return_base = avg_base_volume * (pct_diff / 100) * count
            monthly_return_base = total_return_base / sell_months
            monthly_return_percentage = (monthly_return_base / avg_base_volume) * 100 if avg_base_volume != 0 else 0
            sell_occurrences[f"≥ {pct_diff:.1f}%"] = (count, total_return_base, monthly_return_base, avg_base_volume, monthly_return_percentage)

    sorted_sell_occurrences = filter_best_monthly_return(sell_occurrences)

    print(f"Market Pair: {base_asset}/{quote_asset}")
    print(f"Opportunity Exchange: {opportunity_exchange}")
    print(f"Liquid Exchange: {liquid_exchange}")
    print(f"Timeframe from CSV: {csv_timeframe}")
    print(f"Total Days Analyzed: {total_days}")
    print()
    print(f"Buying Opportunities:")
    print(f"- Number of buying opportunities: {buy_count} over {total_days} days")
    print(f"- Significant/common % difference values:")
    for pct_diff, (count, total_return_quote, monthly_return_quote, avg_quote_volume, monthly_return_percentage) in sorted_buy_occurrences:
        print(f"  {pct_diff} {count} occurrences, Total Return: {total_return_quote:.2f} {quote_asset}, Monthly Return: {monthly_return_quote:.2f} {quote_asset}, Average Volume: {avg_quote_volume:.2f} {quote_asset}, Monthly Return %: {monthly_return_percentage:.2f}%")
    print(f"- Average volume of the base asset: {buy_avg_volume} {base_asset} worth of {quote_asset}")
    print(f"- Average {quote_asset} volume of the base asset: {buy_avg_quote_volume}")
    print(f"- Average buying opportunities per month: {buy_opps_per_month}")
    print(f"- Volume required to be in the 75th volume percentile: {buy_75th_volume} {base_asset} worth of {quote_asset}")
    print(f"- {quote_asset} volume required to be in the 75th volume percentile: {buy_75th_quote_volume}")
    print()
    print(f"Selling Opportunities:")
    print(f"- Number of selling opportunities: {sell_count} over {total_days} days")
    print(f"- Significant/common % difference values:")
    for pct_diff, (count, total_return_base, monthly_return_base, avg_base_volume, monthly_return_percentage) in sorted_sell_occurrences:
        print(f"  {pct_diff} {count} occurrences, Total Return: {total_return_base:.2f} {base_asset}, Monthly Return: {monthly_return_base:.2f} {base_asset}, Average Volume: {avg_base_volume:.2f} {base_asset}, Monthly Return %: {monthly_return_percentage:.2f}%")
    print(f"- Average volume of the base asset: {sell_avg_volume} {base_asset}")
    print(f"- Average selling opportunities per month: {sell_opps_per_month}")
    print(f"- Volume required to be in the 75th volume percentile: {sell_75th_volume} {base_asset}")

    return buying_opportunities, selling_opportunities, sorted_buy_occurrences, sorted_sell_occurrences

# Example usage:
file_path = 'kraken_pump_usd, 1s.csv'
liquid_exchange = "Binance"
start_datetime = pd.to_datetime('2022-12-10 00:00:00')
end_datetime = pd.to_datetime('2029-06-30 00:00:00')
buying_opportunities, selling_opportunities, sorted_buy_occurrences, sorted_sell_occurrences = analyze_opportunities_fixed_bins(file_path, liquid_exchange=liquid_exchange, start_datetime=start_datetime, end_datetime=end_datetime)





# Plot buying opportunities percentage differences
#plot_opportunities_fixed_bins(sorted_buy_occurrences, 'Buying Opportunities Percentage Differences', 'Difference (%)', 'Frequency')

# Plot selling opportunities percentage differences
#plot_opportunities_fixed_bins(sorted_sell_occurrences, 'Selling Opportunities Percentage Differences', 'Difference (%)', 'Frequency')

# Plot average volume of buying opportunities
#buy_avg_volume = buying_opportunities['Volume'].mean()
#plot_volume_distribution(buying_opportunities['Volume'], buy_avg_volume, 'Volume Distribution of Buying Opportunities')

# Plot average volume of selling opportunities
#sell_avg_volume = selling_opportunities['Volume'].mean()
#plot_volume_distribution(selling_opportunities['Volume'], sell_avg_volume, 'Volume Distribution of Selling Opportunities')

# Plot total return from buying opportunities
#plot_histogram_with_shaded_area(buying_opportunities, 'Low Difference (%)', 'Total Return from Buying Opportunities', 'Difference (%)', 'Total Return (%)', is_buying=True)

# Plot total return from selling opportunities
#plot_histogram_with_shaded_area(selling_opportunities, 'High Difference (%)', 'Total Return from Selling Opportunities', 'Difference (%)', 'Total Return (%)', is_buying=False)
