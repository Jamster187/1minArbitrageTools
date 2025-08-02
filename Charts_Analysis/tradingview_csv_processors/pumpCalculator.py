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

def calculate_percentage_changes(df):
    # Calculate percentage changes based on open to low and open to high
    conditions = [
        (df['open'] > df['close']),
        (df['open'] <= df['close'])
    ]
    choices = [
        (df['high'] - df['open']) / df['open'] * 100,
        (df['high'] - df['open']) / df['open'] * 100
    ]
    df['percentage_change'] = np.select(conditions, choices)
    return df

def filter_market_diffs(df, max_diff_pct):
    df = df[(df['Low Difference (%)'] >= -max_diff_pct) & (df['High Difference (%)'] <= max_diff_pct)]
    return df

def count_occurrences(df, threshold):
    return df[df['percentage_change'] >= threshold]

def bin_results(occurrences, bin_width, threshold):
    bins = np.arange(threshold, occurrences['percentage_change'].max() + bin_width, bin_width)
    binned_data = {f"≥ {bin:.2f}%": occurrences[occurrences['percentage_change'] >= bin].shape[0] for bin in bins}
    
    sorted_bins_counts = sorted(binned_data.items(), key=lambda x: x[1], reverse=True)
    result_bins, result_counts = zip(*sorted_bins_counts)
    
    return result_bins, result_counts

def main(file_path, start_datetime, end_datetime, threshold, bin_width, max_diff_pct):
    opportunity_exchange, base_asset, quote_asset, filename_timeframe = parse_filename(file_path)
    
    data = pd.read_csv(file_path)
    data['time'] = pd.to_datetime(data['time'], unit='s')
    data = data[(data['time'] >= start_datetime) & (data['time'] <= end_datetime)]
    
    data = calculate_percentage_changes(data)
    data = filter_market_diffs(data, max_diff_pct)
    
    occurrences = count_occurrences(data, threshold)
    bins, binned_data = bin_results(occurrences, bin_width, threshold)
    
    print(f"Market Pair: {base_asset}/{quote_asset}")
    print(f"Opportunity Exchange: {opportunity_exchange}")
    print(f"Liquid Exchange: {'Binance'}")
    print(f"Timeframe from CSV: {filename_timeframe}")
    print(f"Total Days Analyzed: {(data['time'].max() - data['time'].min()).days + 1}")
    print()
    print("Occurrences of Percentage Changes:")
    for bin_label, count in zip(bins, binned_data):
        if count > 0:
            print(f"{bin_label}: {count} occurrences")

if __name__ == "__main__":
    file_path = 'BINANCE_virtual_USDT, 1S.csv'
    start_datetime = pd.to_datetime('2024-01-01 00:00:00')
    end_datetime = pd.to_datetime('2026-12-30 23:59:59')
    threshold = 0.0  # User-defined threshold for percentage change
    bin_width = 0.01  # User-defined bin width for grouping occurrences
    max_diff_pct = 1  # User-defined maximum market difference percentage

    main(file_path, start_datetime, end_datetime, threshold, bin_width, max_diff_pct)
