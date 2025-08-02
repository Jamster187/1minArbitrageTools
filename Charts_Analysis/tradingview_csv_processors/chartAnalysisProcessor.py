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
    if df['time_diff'].empty:
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

def filter_best_monthly_return(occurrences):
    filtered = {}
    for pct_diff, data in occurrences.items():
        count = data[0]
        if count not in filtered or data[2] > filtered[count][1][2]:
            filtered[count] = (pct_diff, data)
    return [(pct_diff, data) for count, (pct_diff, data) in sorted(filtered.items(), reverse=True)]

def analyze_opportunities_fixed_bins(file_path, liquid_exchange="Binance", threshold=0.8, step=0.1, start_datetime=None, end_datetime=None):
    filename = file_path.split('/')[-1].split('.')[0]
    opportunity_exchange, base_asset, quote_asset, filename_timeframe = parse_filename(filename)

    try:
        df = pd.read_csv(file_path)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        
        if start_datetime and end_datetime:
            df = df[(df['time'] >= start_datetime) & (df['time'] <= end_datetime)]
        
        csv_timeframe = determine_timeframe(df)

        if 'low' not in df.columns:
            raise KeyError(f"The column 'low' does not exist in the CSV file: {file_path}")

        df[f'{quote_asset} Volume'] = df['Volume'] * df['low']
        
        # Exclude rows with NaN in 'Low Difference (%)' or 'High Difference (%)'
        valid_rows = df.dropna(subset=['Low Difference (%)', 'High Difference (%)'])
        total_days = (valid_rows['time'].max() - valid_rows['time'].min()).days + 1 if not valid_rows.empty else 0

        buying_opportunities = valid_rows[valid_rows['Low Difference (%)'] <= -threshold]
        selling_opportunities = valid_rows[valid_rows['High Difference (%)'] >= threshold]

        buy_count = buying_opportunities.shape[0]
        buy_months = total_days / 30 if total_days > 0 else 1
        buy_avg_quote_volume = buying_opportunities[f'{quote_asset} Volume'].mean()
        buy_avg_volume = buying_opportunities['Volume'].mean()

        buy_occurrences = {}
        sell_occurrences = {}

        if not buying_opportunities.empty and abs(buying_opportunities['Low Difference (%)'].min()) > threshold:
            for t in np.arange(threshold, abs(buying_opportunities['Low Difference (%)'].min()), step):
                count = buying_opportunities[buying_opportunities['Low Difference (%)'] <= -t].shape[0]
                if count > 0:
                    pct_diff = t
                    total_return = ((100 / (100 - pct_diff)) - 1) * 100 * count
                    monthly_return = total_return / buy_months
                    buy_occurrences[f"≥ {-pct_diff:.1f}%"] = (count, total_return, monthly_return)

        sorted_buy_occurrences = filter_best_monthly_return(buy_occurrences)
        buy_monthly_returns = [data[2] for _, data in sorted_buy_occurrences]
        median_buy_monthly_return = np.median(buy_monthly_returns) if buy_monthly_returns else 0

        sell_count = selling_opportunities.shape[0]
        sell_months = total_days / 30 if total_days > 0 else 1
        sell_avg_volume = selling_opportunities['Volume'].mean()

        if not selling_opportunities.empty and selling_opportunities['High Difference (%)'].max() > threshold:
            for t in np.arange(threshold, selling_opportunities['High Difference (%)'].max() + step, step):
                count = selling_opportunities[selling_opportunities['High Difference (%)'] >= t].shape[0]
                if count > 0:
                    pct_diff = t
                    total_return = pct_diff * count
                    monthly_return = total_return / sell_months
                    sell_occurrences[f"≥ {pct_diff:.1f}%"] = (count, total_return, monthly_return)

        sorted_sell_occurrences = filter_best_monthly_return(sell_occurrences)
        sell_monthly_returns = [data[2] for _, data in sorted_sell_occurrences]
        median_sell_monthly_return = np.median(sell_monthly_returns) if sell_monthly_returns else 0

        result = {
            "Exchange": opportunity_exchange,
            "Market Pair": f"{base_asset}/{quote_asset}",
            "Base Asset": base_asset,
            "Quote Asset": quote_asset,
            "Price": "",  # Column for price, left blank for user to fill
            "Average Volume (Quote)": round(buy_avg_quote_volume, 2) if not buying_opportunities.empty else 0,
            "Total Value of Asset": "",  # Placeholder, will be calculated in Excel
            "Median Buy Monthly Return (%)": round(median_buy_monthly_return, 2),
            "Notional Buy Monthly Return": "",  # Placeholder, will be calculated in Excel
            "Notional Buy Yearly Return": "",  # Placeholder, will be calculated in Excel
            "Average sell Volume (Base)": round(sell_avg_volume, 2) if not selling_opportunities.empty else 0,
            "Median Sell Monthly Return (%)": round(median_sell_monthly_return, 2),
        }
        
        return result

    except ValueError as e:
        if "0 is not in range" in str(e):
            print(f"Skipping file {file_path} due to ValueError: {e}")
            return None
        else:
            raise e  # Re-raise any other ValueErrors

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None

def save_results_to_excel(results, filename='arbitrage_analysis.xlsx'):
    df_results = pd.DataFrame(results)

    # Calculate the total sums for each base and quote asset
    base_asset_totals = df_results.groupby('Base Asset').agg({
        'Average sell Volume (Base)': 'sum',
        'Median Sell Monthly Return (%)': 'median'
    }).reset_index()
    base_asset_totals['Notional Profit / Month'] = base_asset_totals['Average sell Volume (Base)'] * base_asset_totals['Median Sell Monthly Return (%)'] / 100
    base_asset_totals['Notional Profit / Year'] = base_asset_totals['Notional Profit / Month'] * 12
    base_asset_totals = base_asset_totals.rename(columns={
        'Base Asset': 'Asset',
        'Average sell Volume (Base)': 'Total Volume',
        'Median Sell Monthly Return (%)': '% Profit / Month',
        'Notional Profit / Month': 'Notional Profit / Month',
        'Notional Profit / Year': 'Notional Profit / Year'
    })

    quote_asset_totals = df_results.groupby('Quote Asset').agg({
        'Average Volume (Quote)': 'sum',
        'Median Buy Monthly Return (%)': 'median'
    }).reset_index()
    quote_asset_totals['Notional Profit / Month'] = quote_asset_totals['Average Volume (Quote)'] * quote_asset_totals['Median Buy Monthly Return (%)'] / 100
    quote_asset_totals['Notional Profit / Year'] = quote_asset_totals['Notional Profit / Month'] * 12
    quote_asset_totals = quote_asset_totals.rename(columns={
        'Quote Asset': 'Asset',
        'Average Volume (Quote)': 'Total Volume',
        'Median Buy Monthly Return (%)': '% Profit / Month',
        'Notional Profit / Month': 'Notional Profit / Month',
        'Notional Profit / Year': 'Notional Profit / Year'
    })

    df_totals = pd.concat([base_asset_totals, quote_asset_totals])

    df_totals = df_totals.groupby('Asset').sum().reset_index()

    quote_assets = {
        'BTC': 'BTC',
        'ETH': 'ETH',
        'USD': 'USD',
        'EUR': 'EUR',
        'USDT': ['USDT', 'UST'],
        'GBP': 'GBP',
        'USDC': ['USDC', 'UDC'],
        'CHF': 'CHF',
        'CAD': 'CAD'
    }
    writer = pd.ExcelWriter(filename, engine='openpyxl')
    
    for tab_name, quote_asset in quote_assets.items():
        if isinstance(quote_asset, list):
            filtered_df = df_results[df_results['Quote Asset'].isin(quote_asset)].sort_values(by='Median Buy Monthly Return (%)', ascending=False)
        else:
            filtered_df = df_results[df_results['Quote Asset'] == quote_asset].sort_values(by='Median Buy Monthly Return (%)', ascending=False)
        if not filtered_df.empty:
            filtered_df.to_excel(writer, sheet_name=tab_name, index=False)
    
    df_results.to_excel(writer, sheet_name='Summary', index=False)
    df_totals.to_excel(writer, sheet_name='Totals', index=False)

    for sheetname in writer.sheets:
        worksheet = writer.sheets[sheetname]
        
        # Set width of all columns to triple the default size for better readability
        for col in worksheet.columns:
            max_length = max(len(str(cell.value)) for cell in col) * 1.01
            worksheet.column_dimensions[col[0].column_letter].width = max_length
    
        # Apply currency format to the specified columns
        for row in worksheet.iter_rows(min_row=2, min_col=1, max_col=worksheet.max_column):
            for cell in row:
                if cell.column_letter in ['F', 'G', 'H', 'I', 'J', 'K', 'L']:
                    cell.number_format = '#,##0.00'
                elif cell.column_letter == 'M':
                    cell.number_format = '$#,##0.00'
    writer.close()
    print(f"Results saved to {filename}")

def main(file_paths, liquid_exchange="Binance", start_datetime=None, end_datetime=None):
    all_results = []

    for file_path in file_paths:
        result = analyze_opportunities_fixed_bins(file_path, liquid_exchange=liquid_exchange, start_datetime=start_datetime, end_datetime=end_datetime)
        if result:
            all_results.append(result)
    
    save_results_to_excel(all_results)

if __name__ == "__main__":
    file_paths = [
      'KRAKEN_TAO_USD, 5.csv',
        'KRAKEN_SUI_USD, 5.csv',
        'KRAKEN_DOGE_USD, 5.csv',
        'KRAKEN_SOL_USD, 5.csv',
        'KRAKEN_BCH_USD, 5.csv',
        'KRAKEN_SOL_USDT, 5.csv',
        'KRAKEN_LDO_USD, 5.csv',
        'KRAKEN_PEPE_USD, 5.csv',
        'KRAKEN_TURBO_USD, 5.csv',
        'KRAKEN_STORJ_USD, 5.csv',
        'KRAKEN_JUP_USD, 5.csv',
        'KRAKEN_TIA_USD, 5.csv',
        'KRAKEN_DOT_USD, 5.csv',
        'KRAKEN_SHIB_USD, 5.csv',
        'KRAKEN_ARB_USD, 5.csv',
        'KRAKEN_SEI_USD, 5.csv',
        'KRAKEN_SGB_USD, 5.csv',
        'KRAKEN_SOL_EUR, 5.csv',
        'KRAKEN_ETH_USDT, 5.csv',
        'KRAKEN_USDC_USD, 5.csv',
        'KRAKEN_SOL_ETH, 5.csv',
        'KRAKEN_LINK_USD, 5.csv',
        'KRAKEN_BTC_USD, 5.csv',
        'KRAKEN_AVAX_USD, 5.csv',
        'KRAKEN_SOL_GBP, 5.csv',
        'KRAKEN_SOL_AUD, 5.csv',
        'KRAKEN_ICP_USD, 5.csv',
        'KRAKEN_INJ_USD, 5.csv',
        'KRAKEN_ADA_USD, 5.csv',
        'KRAKEN_ONDO_USD, 5.csv',
        'KRAKEN_TRX_USD, 5.csv',
        'KRAKEN_UNI_USD, 5.csv',
        'KRAKEN_PENDLE_USD, 5.csv',
        'KRAKEN_AAVE_USD, 5.csv',
        'KRAKEN_ETH_USD, 5.csv',
        'KRAKEN_OCEAN_USD, 5.csv',
        'KRAKEN_RUNE_USD, 5.csv',
        'KRAKEN_POL_USD, 5.csv',
        'KRAKEN_LTC_USD, 5.csv',
        'KRAKEN_GALA_USD, 5.csv',
        'KRAKEN_KSM_USD, 5.csv',
        'KRAKEN_NEAR_USD, 5.csv',
        'KRAKEN_SOL_BTC, 5.csv',
        'KRAKEN_BEAM_USD, 5.csv',
        'KRAKEN_CRV_USD, 5.csv',
        'KRAKEN_XRP_USD, 5.csv',
        'KRAKEN_FET_USD, 5.csv',
        'KRAKEN_OMG_USD, 5.csv',
        'KRAKEN_WIF_USD, 5.csv',
        'KRAKEN_SAGA_USD, 5.csv',
        'KRAKEN_STX_USD, 5.csv'
    ]
    start_datetime = pd.to_datetime('2023-01-01 00:00:00')
    end_datetime = pd.to_datetime('2024-09-17 23:59:59')
    main(file_paths, start_datetime=start_datetime, end_datetime=end_datetime)
