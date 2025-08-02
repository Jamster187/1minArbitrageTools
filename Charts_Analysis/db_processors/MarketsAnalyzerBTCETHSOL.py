import pandas as pd
import numpy as np
import xlsxwriter
from sqlalchemy import create_engine, inspect

START_DATE = pd.to_datetime("2025-07-01 00:00:00")
END_DATE = pd.to_datetime("2025-07-15 23:59:59")

FIAT_TO_USD = {
    'eur': ('kraken', 'kraken_eur_usd_1m'),
    'gbp': ('kraken', 'kraken_gbp_usd_1m'),
    'cad': ('kraken', 'kraken_usd_cad_1m')
}

INTERESTING_ASSETS = {"BTC", "ETH", "SOL"}

ALLOWED_QUOTES = {"BTC", "ETH", "SOL", "USD", "USDT", "USDC", "EUR", "CAD", "GBP"}

EXCHANGE_ENGINES = {
    'binance': create_engine("postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Binance"),
    'kraken': create_engine("postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Kraken"),
}

def parse_table_name(table_name):
    parts = table_name.split('_')
    if len(parts) == 4:
        exchange = parts[0]
        base_asset = parts[1]
        quote_asset = parts[2]
        timeframe = parts[3]
        return exchange.upper(), base_asset.upper(), quote_asset.upper(), timeframe
    raise ValueError(f"Table name format is incorrect: '{table_name}'")

def fetch_market_data(engine, table_name, is_liquid=False):
    print(f"Fetching data from: {table_name}")
    try:
        df = pd.read_sql_table(table_name, con=engine.connect())
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df[(df['timestamp'] >= START_DATE) & (df['timestamp'] <= END_DATE)]
        if is_liquid:
            df.rename(columns={
                'low': 'low_liquid',
                'high': 'high_liquid',
                'volume': 'volume_liquid'
            }, inplace=True)
        return df
    except Exception as e:
        print(f"Error fetching {table_name}: {e}")
        return pd.DataFrame()

def requires_synthetic(base, quote):
    return (
        (base.upper() in INTERESTING_ASSETS and quote.upper() in INTERESTING_ASSETS)
        or quote.lower() in FIAT_TO_USD
    )

def is_relevant_market(base, quote):
    return base.upper() in INTERESTING_ASSETS or quote.upper() in INTERESTING_ASSETS

def calculate_synthetic_price(base, quote):
    base_table = f"binance_{base.lower()}_usdt_1m"
    base_df = fetch_market_data(EXCHANGE_ENGINES['binance'], base_table)

    if quote.lower() in FIAT_TO_USD:
        fiat_exchange, quote_table = FIAT_TO_USD[quote.lower()]
        quote_df = fetch_market_data(EXCHANGE_ENGINES[fiat_exchange], quote_table)
    else:
        quote_table = f"binance_{quote.lower()}_usdt_1m"
        quote_df = fetch_market_data(EXCHANGE_ENGINES['binance'], quote_table)

    if base_df.empty or quote_df.empty:
        return pd.DataFrame()

    merged = pd.merge(base_df, quote_df, on='timestamp', suffixes=('_base', '_quote'))
    if quote.lower() == 'cad':
        merged['price_liquid'] = merged['close_base'] * merged['close_quote']
    else:
        merged['price_liquid'] = merged['close_base'] / merged['close_quote']

    merged['timestamp'] = pd.to_datetime(merged['timestamp'])
    merged['low_liquid'] = merged['price_liquid']
    merged['high_liquid'] = merged['price_liquid']
    merged['volume_liquid'] = (
        (merged['volume_base'] + merged['volume_quote']) / 2
        if 'volume_base' in merged.columns and 'volume_quote' in merged.columns
        else 0
    )
    return merged[['timestamp', 'low_liquid', 'high_liquid', 'volume_liquid']]


def calculate_differences(opp_df, liquid_df):
    merged = pd.merge(opp_df, liquid_df, on='timestamp')
    merged['Low Difference (%)'] = ((merged['low'] - merged['low_liquid']) / merged['low_liquid']) * 100
    merged['High Difference (%)'] = ((merged['high'] - merged['high_liquid']) / merged['high_liquid']) * 100
    return merged

def analyze_opportunity(opp_df, liquid_df, opportunity_exchange, liquid_exchange, base, quote):
    df = calculate_differences(opp_df, liquid_df)
    df = df[df['volume'] > 0]
    if df.empty:
        return None, None

    df['quote_volume'] = df['volume'] * df['low']
    days = max((df['timestamp'].max() - df['timestamp'].min()).days, 1)

    buy_df = df[df['Low Difference (%)'] <= -0.5]
    sell_df = df[df['High Difference (%)'] >= 0.5]

    def calculate_metrics(sub_df, label):
        avg_volume_base = sub_df['volume'].mean()
        median_volume_base = sub_df['volume'].median()
        avg_volume_quote = sub_df['quote_volume'].mean()
        median_volume_quote = sub_df['quote_volume'].median()

        total_return = abs(sub_df[label] / 100 * sub_df['quote_volume']).sum()
        monthly_return = (total_return / sub_df['quote_volume'].mean()) * (30 / days) * 100 if not sub_df['quote_volume'].mean() == 0 else 0

        return avg_volume_base, median_volume_base, avg_volume_quote, median_volume_quote, monthly_return

    buy_result = dict(
        opportunity_exchange=opportunity_exchange,
        liquid_exchange=liquid_exchange,
        market_pair=f"{base}/{quote}",
        avg_buy_volume_in_base=None, median_buy_volume_in_base=None,
        avg_buy_volume_in_quote=None, median_buy_volume_in_quote=None,
        monthly_buy_profit_percentage=None
    )
    if not buy_df.empty:
        ab, mb, aq, mq, m = calculate_metrics(buy_df, 'Low Difference (%)')
        buy_result.update(dict(
            avg_buy_volume_in_base=ab, median_buy_volume_in_base=mb,
            avg_buy_volume_in_quote=aq, median_buy_volume_in_quote=mq,
            monthly_buy_profit_percentage=m
        ))

    sell_result = dict(
        opportunity_exchange=opportunity_exchange,
        liquid_exchange=liquid_exchange,
        market_pair=f"{base}/{quote}",
        avg_sell_volume_in_base=None, median_sell_volume_in_base=None,
        avg_sell_volume_in_quote=None, median_sell_volume_in_quote=None,
        monthly_sell_profit_percentage=None
    )
    if not sell_df.empty:
        ab, mb, aq, mq, m = calculate_metrics(sell_df, 'High Difference (%)')
        sell_result.update(dict(
            avg_sell_volume_in_base=ab, median_sell_volume_in_base=mb,
            avg_sell_volume_in_quote=aq, median_sell_volume_in_quote=mq,
            monthly_sell_profit_percentage=m
        ))

    return buy_result, sell_result

def categorize_quote(quote):
    quote = quote.upper()
    if quote in {'USDT', 'USDC', 'USD'}:
        return 'USD'
    elif quote == 'BTC':
        return 'BTC'
    elif quote == 'ETH':
        return 'ETH'
    elif quote == 'SOL':
        return 'SOL'
    else:
        return 'OTHERS'

def save_results_to_excel(results, filename):
    categorized = {
        'USD Buy': [], 'BTC Buy': [], 'ETH Buy': [], 'SOL Buy': [], 'OTHERS Buy': [],
        'USD Sell': [], 'BTC Sell': [], 'ETH Sell': [], 'SOL Sell': [], 'OTHERS Sell': []
    }
    for buy_result, sell_result in results:
        quote_category = categorize_quote(buy_result['market_pair'].split('/')[-1])
        categorized[f"{quote_category} Buy"].append(buy_result)
        categorized[f"{quote_category} Sell"].append(sell_result)
    with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
        for sheet, data in categorized.items():
            if data:
                df = pd.DataFrame(data)
                sort_key = 'monthly_buy_profit_percentage' if 'Buy' in sheet else 'monthly_sell_profit_percentage'
                df = df.sort_values(by=sort_key, ascending=False)
                df.to_excel(writer, sheet_name=sheet[:31], index=False)

def compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe):
    results = []
    for opp_uri in opportunity_exchanges:
        opp_engine = create_engine(opp_uri)
        opp_tables = inspect(opp_engine).get_table_names()
        for table in opp_tables:
            if ':' in table:
                continue
            try:
                exchange, base, quote, tf = parse_table_name(table)
            except:
                continue
            if tf != timeframe:
                continue
            if base.upper() not in INTERESTING_ASSETS and quote.upper() not in INTERESTING_ASSETS:
                continue
            opp_df = fetch_market_data(opp_engine, table)
            if opp_df.empty:
                continue
            for liq_uri in liquid_exchanges:
                liq_engine = create_engine(liq_uri)
                if requires_synthetic(base, quote):
                    liq_df = calculate_synthetic_price(base, quote)
                else:
                    liq_table = f"{liq_uri.split('_')[-1].lower()}_{base.lower()}_{quote.lower()}_{timeframe}"
                    liq_df = fetch_market_data(liq_engine, liq_table, is_liquid=True)
                if liq_df.empty:
                    continue
                buy_result, sell_result = analyze_opportunity(opp_df, liq_df, exchange, liq_uri.split('_')[-1], base, quote)
                if buy_result and sell_result:
                    results.append((buy_result, sell_result))
    save_results_to_excel(results, "arbitrage_report.xlsx")

# Entry point
opportunity_exchanges = [
       "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Binance",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitfinex",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitstamp",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bybit",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Cryptocom",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Gate",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Kraken",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Kucoin",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_OKX",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitget",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Bitvavo",
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Probit"
]

liquid_exchanges = [
    "postgresql+psycopg2://postgres:@localhost:5432/Testing_Data_Collection_Binance"
]

timeframe = '1m'

compare_exchanges(opportunity_exchanges, liquid_exchanges, timeframe)
