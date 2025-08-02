import ccxt

def supports_1s(exchange_id):
    try:
        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class({'enableRateLimit': True})
        exchange.load_markets()
        return '1s' in getattr(exchange, 'timeframes', {})
    except Exception as e:
        return False

def main():
    exchanges_with_1s = []

    for exchange_id in ccxt.exchanges:
        if supports_1s(exchange_id):
            exchanges_with_1s.append(exchange_id)

    print(f"✅ Exchanges that support 1s timeframe:")
    for ex in exchanges_with_1s:
        print(f"  - {ex}")

    print(f"\nTotal: {len(exchanges_with_1s)}")

if __name__ == "__main__":
    main()
