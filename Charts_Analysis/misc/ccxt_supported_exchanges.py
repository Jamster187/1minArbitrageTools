import ccxt

def list_supported_exchanges():
    exchanges = ccxt.exchanges
    print(f"Total supported exchanges: {len(exchanges)}\n")
    for exchange in exchanges:
        print(exchange)

if __name__ == "__main__":
    list_supported_exchanges()
