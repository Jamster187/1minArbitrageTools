import time
import ccxt
import sys

from config import KUCOIN_SPOT_CFG, KUCOIN_FUTURES_CFG, STRATEGY_CFG

# We'll store approximate fill prices for both spot & futures
spot_fill_price = None
fut_fill_price  = None

def init_spot_exchange():
    return ccxt.kucoin(KUCOIN_SPOT_CFG)

def init_futures_exchange():
    return ccxt.kucoinfutures(KUCOIN_FUTURES_CFG)

def fetch_spot_balance(spot_ex, coin):
    bal = spot_ex.fetch_balance()
    return float(bal['total'].get(coin, 0.0))

def fetch_futures_position(fut_ex, symbol):
    """
    Return negative => short, positive => long, 0 => none.
    We'll parse pos['contracts'] * pos['contractSize'] => coin amount.
    If side=short => negative. This ensures we see coin amounts, not contracts.
    """
    try:
        positions = fut_ex.fetch_positions([symbol])
        for pos in positions:
            if pos['symbol'] == symbol:
                contracts = float(pos.get('contracts', 0.0))
                c_size    = float(pos.get('contractSize', 1.0))
                side      = pos.get('side','').lower()
                coin_amt  = contracts * c_size
                return -coin_amt if side=='short' else coin_amt
        return 0.0
    except Exception as e:
        print(f"[ERROR] fetch_futures_position: {e}")
        return 0.0


def try_fetch_order_price(exchange, order_id, symbol, max_retries=3, retry_delay=2):
    """
    Attempt to fetch order details to get the fill price. If 'order not found' (ccxt.OrderNotFound)
    or other exceptions occur, retry up to max_retries. If still fails, fallback to ticker price.
    """
    for attempt in range(max_retries):
        try:
            fetched = exchange.fetch_order(order_id, symbol)
            if fetched and ('price' in fetched) and (fetched['price']):
                return float(fetched['price'])
            else:
                print(f"[WARN] No 'price' in fetched order => fallback after retries.")
                break

        except ccxt.OrderNotFound as e:
            print(f"[WARN] fetch_order => OrderNotFound (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("[WARN] Reached max retries => fallback to ticker.")
        except Exception as e:
            print(f"[WARN] fetch_order => {type(e).__name__} (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                print("[WARN] Reached max retries => fallback to ticker.")

    # Fallback: fetch ticker
    try:
        t = exchange.fetch_ticker(symbol)
        last_price = float(t['last'])
        print(f"[INFO] Fallback => ticker price = {last_price}")
        return last_price
    except Exception as e:
        print(f"[ERROR] fallback to ticker also failed => {e}")
        return None


def create_market_buy_spot(spot_ex, symbol, amount):
    global spot_fill_price
    print(f"[SPOT] BUY {amount} {symbol} (market)")
    order_id = None
    try:
        order = spot_ex.create_order(symbol, 'market', 'buy', amount)
        print(f"[SPOT] buy done => {order['id']}")
        order_id = order['id']
    except Exception as e:
        print(f"[ERROR] buy spot: {e}")
        return  # can't proceed

    fill_price = try_fetch_order_price(spot_ex, order_id, symbol, max_retries=3, retry_delay=2)
    if not fill_price:
        print("[SPOT] Could not determine fill price, ignoring for now.")
        return

    # If first fill or multiple increments => naive average
    if spot_fill_price is None:
        spot_fill_price = fill_price
    else:
        spot_fill_price = (spot_fill_price + fill_price) / 2
    print(f"[DEBUG] updated spot_fill_price => {spot_fill_price}")


def create_market_short_fut(fut_ex, symbol, coin_amount, lev, contract_size):
    """
    We want to short 'coin_amount' coins, but 1 contract = contract_size coins.
    => num_contracts = coin_amount / contract_size
    We'll place the order with `amount=num_contracts` and fetch fill price.
    """
    global fut_fill_price
    num_contracts = coin_amount / contract_size
    print(f"[FUT] SHORT {coin_amount} coins => {num_contracts} contracts @ {symbol}")

    order_id = None
    try:
        order = fut_ex.create_order(
            symbol=symbol,
            type='market',
            side='sell',
            amount=num_contracts,
            params={
                'leverage': str(lev)
            }
        )
        print(f"[FUT] short done => {order['id']}")
        order_id = order['id']
    except Exception as e:
        print(f"[ERROR] short fut: {e}")
        return

    fill_price = try_fetch_order_price(fut_ex, order_id, symbol, max_retries=3, retry_delay=2)
    if not fill_price:
        print("[FUT] Could not determine fill price, ignoring.")
        return

    if fut_fill_price is None:
        fut_fill_price = fill_price
    else:
        fut_fill_price = (fut_fill_price + fill_price)/2
    print(f"[DEBUG] updated fut_fill_price => {fut_fill_price}")


def open_position_incrementally(spot_ex, fut_ex, strategy):
    symbol_spot = strategy['symbol_spot']
    symbol_fut  = strategy['symbol_fut']
    base_coin   = strategy['base_coin']
    ds_spot     = strategy['desired_spot']   # e.g. +200
    ds_fut      = strategy['desired_fut']    # e.g. -200 => short 200 coins
    max_size    = strategy['max_trade_size']
    lev         = strategy['leverage']
    c_size      = strategy['contract_size']

    EPS=1e-3

    while True:
        act_spot = fetch_spot_balance(spot_ex, base_coin)
        act_fut  = fetch_futures_position(fut_ex, symbol_fut)

        spot_diff = ds_spot - act_spot
        fut_diff  = ds_fut  - act_fut

        print(f"[DEBUG] spot={act_spot}, fut={act_fut}, "
              f"spot_diff={spot_diff}, fut_diff={fut_diff}")

        if abs(spot_diff) < EPS and abs(fut_diff) < EPS:
            print("[OPEN] Position matched. Done.")
            break

        if spot_diff>EPS:
            step_spot = min(spot_diff, max_size)
            print(f"[OPEN] Buying {step_spot} on spot.")
            create_market_buy_spot(spot_ex, symbol_spot, step_spot)

        if fut_diff < -EPS:
            needed = abs(fut_diff)
            step_fut = min(needed, max_size)
            print(f"[OPEN] Shorting {step_fut} coins on futures.")
            create_market_short_fut(fut_ex, symbol_fut, step_fut, lev, c_size)

        time.sleep(2)


def fetch_fut_ticker_price(fut_ex, symbol):
    """
    Return last traded price from the futures ticker
    """
    try:
        t = fut_ex.fetch_ticker(symbol)
        return float(t['last'])
    except:
        return None


def close_position(spot_ex, fut_ex, strategy):
    global spot_fill_price, fut_fill_price
    spot_fill_price = None
    fut_fill_price  = None

    symbol_spot= strategy['symbol_spot']
    symbol_fut = strategy['symbol_fut']
    base_coin  = strategy['base_coin']
    lev        = strategy['leverage']
    c_size     = strategy['contract_size']

    spot_amt= fetch_spot_balance(spot_ex, base_coin)
    if spot_amt>1e-8:
        print(f"[CLOSE] SELL {spot_amt} on spot.")
        try:
            spot_ex.create_order(symbol_spot,'market','sell', spot_amt)
        except Exception as e:
            print(f"[ERROR] close spot: {e}")

    fut_amt= fetch_futures_position(fut_ex, symbol_fut)
    if fut_amt< -1e-8:
        amt_cover= abs(fut_amt)
        num_contracts= amt_cover / c_size
        print(f"[CLOSE] Cover short {amt_cover} coins => {num_contracts} contracts.")
        try:
            fut_ex.create_order(
                symbol=symbol_fut,
                type='market',
                side='buy',
                amount=num_contracts,
                params={'leverage':str(lev)}
            )
        except Exception as e:
            print(f"[ERROR] close fut: {e}")

def main():
    spot_ex= init_spot_exchange()
    fut_ex = init_futures_exchange()
    strategy= STRATEGY_CFG

    # 1) Open position
    open_position_incrementally(spot_ex, fut_ex, strategy)

    # 2) We'll close if the short side is down >1% from fill
    threshold = 0.001  # 1%
    global fut_fill_price

    if not fut_fill_price:
        # fallback to current ticker
        fut_fill_price = fetch_fut_ticker_price(fut_ex, strategy['symbol_fut'])
        print(f"[WARN] No fut_fill_price from orders => fallback => {fut_fill_price}")

    while True:
        current_price = fetch_fut_ticker_price(fut_ex, strategy['symbol_fut'])
        if not current_price:
            print("[INFO] can't fetch fut price, sleeping..")
            time.sleep(5)
            continue

        # if short is losing => price up
        if fut_fill_price and (current_price - fut_fill_price)/fut_fill_price >= threshold:
            print(f"[WARNING] Futures price up > {threshold*100}% => forced close.")
            close_position(spot_ex, fut_ex, strategy)
            sys.exit(0)

        # We also show the average fill prices for debugging
        print(f"[INFO] spot_fill={spot_fill_price}, fut_fill={fut_fill_price}, current_fut={current_price}")
        time.sleep(2)

if __name__=="__main__":
    main()
