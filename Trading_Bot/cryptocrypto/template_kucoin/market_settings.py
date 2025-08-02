# most bots

target_exchange_name_string_for_db ='kucoin' #lowercase exchange name
liquid_quote_asset='BTC' # THIS IS THE LIQUID QUOTE ASSET SYMBOL THAT WE WILL GRAB PRICE DATA WITH
base_asset='ETH' # this is the base asset symbol e.g 'SOL', 'BTC'
target_quote_asset='BTC' # this is the target quote asset symbol, i.e on kraken it is USD or EUR often
min_order_value = 0.0002 # in target_quote_asset amount, this is the minimum order value we need for us to place an order
min_spot_price_change = 0.00001  # this is the tick of the target market, lowest price change possible
stale_price_timeout_counter = 10 # this is our tolerance towards on how old the price data we are getting in seconds
stale_orderbook_timeout_counter = 10 # this is our tolerance towards on how old the orderbook data we are getting in seconds
max_price_volatility = 0.25 # this is our tolerance towards how much of our price target can be explained by volatility. if price is 100 and we aim for 105 and the volatility explains 25% of 5, we stop
atr_target_symbol = f"{base_asset}/{liquid_quote_asset}"
max_allowed_competition_sell_volume = 0
max_allowed_competition_buy_volume = 0

# sell_entry_bot

min_sell_premium_list = [0.0001, 0.0075, 0.008, 0.009, 0.19] # this is how much we sell for a premium - negative value means we are paying the market some % to get us out quickly. 0.01 = 1%
max_base_asset_to_use = 0.05 # this the max amount of the base asset we can ever have allocated at once to a particular market. this will take into account the already spent assets so that we do not exceed this budget
sell_entry_sleep_time = 2.5 # time the bot pauses for before repeating loop

# buy_close_bot

buy_closing_discount = -0.0001 #this is how much of a discount we are asking for from the market - negative value means we are paying the market some % to get us out quickly. 0.01 = 1%
buy_close_sleep_time = 2.5 # time the bot pauses for before repeating loop

# buy_entry_bot

min_profitable_discount_list = [0.0001] #this is how much of a discount we are asking for from the market. 0.01 = 1%
max_target_quote_asset_to_use = 0.001 # this the max amount of the target quote asset we can ever have allocated at once to a particular market. this will take into account the already spent assets so that we do not exceed this budget
buy_entry_sleep_time = 2.5 # time the bot pauses for before repeating loop

# sell_close_bot

sell_closing_discount = -0.0001 # this is how much we sell for a premium - negative value means we are paying the market some % to get us out quickly. 0.01 = 1%
sell_close_sleep_time = 2.5 # time the bot pauses for before repeating loop

# Price 1
price_pusher_1_base_asset='BTC'
price_pusher_1_sleep_time=0
price_pusher_1_liquid_quote_asset='USDT'

# Price 2
price_pusher_2_base_asset='ETH'
price_pusher_2_liquid_quote_asset='USDT'
price_pusher_2_sleep_time=0