#!/bin/bash

# Function to open a new Terminal window and run a command
run_command() {
    osascript -e "tell application \"Terminal\"" \
              -e "set newWindow to (do script \"$1\")" \
              -e "tell newWindow to set number of columns to 40" \
              -e "tell newWindow to set number of rows to 6" \
              -e "end tell"
}

# Change directory and run each script in a new Terminal window

#Kraken Sol ETH 
run_command "cd ~/Desktop/New_CCXT_Project/cryptocrypto/krakensoleth; python3 sell_entry_bot_kraken.py"
run_command "cd ~/Desktop/New_CCXT_Project/cryptocrypto/krakensoleth; python3 buy_close_bot_kraken.py"
run_command "cd ~/Desktop/New_CCXT_Project/cryptocrypto/krakensoleth; python3 PricePusher1.py"
run_command "cd ~/Desktop/New_CCXT_Project/cryptocrypto/krakensoleth; python3 PricePusher2.py"
run_command "cd ~/Desktop/New_CCXT_Project/cryptocrypto/krakensoleth; python3 orderbookPusher.py"
run_command "cd ~/Desktop/New_CCXT_Project/cryptocrypto/krakensoleth; python3 buyer_order_id_checker.py"
run_command "cd ~/Desktop/New_CCXT_Project/cryptocrypto/krakensoleth; python3 seller_order_id_checker.py"


