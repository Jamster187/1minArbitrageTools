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

#Kraken tao usd

run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 binance.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 bitfinex.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 bitget.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 bitstamp.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 bybit.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 coinbase.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 cryptocom.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 deribit.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 gate.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 gemini.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 kraken.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 kucoin.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 mexc.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 okx.py"

run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 binance_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 bitfinex_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 bitget_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 bitstamp_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 bybit_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 coinbase_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 cryptocom_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 deribit_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 gate_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 gemini_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 kraken_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 kucoin_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 mexc_1min.py"
run_command "cd ~/Desktop/Charts_Analysis/1minResearchBot; python3 okx_1min.py"






