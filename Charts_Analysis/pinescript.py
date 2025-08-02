//@version=5
indicator("Market Comparison", shorttitle="MC_binanceVStargetPERPS", overlay=true)

// Get the base asset of the current chart
base_asset = syminfo.basecurrency

// Construct the liquid market symbol dynamically
liquid_market_symbol = "BINANCE:" + base_asset + "USDC.P"

// Detect the current chart's timeframe
current_timeframe = timeframe.period

// Define the "liquid market" (dynamically constructed)
liquid_market_high = request.security(liquid_market_symbol, current_timeframe, high)
liquid_market_low = request.security(liquid_market_symbol, current_timeframe, low)

// Define the "opportunity market" (current chart)
opportunity_market_high = request.security(syminfo.tickerid, current_timeframe, high)
opportunity_market_low = request.security(syminfo.tickerid, current_timeframe, low)

// Calculate the percentage differences
low_diff_perc = ((opportunity_market_low - liquid_market_low) / liquid_market_low) * 100
high_diff_perc = ((opportunity_market_high - liquid_market_high) / liquid_market_high) * 100

// Format the percentage differences to two decimal points
formatted_low_diff = math.round(low_diff_perc * 100) / 100
formatted_high_diff = math.round(high_diff_perc * 100) / 100

// Plot the percentage differences with two decimal points of precision
plot(formatted_low_diff, title="Low Difference (%)", color=color.red, linewidth=2, precision=2)
plot(formatted_high_diff, title="High Difference (%)", color=color.green, linewidth=2, precision=2)
hline(0, "Zero Line", color=color.gray, linewidth=1, linestyle=hline.style_dotted)

