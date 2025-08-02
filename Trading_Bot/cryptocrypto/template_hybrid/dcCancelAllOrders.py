import ccxt.pro as ccxtpro
from config import *
import asyncio

async def websocket_monitor():
    try:
        # Subscribe to Kraken WebSocket with cancelOnDisconnect flag
        print("Connecting to Kraken WebSocket with 'Cancel on Disconnect' enabled...")
        params = {
            'cancelOnDisconnect': True  # This ensures all orders are canceled on disconnection
        }
        
        # Subscribing to a ticker or orderbook
        await kraken_arbitrage1_websocket.watch_ticker('SOL/ETH', params)
        
        print("WebSocket connected and 'Cancel on Disconnect' is enabled...")
        
        # Keep the connection alive
        while True:
            await asyncio.sleep(1)  # Just keeping the script running

    except Exception as e:
        print(f"Error: {e}")

    finally:
        await kraken_arbitrage1_websocket.close()

async def main():
    await websocket_monitor()

# Run the program
if __name__ == '__main__':
    asyncio.run(main())
