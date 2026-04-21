import asyncio
import json
import websockets
import pandas as pd
from datetime import datetime
from test import get_market_ids_from_excel

# Global list to store ALL received messages
collected_data = []


async def stream_market_stats(symbol, market_id):
    uri = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"
    subscribe_msg = {
        "type": "subscribe",
        "channel": f"market_stats/{market_id}"
    }

    try:
        async with websockets.connect(uri) as websocket:
            await websocket.send(json.dumps(subscribe_msg))
            print(f"Streaming {symbol} (ID: {market_id})...")

            async for response in websocket:
                data = json.loads(response)
                # Only save actual data messages (skip connection/heartbeat msgs)
                if "type" not in data or data["type"] != "connected":
                    data["symbol"] = symbol
                    data["local_timestamp"] = datetime.now().isoformat()
                    collected_data.append(data)
    except Exception as e:
        print(f"Connection lost for {symbol}: {e}")


async def background_saver(interval_seconds=300):
    """Saves accumulated data to a CSV/Table every 5 minutes (300s)."""
    while True:
        await asyncio.sleep(interval_seconds)
        if collected_data:
            # Convert the list of dictionaries to a DataFrame
            df = pd.DataFrame(collected_data)

            # Save as CSV (more table-like and easier to append to)
            df.to_csv("live_market_table.csv", index=False)

            # Optional: Save as JSON if you prefer
            # df.to_json("live_market_full.json", orient="records", indent=4)

            print(f"[{datetime.now().strftime('%H:%M:%S')}] Saved {len(df)} rows to live_market_table.csv")


async def main():
    my_list = ["AAPL", "AMZN", "BRENTOIL", "COIN", "XAU", "TSLA"]  # Shortened for example
    file_name = "Lighter_Full_Market_List.xlsx"
    market_map = get_market_ids_from_excel(file_name, my_list)

    # 1. Start the streams for all markets
    streaming_tasks = [stream_market_stats(sym, mid) for sym, mid in market_map.items()]

    # 2. Start the background saver
    saver_task = asyncio.create_task(background_saver(300))  # 300s = 5 mins

    # Run everything concurrently
    await asyncio.gather(*streaming_tasks, saver_task)


if __name__ == "__main__":
    asyncio.run(main())
