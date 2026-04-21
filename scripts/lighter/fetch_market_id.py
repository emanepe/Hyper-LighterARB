import requests
import pandas as pd
import openpyxl

# 1. Fetch data from the API
url = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
headers = {"accept": "application/json"}

try:
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # 2. Convert the 'order_books' list into a DataFrame
    # This automatically handles multiple entries (USELESS, ETH, BTC, etc.)
    df = pd.DataFrame(data['order_books'])

    # 3. Reorder columns for better readability (Optional)
    cols = ['symbol', 'market_id', 'market_type', 'status', 'taker_fee', 'maker_fee']
    # Add the remaining columns back
    cols += [c for c in df.columns if c not in cols]
    df = df[cols]

    # 4. Save to Excel
    df.to_excel("Lighter_Full_Market_List.xlsx", index=False)

    print(f"Successfully processed {len(df)} markets.")
    print(df[['symbol', 'market_id', 'market_type', 'status']].head())

except Exception as e:
    print(f"An error occurred: {e}")