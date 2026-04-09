from __future__ import annotations

import csv
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

SYMBOLS = [
    "xyz:AAPL", "xyz:AMD", "xyz:AMZN", "xyz:BRENTOIL", "xyz:COIN",
    "xyz:COPPER", "xyz:CRCL", "xyz:EUR", "xyz:GOLD", "xyz:GOOGL",
    "xyz:HOOD", "xyz:HYUNDAI", "xyz:INTC", "xyz:JPY", "xyz:META",
    "xyz:MSFT", "xyz:MSTR", "xyz:NATGAS", "xyz:NVDA", "xyz:PALLADIUM",
    "xyz:PLATINUM", "xyz:SILVER", "xyz:SMSN", "xyz:SKHX", "xyz:SNDK",
    "xyz:TSLA",
]

API_URL = "https://api.hyperliquid.xyz/info"
INTERVAL = "1m"
DELAY = 1.5  # seconds between symbols
PAGE_DELAY = 1.5  # seconds between pagination requests


def fetch_candles_chunk(symbol: str, start_ms: int, end_ms: int) -> list:
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": INTERVAL,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }
    response = requests.post(API_URL, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_candles(symbol: str, start_ms: int, end_ms: int) -> list:
    all_candles = []
    current_start = start_ms

    while current_start < end_ms:
        chunk = fetch_candles_chunk(symbol, current_start, end_ms)
        if not chunk:
            break
        all_candles.extend(chunk)
        current_start = chunk[-1]["t"] + 1  # move window forward
        if len(chunk) < 500:
            break  # no more pages
        time.sleep(PAGE_DELAY)

    # deduplicate by timestamp and sort ascending
    seen = {}
    for candle in all_candles:
        seen[candle["t"]] = candle
    return sorted(seen.values(), key=lambda c: c["t"])


def parse_candles(raw: list) -> list[dict]:
    rows = []
    for candle in raw:
        dt = datetime.fromtimestamp(candle["t"] / 1000, tz=timezone.utc)
        rows.append({
            "timestamp": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open": candle["o"],
            "high": candle["h"],
            "low": candle["l"],
            "close": candle["c"],
            "volume": candle["v"],
        })
    return rows


def save_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    days = 30
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 24 * 60 * 60 * 1000

    output_dir = Path(__file__).resolve().parents[1] / "data" / f"hyperliquid_{days}d"
    output_dir.mkdir(parents=True, exist_ok=True)

    succeeded = 0
    total_rows = 0
    failed = []

    for symbol in SYMBOLS:
        ticker = symbol.split(":", 1)[1]
        print(f"Fetching {symbol} ...", end=" ", flush=True)
        try:
            raw = fetch_candles(symbol, start_ms, now_ms)
            rows = parse_candles(raw)
            filename = f"hyperliquid_{days}d_candles_{ticker}.csv"
            save_csv(output_dir / filename, rows)
            print(f"{len(rows)} rows -> {filename}")
            succeeded += 1
            total_rows += len(rows)
        except Exception as e:
            print(f"FAILED ({e})")
            failed.append(symbol)

        time.sleep(DELAY)

    print("\n--- Summary ---")
    print(f"Succeeded: {succeeded}/{len(SYMBOLS)}")
    print(f"Total rows collected: {total_rows:,}")
    if failed:
        print(f"Failed symbols: {', '.join(failed)}")


if __name__ == "__main__":
    main()
