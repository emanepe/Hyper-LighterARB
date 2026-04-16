"""
stream_hyperliquid_candles_ws.py
--------------------------------
Streams real-time 1-minute candles for all Hyperliquid DEX tickers via WebSocket.
Covers xyz:, flx:, km:, and cash: prefixes.
A candle is written to CSV the moment it closes (i.e. when the next candle opens).

Output: data/hyperliquid_ws_live/hyperliquid_ws_<PREFIX>_<TICKER>.csv
Each CSV appends rows, so you can leave this running continuously.

Usage:
    pip install websockets
    python stream_hyperliquid_candles_ws.py
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets

# ── Configuration ──────────────────────────────────────────────────────────────

WS_URL = "wss://api.hyperliquid.xyz/ws"
INTERVAL = "1m"

# DEX symbols grouped by prefix
DEX_SYMBOLS: dict[str, list[str]] = {
    "xyz": [
        "AAPL", "AMD", "AMZN", "BRENTOIL", "COIN",
        "COPPER", "CRCL", "EUR", "GOLD", "GOOGL",
        "HOOD", "HYUNDAI", "INTC", "JPY", "META",
        "MSFT", "MSTR", "NATGAS", "NVDA", "PALLADIUM",
        "PLATINUM", "SILVER", "SMSN", "SKHX", "SNDK", "TSLA",
    ],
    "flx": [
        "COIN", "COPPER", "CRCL", "GOLD",
        "NVDA", "PALLADIUM", "PLATINUM", "SILVER", "TSLA",
    ],
    "km": [
        "AAPL", "GOLD", "GOOGL", "NVDA", "SILVER", "TSLA",
    ],
    "cash": [
        "AMZN", "GOLD", "GOOGL", "HOOD", "INTC",
        "META", "MSFT", "NVDA", "SILVER", "TSLA", "WTI",
    ],
}

# Flatten to full symbol list e.g. ["xyz:AAPL", "flx:COIN", ...]
SYMBOLS: list[str] = [
    f"{prefix}:{ticker}"
    for prefix, tickers in DEX_SYMBOLS.items()
    for ticker in tickers
]

# Reconnect settings
RECONNECT_DELAY   = 5   # seconds before reconnect attempt
MAX_RECONNECT_DELAY = 60  # cap backoff at 60 s
PING_INTERVAL     = 20  # websockets library keepalive ping (seconds)

# ── Output directory ────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parents[2] / "data" / "hyperliquid_ws_live"
BASE_DIR.mkdir(parents=True, exist_ok=True)

# ── Logging ─────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── CSV helpers ─────────────────────────────────────────────────────────────────

FIELDNAMES = ["timestamp", "symbol", "open", "high", "low", "close", "volume", "num_trades"]

# One CSV per prefix group
PREFIX_CSVS: dict[str, Path] = {
    prefix: BASE_DIR / f"hyperliquid_ws_{prefix}.csv"
    for prefix in DEX_SYMBOLS
}

def ensure_header(path: Path) -> None:
    """Write CSV header only if the file doesn't exist yet."""
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()

def append_candle(symbol: str, candle: dict) -> None:
    """Append a closed candle row to the correct prefix CSV."""
    prefix = symbol.split(":", 1)[0]
    path   = PREFIX_CSVS[prefix]
    ensure_header(path)
    dt = datetime.fromtimestamp(candle["t"] / 1000, tz=timezone.utc)
    row = {
        "timestamp":  dt.strftime("%Y-%m-%d %H:%M:%S"),
        "symbol":     symbol.split(":", 1)[1],
        "open":       candle["o"],
        "high":       candle["h"],
        "low":        candle["l"],
        "close":      candle["c"],
        "volume":     candle["v"],
        "num_trades": candle.get("n", ""),
    }
    with path.open("a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=FIELDNAMES).writerow(row)
    log.info("  ✓ %s  %s  O:%s H:%s L:%s C:%s  Vol:%s",
             symbol, row["timestamp"],
             row["open"], row["high"], row["low"], row["close"], row["volume"])

# ── WebSocket logic ─────────────────────────────────────────────────────────────

# last_candle[symbol] = most-recent partial candle dict from WebSocket
last_candle: dict[str, dict] = {}

def handle_candle_update(data: dict) -> None:
    """
    Called for every candle update message.
    'data' is the inner payload: {t, T, s, i, o, h, l, c, v, n}
    When the open-time (t) changes, the previous candle has closed → save it.
    """
    symbol = data.get("s", "")
    if not symbol:
        return
    new_t = data["t"]

    prev = last_candle.get(symbol)
    if prev is not None and prev["t"] != new_t:
        # Previous 1-minute candle has closed
        append_candle(symbol, prev)

    last_candle[symbol] = data

def build_subscriptions() -> list[dict]:
    return [
        {"method": "subscribe", "subscription": {"type": "candle", "coin": s, "interval": INTERVAL}}
        for s in SYMBOLS
    ]

async def stream(stop_event: asyncio.Event) -> None:
    """Main streaming loop with automatic reconnect + exponential backoff."""
    delay = RECONNECT_DELAY

    while not stop_event.is_set():
        try:
            log.info("Connecting to %s …", WS_URL)
            async with websockets.connect(
                WS_URL,
                ping_interval=PING_INTERVAL,
                ping_timeout=30,
                close_timeout=10,
            ) as ws:
                log.info("Connected. Subscribing to %d symbols …", len(SYMBOLS))
                for sub_msg in build_subscriptions():
                    await ws.send(json.dumps(sub_msg))

                delay = RECONNECT_DELAY  # reset backoff on successful connect

                log.info("Streaming 1m candles. Saving to: %s", BASE_DIR)

                async for raw in ws:
                    if stop_event.is_set():
                        break
                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    channel = msg.get("channel", "")

                    if channel == "candle":
                        handle_candle_update(msg["data"])

                    elif channel == "subscriptionResponse":
                        sub_type = msg.get("data", {}).get("type", "")
                        coin     = msg.get("data", {}).get("coin", "")
                        if sub_type == "candle":
                            log.debug("Subscribed: %s", coin)

                    # heartbeat / error passthrough
                    elif channel not in ("", "pong"):
                        log.debug("Unhandled channel: %s", channel)

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as exc:
            if stop_event.is_set():
                break
            log.warning("Disconnected (%s). Reconnecting in %ds …", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY)

        except Exception as exc:  # noqa: BLE001
            if stop_event.is_set():
                break
            log.error("Unexpected error: %s. Reconnecting in %ds …", exc, delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, MAX_RECONNECT_DELAY)

# ── Entry point ─────────────────────────────────────────────────────────────────

async def main() -> None:
    stop_event = asyncio.Event()

    # Graceful shutdown on Ctrl-C / SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    log.info("=== Hyperliquid WebSocket Candle Streamer ===")
    log.info("Symbols : %s", ", ".join(SYMBOLS))
    log.info("Interval: %s", INTERVAL)
    log.info("Output  : %s", BASE_DIR)
    log.info("Press Ctrl-C to stop.\n")

    await stream(stop_event)

    # On shutdown, flush the last partial candle for each symbol
    log.info("Flushing last partial candles …")
    for symbol, candle in last_candle.items():
        append_candle(symbol, candle)

    log.info("Done. Goodbye.")


if __name__ == "__main__":
    # Windows compatibility
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
