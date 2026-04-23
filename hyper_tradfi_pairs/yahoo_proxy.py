from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from hyper_tradfi_pairs.config import PairDefinition


CHART_URL_TEMPLATE = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"


@dataclass(frozen=True)
class YahooQuoteSnapshot:
    captured_at_utc: str
    asset: str
    yahoo_symbol: str
    market_state: str | None
    regular_market_time_utc: str | None
    regular_market_price: float | None
    bid: float | None
    ask: float | None
    bid_size: float | None
    ask_size: float | None
    regular_market_volume: float | None
    exchange: str | None
    full_exchange_name: str | None
    currency: str | None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _from_unix_seconds(timestamp: int | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class YahooProxyClient:
    def __init__(self, timeout: int = 30) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/135.0.0.0 Safari/537.36"
                )
            }
        )

    def _resolve_symbol(self, pair: PairDefinition) -> str:
        if not pair.yahoo_fallback_symbol:
            raise ValueError(f"No Yahoo fallback symbol configured for {pair.asset}")
        return pair.yahoo_fallback_symbol

    def fetch_chart(
        self,
        pair: PairDefinition,
        interval: str = "1m",
        range_value: str = "1d",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pd.DataFrame:
        symbol = self._resolve_symbol(pair)
        params: dict[str, Any] = {
            "interval": interval,
            "includePrePost": "true",
            "events": "div,splits,capitalGains",
        }
        if start is not None and end is not None:
            params["period1"] = int(start.timestamp())
            params["period2"] = int(end.timestamp())
        else:
            params["range"] = range_value

        response = self.session.get(
            CHART_URL_TEMPLATE.format(symbol=symbol),
            params=params,
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        quote = result["indicators"]["quote"][0]

        frame = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(timestamps, unit="s", utc=True),
                "asset": pair.asset,
                "yahoo_symbol": symbol,
                "open": quote.get("open", []),
                "high": quote.get("high", []),
                "low": quote.get("low", []),
                "close": quote.get("close", []),
                "volume": quote.get("volume", []),
            }
        )
        frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
        return frame

    def fetch_quote_snapshot(self, pair: PairDefinition) -> YahooQuoteSnapshot:
        symbol = self._resolve_symbol(pair)
        response = self.session.get(
            QUOTE_URL,
            params={"symbols": symbol},
            timeout=self.timeout,
        )
        response.raise_for_status()
        results = response.json()["quoteResponse"]["result"]
        if not results:
            raise ValueError(f"No Yahoo quote result for {symbol}")

        quote = results[0]
        return YahooQuoteSnapshot(
            captured_at_utc=_utc_now_iso(),
            asset=pair.asset,
            yahoo_symbol=symbol,
            market_state=quote.get("marketState"),
            regular_market_time_utc=_from_unix_seconds(quote.get("regularMarketTime")),
            regular_market_price=_safe_float(quote.get("regularMarketPrice")),
            bid=_safe_float(quote.get("bid")),
            ask=_safe_float(quote.get("ask")),
            bid_size=_safe_float(quote.get("bidSize")),
            ask_size=_safe_float(quote.get("askSize")),
            regular_market_volume=_safe_float(quote.get("regularMarketVolume")),
            exchange=quote.get("exchange"),
            full_exchange_name=quote.get("fullExchangeName"),
            currency=quote.get("currency"),
        )

    def write_chart_csv(
        self,
        pair: PairDefinition,
        output_root: Path,
        interval: str = "1m",
        range_value: str = "1d",
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> Path:
        frame = self.fetch_chart(pair=pair, interval=interval, range_value=range_value, start=start, end=end)
        asset_dir = output_root / pair.asset
        asset_dir.mkdir(parents=True, exist_ok=True)

        if start is not None and end is not None:
            file_name = f"{start.date().isoformat()}__{end.date().isoformat()}_{interval}.csv"
        else:
            file_name = f"{range_value}_{interval}.csv"

        output_path = asset_dir / file_name
        frame.to_csv(output_path, index=False)
        return output_path

    def append_quote_snapshot(self, pair: PairDefinition, output_root: Path) -> Path:
        snapshot = self.fetch_quote_snapshot(pair)
        asset_dir = output_root / pair.asset
        asset_dir.mkdir(parents=True, exist_ok=True)
        output_path = asset_dir / "latest_quote_snapshot.csv"
        frame = pd.DataFrame([snapshot.__dict__])
        write_header = not output_path.exists()
        frame.to_csv(output_path, mode="a", header=write_header, index=False)
        return output_path

