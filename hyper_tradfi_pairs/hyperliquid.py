from __future__ import annotations

import csv
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

from hyper_tradfi_pairs.config import PairDefinition


INFO_URL = "https://api.hyperliquid.xyz/info"


@dataclass(frozen=True)
class TopOfBookSnapshot:
    captured_at_utc: str
    exchange_time: str
    exchange_time_ms: int
    asset: str
    coin: str
    best_bid_px: float
    best_bid_sz: float
    best_bid_orders: int
    best_ask_px: float
    best_ask_sz: float
    best_ask_orders: int
    mid_px: float
    spread_px: float
    spread_bps: float
    bid_depth_5_sz: float
    bid_depth_5_notional: float
    ask_depth_5_sz: float
    ask_depth_5_notional: float
    bid_depth_20_sz: float
    bid_depth_20_notional: float
    ask_depth_20_sz: float
    ask_depth_20_notional: float


def _iso_from_ms(timestamp_ms: int) -> str:
    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sum_book_levels(levels: list[dict[str, str]], depth: int) -> tuple[float, float]:
    total_sz = 0.0
    total_notional = 0.0
    for level in levels[:depth]:
        price = float(level["px"])
        size = float(level["sz"])
        total_sz += size
        total_notional += price * size
    return total_sz, total_notional


class HyperliquidClient:
    def __init__(self, timeout: int = 20) -> None:
        self.timeout = timeout
        self.session = requests.Session()

    def post(self, payload: dict) -> dict | list:
        response = self.session.post(INFO_URL, json=payload, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def get_all_mids(self, dex: str | None = None) -> dict[str, str]:
        payload = {"type": "allMids"}
        if dex:
            payload["dex"] = dex
        data = self.post(payload)
        if not isinstance(data, dict):
            raise TypeError(f"Unexpected allMids payload type: {type(data)!r}")
        return data

    def get_l2_book(self, coin: str) -> dict:
        data = self.post({"type": "l2Book", "coin": coin})
        if not isinstance(data, dict):
            raise TypeError(f"Unexpected l2Book payload type: {type(data)!r}")
        return data

    def get_candle_snapshot(
        self,
        coin: str,
        interval: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[dict]:
        data = self.post(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": interval,
                    "startTime": start_time_ms,
                    "endTime": end_time_ms,
                },
            }
        )
        if not isinstance(data, list):
            raise TypeError(f"Unexpected candleSnapshot payload type: {type(data)!r}")
        return data

    def get_top_of_book_snapshot(
        self,
        pair: PairDefinition,
        captured_at_utc: str | None = None,
    ) -> TopOfBookSnapshot:
        book = self.get_l2_book(pair.hyperliquid_coin)
        bids, asks = book["levels"]
        if not bids or not asks:
            raise ValueError(f"Empty top of book for {pair.hyperliquid_coin}")

        best_bid = bids[0]
        best_ask = asks[0]
        best_bid_px = float(best_bid["px"])
        best_ask_px = float(best_ask["px"])
        mid_px = (best_bid_px + best_ask_px) / 2.0
        spread_px = best_ask_px - best_bid_px
        spread_bps = 0.0 if math.isclose(mid_px, 0.0) else 10_000 * spread_px / mid_px

        bid_depth_5_sz, bid_depth_5_notional = _sum_book_levels(bids, 5)
        ask_depth_5_sz, ask_depth_5_notional = _sum_book_levels(asks, 5)
        bid_depth_20_sz, bid_depth_20_notional = _sum_book_levels(bids, 20)
        ask_depth_20_sz, ask_depth_20_notional = _sum_book_levels(asks, 20)

        exchange_time_ms = int(book["time"])
        return TopOfBookSnapshot(
            captured_at_utc=captured_at_utc or _utc_now_iso(),
            exchange_time=_iso_from_ms(exchange_time_ms),
            exchange_time_ms=exchange_time_ms,
            asset=pair.asset,
            coin=pair.hyperliquid_coin,
            best_bid_px=best_bid_px,
            best_bid_sz=float(best_bid["sz"]),
            best_bid_orders=int(best_bid["n"]),
            best_ask_px=best_ask_px,
            best_ask_sz=float(best_ask["sz"]),
            best_ask_orders=int(best_ask["n"]),
            mid_px=mid_px,
            spread_px=spread_px,
            spread_bps=spread_bps,
            bid_depth_5_sz=bid_depth_5_sz,
            bid_depth_5_notional=bid_depth_5_notional,
            ask_depth_5_sz=ask_depth_5_sz,
            ask_depth_5_notional=ask_depth_5_notional,
            bid_depth_20_sz=bid_depth_20_sz,
            bid_depth_20_notional=bid_depth_20_notional,
            ask_depth_20_sz=ask_depth_20_sz,
            ask_depth_20_notional=ask_depth_20_notional,
        )


def _ensure_writer(
    writers: dict[tuple[str, str], tuple[object, csv.DictWriter]],
    output_root: Path,
    asset: str,
    day_string: str,
    fieldnames: list[str],
) -> csv.DictWriter:
    key = (asset, day_string)
    if key in writers:
        return writers[key][1]

    asset_dir = output_root / asset
    asset_dir.mkdir(parents=True, exist_ok=True)
    path = asset_dir / f"{day_string}.csv"
    handle = path.open("a", newline="", encoding="utf-8")
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    if path.stat().st_size == 0:
        writer.writeheader()
    writers[key] = (handle, writer)
    return writer


def collect_top_of_book(
    pairs: list[PairDefinition],
    output_dir: Path,
    interval_seconds: float = 1.0,
    duration_seconds: float | None = None,
) -> None:
    client = HyperliquidClient()
    output_root = output_dir.resolve()
    fieldnames = list(TopOfBookSnapshot.__dataclass_fields__)
    writers: dict[tuple[str, str], tuple[object, csv.DictWriter]] = {}
    start_monotonic = time.monotonic()
    next_tick = math.ceil(time.time() / interval_seconds) * interval_seconds

    try:
        while True:
            sleep_seconds = max(0.0, next_tick - time.time())
            if sleep_seconds:
                time.sleep(sleep_seconds)

            capture_iso = _utc_now_iso()
            day_string = capture_iso[:10]
            for pair in pairs:
                snapshot = client.get_top_of_book_snapshot(pair, captured_at_utc=capture_iso)
                writer = _ensure_writer(
                    writers=writers,
                    output_root=output_root,
                    asset=pair.asset,
                    day_string=day_string,
                    fieldnames=fieldnames,
                )
                writer.writerow(asdict(snapshot))
                writers[(pair.asset, day_string)][0].flush()

            next_tick += interval_seconds
            if duration_seconds is not None and time.monotonic() - start_monotonic >= duration_seconds:
                return
    finally:
        for handle, _writer in writers.values():
            handle.close()

