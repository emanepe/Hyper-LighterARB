from __future__ import annotations

import csv
import json
import math
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests
from websocket import (
    WebSocket,
    WebSocketConnectionClosedException,
    WebSocketTimeoutException,
    create_connection,
)

from hyper_tradfi_pairs.config import PairDefinition


ORDER_BOOKS_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
STREAM_URL = "wss://mainnet.zklighter.elliot.ai/stream?readonly=true"


@dataclass(frozen=True)
class LighterTopOfBookSnapshot:
    captured_at_utc: str
    exchange_time: str
    exchange_time_ms: int
    asset: str
    symbol: str
    market_id: int
    book_nonce: int
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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_now_ms() -> int:
    return int(time.time() * 1000)


def _iso_from_us(timestamp_us: int) -> str:
    dt = datetime.fromtimestamp(timestamp_us / 1_000_000, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _sum_levels(levels: list[tuple[float, float]], depth: int) -> tuple[float, float]:
    total_sz = 0.0
    total_notional = 0.0
    for price, size in levels[:depth]:
        total_sz += size
        total_notional += price * size
    return total_sz, total_notional


class LighterBookState:
    def __init__(self, pair: PairDefinition, symbol: str, market_id: int) -> None:
        self.pair = pair
        self.symbol = symbol
        self.market_id = market_id
        self.book_nonce = 0
        self.last_updated_at_us = 0
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.initialized = False

    def replace_from_snapshot(self, book: dict) -> None:
        self.bids = self._parse_side(book.get("bids", []))
        self.asks = self._parse_side(book.get("asks", []))
        self.book_nonce = int(book.get("nonce", 0))
        self.last_updated_at_us = int(book.get("last_updated_at", 0))
        self.initialized = True

    def apply_delta(self, book: dict) -> None:
        self._merge_side(self.bids, book.get("bids", []))
        self._merge_side(self.asks, book.get("asks", []))
        self.book_nonce = int(book.get("nonce", self.book_nonce))
        self.last_updated_at_us = int(book.get("last_updated_at", self.last_updated_at_us))
        self.initialized = True

    def to_snapshot(self, captured_at_utc: str | None = None) -> LighterTopOfBookSnapshot | None:
        bid_levels = self.bid_levels
        ask_levels = self.ask_levels
        if not bid_levels or not ask_levels:
            return None

        best_bid_px, best_bid_sz = bid_levels[0]
        best_ask_px, best_ask_sz = ask_levels[0]
        mid_px = (best_bid_px + best_ask_px) / 2.0
        spread_px = best_ask_px - best_bid_px
        spread_bps = 0.0 if math.isclose(mid_px, 0.0) else 10_000 * spread_px / mid_px

        bid_depth_5_sz, bid_depth_5_notional = _sum_levels(bid_levels, 5)
        ask_depth_5_sz, ask_depth_5_notional = _sum_levels(ask_levels, 5)
        bid_depth_20_sz, bid_depth_20_notional = _sum_levels(bid_levels, 20)
        ask_depth_20_sz, ask_depth_20_notional = _sum_levels(ask_levels, 20)

        captured_ms = _utc_now_ms()
        exchange_time_ms = self.last_updated_at_us // 1000 if self.last_updated_at_us else captured_ms
        exchange_time = _iso_from_us(self.last_updated_at_us) if self.last_updated_at_us else _utc_now_iso()
        return LighterTopOfBookSnapshot(
            captured_at_utc=captured_at_utc or _utc_now_iso(),
            exchange_time=exchange_time,
            exchange_time_ms=exchange_time_ms,
            asset=self.pair.asset,
            symbol=self.symbol,
            market_id=self.market_id,
            book_nonce=self.book_nonce,
            best_bid_px=best_bid_px,
            best_bid_sz=best_bid_sz,
            best_bid_orders=0,
            best_ask_px=best_ask_px,
            best_ask_sz=best_ask_sz,
            best_ask_orders=0,
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

    @property
    def bid_levels(self) -> list[tuple[float, float]]:
        return sorted(self.bids.items(), key=lambda item: item[0], reverse=True)

    @property
    def ask_levels(self) -> list[tuple[float, float]]:
        return sorted(self.asks.items(), key=lambda item: item[0])

    @staticmethod
    def _parse_side(levels: list[dict[str, str]]) -> dict[float, float]:
        out: dict[float, float] = {}
        for level in levels:
            price = float(level["price"])
            size = float(level["size"])
            if size > 0.0:
                out[price] = size
        return out

    @staticmethod
    def _merge_side(book_side: dict[float, float], levels: list[dict[str, str]]) -> None:
        for level in levels:
            price = float(level["price"])
            size = float(level["size"])
            if size <= 0.0:
                book_side.pop(price, None)
            else:
                book_side[price] = size


class LighterClient:
    def __init__(self, timeout: int = 20, websocket_timeout: float = 5.0) -> None:
        self.timeout = timeout
        self.websocket_timeout = websocket_timeout
        self.session = requests.Session()

    def get_order_books(self) -> list[dict]:
        response = self.session.get(ORDER_BOOKS_URL, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        order_books = payload.get("order_books", [])
        if not isinstance(order_books, list):
            raise TypeError(f"Unexpected order_books payload type: {type(order_books)!r}")
        return order_books

    def resolve_market_states(self, pairs: list[PairDefinition]) -> dict[str, LighterBookState]:
        active_books = {
            book["symbol"]: book
            for book in self.get_order_books()
            if book.get("status") == "active" and book.get("market_type") == "perp"
        }

        states: dict[str, LighterBookState] = {}
        for pair in pairs:
            if not pair.lighter_symbol:
                raise ValueError(f"No Lighter symbol configured for {pair.asset}")
            try:
                book = active_books[pair.lighter_symbol]
            except KeyError as exc:
                raise ValueError(f"Lighter symbol {pair.lighter_symbol!r} not found in active order books") from exc
            states[f"order_book:{book['market_id']}"] = LighterBookState(
                pair=pair,
                symbol=pair.lighter_symbol,
                market_id=int(book["market_id"]),
            )
        return states

    def connect_order_books(self, states: dict[str, LighterBookState]) -> WebSocket:
        ws = create_connection(STREAM_URL, timeout=self.timeout)
        ws.settimeout(self.websocket_timeout)
        connected = json.loads(ws.recv())
        if connected.get("type") != "connected":
            raise ValueError(f"Unexpected first websocket message: {connected}")

        for state in states.values():
            ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{state.market_id}"}))

        deadline = time.monotonic() + max(10.0, len(states) * 3.0)
        while time.monotonic() < deadline:
            try:
                message = json.loads(ws.recv())
            except WebSocketTimeoutException:
                continue
            self.apply_message(message=message, states=states)
            if all(state.initialized for state in states.values()):
                return ws

        missing = [state.symbol for state in states.values() if not state.initialized]
        ws.close()
        raise TimeoutError(f"Timed out waiting for initial Lighter order-book snapshots: {', '.join(missing)}")

    @staticmethod
    def apply_message(message: dict, states: dict[str, LighterBookState]) -> None:
        channel = message.get("channel")
        if not channel or channel not in states:
            return
        payload = message.get("order_book")
        if not isinstance(payload, dict):
            return

        state = states[channel]
        message_type = str(message.get("type", ""))
        if message_type == "subscribed/order_book":
            state.replace_from_snapshot(payload)
        elif message_type == "update/order_book":
            state.apply_delta(payload)


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
    client = LighterClient()
    states = client.resolve_market_states(pairs)
    output_root = output_dir.resolve()
    fieldnames = list(LighterTopOfBookSnapshot.__dataclass_fields__)
    writers: dict[tuple[str, str], tuple[object, csv.DictWriter]] = {}
    start_monotonic = time.monotonic()
    next_tick = math.ceil(time.time() / interval_seconds) * interval_seconds
    next_ping = time.monotonic() + 30.0
    ws: WebSocket | None = None

    try:
        ws = client.connect_order_books(states)
        while True:
            try:
                if ws is None:
                    ws = client.connect_order_books(states)
                wait_seconds = max(0.05, min(0.25, max(0.0, next_tick - time.time())))
                ws.settimeout(wait_seconds)
                message = json.loads(ws.recv())
                client.apply_message(message=message, states=states)
            except WebSocketTimeoutException:
                pass
            except (OSError, WebSocketConnectionClosedException, TimeoutError):
                if ws is not None:
                    try:
                        ws.close()
                    except OSError:
                        pass
                ws = client.connect_order_books(states)
                next_ping = time.monotonic() + 30.0
                continue

            if ws is not None and time.monotonic() >= next_ping:
                try:
                    ws.ping()
                except (OSError, WebSocketConnectionClosedException):
                    try:
                        ws.close()
                    except OSError:
                        pass
                    ws = None
                    continue
                next_ping = time.monotonic() + 30.0

            now = time.time()
            while now >= next_tick:
                capture_iso = _utc_now_iso()
                day_string = capture_iso[:10]
                for state in states.values():
                    snapshot = state.to_snapshot(captured_at_utc=capture_iso)
                    if snapshot is None:
                        continue
                    writer = _ensure_writer(
                        writers=writers,
                        output_root=output_root,
                        asset=state.pair.asset,
                        day_string=day_string,
                        fieldnames=fieldnames,
                    )
                    writer.writerow(asdict(snapshot))
                    writers[(state.pair.asset, day_string)][0].flush()

                next_tick += interval_seconds
                if duration_seconds is not None and time.monotonic() - start_monotonic >= duration_seconds:
                    return
                now = time.time()
    finally:
        if ws is not None:
            try:
                ws.close()
            except OSError:
                pass
        for handle, _writer in writers.values():
            handle.close()
