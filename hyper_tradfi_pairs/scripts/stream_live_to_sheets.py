"""
stream_live_to_sheets.py
------------------------
Combines the Hyperliquid + Lighter live collectors with a direct Google Sheets
uploader in one tight loop. Each second it:

  1. Fetches the Hyperliquid L2 book for every configured asset (REST).
  2. Reads the latest Lighter order-book state from the open WebSocket stream.
  3. Builds compact rows (same 17-column schema as the CSV syncer).
  4. POSTs all rows to the Apps Script Web App (one HTTP call per venue/asset).

No CSV files are written.

Usage:
    export GOOGLE_SHEETS_WEB_APP_URL="https://script.google.com/macros/s/.../exec"
    export GOOGLE_SHEETS_INGEST_SECRET="your-secret"

    # Stream both venues for 1 hour:
    python -m hyper_tradfi_pairs.scripts.stream_live_to_sheets --assets all

    # Preview only (no network calls to Google):
    python -m hyper_tradfi_pairs.scripts.stream_live_to_sheets --assets all --dry-run --duration-seconds 5
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import requests
from websocket import WebSocketConnectionClosedException, WebSocketTimeoutException

from hyper_tradfi_pairs.config import PairDefinition, parse_assets
from hyper_tradfi_pairs.google_sheets_sync import (
    BOOK_BY_VENUE_ASSET,
    COMPACT_HEADERS,
    project_compact_row,
)
from hyper_tradfi_pairs.hyperliquid import HyperliquidClient
from hyper_tradfi_pairs.lighter import LighterBookState, LighterClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S")


def _post_rows(
    web_app_url: str,
    secret: str,
    book: str,
    rows: list[list[Any]],
    max_rows: int,
    dry_run: bool,
) -> None:
    if dry_run:
        print(f"  [dry-run] {book}: {len(rows)} row(s)")
        for row in rows:
            print(f"    {dict(zip(COMPACT_HEADERS, row))}")
        return

    payload = {
        "secret": secret,
        "book": book,
        "headers": COMPACT_HEADERS,
        "rows": rows,
        "max_rows": max_rows,
    }
    response = requests.post(web_app_url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(f"Google Sheets ingest failed for {book}: {data}")


# ---------------------------------------------------------------------------
# Per-venue snapshot builders
# ---------------------------------------------------------------------------


def _hyper_row(pair: PairDefinition, client: HyperliquidClient, capture_iso: str) -> list[Any] | None:
    try:
        snap = client.get_top_of_book_snapshot(pair, captured_at_utc=capture_iso)
        raw = asdict(snap)
        # The 'coin' field maps to 'market' in compact schema
        raw.setdefault("coin", raw.get("coin", ""))
        return project_compact_row(row=raw, venue="hyperliquid", asset=pair.asset)
    except Exception as exc:
        print(f"  [WARN] Hyperliquid/{pair.asset}: {exc}", file=sys.stderr)
        return None


def _lighter_row(pair: PairDefinition, state: LighterBookState, capture_iso: str) -> list[Any] | None:
    snap = state.to_snapshot(captured_at_utc=capture_iso)
    if snap is None:
        return None
    raw = asdict(snap)
    return project_compact_row(row=raw, venue="lighter", asset=pair.asset)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def stream(
    pairs: list[PairDefinition],
    web_app_url: str,
    secret: str,
    interval_seconds: float,
    duration_seconds: float | None,
    max_rows: int,
    dry_run: bool,
) -> None:
    hyper_client = HyperliquidClient()

    lighter_client = LighterClient()
    lighter_pairs = [p for p in pairs if p.lighter_symbol]
    lighter_states: dict[str, LighterBookState] = {}
    ws = None

    if lighter_pairs:
        print(f"[{_ts()}] Resolving Lighter market IDs …")
        lighter_states = lighter_client.resolve_market_states(lighter_pairs)
        print(f"[{_ts()}] Connecting Lighter WebSocket …")
        ws = lighter_client.connect_order_books(lighter_states)
        print(f"[{_ts()}] Lighter WebSocket connected. Starting stream …\n")
    else:
        print(f"[{_ts()}] No Lighter symbols configured. Streaming Hyperliquid only.\n")

    start_monotonic = time.monotonic()
    next_tick = math.ceil(time.time() / interval_seconds) * interval_seconds
    next_ping = time.monotonic() + 30.0
    tick_count = 0

    try:
        while True:
            # ---- Drain Lighter WebSocket until the next tick ---------------
            if ws is not None:
                while True:
                    wait = max(0.05, min(0.25, next_tick - time.time()))
                    try:
                        ws.settimeout(wait)
                        msg = json.loads(ws.recv())
                        lighter_client.apply_message(message=msg, states=lighter_states)
                    except WebSocketTimeoutException:
                        break
                    except (OSError, WebSocketConnectionClosedException):
                        print(f"[{_ts()}] Lighter WS dropped – reconnecting …", file=sys.stderr)
                        try:
                            ws.close()
                        except OSError:
                            pass
                        ws = lighter_client.connect_order_books(lighter_states)
                        next_ping = time.monotonic() + 30.0
                        break

            # ---- Sleep until the next second boundary ----------------------
            sleep_seconds = max(0.0, next_tick - time.time())
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

            capture_iso = _utc_now_iso()
            tick_count += 1
            print(f"[{_ts()}] tick #{tick_count}", end="")

            # ---- Ping Lighter WebSocket if due -----------------------------
            if ws is not None and time.monotonic() >= next_ping:
                try:
                    ws.ping()
                except (OSError, WebSocketConnectionClosedException):
                    try:
                        ws.close()
                    except OSError:
                        pass
                    ws = lighter_client.connect_order_books(lighter_states)
                next_ping = time.monotonic() + 30.0

            # ---- Build rows for this tick ----------------------------------
            rows_by_book: dict[str, list[list[Any]]] = {}

            for pair in pairs:
                # Hyperliquid
                hyper_book = BOOK_BY_VENUE_ASSET.get(("hyperliquid", pair.asset))
                if hyper_book:
                    row = _hyper_row(pair, hyper_client, capture_iso)
                    if row:
                        rows_by_book.setdefault(hyper_book, []).append(row)

                # Lighter
                if ws is not None and lighter_states:
                    lighter_book_key = next(
                        (k for k, s in lighter_states.items() if s.pair.asset == pair.asset), None
                    )
                    lighter_sheet = BOOK_BY_VENUE_ASSET.get(("lighter", pair.asset))
                    if lighter_book_key and lighter_sheet:
                        state = lighter_states[lighter_book_key]
                        row = _lighter_row(pair, state, capture_iso)
                        if row:
                            rows_by_book.setdefault(lighter_sheet, []).append(row)

            # ---- Upload / print --------------------------------------------
            print(f" — uploading {sum(len(v) for v in rows_by_book.values())} row(s) …")
            for book, rows in rows_by_book.items():
                try:
                    _post_rows(
                        web_app_url=web_app_url,
                        secret=secret,
                        book=book,
                        rows=rows,
                        max_rows=max_rows,
                        dry_run=dry_run,
                    )
                except Exception as exc:
                    print(f"  [ERROR] {book}: {exc}", file=sys.stderr)

            next_tick += interval_seconds
            if duration_seconds is not None and time.monotonic() - start_monotonic >= duration_seconds:
                break

    finally:
        if ws is not None:
            try:
                ws.close()
            except OSError:
                pass
        print(f"\n[{_ts()}] Stream stopped after {tick_count} tick(s).")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stream live Hyperliquid + Lighter top-of-book data directly to Google Sheets "
            "every second. No CSV files are written."
        )
    )
    parser.add_argument(
        "--assets",
        default="all",
        help="Comma-separated list of assets or 'all' (default: all).",
    )
    parser.add_argument(
        "--web-app-url",
        default=os.environ.get("GOOGLE_SHEETS_WEB_APP_URL"),
        help="Apps Script /exec URL. Env: GOOGLE_SHEETS_WEB_APP_URL.",
    )
    parser.add_argument(
        "--secret",
        default=os.environ.get("GOOGLE_SHEETS_INGEST_SECRET"),
        help="Shared ingest secret. Env: GOOGLE_SHEETS_INGEST_SECRET.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=1.0,
        help="Seconds between snapshots (default: 1).",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=None,
        help="Stop after this many seconds. Omit to run until Ctrl-C.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=50_000,
        help="Rolling maximum rows per sheet (default: 50 000). Use 0 to disable trimming.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print rows to stdout instead of posting to Google Sheets.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pairs = parse_assets(args.assets)

    if not args.dry_run:
        if not args.web_app_url:
            raise SystemExit("ERROR: Missing --web-app-url or GOOGLE_SHEETS_WEB_APP_URL")
        if not args.secret:
            raise SystemExit("ERROR: Missing --secret or GOOGLE_SHEETS_INGEST_SECRET")

    web_app_url = args.web_app_url or ""
    secret = args.secret or ""

    stream(
        pairs=pairs,
        web_app_url=web_app_url,
        secret=secret,
        interval_seconds=args.interval_seconds,
        duration_seconds=args.duration_seconds,
        max_rows=args.max_rows,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
