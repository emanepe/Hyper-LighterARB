from __future__ import annotations

import csv
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


COMPACT_HEADERS = [
    "venue",
    "asset",
    "market",
    "captured_at_utc",
    "exchange_time",
    "exchange_time_ms",
    "best_bid_px",
    "best_bid_sz",
    "best_ask_px",
    "best_ask_sz",
    "mid_px",
    "spread_px",
    "spread_bps",
    "bid_depth_5_sz",
    "ask_depth_5_sz",
    "bid_depth_20_sz",
    "ask_depth_20_sz",
]


BOOK_BY_VENUE_ASSET = {
    ("hyperliquid", "BRENTOIL"): "Hyper_BRENTOIL",
    ("hyperliquid", "GOLD"): "Hyper_GOLD",
    ("hyperliquid", "SILVER"): "Hyper_SILVER",
    ("hyperliquid", "WTI"): "Hyper_WTI",
    ("lighter", "BRENTOIL"): "Lighter_BRENTOIL",
    ("lighter", "GOLD"): "Lighter_GOLD",
    ("lighter", "SILVER"): "Lighter_SILVER",
    ("lighter", "WTI"): "Lighter_WTI",
}


@dataclass(frozen=True)
class SyncConfig:
    web_app_url: str
    secret: str
    data_root: Path
    state_path: Path
    interval_seconds: float
    batch_size: int
    max_rows: int
    from_end: bool
    once: bool


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"files": {}}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def iter_csv_files(data_root: Path) -> list[tuple[str, str, str, Path]]:
    files: list[tuple[str, str, str, Path]] = []
    for (venue, asset), book in BOOK_BY_VENUE_ASSET.items():
        asset_dir = data_root / venue / asset
        for path in sorted(asset_dir.glob("*.csv")):
            files.append((book, venue, asset, path))
    return files


def read_new_rows(
    path: Path,
    venue: str,
    asset: str,
    state: dict[str, Any],
    batch_size: int,
    from_end: bool,
) -> tuple[list[list[Any]], int | None]:
    file_key = str(path.resolve())
    file_state = state.setdefault("files", {}).setdefault(file_key, {})

    with path.open("r", newline="", encoding="utf-8") as handle:
        header_line = handle.readline()
        if not header_line:
            return [], None
        raw_headers = next(csv.reader([header_line]))
        data_start = handle.tell()

        if "offset" not in file_state:
            if from_end:
                handle.seek(0, os.SEEK_END)
                file_state["offset"] = handle.tell()
                file_state["headers"] = raw_headers
                return [], None
            file_state["offset"] = data_start
            file_state["headers"] = raw_headers

        offset = int(file_state.get("offset", data_start))
        file_size = path.stat().st_size
        if offset > file_size or offset < data_start:
            offset = data_start

        handle.seek(offset)
        rows: list[list[Any]] = []
        new_offset = offset
        while len(rows) < batch_size:
            line = handle.readline()
            if not line:
                break
            new_offset = handle.tell()
            parsed = next(csv.reader([line]))
            if len(parsed) != len(raw_headers):
                continue
            row = dict(zip(raw_headers, parsed))
            rows.append(project_compact_row(row=row, venue=venue, asset=asset))

    return rows, new_offset if rows else None


def project_compact_row(row: dict[str, str], venue: str, asset: str) -> list[Any]:
    market = row.get("coin") or row.get("symbol") or ""
    values = {
        "venue": venue,
        "asset": asset,
        "market": market,
        "captured_at_utc": row.get("captured_at_utc", ""),
        "exchange_time": row.get("exchange_time", ""),
        "exchange_time_ms": row.get("exchange_time_ms", ""),
        "best_bid_px": row.get("best_bid_px", ""),
        "best_bid_sz": row.get("best_bid_sz", ""),
        "best_ask_px": row.get("best_ask_px", ""),
        "best_ask_sz": row.get("best_ask_sz", ""),
        "mid_px": row.get("mid_px", ""),
        "spread_px": row.get("spread_px", ""),
        "spread_bps": row.get("spread_bps", ""),
        "bid_depth_5_sz": row.get("bid_depth_5_sz", ""),
        "ask_depth_5_sz": row.get("ask_depth_5_sz", ""),
        "bid_depth_20_sz": row.get("bid_depth_20_sz", ""),
        "ask_depth_20_sz": row.get("ask_depth_20_sz", ""),
    }
    return [values[name] for name in COMPACT_HEADERS]


def upload_rows(config: SyncConfig, book: str, rows: list[list[Any]]) -> None:
    payload = {
        "secret": config.secret,
        "book": book,
        "headers": COMPACT_HEADERS,
        "rows": rows,
        "max_rows": config.max_rows,
    }
    response = requests.post(config.web_app_url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    if not data.get("ok"):
        raise RuntimeError(f"Google Sheets ingest failed for {book}: {data}")


def sync_once(config: SyncConfig, state: dict[str, Any]) -> int:
    uploaded = 0
    for book, venue, asset, path in iter_csv_files(config.data_root):
        rows, new_offset = read_new_rows(
            path=path,
            venue=venue,
            asset=asset,
            state=state,
            batch_size=config.batch_size,
            from_end=config.from_end,
        )
        if not rows or new_offset is None:
            continue

        upload_rows(config=config, book=book, rows=rows)
        file_key = str(path.resolve())
        state["files"][file_key]["offset"] = new_offset
        uploaded += len(rows)
        save_state(config.state_path, state)
    return uploaded


def run_sync(config: SyncConfig) -> None:
    state = load_state(config.state_path)
    while True:
        try:
            uploaded = sync_once(config=config, state=state)
            if uploaded:
                print(f"uploaded_rows={uploaded}", flush=True)
        except Exception as exc:
            print(f"sync_error={exc}", flush=True)

        if config.once:
            return
        time.sleep(config.interval_seconds)
