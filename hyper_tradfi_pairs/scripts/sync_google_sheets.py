from __future__ import annotations

import argparse
import os
from pathlib import Path

from hyper_tradfi_pairs.google_sheets_sync import SyncConfig, run_sync


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync local Hyperliquid and Lighter 1-second CSV rows to 8 separate Google Sheets files."
    )
    parser.add_argument(
        "--web-app-url",
        default=os.environ.get("GOOGLE_SHEETS_WEB_APP_URL"),
        help="Apps Script web app URL. Can also use GOOGLE_SHEETS_WEB_APP_URL.",
    )
    parser.add_argument(
        "--secret",
        default=os.environ.get("GOOGLE_SHEETS_INGEST_SECRET"),
        help="Shared ingest secret. Can also use GOOGLE_SHEETS_INGEST_SECRET.",
    )
    parser.add_argument(
        "--data-root",
        default="hyper_tradfi_pairs/data",
        help="Root data directory containing hyperliquid/ and lighter/ subdirectories.",
    )
    parser.add_argument(
        "--state-path",
        default="hyper_tradfi_pairs/runtime/google_sheets_sync_state.json",
        help="Local upload offset state file.",
    )
    parser.add_argument("--interval-seconds", type=float, default=10.0)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument(
        "--max-rows",
        type=int,
        default=50_000,
        help="Rolling maximum rows per Google Spreadsheet Data page. Use 0 to disable trimming.",
    )
    parser.add_argument(
        "--from-end",
        action="store_true",
        help="On first run, start from current CSV end instead of uploading existing backlog.",
    )
    parser.add_argument("--once", action="store_true", help="Run one sync pass and exit.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if not args.web_app_url:
        raise SystemExit("Missing --web-app-url or GOOGLE_SHEETS_WEB_APP_URL")
    if not args.secret:
        raise SystemExit("Missing --secret or GOOGLE_SHEETS_INGEST_SECRET")

    run_sync(
        SyncConfig(
            web_app_url=args.web_app_url,
            secret=args.secret,
            data_root=Path(args.data_root),
            state_path=Path(args.state_path),
            interval_seconds=args.interval_seconds,
            batch_size=args.batch_size,
            max_rows=args.max_rows,
            from_end=args.from_end,
            once=args.once,
        )
    )


if __name__ == "__main__":
    main()
