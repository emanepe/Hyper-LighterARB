from __future__ import annotations

import argparse
from pathlib import Path

from hyper_tradfi_pairs.config import parse_assets
from hyper_tradfi_pairs.ibkr import connect_ibkr, write_ibkr_historical_day


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch historical IBKR BID_ASK ticks and aggregate them to 1-second top-of-book rows."
    )
    parser.add_argument("--assets", default="all", help="Comma-separated list or 'all'.")
    parser.add_argument("--date", required=True, help="UTC date in YYYY-MM-DD format.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497)
    parser.add_argument("--client-id", type=int, default=72)
    parser.add_argument("--use-rth", action="store_true", help="Restrict to regular trading hours.")
    parser.add_argument(
        "--no-fill-missing-seconds",
        action="store_true",
        help="Do not forward-fill missing seconds after the first observed quote.",
    )
    parser.add_argument(
        "--output-dir",
        default="hyper_tradfi_pairs/data/tradfi_ibkr_historical",
        help="Directory where per-asset CSV files will be written.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pairs = parse_assets(args.assets)
    ib = connect_ibkr(host=args.host, port=args.port, client_id=args.client_id, readonly=True)
    try:
        for pair in pairs:
            path = write_ibkr_historical_day(
                ib=ib,
                pair=pair,
                output_root=Path(args.output_dir),
                day_string=args.date,
                use_rth=args.use_rth,
                fill_missing_seconds=not args.no_fill_missing_seconds,
            )
            print(f"{pair.asset}: wrote {path}")
    finally:
        ib.disconnect()


if __name__ == "__main__":
    main()

