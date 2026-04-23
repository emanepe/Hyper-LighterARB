from __future__ import annotations

import argparse
from pathlib import Path

from hyper_tradfi_pairs.config import parse_assets
from hyper_tradfi_pairs.databento_http import DatabentoHistoricalClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download 1-second Databento BBO history for the pair-trading assets."
    )
    parser.add_argument("--assets", default="all", help="Comma-separated list or 'all'.")
    parser.add_argument(
        "--output-dir",
        default="hyper_tradfi_pairs/data/tradfi_databento",
        help="Directory where per-asset CSV files will be written.",
    )
    parser.add_argument("--date", default=None, help="UTC session date in YYYY-MM-DD format.")
    parser.add_argument("--start", default=None, help="UTC ISO8601 start time.")
    parser.add_argument("--end", default=None, help="UTC ISO8601 end time.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pairs = parse_assets(args.assets)
    client = DatabentoHistoricalClient()
    output_root = Path(args.output_dir)

    if args.date is None and (args.start is None or args.end is None):
        raise SystemExit("Provide either --date or both --start and --end.")

    for pair in pairs:
        path = client.download_bbo_1s_to_path(
            pair=pair,
            output_root=output_root,
            day_string=args.date,
            start=args.start,
            end=args.end,
        )
        print(f"{pair.asset}: wrote {path}")


if __name__ == "__main__":
    main()

