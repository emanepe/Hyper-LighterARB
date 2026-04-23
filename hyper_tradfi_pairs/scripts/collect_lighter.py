from __future__ import annotations

import argparse
from pathlib import Path

from hyper_tradfi_pairs.config import parse_assets
from hyper_tradfi_pairs.lighter import collect_top_of_book


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Collect 1-second Lighter top-of-book snapshots for the pair-trading assets."
    )
    parser.add_argument("--assets", default="all", help="Comma-separated list or 'all'.")
    parser.add_argument(
        "--output-dir",
        default="hyper_tradfi_pairs/data/lighter",
        help="Directory where per-asset CSV files will be written.",
    )
    parser.add_argument("--interval-seconds", type=float, default=1.0, help="Sampling interval.")
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=None,
        help="Optional collection duration. Omit to run until interrupted.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pairs = parse_assets(args.assets)
    collect_top_of_book(
        pairs=pairs,
        output_dir=Path(args.output_dir),
        interval_seconds=args.interval_seconds,
        duration_seconds=args.duration_seconds,
    )


if __name__ == "__main__":
    main()
