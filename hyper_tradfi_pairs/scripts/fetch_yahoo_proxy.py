from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from hyper_tradfi_pairs.config import parse_assets
from hyper_tradfi_pairs.yahoo_proxy import YahooProxyClient


def _parse_utc(raw_value: str | None) -> datetime | None:
    if raw_value is None:
        return None
    normalized = raw_value.replace("Z", "+00:00")
    value = datetime.fromisoformat(normalized)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Yahoo price-only proxy data for the four tradfi legs."
    )
    parser.add_argument("--assets", default="all", help="Comma-separated list or 'all'.")
    parser.add_argument(
        "--output-dir",
        default="hyper_tradfi_pairs/data/tradfi_yahoo",
        help="Directory where Yahoo proxy CSV files will be written.",
    )
    parser.add_argument("--interval", default="1m", help="Yahoo bar interval. Practical default is 1m.")
    parser.add_argument("--range", dest="range_value", default="1d", help="Yahoo chart range, e.g. 1d or 5d.")
    parser.add_argument("--start", default=None, help="Optional UTC ISO8601 start time.")
    parser.add_argument("--end", default=None, help="Optional UTC ISO8601 end time.")
    parser.add_argument(
        "--skip-quote-snapshot",
        action="store_true",
        help="Do not fetch the latest Yahoo quote snapshot.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pairs = parse_assets(args.assets)
    client = YahooProxyClient()
    output_root = Path(args.output_dir)
    start = _parse_utc(args.start)
    end = _parse_utc(args.end)

    if (start is None) != (end is None):
        raise SystemExit("Provide both --start and --end together.")

    for pair in pairs:
        chart_path = client.write_chart_csv(
            pair=pair,
            output_root=output_root,
            interval=args.interval,
            range_value=args.range_value,
            start=start,
            end=end,
        )
        print(f"{pair.asset}: wrote {chart_path}")

        if not args.skip_quote_snapshot:
            quote_path = client.append_quote_snapshot(pair=pair, output_root=output_root)
            print(f"{pair.asset}: appended {quote_path}")


if __name__ == "__main__":
    main()

