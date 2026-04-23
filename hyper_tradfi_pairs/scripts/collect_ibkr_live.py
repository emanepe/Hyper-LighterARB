from __future__ import annotations

import argparse
from pathlib import Path

from hyper_tradfi_pairs.config import parse_assets
from hyper_tradfi_pairs.ibkr import connect_ibkr, record_ibkr_live_1s


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record 1-second IBKR top-of-book snapshots for the configured futures legs."
    )
    parser.add_argument("--assets", default="all", help="Comma-separated list or 'all'.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7497, help="7497 paper TWS, 7496 live TWS, 4002 paper Gateway.")
    parser.add_argument("--client-id", type=int, default=71)
    parser.add_argument("--market-data-type", type=int, default=1, help="1 live, 2 frozen, 3 delayed, 4 delayed frozen.")
    parser.add_argument(
        "--output-dir",
        default="hyper_tradfi_pairs/data/tradfi_ibkr_live",
        help="Directory where per-asset CSV files will be written.",
    )
    parser.add_argument("--duration-seconds", type=float, default=None, help="Optional collection duration.")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pairs = parse_assets(args.assets)
    ib = connect_ibkr(host=args.host, port=args.port, client_id=args.client_id, readonly=True)
    try:
        record_ibkr_live_1s(
            ib=ib,
            pairs=pairs,
            output_root=Path(args.output_dir),
            duration_seconds=args.duration_seconds,
            market_data_type=args.market_data_type,
        )
    finally:
        ib.disconnect()


if __name__ == "__main__":
    main()

