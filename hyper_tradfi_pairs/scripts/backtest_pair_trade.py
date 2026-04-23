from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from hyper_tradfi_pairs.backtest import (
    BacktestConfig,
    build_signal_frame,
    run_pair_backtest,
    write_backtest_outputs,
)
from hyper_tradfi_pairs.config import parse_assets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest a 1-day Hyperliquid vs tradfi mean-reversion pair strategy."
    )
    parser.add_argument("--assets", default="all", help="Comma-separated list or 'all'.")
    parser.add_argument("--date", required=True, help="UTC date in YYYY-MM-DD format.")
    parser.add_argument(
        "--hyper-dir",
        default="hyper_tradfi_pairs/data/hyperliquid",
        help="Root directory for collected Hyperliquid CSV files.",
    )
    parser.add_argument(
        "--tradfi-dir",
        default="hyper_tradfi_pairs/data/tradfi_databento",
        help="Root directory for Databento BBO CSV files.",
    )
    parser.add_argument(
        "--output-dir",
        default="hyper_tradfi_pairs/output/backtests",
        help="Directory where backtest artifacts will be written.",
    )
    parser.add_argument("--lookback-seconds", type=int, default=600)
    parser.add_argument("--entry-z", type=float, default=2.0)
    parser.add_argument("--exit-z", type=float, default=0.5)
    parser.add_argument("--gross-notional-usd", type=float, default=10_000.0)
    parser.add_argument("--max-holding-seconds", type=int, default=None)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pairs = parse_assets(args.assets)
    config = BacktestConfig(
        lookback_seconds=args.lookback_seconds,
        entry_z=args.entry_z,
        exit_z=args.exit_z,
        gross_notional_usd=args.gross_notional_usd,
        max_holding_seconds=args.max_holding_seconds,
    )

    output_dir = Path(args.output_dir) / args.date
    output_dir.mkdir(parents=True, exist_ok=True)

    summaries: list[dict] = []
    for pair in pairs:
        hyper_path = Path(args.hyper_dir) / pair.asset / f"{args.date}.csv"
        tradfi_path = Path(args.tradfi_dir) / pair.asset / f"{args.date}.csv"
        if not hyper_path.exists():
            print(f"{pair.asset}: missing Hyperliquid file {hyper_path}")
            continue
        if not tradfi_path.exists():
            print(f"{pair.asset}: missing Databento file {tradfi_path}")
            continue

        signal_frame = build_signal_frame(hyper_path=hyper_path, tradfi_path=tradfi_path, config=config)
        trades = run_pair_backtest(signal_frame=signal_frame, config=config)
        summary = write_backtest_outputs(
            output_dir=output_dir,
            asset=pair.asset,
            date_string=args.date,
            config=config,
            signal_frame=signal_frame,
            trades=trades,
        )
        summaries.append(summary)
        print(
            f"{pair.asset}: rows={summary['signal_rows']} trades={summary['trade_count']} pnl={summary['gross_pnl_usd']:.2f}"
        )

    if summaries:
        summary_path = output_dir / "summary.csv"
        pd.DataFrame(summaries).to_csv(summary_path, index=False)
        print(f"summary: wrote {summary_path}")


if __name__ == "__main__":
    main()

