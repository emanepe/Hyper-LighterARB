from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from hyper_tradfi_pairs.config import parse_assets
from hyper_tradfi_pairs.dex_pair_backtest import (
    DexPairBacktestConfig,
    build_hyper_lighter_frame,
    run_convergence_backtest,
    summarize_convergence,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backtest Hyperliquid <> Lighter convergence pairs: long underpriced, short overpriced."
    )
    parser.add_argument("--assets", default="all", help="Comma-separated list or 'all'.")
    parser.add_argument("--date", required=True, help="UTC date in YYYY-MM-DD format.")
    parser.add_argument("--hyper-dir", default="hyper_tradfi_pairs/data/hyperliquid")
    parser.add_argument("--lighter-dir", default="hyper_tradfi_pairs/data/lighter")
    parser.add_argument("--output-dir", default="hyper_tradfi_pairs/output/hyper_lighter_convergence")
    parser.add_argument("--entry-gap-bps", type=float, default=2.0)
    parser.add_argument("--exit-gap-bps", type=float, default=0.25)
    parser.add_argument("--min-entry-edge-bps", type=float, default=0.0)
    parser.add_argument("--min-notional-usd", type=float, default=25.0)
    parser.add_argument("--max-notional-usd", type=float, default=10_000.0)
    parser.add_argument("--max-holding-seconds", type=int, default=300)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    pairs = parse_assets(args.assets)
    config = DexPairBacktestConfig(
        entry_gap_bps=args.entry_gap_bps,
        exit_gap_bps=args.exit_gap_bps,
        min_entry_edge_bps=args.min_entry_edge_bps,
        min_notional_usd=args.min_notional_usd,
        max_notional_usd=args.max_notional_usd,
        max_holding_seconds=args.max_holding_seconds,
    )

    output_dir = Path(args.output_dir) / args.date
    output_dir.mkdir(parents=True, exist_ok=True)
    summaries: list[dict] = []
    all_trades: list[pd.DataFrame] = []

    for pair in pairs:
        hyper_path = Path(args.hyper_dir) / pair.asset / f"{args.date}.csv"
        lighter_path = Path(args.lighter_dir) / pair.asset / f"{args.date}.csv"
        if not hyper_path.exists():
            print(f"{pair.asset}: missing Hyperliquid file {hyper_path}")
            continue
        if not lighter_path.exists():
            print(f"{pair.asset}: missing Lighter file {lighter_path}")
            continue

        signal_frame = build_hyper_lighter_frame(hyper_path=hyper_path, lighter_path=lighter_path)
        trades = run_convergence_backtest(asset=pair.asset, signal_frame=signal_frame, config=config)
        summary = summarize_convergence(asset=pair.asset, signal_frame=signal_frame, trades=trades)
        summaries.append(summary)

        asset_dir = output_dir / pair.asset
        asset_dir.mkdir(parents=True, exist_ok=True)
        signal_frame.to_csv(asset_dir / f"{args.date}_signals.csv", index=False)
        trades.to_csv(asset_dir / f"{args.date}_trades.csv", index=False)
        if not trades.empty:
            all_trades.append(trades)

        print(
            f"{pair.asset}: rows={summary.get('overlap_rows', 0)} "
            f"trades={summary.get('trade_count', 0)} pnl={summary.get('gross_pnl_usd', 0.0):.4f}"
        )

    if summaries:
        summary = pd.DataFrame(summaries)
        summary_path = output_dir / "summary.csv"
        summary.to_csv(summary_path, index=False)
        print(f"summary: wrote {summary_path}")

    if all_trades:
        trades_path = output_dir / "all_trades.csv"
        pd.concat(all_trades, ignore_index=True).to_csv(trades_path, index=False)
        print(f"trades: wrote {trades_path}")


if __name__ == "__main__":
    main()
