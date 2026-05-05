"""Realistic basis-trade backtest with bid/ask execution, configurable fees, and entry threshold.

Improvements over scripts/analyze_no_maxhold_mid_report.py:
  - Aligns both venues on `exchange_time` (the actual book timestamp), not on capture wall-clock.
    Filters Lighter catch-up bursts and frozen-book polling that come from the collector bug.
  - Uses bid/ask crossing on every fill (entry and exit on each leg) instead of mid-price.
  - Charges per-fill taker fees per venue (default: HL 0.3 bps, Lighter 0 bps).
  - Configurable entry-edge threshold (only enter if `edge_after_fees >= --entry-edge-bps`).
  - Runs both single-entry and scaled (layered) variants.

Usage (defaults match the values used in the conversation that produced this script):
    python scripts/analyze_realistic_basis_backtest.py
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "hyper_tradfi_pairs" / "data"
REPORT_DIR = ROOT / "reports"
ASSETS = ("BRENTOIL", "GOLD", "SILVER", "WTI")

EXIT_GAP_BPS = 0.25


@dataclass(frozen=True)
class Config:
    freshness_seconds: float = 2.0
    hyper_fee_bps: float = 0.3
    lighter_fee_bps: float = 0.0
    entry_edge_bps: float = 1.5
    notional_usd: float = 10_000.0
    step_bps: float = 1.0
    max_layers: int = 5


def _load_clean(venue: str, asset: str, freshness_seconds: float) -> pd.DataFrame:
    cols = ["captured_at_utc", "exchange_time", "mid_px", "best_bid_px", "best_ask_px"]
    parts = []
    for path in sorted((DATA_DIR / venue / asset).glob("*.csv")):
        df = pd.read_csv(path, usecols=cols)
        df["cap"] = pd.to_datetime(df["captured_at_utc"], utc=True, errors="coerce")
        df["ex"] = pd.to_datetime(df["exchange_time"], utc=True, errors="coerce")
        df = df.dropna(subset=["cap", "ex", "best_bid_px", "best_ask_px"])
        df = df[(df["cap"] - df["ex"]).dt.total_seconds() <= freshness_seconds]
        df["sec"] = df["ex"].dt.floor("s")
        df = df.sort_values(["sec", "ex"]).groupby("sec", as_index=False).last()
        parts.append(
            df[["sec", "mid_px", "best_bid_px", "best_ask_px"]].rename(
                columns={
                    "mid_px": f"mid_{venue}",
                    "best_bid_px": f"bid_{venue}",
                    "best_ask_px": f"ask_{venue}",
                }
            )
        )
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def build_frame(freshness_seconds: float) -> pd.DataFrame:
    parts = []
    for asset in ASSETS:
        h = _load_clean("hyperliquid", asset, freshness_seconds)
        l = _load_clean("lighter", asset, freshness_seconds)
        if h.empty or l.empty:
            continue
        m = h.merge(l, on="sec", how="inner").sort_values("sec").reset_index(drop=True)
        m["asset"] = asset
        m["mid_gap_bps"] = 10_000 * (m["mid_hyperliquid"] / m["mid_lighter"] - 1.0)
        m["edge_short_hyper_bps"] = 10_000 * (m["bid_hyperliquid"] / m["ask_lighter"] - 1.0)
        m["edge_short_lighter_bps"] = 10_000 * (m["bid_lighter"] / m["ask_hyperliquid"] - 1.0)
        parts.append(m)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _close_position(direction: str, layers: list[dict], r: pd.Series, cfg: Config) -> dict:
    if direction == "short_hyper_long_lighter":
        h_exit = r["ask_hyperliquid"]
        l_exit = r["bid_lighter"]
    else:
        h_exit = r["bid_hyperliquid"]
        l_exit = r["ask_lighter"]
    gross = 0.0
    entry_fees = 0.0
    for layer in layers:
        if direction == "short_hyper_long_lighter":
            gross += layer["h_qty"] * (layer["h_entry"] - h_exit)
            gross += layer["l_qty"] * (l_exit - layer["l_entry"])
        else:
            gross += layer["h_qty"] * (h_exit - layer["h_entry"])
            gross += layer["l_qty"] * (layer["l_entry"] - l_exit)
        entry_fees += layer["entry_fee"]
    exit_fees = sum(
        layer["notional"] * (cfg.hyper_fee_bps + cfg.lighter_fee_bps) / 10_000
        for layer in layers
    )
    return {
        "direction": direction,
        "n_layers": len(layers),
        "total_notional_usd": sum(layer["notional"] for layer in layers),
        "gross_pnl_usd": gross,
        "fees_usd": entry_fees + exit_fees,
        "net_pnl_usd": gross - entry_fees - exit_fees,
        "exit_gap_bps": r["mid_gap_bps"],
    }


def _open_layer(direction: str, r: pd.Series, cfg: Config) -> dict:
    if direction == "short_hyper_long_lighter":
        h_entry = r["bid_hyperliquid"]
        l_entry = r["ask_lighter"]
    else:
        h_entry = r["ask_hyperliquid"]
        l_entry = r["bid_lighter"]
    return {
        "h_entry": h_entry,
        "l_entry": l_entry,
        "h_qty": cfg.notional_usd / h_entry,
        "l_qty": cfg.notional_usd / l_entry,
        "notional": cfg.notional_usd,
        "entry_fee": cfg.notional_usd * (cfg.hyper_fee_bps + cfg.lighter_fee_bps) / 10_000,
    }


def simulate_single(frame: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    fee_total_one_layer = 2 * cfg.hyper_fee_bps + 2 * cfg.lighter_fee_bps
    trades: list[dict] = []
    for asset, group in frame.groupby("asset", sort=False):
        group = group.sort_values("sec").reset_index(drop=True)
        position = None
        for _, r in group.iterrows():
            if position is None:
                gap = r["mid_gap_bps"]
                if gap == 0:
                    continue
                if gap > 0:
                    direction = "short_hyper_long_lighter"
                    edge = r["edge_short_hyper_bps"]
                else:
                    direction = "long_hyper_short_lighter"
                    edge = r["edge_short_lighter_bps"]
                if edge - fee_total_one_layer <= cfg.entry_edge_bps:
                    continue
                position = (direction, [_open_layer(direction, r, cfg)], r["sec"])
                continue
            direction, layers, entry_time = position
            gap = r["mid_gap_bps"]
            done = abs(gap) <= EXIT_GAP_BPS
            if not done and direction == "short_hyper_long_lighter" and gap <= 0:
                done = True
            if not done and direction == "long_hyper_short_lighter" and gap >= 0:
                done = True
            if done:
                trade = _close_position(direction, layers, r, cfg)
                trade["asset"] = asset
                trade["entry_time"] = entry_time
                trade["exit_time"] = r["sec"]
                trade["hold_s"] = (r["sec"] - entry_time).total_seconds()
                trades.append(trade)
                position = None
    return pd.DataFrame(trades)


def simulate_scaled(frame: pd.DataFrame, cfg: Config) -> pd.DataFrame:
    fee_total_one_layer = 2 * cfg.hyper_fee_bps + 2 * cfg.lighter_fee_bps
    trades: list[dict] = []
    for asset, group in frame.groupby("asset", sort=False):
        group = group.sort_values("sec").reset_index(drop=True)
        direction = None
        layers: list[dict] = []
        entry_time = None
        last_layer_abs_gap = None
        for _, r in group.iterrows():
            gap = r["mid_gap_bps"]
            if not layers:
                if gap == 0:
                    continue
                if gap > 0:
                    direction = "short_hyper_long_lighter"
                    edge = r["edge_short_hyper_bps"]
                else:
                    direction = "long_hyper_short_lighter"
                    edge = r["edge_short_lighter_bps"]
                if edge - fee_total_one_layer <= cfg.entry_edge_bps:
                    continue
                layers.append(_open_layer(direction, r, cfg))
                entry_time = r["sec"]
                last_layer_abs_gap = abs(gap)
                continue
            same_side = (
                (direction == "short_hyper_long_lighter" and gap > 0)
                or (direction == "long_hyper_short_lighter" and gap < 0)
            )
            if (
                same_side
                and abs(gap) >= last_layer_abs_gap + cfg.step_bps
                and len(layers) < cfg.max_layers
            ):
                layers.append(_open_layer(direction, r, cfg))
                last_layer_abs_gap = abs(gap)
            done = abs(gap) <= EXIT_GAP_BPS
            if not done and direction == "short_hyper_long_lighter" and gap <= 0:
                done = True
            if not done and direction == "long_hyper_short_lighter" and gap >= 0:
                done = True
            if done:
                trade = _close_position(direction, layers, r, cfg)
                trade["asset"] = asset
                trade["entry_time"] = entry_time
                trade["exit_time"] = r["sec"]
                trade["hold_s"] = (r["sec"] - entry_time).total_seconds()
                trades.append(trade)
                direction = None
                layers = []
                entry_time = None
                last_layer_abs_gap = None
        if layers:
            trade = _close_position(direction, layers, group.iloc[-1], cfg)
            trade["asset"] = asset
            trade["entry_time"] = entry_time
            trade["exit_time"] = group.iloc[-1]["sec"]
            trade["hold_s"] = (group.iloc[-1]["sec"] - entry_time).total_seconds()
            trades.append(trade)
    return pd.DataFrame(trades)


def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    out = trades.groupby("asset").agg(
        trades=("net_pnl_usd", "size"),
        avg_layers=("n_layers", "mean"),
        gross_pnl=("gross_pnl_usd", "sum"),
        fees=("fees_usd", "sum"),
        net_pnl=("net_pnl_usd", "sum"),
        winrate_pct=("net_pnl_usd", lambda s: (s > 0).mean() * 100),
        avg_hold_s=("hold_s", "mean"),
    ).round(2)
    return out


def parse_args() -> Config:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--freshness-seconds", type=float, default=2.0)
    p.add_argument("--hyper-fee-bps", type=float, default=0.3)
    p.add_argument("--lighter-fee-bps", type=float, default=0.0)
    p.add_argument("--entry-edge-bps", type=float, default=1.5,
                   help="Required edge after fees before entering. ~1.5–2 bps is the empirical sweet spot.")
    p.add_argument("--notional-usd", type=float, default=10_000.0)
    p.add_argument("--step-bps", type=float, default=1.0)
    p.add_argument("--max-layers", type=int, default=5)
    args = p.parse_args()
    return Config(
        freshness_seconds=args.freshness_seconds,
        hyper_fee_bps=args.hyper_fee_bps,
        lighter_fee_bps=args.lighter_fee_bps,
        entry_edge_bps=args.entry_edge_bps,
        notional_usd=args.notional_usd,
        step_bps=args.step_bps,
        max_layers=args.max_layers,
    )


def main() -> None:
    cfg = parse_args()
    frame = build_frame(cfg.freshness_seconds)
    print(f"Frame: {len(frame):,} merged exchange-seconds across {len(ASSETS)} assets")
    print(f"Config: HL={cfg.hyper_fee_bps} bps, LT={cfg.lighter_fee_bps} bps, "
          f"entry_edge={cfg.entry_edge_bps} bps, max_layers={cfg.max_layers}, "
          f"step={cfg.step_bps} bps, notional=${cfg.notional_usd:,.0f}\n")

    single = simulate_single(frame, cfg)
    scaled = simulate_scaled(frame, cfg)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = REPORT_DIR / f"realistic_basis_backtest_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not single.empty:
        single.to_csv(out_dir / "single_entry_trades.csv", index=False)
        s_summary = summarize(single)
        s_summary.to_csv(out_dir / "single_entry_summary.csv")
        print("=== SINGLE ENTRY ===")
        print(s_summary.to_string())
        print(f"TOTAL: net=${single['net_pnl_usd'].sum():,.2f}  trades={len(single):,}  "
              f"winrate={(single['net_pnl_usd']>0).mean()*100:.1f}%\n")

    if not scaled.empty:
        scaled.to_csv(out_dir / "scaled_trades.csv", index=False)
        sc_summary = summarize(scaled)
        sc_summary.to_csv(out_dir / "scaled_summary.csv")
        print("=== SCALED (layered) ===")
        print(sc_summary.to_string())
        print(f"TOTAL: net=${scaled['net_pnl_usd'].sum():,.2f}  trades={len(scaled):,}  "
              f"winrate={(scaled['net_pnl_usd']>0).mean()*100:.1f}%\n")

    print(f"Wrote outputs to {out_dir}")


if __name__ == "__main__":
    main()
