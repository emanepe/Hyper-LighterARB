from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DexPairBacktestConfig:
    entry_gap_bps: float = 2.0
    exit_gap_bps: float = 0.25
    min_entry_edge_bps: float = 0.0
    min_notional_usd: float = 25.0
    max_notional_usd: float = 10_000.0
    max_holding_seconds: int = 300


def _load_snapshots(path: Path, prefix: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        return df

    timestamp_column = "captured_at_utc" if "captured_at_utc" in df.columns else "exchange_time"
    df["timestamp"] = pd.to_datetime(df[timestamp_column], utc=True, errors="coerce").dt.floor("s")
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values(["timestamp", timestamp_column]).groupby("timestamp", as_index=False).last()

    keep = [
        "timestamp",
        "asset",
        "best_bid_px",
        "best_bid_sz",
        "best_ask_px",
        "best_ask_sz",
        "mid_px",
        "spread_bps",
        "bid_depth_5_sz",
        "ask_depth_5_sz",
        "bid_depth_20_sz",
        "ask_depth_20_sz",
    ]
    keep = [column for column in keep if column in df.columns]
    return df[keep].rename(columns={column: f"{prefix}_{column}" for column in keep if column != "timestamp"})


def build_hyper_lighter_frame(hyper_path: Path, lighter_path: Path) -> pd.DataFrame:
    hyper = _load_snapshots(hyper_path, "hyper")
    lighter = _load_snapshots(lighter_path, "lighter")
    if hyper.empty or lighter.empty:
        return pd.DataFrame()

    frame = hyper.merge(lighter, on="timestamp", how="inner")
    if frame.empty:
        return frame

    frame["mid_gap_bps"] = 10_000 * (frame["hyper_mid_px"] / frame["lighter_mid_px"] - 1.0)
    frame["hyper_rich_entry_edge_bps"] = 10_000 * (frame["hyper_best_bid_px"] / frame["lighter_best_ask_px"] - 1.0)
    frame["lighter_rich_entry_edge_bps"] = 10_000 * (frame["lighter_best_bid_px"] / frame["hyper_best_ask_px"] - 1.0)

    frame["hyper_bid_notional"] = frame["hyper_best_bid_px"] * frame["hyper_best_bid_sz"]
    frame["hyper_ask_notional"] = frame["hyper_best_ask_px"] * frame["hyper_best_ask_sz"]
    frame["lighter_bid_notional"] = frame["lighter_best_bid_px"] * frame["lighter_best_bid_sz"]
    frame["lighter_ask_notional"] = frame["lighter_best_ask_px"] * frame["lighter_best_ask_sz"]
    frame["hyper_rich_cap_notional"] = np.minimum.reduce(
        [frame["hyper_bid_notional"], frame["lighter_ask_notional"]]
    )
    frame["lighter_rich_cap_notional"] = np.minimum.reduce(
        [frame["lighter_bid_notional"], frame["hyper_ask_notional"]]
    )
    return frame


def _open_position(asset: str, row: pd.Series, config: DexPairBacktestConfig) -> dict | None:
    # Positive gap: Hyperliquid is rich, so short Hyperliquid and long Lighter.
    if row["mid_gap_bps"] >= config.entry_gap_bps:
        if row["hyper_rich_entry_edge_bps"] < config.min_entry_edge_bps:
            return None
        notional = min(float(row["hyper_rich_cap_notional"]), config.max_notional_usd)
        if notional < config.min_notional_usd:
            return None
        hyper_entry_px = float(row["hyper_best_bid_px"])
        lighter_entry_px = float(row["lighter_best_ask_px"])
        return {
            "asset": asset,
            "direction": "short_hyper_long_lighter",
            "entry_time": row["timestamp"],
            "entry_mid_gap_bps": float(row["mid_gap_bps"]),
            "entry_edge_bps": float(row["hyper_rich_entry_edge_bps"]),
            "entry_notional_usd": notional,
            "hyper_entry_px": hyper_entry_px,
            "lighter_entry_px": lighter_entry_px,
            "hyper_qty": notional / hyper_entry_px,
            "lighter_qty": notional / lighter_entry_px,
        }

    # Negative gap: Lighter is rich, so long Hyperliquid and short Lighter.
    if row["mid_gap_bps"] <= -config.entry_gap_bps:
        if row["lighter_rich_entry_edge_bps"] < config.min_entry_edge_bps:
            return None
        notional = min(float(row["lighter_rich_cap_notional"]), config.max_notional_usd)
        if notional < config.min_notional_usd:
            return None
        hyper_entry_px = float(row["hyper_best_ask_px"])
        lighter_entry_px = float(row["lighter_best_bid_px"])
        return {
            "asset": asset,
            "direction": "long_hyper_short_lighter",
            "entry_time": row["timestamp"],
            "entry_mid_gap_bps": float(row["mid_gap_bps"]),
            "entry_edge_bps": float(row["lighter_rich_entry_edge_bps"]),
            "entry_notional_usd": notional,
            "hyper_entry_px": hyper_entry_px,
            "lighter_entry_px": lighter_entry_px,
            "hyper_qty": notional / hyper_entry_px,
            "lighter_qty": notional / lighter_entry_px,
        }

    return None


def _should_close(position: dict, row: pd.Series, config: DexPairBacktestConfig) -> tuple[bool, str]:
    holding_seconds = int((row["timestamp"] - position["entry_time"]).total_seconds())
    if holding_seconds >= config.max_holding_seconds:
        return True, "max_hold"

    gap = float(row["mid_gap_bps"])
    if abs(gap) <= config.exit_gap_bps:
        return True, "converged"

    if position["direction"] == "short_hyper_long_lighter" and gap <= 0.0:
        return True, "flipped"
    if position["direction"] == "long_hyper_short_lighter" and gap >= 0.0:
        return True, "flipped"

    return False, ""


def _close_position(position: dict, row: pd.Series, reason: str) -> dict:
    if position["direction"] == "short_hyper_long_lighter":
        hyper_exit_px = float(row["hyper_best_ask_px"])
        lighter_exit_px = float(row["lighter_best_bid_px"])
        hyper_pnl = position["hyper_qty"] * (position["hyper_entry_px"] - hyper_exit_px)
        lighter_pnl = position["lighter_qty"] * (lighter_exit_px - position["lighter_entry_px"])
    else:
        hyper_exit_px = float(row["hyper_best_bid_px"])
        lighter_exit_px = float(row["lighter_best_ask_px"])
        hyper_pnl = position["hyper_qty"] * (hyper_exit_px - position["hyper_entry_px"])
        lighter_pnl = position["lighter_qty"] * (position["lighter_entry_px"] - lighter_exit_px)

    holding_seconds = int((row["timestamp"] - position["entry_time"]).total_seconds())
    return {
        "asset": position["asset"],
        "direction": position["direction"],
        "entry_time": position["entry_time"],
        "exit_time": row["timestamp"],
        "exit_reason": reason,
        "holding_seconds": holding_seconds,
        "entry_mid_gap_bps": position["entry_mid_gap_bps"],
        "exit_mid_gap_bps": float(row["mid_gap_bps"]),
        "entry_edge_bps": position["entry_edge_bps"],
        "entry_notional_usd": position["entry_notional_usd"],
        "hyper_entry_px": position["hyper_entry_px"],
        "hyper_exit_px": hyper_exit_px,
        "lighter_entry_px": position["lighter_entry_px"],
        "lighter_exit_px": lighter_exit_px,
        "hyper_pnl_usd": hyper_pnl,
        "lighter_pnl_usd": lighter_pnl,
        "gross_pnl_usd": hyper_pnl + lighter_pnl,
    }


def run_convergence_backtest(
    asset: str,
    signal_frame: pd.DataFrame,
    config: DexPairBacktestConfig,
) -> pd.DataFrame:
    if signal_frame.empty:
        return pd.DataFrame()

    trades: list[dict] = []
    position: dict | None = None
    for _index, row in signal_frame.iterrows():
        if position is None:
            position = _open_position(asset=asset, row=row, config=config)
            continue

        should_close, reason = _should_close(position=position, row=row, config=config)
        if should_close:
            trades.append(_close_position(position=position, row=row, reason=reason))
            position = None

    if position is not None:
        trades.append(_close_position(position=position, row=signal_frame.iloc[-1], reason="end_of_sample"))

    return pd.DataFrame(trades)


def summarize_convergence(asset: str, signal_frame: pd.DataFrame, trades: pd.DataFrame) -> dict:
    if signal_frame.empty:
        return {"asset": asset, "overlap_rows": 0}

    summary = {
        "asset": asset,
        "overlap_rows": int(len(signal_frame)),
        "start": signal_frame["timestamp"].min(),
        "end": signal_frame["timestamp"].max(),
        "mid_gap_mean_bps": float(signal_frame["mid_gap_bps"].mean()),
        "mid_gap_min_bps": float(signal_frame["mid_gap_bps"].min()),
        "mid_gap_max_bps": float(signal_frame["mid_gap_bps"].max()),
        "current_mid_gap_bps": float(signal_frame["mid_gap_bps"].iloc[-1]),
        "trade_count": int(len(trades)),
    }
    if trades.empty:
        summary.update(
            {
                "gross_pnl_usd": 0.0,
                "avg_trade_pnl_usd": 0.0,
                "win_rate": 0.0,
                "avg_entry_notional_usd": 0.0,
                "avg_holding_seconds": 0.0,
            }
        )
        return summary

    summary.update(
        {
            "gross_pnl_usd": float(trades["gross_pnl_usd"].sum()),
            "avg_trade_pnl_usd": float(trades["gross_pnl_usd"].mean()),
            "win_rate": float((trades["gross_pnl_usd"] > 0).mean()),
            "avg_entry_notional_usd": float(trades["entry_notional_usd"].mean()),
            "avg_holding_seconds": float(trades["holding_seconds"].mean()),
        }
    )
    return summary
