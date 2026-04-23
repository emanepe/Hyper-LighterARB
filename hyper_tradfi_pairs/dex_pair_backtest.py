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
    persistence_seconds: int = 1
    book_depth: int = 1
    fee_bps_per_leg: float = 0.0


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
        "bid_depth_5_notional",
        "ask_depth_5_sz",
        "ask_depth_5_notional",
        "bid_depth_20_sz",
        "bid_depth_20_notional",
        "ask_depth_20_sz",
        "ask_depth_20_notional",
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
    _ensure_depth_metrics(frame, "hyper")
    _ensure_depth_metrics(frame, "lighter")
    frame["hyper_rich_cap_notional"] = np.minimum.reduce(
        [frame["hyper_bid_notional"], frame["lighter_ask_notional"]]
    )
    frame["lighter_rich_cap_notional"] = np.minimum.reduce(
        [frame["lighter_bid_notional"], frame["hyper_ask_notional"]]
    )
    return frame


def _ensure_depth_metrics(frame: pd.DataFrame, prefix: str) -> None:
    for side in ("bid", "ask"):
        top_px = frame[f"{prefix}_best_{side}_px"]
        top_sz = frame[f"{prefix}_best_{side}_sz"]
        for depth in (5, 20):
            sz_column = f"{prefix}_{side}_depth_{depth}_sz"
            notional_column = f"{prefix}_{side}_depth_{depth}_notional"
            avg_px_column = f"{prefix}_{side}_depth_{depth}_avg_px"
            if sz_column not in frame:
                frame[sz_column] = top_sz
            if notional_column not in frame:
                frame[notional_column] = top_px * frame[sz_column]
            frame[avg_px_column] = np.where(
                frame[sz_column] > 0,
                frame[notional_column] / frame[sz_column],
                top_px,
            )


def _capacity_column(side: str, config: DexPairBacktestConfig) -> str:
    if config.book_depth <= 1:
        return f"{side}_cap_notional"
    if config.book_depth <= 5:
        return f"{side}_depth_5_cap_notional"
    return f"{side}_depth_20_cap_notional"


def _execution_price(row: pd.Series, venue: str, side: str, config: DexPairBacktestConfig) -> float:
    if config.book_depth <= 1:
        return float(row[f"{venue}_best_{side}_px"])
    if config.book_depth <= 5:
        return float(row[f"{venue}_{side}_depth_5_avg_px"])
    return float(row[f"{venue}_{side}_depth_20_avg_px"])


def _with_capacity_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame["hyper_rich_depth_5_cap_notional"] = np.minimum.reduce(
        [frame["hyper_bid_depth_5_notional"], frame["lighter_ask_depth_5_notional"]]
    )
    frame["lighter_rich_depth_5_cap_notional"] = np.minimum.reduce(
        [frame["lighter_bid_depth_5_notional"], frame["hyper_ask_depth_5_notional"]]
    )
    frame["hyper_rich_depth_20_cap_notional"] = np.minimum.reduce(
        [frame["hyper_bid_depth_20_notional"], frame["lighter_ask_depth_20_notional"]]
    )
    frame["lighter_rich_depth_20_cap_notional"] = np.minimum.reduce(
        [frame["lighter_bid_depth_20_notional"], frame["hyper_ask_depth_20_notional"]]
    )
    return frame


def _signal_side(row: pd.Series, config: DexPairBacktestConfig) -> str | None:
    if row["mid_gap_bps"] >= config.entry_gap_bps:
        if row["hyper_rich_entry_edge_bps"] < config.min_entry_edge_bps:
            return None
        cap_column = _capacity_column("hyper_rich", config)
        if float(row[cap_column]) < config.min_notional_usd:
            return None
        return "short_hyper_long_lighter"

    if row["mid_gap_bps"] <= -config.entry_gap_bps:
        if row["lighter_rich_entry_edge_bps"] < config.min_entry_edge_bps:
            return None
        cap_column = _capacity_column("lighter_rich", config)
        if float(row[cap_column]) < config.min_notional_usd:
            return None
        return "long_hyper_short_lighter"

    return None


def _open_position(asset: str, row: pd.Series, config: DexPairBacktestConfig) -> dict | None:
    # Positive gap: Hyperliquid is rich, so short Hyperliquid and long Lighter.
    if row["mid_gap_bps"] >= config.entry_gap_bps:
        if _signal_side(row, config) != "short_hyper_long_lighter":
            return None
        notional = min(float(row[_capacity_column("hyper_rich", config)]), config.max_notional_usd)
        if notional < config.min_notional_usd:
            return None
        hyper_entry_px = _execution_price(row, "hyper", "bid", config)
        lighter_entry_px = _execution_price(row, "lighter", "ask", config)
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
        if _signal_side(row, config) != "long_hyper_short_lighter":
            return None
        notional = min(float(row[_capacity_column("lighter_rich", config)]), config.max_notional_usd)
        if notional < config.min_notional_usd:
            return None
        hyper_entry_px = _execution_price(row, "hyper", "ask", config)
        lighter_entry_px = _execution_price(row, "lighter", "bid", config)
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
        hyper_exit_px = _execution_price(row, "hyper", "ask", position["config"])
        lighter_exit_px = _execution_price(row, "lighter", "bid", position["config"])
        hyper_pnl = position["hyper_qty"] * (position["hyper_entry_px"] - hyper_exit_px)
        lighter_pnl = position["lighter_qty"] * (lighter_exit_px - position["lighter_entry_px"])
    else:
        hyper_exit_px = _execution_price(row, "hyper", "bid", position["config"])
        lighter_exit_px = _execution_price(row, "lighter", "ask", position["config"])
        hyper_pnl = position["hyper_qty"] * (hyper_exit_px - position["hyper_entry_px"])
        lighter_pnl = position["lighter_qty"] * (position["lighter_entry_px"] - lighter_exit_px)

    holding_seconds = int((row["timestamp"] - position["entry_time"]).total_seconds())
    entry_fee_usd = position["entry_notional_usd"] * (position["fee_bps_per_leg"] / 10_000) * 2
    exit_fee_usd = position["entry_notional_usd"] * (position["fee_bps_per_leg"] / 10_000) * 2
    gross_pnl = hyper_pnl + lighter_pnl
    total_fees_usd = entry_fee_usd + exit_fee_usd
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
        "gross_pnl_usd": gross_pnl,
        "fees_usd": total_fees_usd,
        "net_pnl_usd": gross_pnl - total_fees_usd,
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
    qualifying_side: str | None = None
    qualifying_count = 0
    frame = _with_capacity_columns(signal_frame)
    for _index, row in frame.iterrows():
        if position is None:
            side = _signal_side(row, config)
            if side is None:
                qualifying_side = None
                qualifying_count = 0
                continue

            if side == qualifying_side:
                qualifying_count += 1
            else:
                qualifying_side = side
                qualifying_count = 1

            if qualifying_count >= max(1, config.persistence_seconds):
                position = _open_position(asset=asset, row=row, config=config)
                if position is not None:
                    position["fee_bps_per_leg"] = config.fee_bps_per_leg
                    position["config"] = config
                qualifying_side = None
                qualifying_count = 0
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
                "fees_usd": 0.0,
                "net_pnl_usd": 0.0,
                "avg_trade_pnl_usd": 0.0,
                "avg_trade_net_pnl_usd": 0.0,
                "win_rate": 0.0,
                "net_win_rate": 0.0,
                "avg_entry_notional_usd": 0.0,
                "avg_holding_seconds": 0.0,
            }
        )
        return summary

    summary.update(
        {
            "gross_pnl_usd": float(trades["gross_pnl_usd"].sum()),
            "fees_usd": float(trades["fees_usd"].sum()) if "fees_usd" in trades else 0.0,
            "net_pnl_usd": float(trades["net_pnl_usd"].sum()) if "net_pnl_usd" in trades else float(trades["gross_pnl_usd"].sum()),
            "avg_trade_pnl_usd": float(trades["gross_pnl_usd"].mean()),
            "avg_trade_net_pnl_usd": float(trades["net_pnl_usd"].mean()) if "net_pnl_usd" in trades else float(trades["gross_pnl_usd"].mean()),
            "win_rate": float((trades["gross_pnl_usd"] > 0).mean()),
            "net_win_rate": float((trades["net_pnl_usd"] > 0).mean()) if "net_pnl_usd" in trades else float((trades["gross_pnl_usd"] > 0).mean()),
            "avg_entry_notional_usd": float(trades["entry_notional_usd"].mean()),
            "avg_holding_seconds": float(trades["holding_seconds"].mean()),
        }
    )
    return summary
