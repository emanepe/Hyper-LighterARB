from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class BacktestConfig:
    lookback_seconds: int = 600
    entry_z: float = 2.0
    exit_z: float = 0.5
    gross_notional_usd: float = 10_000.0
    max_holding_seconds: int | None = None


def _first_present(columns: list[str], candidates: list[str]) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise ValueError(f"Missing expected columns. Tried: {', '.join(candidates)}")


def load_hyperliquid_snapshots(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        return df

    timestamp_column = _first_present(df.columns.tolist(), ["exchange_time", "captured_at_utc"])
    df["timestamp"] = pd.to_datetime(df[timestamp_column], utc=True, errors="coerce").dt.floor("s")
    df = df.sort_values(["timestamp", "captured_at_utc"]).dropna(subset=["timestamp"])
    df = df.groupby("timestamp", as_index=False).last()
    return df.rename(
        columns={
            "best_bid_px": "hyper_bid_px",
            "best_ask_px": "hyper_ask_px",
            "best_bid_sz": "hyper_bid_sz",
            "best_ask_sz": "hyper_ask_sz",
            "best_bid_orders": "hyper_bid_orders",
            "best_ask_orders": "hyper_ask_orders",
            "mid_px": "hyper_mid_px",
            "spread_bps": "hyper_spread_bps",
        }
    )[
        [
            "timestamp",
            "asset",
            "coin",
            "hyper_bid_px",
            "hyper_ask_px",
            "hyper_bid_sz",
            "hyper_ask_sz",
            "hyper_bid_orders",
            "hyper_ask_orders",
            "hyper_mid_px",
            "hyper_spread_bps",
        ]
    ]


def load_databento_bbo(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        return df

    timestamp_column = _first_present(df.columns.tolist(), ["ts_recv", "ts_event", "timestamp"])
    bid_px_column = _first_present(df.columns.tolist(), ["bid_px_00", "bid_px"])
    ask_px_column = _first_present(df.columns.tolist(), ["ask_px_00", "ask_px"])
    bid_sz_column = _first_present(df.columns.tolist(), ["bid_sz_00", "bid_sz"])
    ask_sz_column = _first_present(df.columns.tolist(), ["ask_sz_00", "ask_sz"])
    symbol_column = _first_present(df.columns.tolist(), ["symbol", "raw_symbol", "stype_out_symbol"])

    bid_ct_column = "bid_ct_00" if "bid_ct_00" in df.columns else None
    ask_ct_column = "ask_ct_00" if "ask_ct_00" in df.columns else None

    out = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(df[timestamp_column], utc=True, errors="coerce").dt.floor("s"),
            "tradfi_symbol": df[symbol_column],
            "tradfi_bid_px": pd.to_numeric(df[bid_px_column], errors="coerce"),
            "tradfi_ask_px": pd.to_numeric(df[ask_px_column], errors="coerce"),
            "tradfi_bid_sz": pd.to_numeric(df[bid_sz_column], errors="coerce"),
            "tradfi_ask_sz": pd.to_numeric(df[ask_sz_column], errors="coerce"),
            "tradfi_bid_orders": pd.to_numeric(df[bid_ct_column], errors="coerce") if bid_ct_column else 0,
            "tradfi_ask_orders": pd.to_numeric(df[ask_ct_column], errors="coerce") if ask_ct_column else 0,
        }
    )
    out = out.dropna(subset=["timestamp", "tradfi_bid_px", "tradfi_ask_px"])
    out = out.groupby("timestamp", as_index=False).last()
    out["tradfi_mid_px"] = (out["tradfi_bid_px"] + out["tradfi_ask_px"]) / 2.0
    out["tradfi_spread_bps"] = 10_000 * (out["tradfi_ask_px"] - out["tradfi_bid_px"]) / out["tradfi_mid_px"]
    return out


def build_signal_frame(hyper_path: Path, tradfi_path: Path, config: BacktestConfig) -> pd.DataFrame:
    hyper = load_hyperliquid_snapshots(hyper_path)
    tradfi = load_databento_bbo(tradfi_path)
    merged = hyper.merge(tradfi, on="timestamp", how="inner")
    if merged.empty:
        return merged

    merged["spread_bps"] = 10_000 * (merged["hyper_mid_px"] / merged["tradfi_mid_px"] - 1.0)
    rolling = merged["spread_bps"].rolling(config.lookback_seconds, min_periods=config.lookback_seconds)
    merged["roll_mean_bps"] = rolling.mean().shift(1)
    merged["roll_std_bps"] = rolling.std(ddof=0).shift(1)
    merged["zscore"] = (merged["spread_bps"] - merged["roll_mean_bps"]) / merged["roll_std_bps"]
    merged = merged.dropna(subset=["zscore"]).reset_index(drop=True)
    return merged


def _entry_payload(row: pd.Series, config: BacktestConfig, side: int) -> dict:
    if side == 1:
        hyper_entry_px = float(row["hyper_ask_px"])
        tradfi_entry_px = float(row["tradfi_bid_px"])
    else:
        hyper_entry_px = float(row["hyper_bid_px"])
        tradfi_entry_px = float(row["tradfi_ask_px"])

    return {
        "side": side,
        "entry_time": row["timestamp"],
        "entry_zscore": float(row["zscore"]),
        "entry_spread_bps": float(row["spread_bps"]),
        "hyper_entry_px": hyper_entry_px,
        "tradfi_entry_px": tradfi_entry_px,
        "hyper_qty": config.gross_notional_usd / hyper_entry_px,
        "tradfi_qty": config.gross_notional_usd / tradfi_entry_px,
    }


def _close_trade(position: dict, row: pd.Series) -> dict:
    if position["side"] == 1:
        hyper_exit_px = float(row["hyper_bid_px"])
        tradfi_exit_px = float(row["tradfi_ask_px"])
        hyper_pnl = position["hyper_qty"] * (hyper_exit_px - position["hyper_entry_px"])
        tradfi_pnl = position["tradfi_qty"] * (position["tradfi_entry_px"] - tradfi_exit_px)
        direction = "long_hyper_short_tradfi"
    else:
        hyper_exit_px = float(row["hyper_ask_px"])
        tradfi_exit_px = float(row["tradfi_bid_px"])
        hyper_pnl = position["hyper_qty"] * (position["hyper_entry_px"] - hyper_exit_px)
        tradfi_pnl = position["tradfi_qty"] * (tradfi_exit_px - position["tradfi_entry_px"])
        direction = "short_hyper_long_tradfi"

    holding_seconds = int((row["timestamp"] - position["entry_time"]).total_seconds())
    return {
        "entry_time": position["entry_time"],
        "exit_time": row["timestamp"],
        "direction": direction,
        "entry_zscore": position["entry_zscore"],
        "exit_zscore": float(row["zscore"]),
        "entry_spread_bps": position["entry_spread_bps"],
        "exit_spread_bps": float(row["spread_bps"]),
        "hyper_entry_px": position["hyper_entry_px"],
        "hyper_exit_px": hyper_exit_px,
        "tradfi_entry_px": position["tradfi_entry_px"],
        "tradfi_exit_px": tradfi_exit_px,
        "hyper_leg_pnl_usd": hyper_pnl,
        "tradfi_leg_pnl_usd": tradfi_pnl,
        "gross_pnl_usd": hyper_pnl + tradfi_pnl,
        "holding_seconds": holding_seconds,
    }


def run_pair_backtest(signal_frame: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    if signal_frame.empty:
        return pd.DataFrame()

    trades: list[dict] = []
    position: dict | None = None

    for _index, row in signal_frame.iterrows():
        if position is None:
            if row["zscore"] <= -config.entry_z:
                position = _entry_payload(row=row, config=config, side=1)
            elif row["zscore"] >= config.entry_z:
                position = _entry_payload(row=row, config=config, side=-1)
            continue

        timed_out = (
            config.max_holding_seconds is not None
            and (row["timestamp"] - position["entry_time"]).total_seconds() >= config.max_holding_seconds
        )
        exited_normally = abs(float(row["zscore"])) <= config.exit_z

        if timed_out or exited_normally:
            trades.append(_close_trade(position, row))
            position = None

    if position is not None:
        trades.append(_close_trade(position, signal_frame.iloc[-1]))

    return pd.DataFrame(trades)


def summarize_trades(asset: str, trades: pd.DataFrame, config: BacktestConfig) -> dict:
    if trades.empty:
        return {
            "asset": asset,
            "trade_count": 0,
            "gross_pnl_usd": 0.0,
            "avg_trade_pnl_usd": 0.0,
            "win_rate": 0.0,
            "avg_holding_seconds": 0.0,
            "entry_z": config.entry_z,
            "exit_z": config.exit_z,
            "gross_notional_usd_per_leg": config.gross_notional_usd,
        }

    return {
        "asset": asset,
        "trade_count": int(len(trades)),
        "gross_pnl_usd": float(trades["gross_pnl_usd"].sum()),
        "avg_trade_pnl_usd": float(trades["gross_pnl_usd"].mean()),
        "win_rate": float((trades["gross_pnl_usd"] > 0).mean()),
        "avg_holding_seconds": float(trades["holding_seconds"].mean()),
        "entry_z": config.entry_z,
        "exit_z": config.exit_z,
        "gross_notional_usd_per_leg": config.gross_notional_usd,
    }


def write_backtest_outputs(
    output_dir: Path,
    asset: str,
    date_string: str,
    config: BacktestConfig,
    signal_frame: pd.DataFrame,
    trades: pd.DataFrame,
) -> dict:
    asset_dir = output_dir / asset
    asset_dir.mkdir(parents=True, exist_ok=True)

    signal_path = asset_dir / f"{date_string}_signals.csv"
    trades_path = asset_dir / f"{date_string}_trades.csv"

    signal_frame.to_csv(signal_path, index=False)
    trades.to_csv(trades_path, index=False)

    summary = summarize_trades(asset=asset, trades=trades, config=config)
    summary["signal_rows"] = int(len(signal_frame))
    summary["signal_path"] = str(signal_path)
    summary["trades_path"] = str(trades_path)
    return summary

