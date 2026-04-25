from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "hyper_tradfi_pairs" / "data"
REPORT_DIR = ROOT / "reports"
ASSETS = ("BRENTOIL", "GOLD", "SILVER", "WTI")
VENUES = ("hyperliquid", "lighter")
FRESHNESS_SECONDS = 2.0


@dataclass(frozen=True)
class TradeConfig:
    entry_gap_bps: float = 8.0
    entry_edge_bps: float = 5.0
    exit_gap_bps: float = 0.25
    min_depth5_notional_usd: float = 100.0
    max_notional_usd: float = 10_000.0
    max_holding_seconds: int = 600
    persistence_seconds: int = 2
    fee_bps_per_leg: float = 1.0


def load_snapshots(path: Path, prefix: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame()

    df["captured_at"] = pd.to_datetime(df["captured_at_utc"], utc=True, errors="coerce")
    df["exchange_at"] = pd.to_datetime(df["exchange_time"], utc=True, errors="coerce")
    df["timestamp"] = df["captured_at"].dt.floor("s")
    df["lag_seconds"] = (df["captured_at"] - df["exchange_at"]).dt.total_seconds().abs()
    df = df.dropna(subset=["timestamp"])
    df = df.sort_values(["timestamp", "captured_at_utc"]).groupby("timestamp", as_index=False).last()

    keep = [
        "timestamp",
        "lag_seconds",
        "mid_px",
        "best_bid_px",
        "best_bid_sz",
        "best_ask_px",
        "best_ask_sz",
        "spread_bps",
        "bid_depth_5_notional",
        "ask_depth_5_notional",
        "bid_depth_20_notional",
        "ask_depth_20_notional",
    ]
    keep = [column for column in keep if column in df.columns]
    out = df[keep].copy()
    return out.rename(columns={column: f"{prefix}_{column}" for column in keep if column != "timestamp"})


def available_dates() -> list[str]:
    dates: set[str] = set()
    for asset in ASSETS:
        for venue in VENUES:
            for path in (DATA_DIR / venue / asset).glob("*.csv"):
                dates.add(path.stem)
    return sorted(dates)


def build_asset_date_frame(asset: str, date: str) -> pd.DataFrame:
    hyper_path = DATA_DIR / "hyperliquid" / asset / f"{date}.csv"
    lighter_path = DATA_DIR / "lighter" / asset / f"{date}.csv"
    if not hyper_path.exists() or not lighter_path.exists():
        return pd.DataFrame()

    hyper = load_snapshots(hyper_path, "hyper")
    lighter = load_snapshots(lighter_path, "lighter")
    if hyper.empty or lighter.empty:
        return pd.DataFrame()

    frame = hyper.merge(lighter, on="timestamp", how="inner")
    if frame.empty:
        return frame

    numeric_cols = [column for column in frame.columns if column != "timestamp"]
    frame[numeric_cols] = frame[numeric_cols].apply(pd.to_numeric, errors="coerce")
    frame["asset"] = asset
    frame["date"] = date
    frame["fresh"] = (
        (frame["hyper_lag_seconds"] <= FRESHNESS_SECONDS)
        & (frame["lighter_lag_seconds"] <= FRESHNESS_SECONDS)
    )
    frame["mid_gap_bps"] = 10_000 * (frame["hyper_mid_px"] / frame["lighter_mid_px"] - 1.0)
    frame["abs_gap_bps"] = frame["mid_gap_bps"].abs()
    frame["rich_exchange"] = np.where(
        frame["mid_gap_bps"] > 0,
        "Hyperliquid",
        np.where(frame["mid_gap_bps"] < 0, "Lighter", "Flat"),
    )
    frame["hyper_rich_entry_edge_bps"] = 10_000 * (
        frame["hyper_best_bid_px"] / frame["lighter_best_ask_px"] - 1.0
    )
    frame["lighter_rich_entry_edge_bps"] = 10_000 * (
        frame["lighter_best_bid_px"] / frame["hyper_best_ask_px"] - 1.0
    )
    frame["hyper_bid_notional"] = frame["hyper_best_bid_px"] * frame["hyper_best_bid_sz"]
    frame["hyper_ask_notional"] = frame["hyper_best_ask_px"] * frame["hyper_best_ask_sz"]
    frame["lighter_bid_notional"] = frame["lighter_best_bid_px"] * frame["lighter_best_bid_sz"]
    frame["lighter_ask_notional"] = frame["lighter_best_ask_px"] * frame["lighter_best_ask_sz"]
    frame["hyper_rich_top_cap_usd"] = np.minimum(
        frame["hyper_bid_notional"], frame["lighter_ask_notional"]
    )
    frame["lighter_rich_top_cap_usd"] = np.minimum(
        frame["lighter_bid_notional"], frame["hyper_ask_notional"]
    )
    frame["hyper_rich_depth5_cap_usd"] = np.minimum(
        frame["hyper_bid_depth_5_notional"], frame["lighter_ask_depth_5_notional"]
    )
    frame["lighter_rich_depth5_cap_usd"] = np.minimum(
        frame["lighter_bid_depth_5_notional"], frame["hyper_ask_depth_5_notional"]
    )
    return frame


def build_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    frames = [build_asset_date_frame(asset, date) for asset in ASSETS for date in available_dates()]
    all_rows = pd.concat([frame for frame in frames if not frame.empty], ignore_index=True)
    fresh = all_rows[all_rows["fresh"]].copy()
    fresh = fresh.sort_values(["asset", "timestamp"]).reset_index(drop=True)
    return all_rows, fresh


def sign_cross_count(series: pd.Series) -> int:
    signs = np.sign(series).replace(0, np.nan).ffill()
    return int(((signs.diff() != 0) & signs.notna()).sum())


def price_gap_summary(fresh: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for asset, group in fresh.groupby("asset", sort=False):
        rows.append(
            {
                "asset": asset,
                "fresh_seconds": len(group),
                "start_utc": group["timestamp"].min(),
                "end_utc": group["timestamp"].max(),
                "mean_gap_bps": group["mid_gap_bps"].mean(),
                "median_gap_bps": group["mid_gap_bps"].median(),
                "std_gap_bps": group["mid_gap_bps"].std(),
                "abs_gap_p50_bps": group["abs_gap_bps"].quantile(0.50),
                "abs_gap_p90_bps": group["abs_gap_bps"].quantile(0.90),
                "abs_gap_p99_bps": group["abs_gap_bps"].quantile(0.99),
                "hyper_expensive_pct": (group["mid_gap_bps"] > 0).mean() * 100,
                "lighter_expensive_pct": (group["mid_gap_bps"] < 0).mean() * 100,
                "zero_crosses": sign_cross_count(group["mid_gap_bps"]),
                "pct_abs_gap_gt_5bps": (group["abs_gap_bps"] > 5).mean() * 100,
                "pct_abs_gap_gt_8bps": (group["abs_gap_bps"] > 8).mean() * 100,
                "median_top_cap_usd": pd.concat(
                    [group["hyper_rich_top_cap_usd"], group["lighter_rich_top_cap_usd"]]
                ).median(),
                "median_depth5_cap_usd": pd.concat(
                    [group["hyper_rich_depth5_cap_usd"], group["lighter_rich_depth5_cap_usd"]]
                ).median(),
            }
        )
    return pd.DataFrame(rows)


def cluster_summary(fresh: pd.DataFrame, config: TradeConfig) -> pd.DataFrame:
    frame = fresh.copy()
    frame["bucket5_utc"] = frame["timestamp"].dt.floor("5min")
    frame["strict_signal"] = (
        (
            (frame["mid_gap_bps"] >= config.entry_gap_bps)
            & (frame["hyper_rich_entry_edge_bps"] >= config.entry_edge_bps)
        )
        | (
            (frame["mid_gap_bps"] <= -config.entry_gap_bps)
            & (frame["lighter_rich_entry_edge_bps"] >= config.entry_edge_bps)
        )
    )
    grouped = (
        frame.groupby(["asset", "bucket5_utc"])
        .agg(
            rows=("mid_gap_bps", "size"),
            mean_gap_bps=("mid_gap_bps", "mean"),
            mean_abs_gap_bps=("abs_gap_bps", "mean"),
            max_abs_gap_bps=("abs_gap_bps", "max"),
            seconds_abs_gap_gt_5bps=("abs_gap_bps", lambda s: int((s > 5).sum())),
            strict_signal_seconds=("strict_signal", "sum"),
            hyper_expensive_pct=("mid_gap_bps", lambda s: (s > 0).mean() * 100),
        )
        .reset_index()
    )
    grouped["pct_seconds_abs_gap_gt_5bps"] = (
        grouped["seconds_abs_gap_gt_5bps"] / grouped["rows"] * 100
    )
    return grouped.sort_values(["seconds_abs_gap_gt_5bps", "max_abs_gap_bps"], ascending=False)


def signal_side(row: pd.Series, config: TradeConfig) -> str | None:
    if (
        row["mid_gap_bps"] >= config.entry_gap_bps
        and row["hyper_rich_entry_edge_bps"] >= config.entry_edge_bps
        and row["hyper_rich_depth5_cap_usd"] >= config.min_depth5_notional_usd
    ):
        return "short_hyper_long_lighter"
    if (
        row["mid_gap_bps"] <= -config.entry_gap_bps
        and row["lighter_rich_entry_edge_bps"] >= config.entry_edge_bps
        and row["lighter_rich_depth5_cap_usd"] >= config.min_depth5_notional_usd
    ):
        return "long_hyper_short_lighter"
    return None


def open_position(row: pd.Series, config: TradeConfig) -> dict | None:
    side = signal_side(row, config)
    if side == "short_hyper_long_lighter":
        notional = min(row["hyper_rich_depth5_cap_usd"], config.max_notional_usd)
        return {
            "asset": row["asset"],
            "direction": side,
            "entry_time": row["timestamp"],
            "entry_mid_gap_bps": row["mid_gap_bps"],
            "entry_edge_bps": row["hyper_rich_entry_edge_bps"],
            "entry_notional_usd": notional,
            "hyper_entry_px": row["hyper_best_bid_px"],
            "lighter_entry_px": row["lighter_best_ask_px"],
            "hyper_qty": notional / row["hyper_best_bid_px"],
            "lighter_qty": notional / row["lighter_best_ask_px"],
        }
    if side == "long_hyper_short_lighter":
        notional = min(row["lighter_rich_depth5_cap_usd"], config.max_notional_usd)
        return {
            "asset": row["asset"],
            "direction": side,
            "entry_time": row["timestamp"],
            "entry_mid_gap_bps": row["mid_gap_bps"],
            "entry_edge_bps": row["lighter_rich_entry_edge_bps"],
            "entry_notional_usd": notional,
            "hyper_entry_px": row["hyper_best_ask_px"],
            "lighter_entry_px": row["lighter_best_bid_px"],
            "hyper_qty": notional / row["hyper_best_ask_px"],
            "lighter_qty": notional / row["lighter_best_bid_px"],
        }
    return None


def close_position(position: dict, row: pd.Series, reason: str, config: TradeConfig) -> dict:
    if position["direction"] == "short_hyper_long_lighter":
        hyper_exit_px = row["hyper_best_ask_px"]
        lighter_exit_px = row["lighter_best_bid_px"]
        hyper_pnl = position["hyper_qty"] * (position["hyper_entry_px"] - hyper_exit_px)
        lighter_pnl = position["lighter_qty"] * (lighter_exit_px - position["lighter_entry_px"])
    else:
        hyper_exit_px = row["hyper_best_bid_px"]
        lighter_exit_px = row["lighter_best_ask_px"]
        hyper_pnl = position["hyper_qty"] * (hyper_exit_px - position["hyper_entry_px"])
        lighter_pnl = position["lighter_qty"] * (position["lighter_entry_px"] - lighter_exit_px)

    gross_pnl = hyper_pnl + lighter_pnl
    fees = position["entry_notional_usd"] * (config.fee_bps_per_leg / 10_000) * 4
    return {
        "asset": position["asset"],
        "direction": position["direction"],
        "entry_time": position["entry_time"],
        "exit_time": row["timestamp"],
        "exit_reason": reason,
        "holding_seconds": (row["timestamp"] - position["entry_time"]).total_seconds(),
        "entry_mid_gap_bps": position["entry_mid_gap_bps"],
        "exit_mid_gap_bps": row["mid_gap_bps"],
        "entry_edge_bps": position["entry_edge_bps"],
        "entry_notional_usd": position["entry_notional_usd"],
        "gross_pnl_usd": gross_pnl,
        "fees_usd": fees,
        "net_pnl_usd": gross_pnl - fees,
    }


def run_basis_trade_backtest(fresh: pd.DataFrame, config: TradeConfig) -> pd.DataFrame:
    trades = []
    for asset, group in fresh.groupby("asset", sort=False):
        position = None
        qualifying_side = None
        qualifying_count = 0
        group = group.sort_values("timestamp")

        for _, row in group.iterrows():
            if position is None:
                side = signal_side(row, config)
                if side is None:
                    qualifying_side = None
                    qualifying_count = 0
                    continue
                if side == qualifying_side:
                    qualifying_count += 1
                else:
                    qualifying_side = side
                    qualifying_count = 1
                if qualifying_count >= config.persistence_seconds:
                    position = open_position(row, config)
                    qualifying_side = None
                    qualifying_count = 0
                continue

            hold_seconds = (row["timestamp"] - position["entry_time"]).total_seconds()
            should_close = False
            reason = ""
            if hold_seconds >= config.max_holding_seconds:
                should_close = True
                reason = "max_hold"
            elif abs(row["mid_gap_bps"]) <= config.exit_gap_bps:
                should_close = True
                reason = "converged"
            elif position["direction"] == "short_hyper_long_lighter" and row["mid_gap_bps"] <= 0:
                should_close = True
                reason = "crossed"
            elif position["direction"] == "long_hyper_short_lighter" and row["mid_gap_bps"] >= 0:
                should_close = True
                reason = "crossed"

            if should_close:
                trades.append(close_position(position, row, reason, config))
                position = None

    return pd.DataFrame(trades)


def unresolved_signal_summary(fresh: pd.DataFrame, config: TradeConfig) -> pd.DataFrame:
    frame = fresh.copy()
    frame["entry_signal"] = frame.apply(lambda row: signal_side(row, config), axis=1)
    frame["active_entry_edge_bps"] = np.where(
        frame["entry_signal"] == "short_hyper_long_lighter",
        frame["hyper_rich_entry_edge_bps"],
        frame["lighter_rich_entry_edge_bps"],
    )
    unresolved = frame[frame["entry_signal"].notna()].groupby(["asset", "date", "entry_signal"]).agg(
        signal_seconds=("entry_signal", "size"),
        first_signal=("timestamp", "min"),
        last_signal=("timestamp", "max"),
        mean_gap_bps=("mid_gap_bps", "mean"),
        mean_edge_bps=("active_entry_edge_bps", "mean"),
    )
    if unresolved.empty:
        return pd.DataFrame()
    return unresolved.reset_index().sort_values("signal_seconds", ascending=False)


def add_text_page(pdf: PdfPages, lines: list[str], title: str | None = None) -> None:
    fig = plt.figure(figsize=(8.5, 11))
    fig.patch.set_facecolor("white")
    y = 0.96
    if title:
        fig.text(0.06, y, title, family="DejaVu Sans Mono", fontsize=15, weight="bold", va="top")
        y -= 0.04
    for line in lines:
        fontsize = 9.5
        weight = "normal"
        if line and set(line) == {"="}:
            fontsize = 8
        elif line.isupper() and len(line) < 60:
            fontsize = 11
            weight = "bold"
        fig.text(0.06, y, line, family="DejaVu Sans Mono", fontsize=fontsize, weight=weight, va="top")
        y -= 0.021 if line else 0.014
    fig.subplots_adjust(0, 0, 1, 1)
    pdf.savefig(fig)
    plt.close(fig)


def save_fig(pdf: PdfPages, fig: plt.Figure, path: Path) -> None:
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(path, dpi=140, bbox_inches="tight")
    pdf.savefig(fig)
    plt.close(fig)


def make_distribution_page(fresh: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Price Gap Distribution Analysis", fontsize=15, weight="bold")

    ax = axes[0, 0]
    for asset, group in fresh.groupby("asset", sort=False):
        ax.hist(group["mid_gap_bps"].clip(-15, 15), bins=60, alpha=0.45, label=asset)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.axvline(5, color="crimson", linestyle="--", linewidth=0.8)
    ax.axvline(-5, color="crimson", linestyle="--", linewidth=0.8)
    ax.set_title("Mid Gap Distribution (clipped +/-15 bps)")
    ax.set_xlabel("Hyperliquid mid - Lighter mid (bps)")
    ax.set_ylabel("Fresh seconds")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.25)

    ax = axes[0, 1]
    box_data = [group["mid_gap_bps"].clip(-15, 15).values for _, group in fresh.groupby("asset", sort=False)]
    ax.boxplot(box_data, tick_labels=list(ASSETS), showfliers=False)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("Gap Range by Asset (no outliers)")
    ax.set_ylabel("Gap bps")
    ax.grid(alpha=0.25)

    ax = axes[1, 0]
    summary = price_gap_summary(fresh)
    x = np.arange(len(summary))
    ax.bar(x - 0.2, summary["hyper_expensive_pct"], width=0.4, label="Hyperliquid expensive")
    ax.bar(x + 0.2, summary["lighter_expensive_pct"], width=0.4, label="Lighter expensive")
    ax.set_xticks(x)
    ax.set_xticklabels(summary["asset"])
    ax.set_ylim(0, 100)
    ax.set_title("Which Exchange Was More Expensive")
    ax.set_ylabel("% of fresh seconds")
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.25)

    ax = axes[1, 1]
    ax.bar(summary["asset"], summary["abs_gap_p90_bps"], label="P90 abs gap")
    ax.bar(summary["asset"], summary["abs_gap_p50_bps"], label="Median abs gap")
    ax.set_title("Typical vs Tail Gap Size")
    ax.set_ylabel("Absolute gap (bps)")
    ax.legend(fontsize=8)
    ax.grid(axis="y", alpha=0.25)
    return fig


def make_timing_page(fresh: pd.DataFrame, config: TradeConfig) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5), sharex=False)
    fig.suptitle("Timing and Clustering Analysis", fontsize=15, weight="bold")
    for ax, (asset, group) in zip(axes.ravel(), fresh.groupby("asset", sort=False)):
        group = group.sort_values("timestamp")
        strict = group[
            (
                (group["mid_gap_bps"] >= config.entry_gap_bps)
                & (group["hyper_rich_entry_edge_bps"] >= config.entry_edge_bps)
            )
            | (
                (group["mid_gap_bps"] <= -config.entry_gap_bps)
                & (group["lighter_rich_entry_edge_bps"] >= config.entry_edge_bps)
            )
        ]
        ax.plot(group["timestamp"], group["mid_gap_bps"], linewidth=0.65, color="#1f4e79")
        if not strict.empty:
            ax.scatter(strict["timestamp"], strict["mid_gap_bps"], s=8, color="#d55e00", label="strict signal")
        ax.axhline(0, color="black", linewidth=0.7)
        ax.axhline(config.entry_gap_bps, color="crimson", linestyle="--", linewidth=0.8)
        ax.axhline(-config.entry_gap_bps, color="crimson", linestyle="--", linewidth=0.8)
        ax.set_title(asset)
        ax.set_ylabel("Gap bps")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))
        ax.grid(alpha=0.25)
        if not strict.empty:
            ax.legend(fontsize=7)
    return fig


def make_trading_page(
    fresh: pd.DataFrame,
    clusters: pd.DataFrame,
    trades: pd.DataFrame,
) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Basis Convergence Analysis", fontsize=15, weight="bold")

    ax = axes[0, 0]
    top = clusters.head(10).sort_values("seconds_abs_gap_gt_5bps")
    labels = [f"{row.asset}\n{row.bucket5_utc:%m-%d %H:%M}" for row in top.itertuples()]
    ax.barh(labels, top["seconds_abs_gap_gt_5bps"], color="#4c78a8")
    ax.set_title("Top 5-Minute Gap Clusters")
    ax.set_xlabel("Seconds with |gap| > 5 bps")
    ax.grid(axis="x", alpha=0.25)

    ax = axes[0, 1]
    hourly = fresh.copy()
    hourly["utc_hour"] = hourly["timestamp"].dt.hour
    pivot = hourly.pivot_table(index="asset", columns="utc_hour", values="abs_gap_bps", aggfunc="mean")
    image = ax.imshow(pivot.values, aspect="auto", cmap="YlOrRd")
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns)
    ax.set_title("Mean Absolute Gap by UTC Hour")
    ax.set_xlabel("UTC hour")
    plt.colorbar(image, ax=ax, label="Abs gap bps")

    ax = axes[1, 0]
    if trades.empty:
        ax.text(0.5, 0.5, "No realized trades under strict rules", ha="center", va="center")
    else:
        by_asset = trades.groupby("asset")["net_pnl_usd"].sum().reindex(ASSETS).fillna(0)
        colors = ["#2ca02c" if value >= 0 else "#d62728" for value in by_asset]
        ax.bar(by_asset.index, by_asset.values, color=colors)
        ax.axhline(0, color="black", linewidth=0.8)
        ax.set_title("Strict Taker Backtest Net PnL")
        ax.set_ylabel("USD")
        ax.grid(axis="y", alpha=0.25)

    ax = axes[1, 1]
    if trades.empty:
        ax.text(0.5, 0.5, "No holding-period sample", ha="center", va="center")
    else:
        hold = trades.groupby("asset")["holding_seconds"].mean().reindex(ASSETS)
        prof = trades[trades["net_pnl_usd"] > 0].groupby("asset")["holding_seconds"].mean().reindex(ASSETS)
        x = np.arange(len(ASSETS))
        ax.bar(x - 0.2, hold.fillna(0), width=0.4, label="All realized")
        ax.bar(x + 0.2, prof.fillna(0), width=0.4, label="Profitable realized")
        ax.set_xticks(x)
        ax.set_xticklabels(ASSETS)
        ax.set_title("Average Holding Period")
        ax.set_ylabel("Seconds")
        ax.legend(fontsize=8)
        ax.grid(axis="y", alpha=0.25)
    return fig


def lines_for_executive(
    all_rows: pd.DataFrame,
    fresh: pd.DataFrame,
    summary: pd.DataFrame,
    trades: pd.DataFrame,
    config: TradeConfig,
) -> list[str]:
    stale_rows = len(all_rows) - len(fresh)
    stale_pct = stale_rows / len(all_rows) * 100 if len(all_rows) else 0
    lines = [
        "HYPERLIQUID <> LIGHTER RWA PRICE GAP ANALYSIS",
        "=" * 60,
        f"Report Generated: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"Repository: {ROOT}",
        f"Data Range: {fresh['timestamp'].min()} to {fresh['timestamp'].max()}",
        f"Assets: {', '.join(ASSETS)}",
        "",
        "EXECUTIVE SUMMARY",
        "=" * 60,
        f"Raw Matched Seconds: {len(all_rows):,}",
        f"Fresh Matched Seconds Used: {len(fresh):,}",
        f"Rows Removed by Freshness Filter: {stale_rows:,} ({stale_pct:.1f}%)",
        f"Freshness Rule: abs(captured_at_utc - exchange_time) <= {FRESHNESS_SECONDS:.0f}s on both venues",
        "",
        "PRICE GAP SUMMARY",
        "-----------------",
    ]
    for row in summary.itertuples(index=False):
        rich = "Hyperliquid" if row.hyper_expensive_pct >= row.lighter_expensive_pct else "Lighter"
        lines.append(
            f"{row.asset:<8} mean={row.mean_gap_bps:>6.2f} bps | median={row.median_gap_bps:>6.2f} bps | "
            f"p90_abs={row.abs_gap_p90_bps:>5.2f} bps | richer={rich:<11} | crosses={row.zero_crosses:>4}"
        )
    lines += [
        "",
        "STRICT PAIR-TRADE TEST",
        "----------------------",
        f"Entry: |mid gap| >= {config.entry_gap_bps:.1f} bps, executable edge >= {config.entry_edge_bps:.1f} bps",
        f"Depth: depth-5 capacity >= ${config.min_depth5_notional_usd:,.0f}, max notional ${config.max_notional_usd:,.0f}",
        f"Exit: gap <= {config.exit_gap_bps:.2f} bps, price cross, or {config.max_holding_seconds}s max hold",
        f"Fees: {config.fee_bps_per_leg:.1f} bps per leg per fill, four fills per basis trade",
    ]
    if trades.empty:
        lines.append("No realized trades under the strict rules.")
    else:
        lines += [
            f"Realized Trades: {len(trades):,}",
            f"Gross PnL: ${trades['gross_pnl_usd'].sum():,.2f}",
            f"Fees: ${trades['fees_usd'].sum():,.2f}",
            f"Net PnL: ${trades['net_pnl_usd'].sum():,.2f}",
            f"Net Win Rate: {(trades['net_pnl_usd'] > 0).mean() * 100:.1f}%",
            f"Avg Holding Period: {trades['holding_seconds'].mean():.1f}s",
            f"Avg Holding Period for Profitable Trades: {trades.loc[trades['net_pnl_usd'] > 0, 'holding_seconds'].mean():.1f}s",
        ]
    return lines


def lines_for_strategy(
    summary: pd.DataFrame,
    clusters: pd.DataFrame,
    trades: pd.DataFrame,
    unresolved: pd.DataFrame,
) -> list[str]:
    lines = [
        "PAIR-TRADE STRATEGY ANALYSIS",
        "=" * 60,
        "EXCHANGE RICHNESS",
        "-----------------",
    ]
    for row in summary.itertuples(index=False):
        if row.hyper_expensive_pct > row.lighter_expensive_pct:
            lines.append(f"{row.asset:<8} Hyperliquid was richer {row.hyper_expensive_pct:.1f}% of fresh seconds.")
        else:
            lines.append(f"{row.asset:<8} Lighter was richer {row.lighter_expensive_pct:.1f}% of fresh seconds.")

    lines += [
        "",
        "CONVERGENCE / CROSSING BEHAVIOR",
        "-------------------------------",
    ]
    for row in summary.itertuples(index=False):
        lines.append(
            f"{row.asset:<8} zero-cross count={row.zero_crosses:>4}, "
            f"|gap|>5bps={row.pct_abs_gap_gt_5bps:>5.1f}%, |gap|>8bps={row.pct_abs_gap_gt_8bps:>5.1f}%."
        )

    lines += [
        "",
        "TIME CLUSTERING",
        "---------------",
    ]
    for row in clusters.head(8).itertuples(index=False):
        lines.append(
            f"{row.asset:<8} {row.bucket5_utc:%Y-%m-%d %H:%M UTC}: "
            f"{row.seconds_abs_gap_gt_5bps:>3}s above 5bps, max_abs={row.max_abs_gap_bps:>5.2f}bps, "
            f"strict_signal_seconds={int(row.strict_signal_seconds)}."
        )

    lines += [
        "",
        "STRICT BACKTEST BY ASSET",
        "------------------------",
    ]
    if trades.empty:
        lines.append("No realized strict basis trades.")
    else:
        by_asset = trades.groupby("asset").agg(
            trades=("net_pnl_usd", "size"),
            net_pnl_usd=("net_pnl_usd", "sum"),
            win_rate=("net_pnl_usd", lambda s: (s > 0).mean() * 100),
            avg_hold_s=("holding_seconds", "mean"),
            profitable_avg_hold_s=("holding_seconds", lambda s: s[trades.loc[s.index, "net_pnl_usd"] > 0].mean()),
        )
        for row in by_asset.reset_index().itertuples(index=False):
            lines.append(
                f"{row.asset:<8} trades={row.trades:>2}, net=${row.net_pnl_usd:>7.2f}, "
                f"win={row.win_rate:>5.1f}%, avg_hold={row.avg_hold_s:>6.1f}s, "
                f"profitable_avg_hold={row.profitable_avg_hold_s:>6.1f}s."
            )

    lines += [
        "",
        "KEY OBSERVATIONS",
        "----------------",
        "1. Most gaps are small: the normal regime is roughly 2-6 bps, so taker fees matter.",
        "2. Hyperliquid was structurally richer in BRENTOIL and GOLD; Lighter was richer in SILVER and WTI.",
        "3. The strongest strict realized opportunities came from BRENTOIL around 2026-04-23 12:45 UTC.",
        "4. WTI showed frequent convergence but poor displayed Hyperliquid capacity, so size must be capped.",
        "5. SILVER had large late-sample Lighter-rich clusters, but the sample ended before proving realized convergence.",
    ]
    if not unresolved.empty:
        top = unresolved.head(3)
        lines += [
            "",
            "SUSTAINED STRICT SIGNAL CLUSTERS",
            "--------------------------------",
        ]
        for row in top.itertuples(index=False):
            lines.append(
                f"{row.asset:<8} {row.date} {row.entry_signal}: {row.signal_seconds}s of signals, "
                f"{row.first_signal:%H:%M:%S}-{row.last_signal:%H:%M:%S} UTC."
            )
    return lines


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = REPORT_DIR / f"hyper_lighter_gap_analysis_{datetime.now():%Y%m%d_%H%M%S}"
    output_dir.mkdir(parents=True, exist_ok=True)

    config = TradeConfig()
    all_rows, fresh = build_frames()
    summary = price_gap_summary(fresh)
    clusters = cluster_summary(fresh, config)
    trades = run_basis_trade_backtest(fresh, config)
    unresolved = unresolved_signal_summary(fresh, config)

    summary.to_csv(output_dir / "price_gap_summary.csv", index=False)
    clusters.to_csv(output_dir / "time_cluster_summary.csv", index=False)
    trades.to_csv(output_dir / "strict_basis_trade_trades.csv", index=False)
    unresolved.to_csv(output_dir / "strict_signal_seconds.csv", index=False)

    pdf_path = output_dir / "hyper_lighter_gap_analysis_report.pdf"
    with PdfPages(pdf_path) as pdf:
        add_text_page(pdf, lines_for_executive(all_rows, fresh, summary, trades, config))
        save_fig(pdf, make_distribution_page(fresh), output_dir / "price_gap_distribution.png")
        save_fig(pdf, make_timing_page(fresh, config), output_dir / "timing_gap_clusters.png")
        save_fig(pdf, make_trading_page(fresh, clusters, trades), output_dir / "basis_convergence.png")
        add_text_page(pdf, lines_for_strategy(summary, clusters, trades, unresolved))

    print(f"report_dir={output_dir}")
    print(f"pdf={pdf_path}")
    print(summary.round(3).to_string(index=False))
    if not trades.empty:
        print(
            trades.groupby("asset")
            .agg(
                trades=("net_pnl_usd", "size"),
                gross_pnl_usd=("gross_pnl_usd", "sum"),
                net_pnl_usd=("net_pnl_usd", "sum"),
                win_rate=("net_pnl_usd", lambda s: (s > 0).mean() * 100),
                avg_hold_s=("holding_seconds", "mean"),
            )
            .round(3)
            .to_string()
        )


if __name__ == "__main__":
    main()
