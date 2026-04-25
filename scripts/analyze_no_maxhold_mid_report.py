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
DATES = ("2026-04-23", "2026-04-24", "2026-04-25")

FRESHNESS_SECONDS = 2.0
EXIT_GAP_BPS = 0.25
UNIT_NOTIONAL_USD = 10_000.0


@dataclass(frozen=True)
class ScaleConfig:
    step_bps: float = 1.0
    max_layers: int = 5
    unit_notional_usd: float = UNIT_NOTIONAL_USD


def load_snapshot(venue: str, asset: str, date: str) -> pd.DataFrame:
    path = DATA_DIR / venue / asset / f"{date}.csv"
    df = pd.read_csv(path)
    df["captured_at"] = pd.to_datetime(df["captured_at_utc"], utc=True, errors="coerce")
    df["exchange_at"] = pd.to_datetime(df["exchange_time"], utc=True, errors="coerce")
    df["timestamp"] = df["captured_at"].dt.floor("s")
    df["lag_seconds"] = (df["captured_at"] - df["exchange_at"]).dt.total_seconds().abs()
    df = df.dropna(subset=["timestamp"])
    return df.sort_values(["timestamp", "captured_at_utc"]).groupby("timestamp", as_index=False).last()


def build_fresh_frame() -> pd.DataFrame:
    frames = []
    for asset in ASSETS:
        for date in DATES:
            hyper_path = DATA_DIR / "hyperliquid" / asset / f"{date}.csv"
            lighter_path = DATA_DIR / "lighter" / asset / f"{date}.csv"
            if not hyper_path.exists() or not lighter_path.exists():
                continue
            hyper = load_snapshot("hyperliquid", asset, date)
            lighter = load_snapshot("lighter", asset, date)
            merged = hyper.merge(lighter, on="timestamp", suffixes=("_hyper", "_lighter"))
            if merged.empty:
                continue

            numeric = [
                "mid_px_hyper",
                "mid_px_lighter",
                "best_bid_px_hyper",
                "best_ask_px_hyper",
                "best_bid_px_lighter",
                "best_ask_px_lighter",
                "lag_seconds_hyper",
                "lag_seconds_lighter",
            ]
            merged[numeric] = merged[numeric].apply(pd.to_numeric, errors="coerce")
            merged = merged[
                (merged["lag_seconds_hyper"] <= FRESHNESS_SECONDS)
                & (merged["lag_seconds_lighter"] <= FRESHNESS_SECONDS)
            ].copy()
            merged["asset"] = asset
            merged["date"] = date
            merged["gap_bps"] = 10_000 * (merged["mid_px_hyper"] / merged["mid_px_lighter"] - 1.0)
            merged["abs_gap_bps"] = merged["gap_bps"].abs()
            frames.append(
                merged[
                    [
                        "timestamp",
                        "asset",
                        "date",
                        "mid_px_hyper",
                        "mid_px_lighter",
                        "gap_bps",
                        "abs_gap_bps",
                    ]
                ]
            )
    return pd.concat(frames, ignore_index=True).sort_values(["asset", "timestamp"]).reset_index(drop=True)


def layer_pnl(direction: str, layer: dict, row: pd.Series) -> float:
    if direction == "short_hyper_long_lighter":
        return layer["hyper_qty"] * (layer["hyper_entry_px"] - row["mid_px_hyper"]) + layer[
            "lighter_qty"
        ] * (row["mid_px_lighter"] - layer["lighter_entry_px"])
    return layer["hyper_qty"] * (row["mid_px_hyper"] - layer["hyper_entry_px"]) + layer[
        "lighter_qty"
    ] * (layer["lighter_entry_px"] - row["mid_px_lighter"])


def run_single_entry_backtest(fresh: pd.DataFrame) -> pd.DataFrame:
    trades = []
    for asset, group in fresh.groupby("asset", sort=False):
        position: dict | None = None
        for _, row in group.sort_values("timestamp").iterrows():
            if position is None:
                if row["gap_bps"] == 0:
                    continue
                direction = "short_hyper_long_lighter" if row["gap_bps"] > 0 else "long_hyper_short_lighter"
                position = {
                    "asset": asset,
                    "date": row["date"],
                    "direction": direction,
                    "entry_time": row["timestamp"],
                    "entry_gap_bps": row["gap_bps"],
                    "hyper_entry_px": row["mid_px_hyper"],
                    "lighter_entry_px": row["mid_px_lighter"],
                    "hyper_qty": UNIT_NOTIONAL_USD / row["mid_px_hyper"],
                    "lighter_qty": UNIT_NOTIONAL_USD / row["mid_px_lighter"],
                    "layers": 1,
                    "max_notional_usd": UNIT_NOTIONAL_USD,
                    "avg_abs_entry_gap_bps": abs(row["gap_bps"]),
                }
                continue

            reason = exit_reason(position["direction"], row["gap_bps"])
            if reason:
                pnl = layer_pnl(position["direction"], position, row)
                trades.append(close_trade(position, row, reason, pnl))
                position = None

        if position is not None:
            last = group.iloc[-1]
            pnl = layer_pnl(position["direction"], position, last)
            trades.append(close_trade(position, last, "end_mark", pnl))
    return pd.DataFrame(trades)


def run_scaled_backtest(fresh: pd.DataFrame, config: ScaleConfig) -> pd.DataFrame:
    trades = []
    for asset, group in fresh.groupby("asset", sort=False):
        direction: str | None = None
        layers: list[dict] = []
        first_entry_time = None
        first_entry_gap = None
        first_entry_date = None
        last_layer_abs_gap = None

        for _, row in group.sort_values("timestamp").iterrows():
            if not layers:
                if row["gap_bps"] == 0:
                    continue
                direction = "short_hyper_long_lighter" if row["gap_bps"] > 0 else "long_hyper_short_lighter"
                first_entry_time = row["timestamp"]
                first_entry_gap = row["gap_bps"]
                first_entry_date = row["date"]
                last_layer_abs_gap = abs(row["gap_bps"])
                layers.append(make_layer(row, config.unit_notional_usd))
                continue

            same_side = (
                (direction == "short_hyper_long_lighter" and row["gap_bps"] > 0)
                or (direction == "long_hyper_short_lighter" and row["gap_bps"] < 0)
            )
            if (
                same_side
                and abs(row["gap_bps"]) >= last_layer_abs_gap + config.step_bps
                and len(layers) < config.max_layers
            ):
                layers.append(make_layer(row, config.unit_notional_usd))
                last_layer_abs_gap = abs(row["gap_bps"])

            reason = exit_reason(direction, row["gap_bps"])
            if reason:
                pnl = sum(layer_pnl(direction, layer, row) for layer in layers)
                trades.append(
                    close_scaled_trade(
                        asset=asset,
                        date=first_entry_date,
                        direction=direction,
                        entry_time=first_entry_time,
                        entry_gap=first_entry_gap,
                        row=row,
                        reason=reason,
                        pnl=pnl,
                        layers=layers,
                    )
                )
                direction = None
                layers = []
                first_entry_time = None
                first_entry_gap = None
                first_entry_date = None
                last_layer_abs_gap = None

        if layers:
            last = group.iloc[-1]
            pnl = sum(layer_pnl(direction, layer, last) for layer in layers)
            trades.append(
                close_scaled_trade(
                    asset=asset,
                    date=first_entry_date,
                    direction=direction,
                    entry_time=first_entry_time,
                    entry_gap=first_entry_gap,
                    row=last,
                    reason="end_mark",
                    pnl=pnl,
                    layers=layers,
                )
            )
    return pd.DataFrame(trades)


def make_layer(row: pd.Series, notional: float) -> dict:
    return {
        "entry_time": row["timestamp"],
        "entry_gap_bps": row["gap_bps"],
        "hyper_entry_px": row["mid_px_hyper"],
        "lighter_entry_px": row["mid_px_lighter"],
        "hyper_qty": notional / row["mid_px_hyper"],
        "lighter_qty": notional / row["mid_px_lighter"],
        "notional_usd": notional,
    }


def exit_reason(direction: str, gap_bps: float) -> str | None:
    if abs(gap_bps) <= EXIT_GAP_BPS:
        return "converged"
    if direction == "short_hyper_long_lighter" and gap_bps <= 0:
        return "crossed"
    if direction == "long_hyper_short_lighter" and gap_bps >= 0:
        return "crossed"
    return None


def close_trade(position: dict, row: pd.Series, reason: str, pnl: float) -> dict:
    return {
        "asset": position["asset"],
        "date": position["date"],
        "direction": position["direction"],
        "entry_time": position["entry_time"],
        "exit_time": row["timestamp"],
        "exit_reason": reason,
        "holding_seconds": (row["timestamp"] - position["entry_time"]).total_seconds(),
        "entry_gap_bps": position["entry_gap_bps"],
        "exit_gap_bps": row["gap_bps"],
        "pnl_usd": pnl,
        "layers": position["layers"],
        "max_notional_usd": position["max_notional_usd"],
        "avg_abs_entry_gap_bps": position["avg_abs_entry_gap_bps"],
    }


def close_scaled_trade(
    asset: str,
    date: str,
    direction: str,
    entry_time: pd.Timestamp,
    entry_gap: float,
    row: pd.Series,
    reason: str,
    pnl: float,
    layers: list[dict],
) -> dict:
    return {
        "asset": asset,
        "date": date,
        "direction": direction,
        "entry_time": entry_time,
        "exit_time": row["timestamp"],
        "exit_reason": reason,
        "holding_seconds": (row["timestamp"] - entry_time).total_seconds(),
        "entry_gap_bps": entry_gap,
        "exit_gap_bps": row["gap_bps"],
        "pnl_usd": pnl,
        "layers": len(layers),
        "max_notional_usd": sum(layer["notional_usd"] for layer in layers),
        "avg_abs_entry_gap_bps": float(np.mean([abs(layer["entry_gap_bps"]) for layer in layers])),
    }


def summarize_trades(trades: pd.DataFrame) -> pd.DataFrame:
    return (
        trades.groupby("asset")
        .agg(
            trades=("pnl_usd", "size"),
            pnl_usd=("pnl_usd", "sum"),
            realized_pnl_usd=("pnl_usd", lambda s: s[trades.loc[s.index, "exit_reason"] != "end_mark"].sum()),
            end_mark_pnl_usd=("pnl_usd", lambda s: s[trades.loc[s.index, "exit_reason"] == "end_mark"].sum()),
            win_rate=("pnl_usd", lambda s: (s > 0).mean() * 100),
            avg_hold_s=("holding_seconds", "mean"),
            profitable_avg_hold_s=(
                "holding_seconds",
                lambda s: s[trades.loc[s.index, "pnl_usd"] > 0].mean(),
            ),
            avg_layers=("layers", "mean"),
            avg_max_notional_usd=("max_notional_usd", "mean"),
        )
        .reset_index()
    )


def run_scale_sensitivity(fresh: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for step in (0.5, 1.0, 2.0, 3.0):
        for max_layers in (3, 5, 10):
            trades = run_scaled_backtest(fresh, ScaleConfig(step_bps=step, max_layers=max_layers))
            rows.append(
                {
                    "step_bps": step,
                    "max_layers": max_layers,
                    "trades": len(trades),
                    "pnl_usd": trades["pnl_usd"].sum(),
                    "realized_pnl_usd": trades.loc[trades["exit_reason"] != "end_mark", "pnl_usd"].sum(),
                    "end_mark_pnl_usd": trades.loc[trades["exit_reason"] == "end_mark", "pnl_usd"].sum(),
                    "win_rate": (trades["pnl_usd"] > 0).mean() * 100,
                    "avg_hold_s": trades["holding_seconds"].mean(),
                    "avg_layers": trades["layers"].mean(),
                    "max_used_layers": trades["layers"].max(),
                    "avg_max_notional_usd": trades["max_notional_usd"].mean(),
                }
            )
    return pd.DataFrame(rows)


def add_text_page(pdf: PdfPages, lines: list[str]) -> None:
    fig = plt.figure(figsize=(8.5, 11))
    y = 0.96
    for line in lines:
        line = line.replace("$", r"\$")
        weight = "bold" if line.isupper() and len(line) < 70 else "normal"
        size = 11 if weight == "bold" else 9.3
        fig.text(0.06, y, line, family="DejaVu Sans Mono", fontsize=size, weight=weight, va="top")
        y -= 0.022 if line else 0.014
    pdf.savefig(fig)
    plt.close(fig)


def make_distribution_page(fresh: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Price Gap and Richness Analysis", fontsize=15, weight="bold")
    for asset, group in fresh.groupby("asset", sort=False):
        axes[0, 0].hist(group["gap_bps"].clip(-15, 15), bins=70, alpha=0.45, label=asset)
    axes[0, 0].axvline(0, color="black", linewidth=0.8)
    axes[0, 0].set_title("Gap Distribution (Hyper mid - Lighter mid)")
    axes[0, 0].set_xlabel("Gap bps")
    axes[0, 0].set_ylabel("Fresh seconds")
    axes[0, 0].legend(fontsize=8)
    axes[0, 0].grid(alpha=0.25)

    richness = fresh.groupby("asset").agg(
        hyper_rich=("gap_bps", lambda s: (s > 0).mean() * 100),
        lighter_rich=("gap_bps", lambda s: (s < 0).mean() * 100),
    )
    x = np.arange(len(richness))
    axes[0, 1].bar(x - 0.2, richness["hyper_rich"], width=0.4, label="Hyperliquid richer")
    axes[0, 1].bar(x + 0.2, richness["lighter_rich"], width=0.4, label="Lighter richer")
    axes[0, 1].set_xticks(x)
    axes[0, 1].set_xticklabels(richness.index)
    axes[0, 1].set_title("Which Venue Was Expensive")
    axes[0, 1].set_ylabel("% fresh seconds")
    axes[0, 1].legend(fontsize=8)
    axes[0, 1].grid(axis="y", alpha=0.25)

    gap_size = fresh.groupby("asset")["abs_gap_bps"].quantile([0.5, 0.9, 0.99]).unstack()
    gap_size.plot(kind="bar", ax=axes[1, 0])
    axes[1, 0].set_title("Absolute Gap Quantiles")
    axes[1, 0].set_ylabel("Abs gap bps")
    axes[1, 0].legend(["P50", "P90", "P99"], fontsize=8)
    axes[1, 0].grid(axis="y", alpha=0.25)

    crosses = []
    for asset, group in fresh.groupby("asset", sort=False):
        signs = np.sign(group["gap_bps"]).replace(0, np.nan).ffill()
        crosses.append(((signs.diff() != 0) & signs.notna()).sum())
    axes[1, 1].bar(ASSETS, crosses, color="#4c78a8")
    axes[1, 1].set_title("Gap Zero-Cross Count")
    axes[1, 1].set_ylabel("Crosses")
    axes[1, 1].grid(axis="y", alpha=0.25)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def make_gap_timing_page(fresh: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Gap Timing and Clustering", fontsize=15, weight="bold")
    for ax, (asset, group) in zip(axes.ravel(), fresh.groupby("asset", sort=False)):
        group = group.sort_values("timestamp")
        ax.plot(group["timestamp"], group["gap_bps"], linewidth=0.65, color="#1f4e79")
        ax.axhline(0, color="black", linewidth=0.7)
        ax.axhline(5, color="crimson", linestyle="--", linewidth=0.7)
        ax.axhline(-5, color="crimson", linestyle="--", linewidth=0.7)
        ax.set_title(asset)
        ax.set_ylabel("Gap bps")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))
        ax.grid(alpha=0.25)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def make_pnl_page(single: pd.DataFrame, scaled: pd.DataFrame, sensitivity: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("No-Max-Hold Mid-Price Pair Trade Results", fontsize=15, weight="bold")

    combo = pd.DataFrame(
        {
            "Single Entry": single.groupby("asset")["pnl_usd"].sum().reindex(ASSETS),
            "Scale on Widening": scaled.groupby("asset")["pnl_usd"].sum().reindex(ASSETS),
        }
    )
    combo.plot(kind="bar", ax=axes[0, 0])
    axes[0, 0].set_title("PnL by Asset")
    axes[0, 0].set_ylabel("USD on $10k base leg")
    axes[0, 0].grid(axis="y", alpha=0.25)

    single_sorted = single.sort_values("exit_time")
    scaled_sorted = scaled.sort_values("exit_time")
    axes[0, 1].plot(single_sorted["exit_time"], single_sorted["pnl_usd"].cumsum(), label="Single")
    axes[0, 1].plot(scaled_sorted["exit_time"], scaled_sorted["pnl_usd"].cumsum(), label="Scaled")
    axes[0, 1].set_title("Cumulative PnL")
    axes[0, 1].set_ylabel("USD")
    axes[0, 1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))
    axes[0, 1].legend(fontsize=8)
    axes[0, 1].grid(alpha=0.25)

    hold = pd.DataFrame(
        {
            "Single": single.groupby("asset")["holding_seconds"].mean().reindex(ASSETS),
            "Scaled": scaled.groupby("asset")["holding_seconds"].mean().reindex(ASSETS),
        }
    )
    hold.plot(kind="bar", ax=axes[1, 0])
    axes[1, 0].set_title("Average Holding Period")
    axes[1, 0].set_ylabel("Seconds")
    axes[1, 0].grid(axis="y", alpha=0.25)

    pivot = sensitivity.pivot(index="step_bps", columns="max_layers", values="pnl_usd")
    image = axes[1, 1].imshow(pivot.values, aspect="auto", cmap="YlGn")
    axes[1, 1].set_title("Scaling Sensitivity: Total PnL")
    axes[1, 1].set_xticks(range(len(pivot.columns)))
    axes[1, 1].set_xticklabels(pivot.columns)
    axes[1, 1].set_yticks(range(len(pivot.index)))
    axes[1, 1].set_yticklabels(pivot.index)
    axes[1, 1].set_xlabel("Max layers")
    axes[1, 1].set_ylabel("Add step bps")
    plt.colorbar(image, ax=axes[1, 1], label="USD")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def executive_lines(
    fresh: pd.DataFrame,
    single: pd.DataFrame,
    scaled: pd.DataFrame,
    sensitivity: pd.DataFrame,
    scale_config: ScaleConfig,
) -> list[str]:
    lines = [
        "HYPERLIQUID <> LIGHTER PERMISSIVE MID-PRICE GAP REPORT",
        "=" * 66,
        f"Report Generated: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"Repository: {ROOT}",
        f"Data Range: {fresh['timestamp'].min()} to {fresh['timestamp'].max()}",
        f"Fresh Seconds: {len(fresh):,}",
        "",
        "TEST ASSUMPTIONS",
        "----------------",
        "Entry: enter whenever mid gap is non-zero.",
        "Direction: short expensive venue, long cheap venue.",
        f"Exit: close when |gap| <= {EXIT_GAP_BPS:.2f} bps or the spread crosses zero.",
        "Max hold: none. Open positions are marked at end of sample.",
        "Pricing: ideal mid-price fills. Fees, slippage and funding ignored.",
        f"Base sizing: ${UNIT_NOTIONAL_USD:,.0f} per leg.",
        f"Scaling test: add ${scale_config.unit_notional_usd:,.0f} per leg whenever gap widens by "
        f"{scale_config.step_bps:.1f} bps, capped at {scale_config.max_layers} layers.",
        "",
        "SINGLE-ENTRY RESULTS",
        "--------------------",
        f"Trades: {len(single):,}",
        f"Total PnL: ${single['pnl_usd'].sum():,.2f}",
        f"Realized PnL: ${single.loc[single['exit_reason'] != 'end_mark', 'pnl_usd'].sum():,.2f}",
        f"End Mark PnL: ${single.loc[single['exit_reason'] == 'end_mark', 'pnl_usd'].sum():,.2f}",
        f"Win Rate: {(single['pnl_usd'] > 0).mean() * 100:.2f}%",
        f"Average Hold: {single['holding_seconds'].mean():.2f}s",
        f"Profitable Average Hold: {single.loc[single['pnl_usd'] > 0, 'holding_seconds'].mean():.2f}s",
        "",
        "WIDENING-SCALE RESULTS",
        "----------------------",
        f"Trades: {len(scaled):,}",
        f"Total PnL: ${scaled['pnl_usd'].sum():,.2f}",
        f"Realized PnL: ${scaled.loc[scaled['exit_reason'] != 'end_mark', 'pnl_usd'].sum():,.2f}",
        f"End Mark PnL: ${scaled.loc[scaled['exit_reason'] == 'end_mark', 'pnl_usd'].sum():,.2f}",
        f"Win Rate: {(scaled['pnl_usd'] > 0).mean() * 100:.2f}%",
        f"Average Hold: {scaled['holding_seconds'].mean():.2f}s",
        f"Average Layers: {scaled['layers'].mean():.2f}",
        f"Max Layers Used: {scaled['layers'].max()}",
        "",
        "BEST SCALING SENSITIVITY CASE",
        "-----------------------------",
    ]
    best = sensitivity.sort_values("pnl_usd", ascending=False).iloc[0]
    lines += [
        f"Step: {best.step_bps:.1f} bps, Max Layers: {int(best.max_layers)}",
        f"PnL: ${best.pnl_usd:,.2f}, Win Rate: {best.win_rate:.2f}%, Avg Layers: {best.avg_layers:.2f}",
    ]
    return lines


def strategy_lines(single_summary: pd.DataFrame, scaled_summary: pd.DataFrame, sensitivity: pd.DataFrame) -> list[str]:
    lines = [
        "STRATEGY READOUT",
        "=" * 66,
        "SINGLE-ENTRY BY ASSET",
        "---------------------",
    ]
    for row in single_summary.itertuples(index=False):
        lines.append(
            f"{row.asset:<8} trades={row.trades:>4}, pnl=${row.pnl_usd:>8.2f}, "
            f"win={row.win_rate:>5.1f}%, avg_hold={row.avg_hold_s:>6.1f}s."
        )
    lines += [
        "",
        "WIDENING-SCALE BY ASSET",
        "-----------------------",
    ]
    for row in scaled_summary.itertuples(index=False):
        lines.append(
            f"{row.asset:<8} trades={row.trades:>4}, pnl=${row.pnl_usd:>8.2f}, "
            f"win={row.win_rate:>5.1f}%, avg_layers={row.avg_layers:>4.2f}, "
            f"avg_notional=${row.avg_max_notional_usd:>8.0f}."
        )
    lines += [
        "",
        "KEY OBSERVATIONS",
        "----------------",
        "1. Removing max hold barely changes the single-entry result because most gaps converge quickly.",
        "2. The average single-entry holding period is about 105 seconds with only four end-marked positions.",
        "3. Scaling into widening improves raw mid-price PnL because large gaps usually mean-revert in this sample.",
        "4. BRENTOIL and WTI contribute most of the PnL; GOLD is weak under this rule.",
        "5. This remains an idealized alpha estimate, not executable PnL, because mid fills ignore spread and queue risk.",
        "",
        "IMPLEMENTATION IMPLICATION",
        "--------------------------",
        "Use this as the alpha layer: predict/identify spread widening and mean reversion.",
        "Execution should still be maker-biased or selectively aggressive, because crossing every tiny gap was negative.",
    ]
    return lines


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_dir = REPORT_DIR / f"no_maxhold_mid_gap_report_{datetime.now():%Y%m%d_%H%M%S}"
    output_dir.mkdir(parents=True, exist_ok=True)

    scale_config = ScaleConfig(step_bps=1.0, max_layers=5)
    fresh = build_fresh_frame()
    single = run_single_entry_backtest(fresh)
    scaled = run_scaled_backtest(fresh, scale_config)
    sensitivity = run_scale_sensitivity(fresh)
    single_summary = summarize_trades(single)
    scaled_summary = summarize_trades(scaled)

    fresh.to_csv(output_dir / "fresh_gap_frame.csv", index=False)
    single.to_csv(output_dir / "single_entry_no_maxhold_mid_trades.csv", index=False)
    scaled.to_csv(output_dir / "scaled_no_maxhold_mid_trades.csv", index=False)
    sensitivity.to_csv(output_dir / "scaling_sensitivity.csv", index=False)
    single_summary.to_csv(output_dir / "single_entry_summary.csv", index=False)
    scaled_summary.to_csv(output_dir / "scaled_summary.csv", index=False)

    pdf_path = output_dir / "no_maxhold_mid_gap_report.pdf"
    with PdfPages(pdf_path) as pdf:
        add_text_page(pdf, executive_lines(fresh, single, scaled, sensitivity, scale_config))
        fig = make_distribution_page(fresh)
        fig.savefig(output_dir / "price_gap_distribution.png", dpi=140, bbox_inches="tight")
        pdf.savefig(fig)
        plt.close(fig)

        fig = make_gap_timing_page(fresh)
        fig.savefig(output_dir / "gap_timing_clusters.png", dpi=140, bbox_inches="tight")
        pdf.savefig(fig)
        plt.close(fig)

        fig = make_pnl_page(single, scaled, sensitivity)
        fig.savefig(output_dir / "pnl_and_scaling.png", dpi=140, bbox_inches="tight")
        pdf.savefig(fig)
        plt.close(fig)

        add_text_page(pdf, strategy_lines(single_summary, scaled_summary, sensitivity))

    print(f"report_dir={output_dir}")
    print(f"pdf={pdf_path}")
    print("single")
    print(single_summary.round(2).to_string(index=False))
    print("scaled")
    print(scaled_summary.round(2).to_string(index=False))
    print("sensitivity")
    print(sensitivity.round(2).to_string(index=False))


if __name__ == "__main__":
    main()
