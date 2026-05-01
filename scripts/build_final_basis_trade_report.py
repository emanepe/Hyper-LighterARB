from __future__ import annotations

from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "reports"
ASSETS = ("BRENTOIL", "GOLD", "SILVER", "WTI")


def latest_report_dir(pattern: str, required_file: str) -> Path:
    candidates = [path for path in REPORTS.glob(pattern) if (path / required_file).exists()]
    if not candidates:
        raise FileNotFoundError(f"No report directory matching {pattern} with {required_file}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def money(value: float) -> str:
    return f"${value:,.2f}"


def pct(value: float) -> str:
    return f"{value:.2f}%"


def markdown_table(df: pd.DataFrame, floatfmt: str = ".2f") -> str:
    if df.empty:
        return "_No rows._"
    headers = [str(column) for column in df.columns]
    rows = []
    for _, row in df.iterrows():
        rendered = []
        for value in row:
            if isinstance(value, (float, np.floating)):
                rendered.append(format(float(value), floatfmt))
            elif isinstance(value, (int, np.integer)):
                rendered.append(str(int(value)))
            else:
                rendered.append(str(value))
        rows.append(rendered)
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    header_line = "| " + " | ".join(headers[i].ljust(widths[i]) for i in range(len(headers))) + " |"
    sep_line = "| " + " | ".join("-" * widths[i] for i in range(len(headers))) + " |"
    row_lines = [
        "| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(headers))) + " |"
        for row in rows
    ]
    return "\n".join([header_line, sep_line, *row_lines])


def load_inputs() -> dict[str, pd.DataFrame | Path]:
    strict_dir = latest_report_dir("hyper_lighter_gap_analysis_*", "price_gap_summary.csv")
    mid_dir = latest_report_dir("no_maxhold_mid_gap_report_*", "single_entry_summary.csv")
    return {
        "strict_dir": strict_dir,
        "mid_dir": mid_dir,
        "price_summary": pd.read_csv(strict_dir / "price_gap_summary.csv"),
        "threshold_sensitivity": pd.read_csv(strict_dir / "permissive_pair_trade_threshold_sensitivity.csv"),
        "fresh_gap_frame": pd.read_csv(mid_dir / "fresh_gap_frame.csv", parse_dates=["timestamp"]),
        "single_trades": pd.read_csv(mid_dir / "single_entry_no_maxhold_mid_trades.csv", parse_dates=["entry_time", "exit_time"]),
        "scaled_trades": pd.read_csv(mid_dir / "scaled_no_maxhold_mid_trades.csv", parse_dates=["entry_time", "exit_time"]),
        "single_summary": pd.read_csv(mid_dir / "single_entry_summary.csv"),
        "scaled_summary": pd.read_csv(mid_dir / "scaled_summary.csv"),
        "scaling_sensitivity": pd.read_csv(mid_dir / "scaling_sensitivity.csv"),
    }


def text_page(pdf: PdfPages, lines: list[str]) -> None:
    fig = plt.figure(figsize=(8.5, 11))
    fig.patch.set_facecolor("white")
    y = 0.965
    for line in lines:
        safe_line = line.replace("$", r"\$")
        is_header = line.isupper() and len(line) < 75
        fig.text(
            0.06,
            y,
            safe_line,
            family="DejaVu Sans Mono",
            fontsize=11 if is_header else 9.2,
            weight="bold" if is_header else "normal",
            va="top",
        )
        y -= 0.022 if line else 0.014
    pdf.savefig(fig)
    plt.close(fig)


def save_chart(pdf: PdfPages, fig: plt.Figure, path: Path) -> None:
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(path, dpi=145, bbox_inches="tight")
    pdf.savefig(fig)
    plt.close(fig)


def convergence_rate(trades: pd.DataFrame) -> float:
    return (trades["exit_reason"].isin(["converged", "crossed"]).mean() * 100) if len(trades) else 0.0


def profitable_avg_hold(trades: pd.DataFrame) -> float:
    winners = trades[trades["pnl_usd"] > 0]
    return float(winners["holding_seconds"].mean()) if len(winners) else float("nan")


def build_markdown(data: dict[str, pd.DataFrame | Path], output_dir: Path) -> str:
    price = data["price_summary"]
    threshold = data["threshold_sensitivity"]
    single = data["single_trades"]
    scaled = data["scaled_trades"]
    single_summary = data["single_summary"]
    scaled_summary = data["scaled_summary"]
    sensitivity = data["scaling_sensitivity"]
    fresh = data["fresh_gap_frame"]

    single_pnl = single["pnl_usd"].sum()
    scaled_pnl = scaled["pnl_usd"].sum()
    single_realized = single.loc[single["exit_reason"] != "end_mark", "pnl_usd"].sum()
    scaled_realized = scaled.loc[scaled["exit_reason"] != "end_mark", "pnl_usd"].sum()
    best_scale = sensitivity.sort_values("pnl_usd", ascending=False).iloc[0]

    basis_table = price[
        [
            "asset",
            "mean_gap_bps",
            "median_gap_bps",
            "abs_gap_p90_bps",
            "hyper_expensive_pct",
            "lighter_expensive_pct",
            "zero_crosses",
            "pct_abs_gap_gt_5bps",
        ]
    ].copy()
    basis_table["dominant_rich_venue"] = np.where(
        basis_table["hyper_expensive_pct"] >= basis_table["lighter_expensive_pct"],
        "Hyperliquid",
        "Lighter",
    )
    basis_table = basis_table[
        [
            "asset",
            "dominant_rich_venue",
            "mean_gap_bps",
            "median_gap_bps",
            "abs_gap_p90_bps",
            "hyper_expensive_pct",
            "lighter_expensive_pct",
            "zero_crosses",
            "pct_abs_gap_gt_5bps",
        ]
    ]

    threshold_table = threshold.pivot_table(
        index="entry_gap_threshold",
        columns="pricing",
        values=["pnl", "trades", "win_rate"],
        aggfunc="first",
    )

    lines = [
        "# Final Cross-Venue RWA Basis Trade Analysis",
        "",
        f"Generated: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"Repository: `{ROOT}`",
        "",
        "## Strategy Definition",
        "",
        "This is a cross-venue basis trade, not classic equity pair trading.",
        "",
        "```text",
        "basis_bps = 10,000 * (Hyperliquid_mid / Lighter_mid - 1)",
        "basis > 0: Hyperliquid is rich, short Hyperliquid and long Lighter",
        "basis < 0: Lighter is rich, long Hyperliquid and short Lighter",
        "```",
        "",
        "The objective is to monetize short-horizon basis convergence while keeping directional exposure approximately neutral.",
        "",
        "## Data Set",
        "",
        f"- Assets: `{', '.join(ASSETS)}`",
        f"- Fresh matched seconds: `{len(fresh):,}`",
        f"- Date range: `{fresh['timestamp'].min()}` to `{fresh['timestamp'].max()}`",
        "- Freshness filter: both venue exchange timestamps within 2 seconds of capture time",
        "- Base capital assumption: `$10,000 per leg`, so `$20,000 gross notional` per single-entry trade",
        "",
        "## Basis Behavior",
        "",
        markdown_table(basis_table, floatfmt=".2f"),
        "",
        "Interpretation: Hyperliquid was usually richer for BRENTOIL and GOLD; Lighter was usually richer for SILVER and WTI.",
        "",
        "## No-Max-Hold Ideal Mid-Price Test",
        "",
        "Assumptions: enter whenever basis is non-zero, fill at mid, ignore fees/slippage/funding, close only when basis converges to <= 0.25 bps or crosses zero. Positions still open at sample end are marked at the final mid.",
        "",
        f"- Trades: `{len(single):,}`",
        f"- Total PnL: `{money(single_pnl)}`",
        f"- Realized PnL: `{money(single_realized)}`",
        f"- End-mark PnL: `{money(single.loc[single['exit_reason'] == 'end_mark', 'pnl_usd'].sum())}`",
        f"- Win rate: `{pct((single['pnl_usd'] > 0).mean() * 100)}`",
        f"- Convergence/cross rate: `{pct(convergence_rate(single))}`",
        f"- Average hold: `{single['holding_seconds'].mean():.2f}s`",
        f"- Profitable average hold: `{profitable_avg_hold(single):.2f}s`",
        "",
        markdown_table(single_summary, floatfmt=".2f"),
        "",
        "## Scaling When Basis Widens",
        "",
        "Scaling test: start with `$10,000 per leg`; add another `$10,000 per leg` every time the basis widens by 1 bps in the same direction, capped at 5 layers.",
        "",
        f"- Trades: `{len(scaled):,}`",
        f"- Total PnL: `{money(scaled_pnl)}`",
        f"- Realized PnL: `{money(scaled_realized)}`",
        f"- End-mark PnL: `{money(scaled.loc[scaled['exit_reason'] == 'end_mark', 'pnl_usd'].sum())}`",
        f"- Win rate: `{pct((scaled['pnl_usd'] > 0).mean() * 100)}`",
        f"- Average layers: `{scaled['layers'].mean():.2f}`",
        f"- Average max notional per leg: `{money(scaled['max_notional_usd'].mean())}`",
        "",
        markdown_table(scaled_summary, floatfmt=".2f"),
        "",
        "Best sensitivity case tested:",
        "",
        f"- Add step: `{best_scale['step_bps']:.1f} bps`",
        f"- Max layers: `{int(best_scale['max_layers'])}`",
        f"- Total PnL: `{money(best_scale['pnl_usd'])}`",
        f"- Average layers: `{best_scale['avg_layers']:.2f}`",
        f"- Max used layers: `{int(best_scale['max_used_layers'])}`",
        "",
        "## Execution Reality Check",
        "",
        "The mid-price result measures the basis alpha. It is not the same as executable PnL.",
        "",
        "When both legs cross bid/ask and fees are still ignored, taking every tiny basis is negative. The spread-aware test improves only after filtering for wider gaps:",
        "",
        markdown_table(threshold_table.round(2).reset_index(), floatfmt=".2f"),
        "",
        "This is the main implementation constraint: the strategy should be maker-biased or selectively aggressive, not blind taker execution on every non-zero basis.",
        "",
        "## Final Alpha Verdict",
        "",
        "There is strong evidence of short-horizon basis mean reversion in this sample. Nearly all ideal mid-price positions closed by convergence or crossing, and scaling into widening improved raw alpha materially.",
        "",
        "The alpha is not automatically scalable. The trade becomes attractive only if execution preserves enough of the basis through maker fills, rebates, low fee tiers, selective taker use, or larger dislocation thresholds.",
        "",
        "Recommended production framing:",
        "",
        "1. Monitor Hyperliquid-Lighter RWA basis in real time.",
        "2. Enter long cheap / short rich when stale-data filters pass.",
        "3. Use dynamic scaling when the basis widens, with hard layer caps.",
        "4. Exit on convergence, crossing, funding deterioration, stale oracle behavior, or contract-definition mismatch.",
        "5. Track funding rates separately because persistent premium may reflect carry, not free alpha.",
        "",
        "## Files",
        "",
        f"- PDF report: `{output_dir / 'final_basis_trade_analysis_report.pdf'}`",
        f"- Basis distribution chart: `{output_dir / 'basis_distribution_and_richness.png'}`",
        f"- Basis timing chart: `{output_dir / 'basis_timing.png'}`",
        f"- PnL and scaling chart: `{output_dir / 'basis_pnl_and_scaling.png'}`",
        f"- Execution sensitivity chart: `{output_dir / 'execution_sensitivity.png'}`",
    ]
    return "\n".join(lines) + "\n"


def executive_lines(data: dict[str, pd.DataFrame | Path]) -> list[str]:
    price = data["price_summary"]
    fresh = data["fresh_gap_frame"]
    single = data["single_trades"]
    scaled = data["scaled_trades"]
    sensitivity = data["scaling_sensitivity"]
    best_scale = sensitivity.sort_values("pnl_usd", ascending=False).iloc[0]

    return [
        "FINAL CROSS-VENUE RWA BASIS TRADE ANALYSIS",
        "=" * 66,
        f"Generated: {datetime.now():%Y-%m-%d %H:%M:%S}",
        f"Repository: {ROOT}",
        "",
        "STRATEGY DEFINITION",
        "-------------------",
        "basis_bps = 10,000 * (Hyperliquid_mid / Lighter_mid - 1)",
        "Positive basis: Hyperliquid rich, short Hyperliquid / long Lighter.",
        "Negative basis: Lighter rich, long Hyperliquid / short Lighter.",
        "This is cross-venue basis convergence, not classic two-asset pair trading.",
        "",
        "DATA SET",
        "--------",
        f"Assets: {', '.join(ASSETS)}",
        f"Fresh matched seconds: {len(fresh):,}",
        f"Date range: {fresh['timestamp'].min()} to {fresh['timestamp'].max()}",
        "Freshness rule: both venues within 2 seconds of exchange timestamp.",
        "Base size: $10,000 per leg, $20,000 gross notional per single-entry trade.",
        "",
        "BASIS RICHNESS",
        "--------------",
        *[
            (
                f"{row.asset:<8} mean={row.mean_gap_bps:>6.2f} bps, p90_abs={row.abs_gap_p90_bps:>5.2f} bps, "
                f"rich venue={'Hyperliquid' if row.hyper_expensive_pct >= row.lighter_expensive_pct else 'Lighter':<11}, "
                f"crosses={int(row.zero_crosses):>4}"
            )
            for row in price.itertuples(index=False)
        ],
        "",
        "NO-MAX-HOLD MID-PRICE RESULTS",
        "-----------------------------",
        f"Single-entry trades: {len(single):,}",
        f"Single-entry total PnL: {money(single['pnl_usd'].sum())}",
        f"Single-entry win rate: {pct((single['pnl_usd'] > 0).mean() * 100)}",
        f"Single-entry convergence/cross rate: {pct(convergence_rate(single))}",
        f"Single-entry avg hold: {single['holding_seconds'].mean():.2f}s",
        f"Scaled total PnL, 1 bps step / 5 layer cap: {money(scaled['pnl_usd'].sum())}",
        f"Scaled avg layers: {scaled['layers'].mean():.2f}",
        f"Scaled avg max notional per leg: {money(scaled['max_notional_usd'].mean())}",
        f"Best tested scale: {best_scale.step_bps:.1f} bps step, {int(best_scale.max_layers)} layers, "
        f"PnL {money(best_scale.pnl_usd)}",
    ]


def strategy_lines(data: dict[str, pd.DataFrame | Path]) -> list[str]:
    single_summary = data["single_summary"]
    scaled_summary = data["scaled_summary"]
    threshold = data["threshold_sensitivity"]
    bidask = threshold[threshold["pricing"] == "bidask"].sort_values("entry_gap_threshold")

    lines = [
        "STRATEGY READOUT",
        "=" * 66,
        "SINGLE-ENTRY MID-PRICE PNL BY ASSET",
        "-----------------------------------",
    ]
    for row in single_summary.itertuples(index=False):
        lines.append(
            f"{row.asset:<8} trades={int(row.trades):>4}, pnl={money(row.pnl_usd):>12}, "
            f"win={row.win_rate:>5.1f}%, avg_hold={row.avg_hold_s:>6.1f}s"
        )
    lines += [
        "",
        "SCALED MID-PRICE PNL BY ASSET",
        "-----------------------------",
    ]
    for row in scaled_summary.itertuples(index=False):
        lines.append(
            f"{row.asset:<8} trades={int(row.trades):>4}, pnl={money(row.pnl_usd):>12}, "
            f"win={row.win_rate:>5.1f}%, avg_layers={row.avg_layers:>4.2f}, "
            f"avg_leg_notional={money(row.avg_max_notional_usd):>12}"
        )
    lines += [
        "",
        "EXECUTION REALITY CHECK",
        "-----------------------",
        "Ideal mid fills show the alpha process. Crossing every tiny gap does not.",
    ]
    for row in bidask.itertuples(index=False):
        lines.append(
            f"Bid/ask entry gap > {row.entry_gap_threshold:>4.2f} bps: "
            f"trades={int(row.trades):>4}, pnl={money(row.pnl):>12}, win={row.win_rate:>5.1f}%"
        )
    lines += [
        "",
        "FINAL VERDICT",
        "-------------",
        "The basis is highly mean-reverting in the sample and supports a real alpha thesis.",
        "The strategy should be described as cross-venue RWA basis convergence.",
        "Scaling into widening improved ideal PnL, but it increases tail exposure.",
        "Production must use maker-biased execution, freshness checks, funding-rate monitoring, and layer caps.",
    ]
    return lines


def chart_distribution(data: dict[str, pd.DataFrame | Path]) -> plt.Figure:
    fresh = data["fresh_gap_frame"]
    price = data["price_summary"]
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Basis Distribution and Venue Richness", fontsize=15, weight="bold")

    for asset, group in fresh.groupby("asset", sort=False):
        axes[0, 0].hist(group["gap_bps"].clip(-15, 15), bins=70, alpha=0.45, label=asset)
    axes[0, 0].axvline(0, color="black", linewidth=0.8)
    axes[0, 0].set_title("Basis Distribution")
    axes[0, 0].set_xlabel("Basis bps: Hyperliquid mid - Lighter mid")
    axes[0, 0].set_ylabel("Fresh seconds")
    axes[0, 0].legend(fontsize=8)
    axes[0, 0].grid(alpha=0.25)

    x = np.arange(len(price))
    axes[0, 1].bar(x - 0.2, price["hyper_expensive_pct"], width=0.4, label="Hyperliquid rich")
    axes[0, 1].bar(x + 0.2, price["lighter_expensive_pct"], width=0.4, label="Lighter rich")
    axes[0, 1].set_xticks(x)
    axes[0, 1].set_xticklabels(price["asset"])
    axes[0, 1].set_ylim(0, 100)
    axes[0, 1].set_title("Dominant Rich Venue")
    axes[0, 1].set_ylabel("% fresh seconds")
    axes[0, 1].legend(fontsize=8)
    axes[0, 1].grid(axis="y", alpha=0.25)

    axes[1, 0].bar(price["asset"], price["abs_gap_p90_bps"], label="P90 abs basis")
    axes[1, 0].bar(price["asset"], price["abs_gap_p50_bps"], label="Median abs basis")
    axes[1, 0].set_title("Typical vs Tail Basis")
    axes[1, 0].set_ylabel("Abs basis bps")
    axes[1, 0].legend(fontsize=8)
    axes[1, 0].grid(axis="y", alpha=0.25)

    axes[1, 1].bar(price["asset"], price["zero_crosses"], color="#4c78a8")
    axes[1, 1].set_title("Basis Zero-Cross Count")
    axes[1, 1].set_ylabel("Crosses")
    axes[1, 1].grid(axis="y", alpha=0.25)
    return fig


def chart_timing(data: dict[str, pd.DataFrame | Path]) -> plt.Figure:
    fresh = data["fresh_gap_frame"]
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Basis Timing", fontsize=15, weight="bold")
    for ax, (asset, group) in zip(axes.ravel(), fresh.groupby("asset", sort=False)):
        group = group.sort_values("timestamp")
        ax.plot(group["timestamp"], group["gap_bps"], linewidth=0.65, color="#1f4e79")
        ax.axhline(0, color="black", linewidth=0.7)
        ax.axhline(5, color="crimson", linestyle="--", linewidth=0.7)
        ax.axhline(-5, color="crimson", linestyle="--", linewidth=0.7)
        ax.set_title(asset)
        ax.set_ylabel("Basis bps")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))
        ax.grid(alpha=0.25)
    return fig


def chart_pnl(data: dict[str, pd.DataFrame | Path]) -> plt.Figure:
    single = data["single_trades"]
    scaled = data["scaled_trades"]
    sensitivity = data["scaling_sensitivity"]
    fig, axes = plt.subplots(2, 2, figsize=(11, 8.5))
    fig.suptitle("Basis Trade PnL and Scaling", fontsize=15, weight="bold")

    by_asset = pd.DataFrame(
        {
            "Single entry": single.groupby("asset")["pnl_usd"].sum().reindex(ASSETS),
            "Scale on widening": scaled.groupby("asset")["pnl_usd"].sum().reindex(ASSETS),
        }
    )
    by_asset.plot(kind="bar", ax=axes[0, 0])
    axes[0, 0].set_title("PnL by Asset")
    axes[0, 0].set_ylabel("USD")
    axes[0, 0].grid(axis="y", alpha=0.25)

    axes[0, 1].plot(
        single.sort_values("exit_time")["exit_time"],
        single.sort_values("exit_time")["pnl_usd"].cumsum(),
        label="Single entry",
    )
    axes[0, 1].plot(
        scaled.sort_values("exit_time")["exit_time"],
        scaled.sort_values("exit_time")["pnl_usd"].cumsum(),
        label="Scaled",
    )
    axes[0, 1].set_title("Cumulative Mid-Price PnL")
    axes[0, 1].set_ylabel("USD")
    axes[0, 1].xaxis.set_major_formatter(mdates.DateFormatter("%m-%d\n%H:%M"))
    axes[0, 1].legend(fontsize=8)
    axes[0, 1].grid(alpha=0.25)

    hold = pd.DataFrame(
        {
            "Single entry": single.groupby("asset")["holding_seconds"].mean().reindex(ASSETS),
            "Scaled": scaled.groupby("asset")["holding_seconds"].mean().reindex(ASSETS),
        }
    )
    hold.plot(kind="bar", ax=axes[1, 0])
    axes[1, 0].set_title("Average Hold")
    axes[1, 0].set_ylabel("Seconds")
    axes[1, 0].grid(axis="y", alpha=0.25)

    pivot = sensitivity.pivot(index="step_bps", columns="max_layers", values="pnl_usd")
    img = axes[1, 1].imshow(pivot.values, aspect="auto", cmap="YlGn")
    axes[1, 1].set_title("Scaling Sensitivity PnL")
    axes[1, 1].set_xticks(range(len(pivot.columns)))
    axes[1, 1].set_xticklabels(pivot.columns)
    axes[1, 1].set_yticks(range(len(pivot.index)))
    axes[1, 1].set_yticklabels(pivot.index)
    axes[1, 1].set_xlabel("Max layers")
    axes[1, 1].set_ylabel("Add step bps")
    plt.colorbar(img, ax=axes[1, 1], label="USD")
    return fig


def chart_execution(data: dict[str, pd.DataFrame | Path]) -> plt.Figure:
    threshold = data["threshold_sensitivity"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.8))
    fig.suptitle("Execution Reality: Mid Alpha vs Bid/Ask Crossing", fontsize=15, weight="bold")
    for pricing, group in threshold.groupby("pricing"):
        group = group.sort_values("entry_gap_threshold")
        axes[0].plot(group["entry_gap_threshold"], group["pnl"], marker="o", label=pricing)
        axes[1].plot(group["entry_gap_threshold"], group["win_rate"], marker="o", label=pricing)
    axes[0].axhline(0, color="black", linewidth=0.8)
    axes[0].set_title("PnL by Entry Threshold")
    axes[0].set_xlabel("Entry basis threshold (bps)")
    axes[0].set_ylabel("USD")
    axes[0].legend(fontsize=8)
    axes[0].grid(alpha=0.25)
    axes[1].set_title("Win Rate by Entry Threshold")
    axes[1].set_xlabel("Entry basis threshold (bps)")
    axes[1].set_ylabel("Win rate %")
    axes[1].legend(fontsize=8)
    axes[1].grid(alpha=0.25)
    return fig


def main() -> None:
    REPORTS.mkdir(parents=True, exist_ok=True)
    output_dir = REPORTS / f"final_basis_trade_analysis_{datetime.now():%Y%m%d_%H%M%S}"
    output_dir.mkdir(parents=True, exist_ok=True)

    data = load_inputs()
    markdown = build_markdown(data, output_dir)
    md_path = output_dir / "FINAL_BASIS_TRADE_ANALYSIS_REPORT.md"
    md_path.write_text(markdown, encoding="utf-8")

    pdf_path = output_dir / "final_basis_trade_analysis_report.pdf"
    with PdfPages(pdf_path) as pdf:
        text_page(pdf, executive_lines(data))
        save_chart(pdf, chart_distribution(data), output_dir / "basis_distribution_and_richness.png")
        save_chart(pdf, chart_timing(data), output_dir / "basis_timing.png")
        save_chart(pdf, chart_pnl(data), output_dir / "basis_pnl_and_scaling.png")
        save_chart(pdf, chart_execution(data), output_dir / "execution_sensitivity.png")
        text_page(pdf, strategy_lines(data))

    print(f"report_dir={output_dir}")
    print(f"pdf={pdf_path}")
    print(f"markdown={md_path}")


if __name__ == "__main__":
    main()
