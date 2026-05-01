# Final Cross-Venue RWA Basis Trade Analysis

Generated: 2026-04-29 14:29:24
Repository: `C:\Users\user\Desktop\MoneyLaundering\Hyper-LighterARB`

## Strategy Definition

This is a cross-venue basis trade, not classic equity pair trading.

```text
basis_bps = 10,000 * (Hyperliquid_mid / Lighter_mid - 1)
basis > 0: Hyperliquid is rich, short Hyperliquid and long Lighter
basis < 0: Lighter is rich, long Hyperliquid and short Lighter
```

The objective is to monetize short-horizon basis convergence while keeping directional exposure approximately neutral.

## Data Set

- Assets: `BRENTOIL, GOLD, SILVER, WTI`
- Fresh matched seconds: `92,079`
- Date range: `2026-04-23 08:17:56+00:00` to `2026-04-25 07:49:56+00:00`
- Freshness filter: both venue exchange timestamps within 2 seconds of capture time
- Base capital assumption: `$10,000 per leg`, so `$20,000 gross notional` per single-entry trade

## Basis Behavior

| asset    | dominant_rich_venue | mean_gap_bps | median_gap_bps | abs_gap_p90_bps | hyper_expensive_pct | lighter_expensive_pct | zero_crosses | pct_abs_gap_gt_5bps |
| -------- | ------------------- | ------------ | -------------- | --------------- | ------------------- | --------------------- | ------------ | ------------------- |
| BRENTOIL | Hyperliquid         | 1.58         | 1.80           | 3.44            | 83.54               | 15.90                 | 1520         | 1.68                |
| GOLD     | Hyperliquid         | 1.91         | 2.06           | 3.63            | 89.36               | 10.58                 | 238          | 0.84                |
| SILVER   | Lighter             | -2.37        | -1.96          | 5.73            | 17.72               | 82.23                 | 1087         | 17.89               |
| WTI      | Lighter             | -2.36        | -2.48          | 5.17            | 11.32               | 88.08                 | 1160         | 11.39               |

Interpretation: Hyperliquid was usually richer for BRENTOIL and GOLD; Lighter was usually richer for SILVER and WTI.

## No-Max-Hold Ideal Mid-Price Test

Assumptions: enter whenever basis is non-zero, fill at mid, ignore fees/slippage/funding, close only when basis converges to <= 0.25 bps or crosses zero. Positions still open at sample end are marked at the final mid.

- Trades: `4,224`
- Total PnL: `$5,717.90`
- Realized PnL: `$5,720.05`
- End-mark PnL: `$-2.15`
- Win rate: `84.23%`
- Convergence/cross rate: `99.91%`
- Average hold: `104.55s`
- Profitable average hold: `123.46s`

| asset    | trades | pnl_usd | realized_pnl_usd | end_mark_pnl_usd | win_rate | avg_hold_s | profitable_avg_hold_s | avg_layers | avg_max_notional_usd |
| -------- | ------ | ------- | ---------------- | ---------------- | -------- | ---------- | --------------------- | ---------- | -------------------- |
| BRENTOIL | 1356   | 2237.20 | 2240.18          | -2.99            | 91.00    | 93.23      | 102.08                | 1.00       | 10000.00             |
| GOLD     | 606    | 192.70  | 194.79           | -2.09            | 53.30    | 124.97     | 230.51                | 1.00       | 10000.00             |
| SILVER   | 1083   | 1178.04 | 1177.32          | 0.71             | 92.71    | 65.21      | 69.97                 | 1.00       | 10000.00             |
| WTI      | 1179   | 2109.96 | 2107.75          | 2.21             | 84.56    | 143.21     | 169.10                | 1.00       | 10000.00             |

## Scaling When Basis Widens

Scaling test: start with `$10,000 per leg`; add another `$10,000 per leg` every time the basis widens by 1 bps in the same direction, capped at 5 layers.

- Trades: `4,224`
- Total PnL: `$13,667.57`
- Realized PnL: `$13,671.90`
- End-mark PnL: `$-4.33`
- Win rate: `84.92%`
- Average layers: `1.43`
- Average max notional per leg: `$14,259.00`

| asset    | trades | pnl_usd | realized_pnl_usd | end_mark_pnl_usd | win_rate | avg_hold_s | profitable_avg_hold_s | avg_layers | avg_max_notional_usd |
| -------- | ------ | ------- | ---------------- | ---------------- | -------- | ---------- | --------------------- | ---------- | -------------------- |
| BRENTOIL | 1356   | 5252.98 | 5257.46          | -4.48            | 91.37    | 93.23      | 101.69                | 1.49       | 14859.88             |
| GOLD     | 606    | 598.45  | 601.23           | -2.78            | 53.96    | 124.97     | 229.24                | 1.23       | 12277.23             |
| SILVER   | 1083   | 2186.41 | 2185.69          | 0.71             | 94.09    | 65.21      | 69.21                 | 1.30       | 13047.09             |
| WTI      | 1179   | 5629.72 | 5627.51          | 2.21             | 84.99    | 143.21     | 168.31                | 1.57       | 15699.75             |

Best sensitivity case tested:

- Add step: `0.5 bps`
- Max layers: `10`
- Total PnL: `$17,034.64`
- Average layers: `1.69`
- Max used layers: `9`

## Execution Reality Check

The mid-price result measures the basis alpha. It is not the same as executable PnL.

When both legs cross bid/ask and fees are still ignored, taking every tiny basis is negative. The spread-aware test improves only after filtering for wider gaps:

| ('entry_gap_threshold', '') | ('pnl', 'bidask') | ('pnl', 'mid') | ('trades', 'bidask') | ('trades', 'mid') | ('win_rate', 'bidask') | ('win_rate', 'mid') |
| --------------------------- | ----------------- | -------------- | -------------------- | ----------------- | ---------------------- | ------------------- |
| 0.00                        | -6713.65          | 5724.54        | 4274.00              | 4274.00           | 12.33                  | 83.79               |
| 0.25                        | -4790.40          | 5772.87        | 3297.00              | 3297.00           | 16.26                  | 99.15               |
| 0.50                        | -3671.41          | 5793.08        | 2876.00              | 2876.00           | 20.41                  | 99.03               |
| 1.00                        | -2127.96          | 5796.02        | 2319.00              | 2319.00           | 27.38                  | 98.88               |
| 2.00                        | -148.40           | 5250.50        | 1488.00              | 1488.00           | 47.11                  | 98.59               |
| 3.00                        | 891.92            | 4495.25        | 952.00               | 952.00            | 70.90                  | 98.32               |
| 5.00                        | 1173.33           | 2882.69        | 408.00               | 408.00            | 87.99                  | 99.26               |
| 8.00                        | 564.45            | 1088.95        | 101.00               | 101.00            | 92.08                  | 100.00              |

This is the main implementation constraint: the strategy should be maker-biased or selectively aggressive, not blind taker execution on every non-zero basis.

## Final Alpha Verdict

There is strong evidence of short-horizon basis mean reversion in this sample. Nearly all ideal mid-price positions closed by convergence or crossing, and scaling into widening improved raw alpha materially.

The alpha is not automatically scalable. The trade becomes attractive only if execution preserves enough of the basis through maker fills, rebates, low fee tiers, selective taker use, or larger dislocation thresholds.

Recommended production framing:

1. Monitor Hyperliquid-Lighter RWA basis in real time.
2. Enter long cheap / short rich when stale-data filters pass.
3. Use dynamic scaling when the basis widens, with hard layer caps.
4. Exit on convergence, crossing, funding deterioration, stale oracle behavior, or contract-definition mismatch.
5. Track funding rates separately because persistent premium may reflect carry, not free alpha.

## Files

- PDF report: `C:\Users\user\Desktop\MoneyLaundering\Hyper-LighterARB\reports\final_basis_trade_analysis_20260429_142923\final_basis_trade_analysis_report.pdf`
- Basis distribution chart: `C:\Users\user\Desktop\MoneyLaundering\Hyper-LighterARB\reports\final_basis_trade_analysis_20260429_142923\basis_distribution_and_richness.png`
- Basis timing chart: `C:\Users\user\Desktop\MoneyLaundering\Hyper-LighterARB\reports\final_basis_trade_analysis_20260429_142923\basis_timing.png`
- PnL and scaling chart: `C:\Users\user\Desktop\MoneyLaundering\Hyper-LighterARB\reports\final_basis_trade_analysis_20260429_142923\basis_pnl_and_scaling.png`
- Execution sensitivity chart: `C:\Users\user\Desktop\MoneyLaundering\Hyper-LighterARB\reports\final_basis_trade_analysis_20260429_142923\execution_sensitivity.png`
