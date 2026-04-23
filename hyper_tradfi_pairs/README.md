# Hyperliquid <> Tradfi Pair Trading

This workspace package is a starting point for a four-asset pair-trading workflow:

- `BRENTOIL`
- `GOLD`
- `SILVER`
- `WTI`

It does four things:

1. Collects live 1-second Hyperliquid top-of-book snapshots.
2. Collects live 1-second Lighter top-of-book snapshots.
3. Downloads 1-second tradfi BBO history from Databento.
4. Runs a simple 1-day mean-reversion backtest using crossed bid/ask prices.

It also has a separate Yahoo proxy downloader for coarse price comparison only.
It now also has an IBKR collector for historical and live 1-second top-of-book recording.

## Verified Mapping

The current Hyperliquid symbols below were verified live on 2026-04-23:

- `BRENTOIL` -> `xyz:BRENTOIL`
- `GOLD` -> `xyz:GOLD`
- `SILVER` -> `xyz:SILVER`
- `WTI` -> `cash:WTI`

The current Lighter symbols below were verified live on 2026-04-23:

- `BRENTOIL` -> `BRENTOIL`
- `GOLD` -> `XAU`
- `SILVER` -> `XAG`
- `WTI` -> `WTI`

Alternate Hyperliquid symbols that may be worth comparing later:

- `GOLD` -> `cash:GOLD`, `flx:GOLD`, `km:GOLD`
- `SILVER` -> `cash:SILVER`, `flx:SILVER`, `km:SILVER`
- `WTI` -> `xyz:CL`, `flx:OIL`, `km:USOIL`

Recommended tradfi leg for production-quality 1-second history:

- `BRENTOIL` -> Databento `IFEU.IMPACT` / `BRN.c.0`
- `GOLD` -> Databento `GLBX.MDP3` / `GC.c.0`
- `SILVER` -> Databento `GLBX.MDP3` / `SI.c.0`
- `WTI` -> Databento `GLBX.MDP3` / `CL.c.0`

IBKR mapping used by the local collector:

- `BRENTOIL` -> `BZ` on `NYMEX`
- `GOLD` -> `GC` on `COMEX`
- `SILVER` -> `SI` on `COMEX`
- `WTI` -> `CL` on `NYMEX`

The IBKR collector resolves the nearest non-expired futures contract at runtime and writes a 1-second top-of-book series with bid price, ask price, bid size and ask size.

Rough Yahoo proxies if you only want minute bars:

- `BZ=F`, `GC=F`, `SI=F`, `CL=F`

Yahoo is not used by the code here because it does not solve the actual requirement: historical 1-second bid/ask liquidity.

TradingView is not wired in here because TradingView's charting stack expects you to supply your own datafeed or a third-party provider; it is not an official standalone source of historical 1-second futures depth.

## Reference Workbooks

The provided workbooks were inspected and are consistent with the symbol-overlap idea:

- `HyperLiquid_Lighter.xlsx` contains:
  - `Collision List`
  - `PriceData_xyz`
  - `PriceData_flx`
  - `PriceData_km`
  - `PriceData_cash`
- `Lighter_Market_Data.xlsx` contains one sheet with time-series rows that look like minute OHLC data.

## Quick Start

Collect live Hyperliquid 1-second snapshots:

```powershell
python -m hyper_tradfi_pairs.scripts.collect_hyperliquid --assets all --duration-seconds 3600
```

Collect live Lighter 1-second snapshots:

```powershell
python -m hyper_tradfi_pairs.scripts.collect_lighter --assets all --duration-seconds 3600
```

Download a 1-day tradfi BBO history from Databento:

```powershell
$env:DATABENTO_API_KEY="db-...your-key..."
python -m hyper_tradfi_pairs.scripts.fetch_databento_bbo --assets all --date 2026-04-23
```

Run the 1-day pair-trade backtest:

```powershell
python -m hyper_tradfi_pairs.scripts.backtest_pair_trade --assets all --date 2026-04-23
```

Fetch Yahoo price-only proxy data:

```powershell
python -m hyper_tradfi_pairs.scripts.fetch_yahoo_proxy --assets all --range 1d --interval 1m
```

Fetch historical IBKR `BID_ASK` data and aggregate to 1-second rows:

```powershell
python -m hyper_tradfi_pairs.scripts.fetch_ibkr_historical --assets all --date 2026-04-23 --port 7497
```

Record ongoing 1-second IBKR top-of-book data:

```powershell
python -m hyper_tradfi_pairs.scripts.collect_ibkr_live --assets all --port 7497 --duration-seconds 3600
```

Outputs land under:

- `hyper_tradfi_pairs/data/hyperliquid`
- `hyper_tradfi_pairs/data/lighter`
- `hyper_tradfi_pairs/data/tradfi_databento`
- `hyper_tradfi_pairs/data/tradfi_ibkr_historical`
- `hyper_tradfi_pairs/data/tradfi_ibkr_live`
- `hyper_tradfi_pairs/data/tradfi_yahoo`
- `hyper_tradfi_pairs/output/backtests`

## What The Backtest Does

The backtest is intentionally simple:

- Builds a 1-second spread in basis points:
  - `hyper_mid / tradfi_mid - 1`
- Computes a rolling z-score.
- Enters when the spread is far from its recent mean.
- Exits when the spread mean-reverts toward zero.
- Prices entries and exits off the displayed top-of-book bid/ask, not mids.

## Important Limits

- Hyperliquid API history is available for candles only from `1m` and up. There is no direct official API for historical 1-second BBO.
- The Lighter collector uses the public websocket `order_book/{market_id}` stream and records one local snapshot per second. The stream provides aggregated price-level depth, not per-price order counts, so `best_bid_orders` and `best_ask_orders` are recorded as `0`.
- Hyperliquid documents an S3 archive for historical L2 snapshots, but it is separate from the normal REST API and may be incomplete or delayed.
- The current backtest uses equal-dollar notionals per leg. It does not model futures contract multipliers, fees, funding, roll logic, or exchange session nuances.
- Databento requires an API key. Without one, you can still collect Hyperliquid live data immediately.
- Yahoo proxy data is only a `1m`-bar price fallback. It does not provide historical 1-second quotes or historical bid/ask depth.
- IBKR does not use a separate API key, but it does require a running TWS or IB Gateway session and the appropriate market data permissions for the contracts you request.
