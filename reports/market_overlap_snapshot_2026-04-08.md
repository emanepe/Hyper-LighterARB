# Lighter RWA vs Hyperliquid TradFi/HIP-3 Snapshot

Snapshot date: 2026-04-08

Data sources:
- Lighter official RWA market-spec page: https://docs.lighter.xyz/trading/real-world-assets-rwas/market-specifications
- Lighter public inventory endpoints: `/api/v1/orderBooks` and `/api/v1/orderBookDetails`
- Hyperliquid official info endpoint: `perpCategories`, `allPerpMetas`, and `allMids` with `dex=xyz`

Headline counts:
- Lighter RWA markets tracked: 40
- Hyperliquid non-crypto TradFi/HIP-3 markets from `perpCategories`: 94
- Hyperliquid additional live-only markets confirmed outside `perpCategories`: 23
- Exact or high-confidence normalized overlaps: 27
- Related-but-not-same ETF/index or proxy matches: 8

Key Hyperliquid nuance:
- `perpCategories` is not exhaustive for every live builder-deployed market.
- The live endpoints confirmed a separate `cash:` universe that is absent from `perpCategories`.
- The live `xyz` inventory also exposed symbols absent from `perpCategories`, including `xyz:CORN`, `xyz:WHEAT`, `xyz:TTF`, `xyz:URANIUM`, and `xyz:XLE`.

High-confidence overlap set:
- AAPL -> km:AAPL;xyz:AAPL
- AMD -> xyz:AMD
- AMZN -> xyz:AMZN;cash:AMZN
- BRENTOIL -> xyz:BRENTOIL
- COIN -> flx:COIN;xyz:COIN
- XCU -> flx:COPPER;xyz:COPPER
- CRCL -> flx:CRCL;xyz:CRCL
- EURUSD -> xyz:EUR
- XAU -> flx:GOLD;km:GOLD;xyz:GOLD;cash:GOLD
- GOOGL -> km:GOOGL;xyz:GOOGL;cash:GOOGL
- HOOD -> xyz:HOOD;cash:HOOD
- HYUNDAI -> xyz:HYUNDAI
- INTC -> xyz:INTC;cash:INTC
- META -> xyz:META;cash:META
- MSFT -> xyz:MSFT;cash:MSFT
- MSTR -> xyz:MSTR
- NATGAS -> xyz:NATGAS
- NVDA -> flx:NVDA;km:NVDA;xyz:NVDA;cash:NVDA
- XPD -> flx:PALLADIUM;xyz:PALLADIUM
- XPT -> flx:PLATINUM;xyz:PLATINUM
- SAMSUNG -> xyz:SMSN
- XAG -> flx:SILVER;km:SILVER;xyz:SILVER;cash:SILVER
- SKHYNIX -> xyz:SKHX
- SNDK -> xyz:SNDK
- TSLA -> flx:TSLA;km:TSLA;xyz:TSLA;cash:TSLA
- USDJPY -> xyz:JPY
- WTI -> cash:WTI

Related but not exact matches:
- WTI -> flx:OIL;km:USOIL;xyz:CL (same crude-oil exposure family, but different symbols/contracts)
- NATGAS -> flx:GAS (probable natural-gas proxy, but the Hyperliquid symbol is less explicit)
- KRCOMP -> xyz:KR200 (Korea broad-market exposure vs KOSPI 200, related but not the same index)
- SPY -> flx:USA500;km:US500;xyz:SP500;cash:USA500 (SPY ETF vs S&P 500 index contracts)
- QQQ -> km:USTECH (QQQ ETF vs Nasdaq/US tech index exposure)
- BOTZ -> vntl:ROBOT (robotics/AI thematic exposure, not the same ETF)
- MAGS -> vntl:MAG7 (Magnificent Seven ETF vs Magnificent Seven index basket)
- IWM -> km:SMALL2000 (Russell 2000 ETF vs small-cap index basket)
