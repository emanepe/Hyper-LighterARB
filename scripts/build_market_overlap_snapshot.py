from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path


SNAPSHOT_DATE = "2026-04-08"

# Official lighter RWA market-spec page snapshot on 2026-04-08.
LIGHTER_RWA_MARKETS = [
    {"symbol": "XAU", "market_type": "commodity"},
    {"symbol": "XAG", "market_type": "commodity"},
    {"symbol": "XCU", "market_type": "commodity"},
    {"symbol": "XPT", "market_type": "commodity"},
    {"symbol": "XPD", "market_type": "commodity"},
    {"symbol": "WTI", "market_type": "commodity"},
    {"symbol": "NATGAS", "market_type": "commodity"},
    {"symbol": "BRENTOIL", "market_type": "commodity"},
    {"symbol": "EURUSD", "market_type": "fx"},
    {"symbol": "USDKRW", "market_type": "fx"},
    {"symbol": "USDJPY", "market_type": "fx"},
    {"symbol": "GBPUSD", "market_type": "fx"},
    {"symbol": "USDCHF", "market_type": "fx"},
    {"symbol": "NZDUSD", "market_type": "fx"},
    {"symbol": "SKHYNIX", "market_type": "equity"},
    {"symbol": "SAMSUNG", "market_type": "equity"},
    {"symbol": "HYUNDAI", "market_type": "equity"},
    {"symbol": "HANMI", "market_type": "equity"},
    {"symbol": "KRCOMP", "market_type": "index"},
    {"symbol": "NVDA", "market_type": "equity"},
    {"symbol": "TSLA", "market_type": "equity"},
    {"symbol": "CRCL", "market_type": "equity"},
    {"symbol": "GOOGL", "market_type": "equity"},
    {"symbol": "MSTR", "market_type": "equity"},
    {"symbol": "MSFT", "market_type": "equity"},
    {"symbol": "AMZN", "market_type": "equity"},
    {"symbol": "AAPL", "market_type": "equity"},
    {"symbol": "COIN", "market_type": "equity"},
    {"symbol": "META", "market_type": "equity"},
    {"symbol": "INTC", "market_type": "equity"},
    {"symbol": "HOOD", "market_type": "equity"},
    {"symbol": "ASML", "market_type": "equity"},
    {"symbol": "AMD", "market_type": "equity"},
    {"symbol": "SNDK", "market_type": "equity"},
    {"symbol": "SPY", "market_type": "index"},
    {"symbol": "QQQ", "market_type": "index"},
    {"symbol": "DIA", "market_type": "index"},
    {"symbol": "BOTZ", "market_type": "index"},
    {"symbol": "MAGS", "market_type": "index"},
    {"symbol": "IWM", "market_type": "index"},
]

# Hyperliquid perpCategories snapshot, filtered to non-crypto buckets.
HYPERLIQUID_CATEGORY_MARKETS = [
    {"symbol": "flx:COIN", "bucket": "stocks", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:COPPER", "bucket": "commodities", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:CRCL", "bucket": "stocks", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:GAS", "bucket": "commodities", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:GOLD", "bucket": "commodities", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:NVDA", "bucket": "stocks", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:OIL", "bucket": "commodities", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:PALLADIUM", "bucket": "commodities", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:PLATINUM", "bucket": "commodities", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:SILVER", "bucket": "commodities", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:TSLA", "bucket": "stocks", "dex": "flx", "source": "perpCategories"},
    {"symbol": "flx:USA500", "bucket": "indices", "dex": "flx", "source": "perpCategories"},
    {"symbol": "km:AAPL", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:BABA", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:BMNR", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:GLDMINE", "bucket": "indices", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:GOLD", "bucket": "commodities", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:GOOGL", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:MU", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:NVDA", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:PLTR", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:RTX", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:SEMI", "bucket": "indices", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:SILVER", "bucket": "commodities", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:SMALL2000", "bucket": "indices", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:TSLA", "bucket": "stocks", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:US500", "bucket": "indices", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:USENERGY", "bucket": "indices", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:USOIL", "bucket": "commodities", "dex": "km", "source": "perpCategories"},
    {"symbol": "km:USTECH", "bucket": "indices", "dex": "km", "source": "perpCategories"},
    {"symbol": "vntl:ANTHROPIC", "bucket": "preipo", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:BIOTECH", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:DEFENSE", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:ENERGY", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:GOLDJM", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:INFOTECH", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:MAG7", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:NUCLEAR", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:OPENAI", "bucket": "preipo", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:ROBOT", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:SEMIS", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:SILVERJM", "bucket": "indices", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "vntl:SPACEX", "bucket": "preipo", "dex": "vntl", "source": "perpCategories"},
    {"symbol": "xyz:AAPL", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:ALUMINIUM", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:AMD", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:AMZN", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:BABA", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:BRENTOIL", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:CL", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:COIN", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:COPPER", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:COST", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:CRCL", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:CRWV", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:DXY", "bucket": "fx", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:EUR", "bucket": "fx", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:EWJ", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:EWY", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:GME", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:GOLD", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:GOOGL", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:HOOD", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:HYUNDAI", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:INTC", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:JP225", "bucket": "indices", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:JPY", "bucket": "fx", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:KIOXIA", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:KR200", "bucket": "indices", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:LLY", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:META", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:MSFT", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:MSTR", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:MU", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:NATGAS", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:NFLX", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:NVDA", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:ORCL", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:PALLADIUM", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:PLATINUM", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:PLTR", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:RIVN", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:SILVER", "bucket": "commodities", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:SKHX", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:SMSN", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:SNDK", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:SOFTBANK", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:SP500", "bucket": "indices", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:TSLA", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:TSM", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:URNM", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:USAR", "bucket": "stocks", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:VIX", "bucket": "indices", "dex": "xyz", "source": "perpCategories"},
    {"symbol": "xyz:XYZ100", "bucket": "indices", "dex": "xyz", "source": "perpCategories"},
]

# Live markets that were visible in the official Hyperliquid live endpoints but were absent
# from perpCategories. This captures the interrupted-run nuance.
HYPERLIQUID_LIVE_ONLY_MARKETS = [
    {"symbol": "cash:USA500", "bucket": "indices", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:TSLA", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:NVDA", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:HOOD", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:GOOGL", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:INTC", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:AMZN", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:MSFT", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:META", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:GOLD", "bucket": "commodities", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:SILVER", "bucket": "commodities", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:EWY", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:WTI", "bucket": "commodities", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "cash:KWEB", "bucket": "stocks", "dex": "cash", "source": "allPerpMetas", "note": "live in allPerpMetas but absent from perpCategories"},
    {"symbol": "xyz:BX", "bucket": "stocks", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
    {"symbol": "xyz:CORN", "bucket": "commodities", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
    {"symbol": "xyz:DKNG", "bucket": "stocks", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
    {"symbol": "xyz:HIMS", "bucket": "stocks", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
    {"symbol": "xyz:LITE", "bucket": "stocks", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
    {"symbol": "xyz:TTF", "bucket": "commodities", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
    {"symbol": "xyz:URANIUM", "bucket": "commodities", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
    {"symbol": "xyz:WHEAT", "bucket": "commodities", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
    {"symbol": "xyz:XLE", "bucket": "stocks", "dex": "xyz", "source": "allMids(xyz)", "note": "live in allMids(xyz) but absent from perpCategories"},
]

LIGHTER_CANONICAL = {
    "XAU": "GOLD",
    "XAG": "SILVER",
    "XCU": "COPPER",
    "XPT": "PLATINUM",
    "XPD": "PALLADIUM",
    "EURUSD": "EURUSD",
    "USDJPY": "USDJPY",
    "SKHYNIX": "SKHYNIX",
    "SAMSUNG": "SAMSUNG",
}

HYPERLIQUID_CANONICAL = {
    "xyz:EUR": "EURUSD",
    "xyz:JPY": "USDJPY",
    "xyz:SKHX": "SKHYNIX",
    "xyz:SMSN": "SAMSUNG",
    "cash:WTI": "WTI",
}

RELATED_MATCHES = [
    {
        "lighter_symbol": "WTI",
        "lighter_market_type": "commodity",
        "hyperliquid_symbols": "flx:OIL;km:USOIL;xyz:CL",
        "reason": "same crude-oil exposure family, but different symbols/contracts",
    },
    {
        "lighter_symbol": "NATGAS",
        "lighter_market_type": "commodity",
        "hyperliquid_symbols": "flx:GAS",
        "reason": "probable natural-gas proxy, but the Hyperliquid symbol is less explicit",
    },
    {
        "lighter_symbol": "KRCOMP",
        "lighter_market_type": "index",
        "hyperliquid_symbols": "xyz:KR200",
        "reason": "Korea broad-market exposure vs KOSPI 200, related but not the same index",
    },
    {
        "lighter_symbol": "SPY",
        "lighter_market_type": "index",
        "hyperliquid_symbols": "flx:USA500;km:US500;xyz:SP500;cash:USA500",
        "reason": "SPY ETF vs S&P 500 index contracts",
    },
    {
        "lighter_symbol": "QQQ",
        "lighter_market_type": "index",
        "hyperliquid_symbols": "km:USTECH",
        "reason": "QQQ ETF vs Nasdaq/US tech index exposure",
    },
    {
        "lighter_symbol": "BOTZ",
        "lighter_market_type": "index",
        "hyperliquid_symbols": "vntl:ROBOT",
        "reason": "robotics/AI thematic exposure, not the same ETF",
    },
    {
        "lighter_symbol": "MAGS",
        "lighter_market_type": "index",
        "hyperliquid_symbols": "vntl:MAG7",
        "reason": "Magnificent Seven ETF vs Magnificent Seven index basket",
    },
    {
        "lighter_symbol": "IWM",
        "lighter_market_type": "index",
        "hyperliquid_symbols": "km:SMALL2000",
        "reason": "Russell 2000 ETF vs small-cap index basket",
    },
]

REPORT_TEMPLATE = """# lighter RWA vs Hyperliquid TradFi/HIP-3 Snapshot

Snapshot date: {snapshot_date}

Data sources:
- lighter official RWA market-spec page: https://docs.lighter.xyz/trading/real-world-assets-rwas/market-specifications
- lighter public inventory endpoints: `/api/v1/orderBooks` and `/api/v1/orderBookDetails`
- Hyperliquid official info endpoint: `perpCategories`, `allPerpMetas`, and `allMids` with `dex=xyz`

Headline counts:
- lighter RWA markets tracked: {lighter_count}
- Hyperliquid non-crypto TradFi/HIP-3 markets from `perpCategories`: {hyper_category_count}
- Hyperliquid additional live-only markets confirmed outside `perpCategories`: {hyper_live_only_count}
- Exact or high-confidence normalized overlaps: {exact_overlap_count}
- Related-but-not-same ETF/index or proxy matches: {related_count}

Key Hyperliquid nuance:
- `perpCategories` is not exhaustive for every live builder-deployed market.
- The live endpoints confirmed a separate `cash:` universe that is absent from `perpCategories`.
- The live `xyz` inventory also exposed symbols absent from `perpCategories`, including `xyz:CORN`, `xyz:WHEAT`, `xyz:TTF`, `xyz:URANIUM`, and `xyz:XLE`.

High-confidence overlap set:
{exact_overlap_lines}

Related but not exact matches:
{related_lines}
"""


def canonicalize_lighter(symbol: str) -> str:
    return LIGHTER_CANONICAL.get(symbol, symbol)


def canonicalize_hyper(symbol: str) -> str:
    if symbol in HYPERLIQUID_CANONICAL:
        return HYPERLIQUID_CANONICAL[symbol]
    return symbol.split(":", 1)[1]


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_exact_overlaps() -> list[dict[str, str]]:
    lighter_by_canonical = {}
    for market in LIGHTER_RWA_MARKETS:
        lighter_by_canonical[canonicalize_lighter(market["symbol"])] = market

    hyper_grouped = defaultdict(list)
    for market in HYPERLIQUID_CATEGORY_MARKETS + HYPERLIQUID_LIVE_ONLY_MARKETS:
        hyper_grouped[canonicalize_hyper(market["symbol"])].append(market)

    overlap_rows = []
    for canonical, lighter_market in sorted(lighter_by_canonical.items()):
        matching_hyper = hyper_grouped.get(canonical, [])
        if not matching_hyper:
            continue
        overlap_rows.append(
            {
                "canonical_underlying": canonical,
                "lighter_symbol": lighter_market["symbol"],
                "lighter_market_type": lighter_market["market_type"],
                "hyperliquid_symbols": ";".join(market["symbol"] for market in matching_hyper),
                "hyperliquid_buckets": ";".join(sorted({market["bucket"] for market in matching_hyper})),
                "hyperliquid_sources": ";".join(sorted({market["source"] for market in matching_hyper})),
            }
        )
    return overlap_rows


def build_collision_rows(exact_overlap_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        {
            "Asset": row["canonical_underlying"],
            "Hyper": row["hyperliquid_symbols"],
            "lighter": row["lighter_symbol"],
        }
        for row in exact_overlap_rows
    ]


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    data_dir = repo_root / "data"
    reports_dir = repo_root / "reports"
    data_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    lighter_rows = []
    for market in LIGHTER_RWA_MARKETS:
        row = dict(market)
        row["canonical_underlying"] = canonicalize_lighter(market["symbol"])
        lighter_rows.append(row)

    hyper_rows = []
    for market in HYPERLIQUID_CATEGORY_MARKETS + HYPERLIQUID_LIVE_ONLY_MARKETS:
        row = dict(market)
        row["canonical_underlying"] = canonicalize_hyper(market["symbol"])
        row["live_only"] = "yes" if market["source"] != "perpCategories" else "no"
        row["note"] = market.get("note", "")
        hyper_rows.append(row)

    exact_overlap_rows = build_exact_overlaps()
    collision_rows = build_collision_rows(exact_overlap_rows)

    write_csv(
        data_dir / f"lighter_rwa_markets_{SNAPSHOT_DATE}.csv",
        ["symbol", "market_type", "canonical_underlying"],
        lighter_rows,
    )
    write_csv(
        data_dir / f"hyperliquid_tradfi_hip3_markets_{SNAPSHOT_DATE}.csv",
        ["symbol", "bucket", "dex", "source", "live_only", "canonical_underlying", "note"],
        hyper_rows,
    )
    write_csv(
        data_dir / f"lighter_hyperliquid_exact_overlap_{SNAPSHOT_DATE}.csv",
        [
            "canonical_underlying",
            "lighter_symbol",
            "lighter_market_type",
            "hyperliquid_symbols",
            "hyperliquid_buckets",
            "hyperliquid_sources",
        ],
        exact_overlap_rows,
    )
    write_csv(
        data_dir / f"lighter_hyperliquid_collision_list_{SNAPSHOT_DATE}.csv",
        ["Asset", "Hyper", "lighter"],
        collision_rows,
    )
    write_csv(
        data_dir / f"lighter_hyperliquid_related_matches_{SNAPSHOT_DATE}.csv",
        ["lighter_symbol", "lighter_market_type", "hyperliquid_symbols", "reason"],
        RELATED_MATCHES,
    )
    write_csv(
        data_dir / f"hyperliquid_live_only_additions_{SNAPSHOT_DATE}.csv",
        ["symbol", "bucket", "dex", "source", "note"],
        HYPERLIQUID_LIVE_ONLY_MARKETS,
    )

    exact_overlap_lines = "\n".join(
        f"- {row['lighter_symbol']} -> {row['hyperliquid_symbols']}" for row in exact_overlap_rows
    )
    related_lines = "\n".join(
        f"- {row['lighter_symbol']} -> {row['hyperliquid_symbols']} ({row['reason']})"
        for row in RELATED_MATCHES
    )

    report = REPORT_TEMPLATE.format(
        snapshot_date=SNAPSHOT_DATE,
        lighter_count=len(LIGHTER_RWA_MARKETS),
        hyper_category_count=len(HYPERLIQUID_CATEGORY_MARKETS),
        hyper_live_only_count=len(HYPERLIQUID_LIVE_ONLY_MARKETS),
        exact_overlap_count=len(exact_overlap_rows),
        related_count=len(RELATED_MATCHES),
        exact_overlap_lines=exact_overlap_lines,
        related_lines=related_lines,
    )
    (reports_dir / f"market_overlap_snapshot_{SNAPSHOT_DATE}.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
