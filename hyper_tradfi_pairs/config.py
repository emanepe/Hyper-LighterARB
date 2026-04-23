from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IBKRFutureSpec:
    symbol: str
    exchange: str
    currency: str = "USD"


@dataclass(frozen=True)
class PairDefinition:
    asset: str
    hyperliquid_coin: str
    hyperliquid_alternates: tuple[str, ...]
    lighter_symbol: str | None
    tradfi_dataset: str
    tradfi_symbol: str
    tradfi_stype_in: str
    tradfi_label: str
    ibkr_future: IBKRFutureSpec | None = None
    yahoo_fallback_symbol: str | None = None


# Verified against the Hyperliquid info endpoint on 2026-04-23.
PAIR_DEFINITIONS: dict[str, PairDefinition] = {
    "BRENTOIL": PairDefinition(
        asset="BRENTOIL",
        hyperliquid_coin="xyz:BRENTOIL",
        hyperliquid_alternates=(),
        lighter_symbol="BRENTOIL",
        tradfi_dataset="IFEU.IMPACT",
        tradfi_symbol="BRN.c.0",
        tradfi_stype_in="continuous",
        tradfi_label="ICE Brent front-month futures",
        ibkr_future=IBKRFutureSpec(symbol="BZ", exchange="NYMEX"),
        yahoo_fallback_symbol="BZ=F",
    ),
    "GOLD": PairDefinition(
        asset="GOLD",
        hyperliquid_coin="xyz:GOLD",
        hyperliquid_alternates=("cash:GOLD", "flx:GOLD", "km:GOLD"),
        lighter_symbol="XAU",
        tradfi_dataset="GLBX.MDP3",
        tradfi_symbol="GC.c.0",
        tradfi_stype_in="continuous",
        tradfi_label="COMEX Gold front-month futures",
        ibkr_future=IBKRFutureSpec(symbol="GC", exchange="COMEX"),
        yahoo_fallback_symbol="GC=F",
    ),
    "SILVER": PairDefinition(
        asset="SILVER",
        hyperliquid_coin="xyz:SILVER",
        hyperliquid_alternates=("cash:SILVER", "flx:SILVER", "km:SILVER"),
        lighter_symbol="XAG",
        tradfi_dataset="GLBX.MDP3",
        tradfi_symbol="SI.c.0",
        tradfi_stype_in="continuous",
        tradfi_label="COMEX Silver front-month futures",
        ibkr_future=IBKRFutureSpec(symbol="SI", exchange="COMEX"),
        yahoo_fallback_symbol="SI=F",
    ),
    "WTI": PairDefinition(
        asset="WTI",
        hyperliquid_coin="cash:WTI",
        hyperliquid_alternates=("xyz:CL", "flx:OIL", "km:USOIL"),
        lighter_symbol="WTI",
        tradfi_dataset="GLBX.MDP3",
        tradfi_symbol="CL.c.0",
        tradfi_stype_in="continuous",
        tradfi_label="NYMEX WTI front-month futures",
        ibkr_future=IBKRFutureSpec(symbol="CL", exchange="NYMEX"),
        yahoo_fallback_symbol="CL=F",
    ),
}

DEFAULT_ASSETS = tuple(PAIR_DEFINITIONS)


def get_pair_definition(asset: str) -> PairDefinition:
    normalized = asset.strip().upper()
    try:
        return PAIR_DEFINITIONS[normalized]
    except KeyError as exc:
        valid = ", ".join(DEFAULT_ASSETS)
        raise ValueError(f"Unsupported asset '{asset}'. Valid values: {valid}") from exc


def parse_assets(raw_assets: str | None) -> list[PairDefinition]:
    if raw_assets is None or raw_assets.strip().lower() == "all":
        return [PAIR_DEFINITIONS[asset] for asset in DEFAULT_ASSETS]
    return [get_pair_definition(chunk) for chunk in raw_assets.split(",") if chunk.strip()]
