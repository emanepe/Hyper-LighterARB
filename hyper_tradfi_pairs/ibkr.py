from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
from ib_insync import Future, IB, Ticker, util

from hyper_tradfi_pairs.config import PairDefinition


util.patchAsyncio()


@dataclass(frozen=True)
class ResolvedIBKRContract:
    asset: str
    symbol: str
    local_symbol: str
    exchange: str
    currency: str
    con_id: int
    last_trade_date_or_contract_month: str
    contract: Future


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _ib_datetime_string(value: datetime) -> str:
    utc_value = _ensure_utc(value)
    return utc_value.strftime("%Y%m%d %H:%M:%S UTC")


def _contract_month_to_date(raw_value: str) -> date:
    if len(raw_value) >= 8:
        return datetime.strptime(raw_value[:8], "%Y%m%d").date()
    if len(raw_value) == 6:
        return datetime.strptime(raw_value, "%Y%m").date()
    raise ValueError(f"Unsupported contract month format: {raw_value}")


def _resolve_asof_date(start: datetime | None = None, day_string: str | None = None) -> date:
    if start is not None:
        return _ensure_utc(start).date()
    if day_string is not None:
        return date.fromisoformat(day_string)
    return datetime.now(timezone.utc).date()


def connect_ibkr(
    host: str = "127.0.0.1",
    port: int = 7497,
    client_id: int = 71,
    readonly: bool = True,
    timeout: int = 20,
) -> IB:
    ib = IB()
    ib.connect(host, port, clientId=client_id, readonly=readonly, timeout=timeout)
    return ib


def resolve_front_future_contract(
    ib: IB,
    pair: PairDefinition,
    asof_date: date | None = None,
) -> ResolvedIBKRContract:
    if pair.ibkr_future is None:
        raise ValueError(f"No IBKR contract mapping configured for {pair.asset}")

    asof = asof_date or datetime.now(timezone.utc).date()
    spec = pair.ibkr_future
    probe = Future(symbol=spec.symbol, exchange=spec.exchange, currency=spec.currency)
    details = ib.reqContractDetails(probe)
    if not details:
        raise ValueError(
            f"IBKR returned no contract details for {pair.asset} "
            f"({spec.symbol} on {spec.exchange})"
        )

    candidates = []
    for detail in details:
        contract = detail.contract
        ltd = contract.lastTradeDateOrContractMonth or ""
        if not ltd:
            continue
        try:
            expiry_date = _contract_month_to_date(ltd)
        except ValueError:
            continue
        if expiry_date < asof:
            continue
        candidates.append((expiry_date, contract))

    if not candidates:
        raise ValueError(
            f"No non-expired IBKR futures contracts found for {pair.asset} "
            f"on or after {asof.isoformat()}"
        )

    candidates.sort(key=lambda item: item[0])
    resolved = candidates[0][1]
    ib.qualifyContracts(resolved)
    return ResolvedIBKRContract(
        asset=pair.asset,
        symbol=resolved.symbol,
        local_symbol=resolved.localSymbol,
        exchange=resolved.exchange,
        currency=resolved.currency,
        con_id=resolved.conId,
        last_trade_date_or_contract_month=resolved.lastTradeDateOrContractMonth,
        contract=resolved,
    )


def _build_snapshot_row(
    *,
    pair: PairDefinition,
    resolved: ResolvedIBKRContract,
    timestamp: datetime,
    bid_px: float,
    ask_px: float,
    bid_sz: float,
    ask_sz: float,
    source: str,
    capture_time_utc: str | None = None,
) -> dict[str, object]:
    mid_px = (bid_px + ask_px) / 2.0
    spread_px = ask_px - bid_px
    spread_bps = 0.0 if math.isclose(mid_px, 0.0) else 10_000 * spread_px / mid_px
    return {
        "timestamp": _ensure_utc(timestamp).isoformat().replace("+00:00", "Z"),
        "capture_time_utc": capture_time_utc or _utc_now_iso(),
        "asset": pair.asset,
        "symbol": resolved.symbol,
        "local_symbol": resolved.local_symbol,
        "exchange": resolved.exchange,
        "currency": resolved.currency,
        "con_id": resolved.con_id,
        "bid_px_00": bid_px,
        "ask_px_00": ask_px,
        "bid_sz_00": bid_sz,
        "ask_sz_00": ask_sz,
        "bid_ct_00": 0,
        "ask_ct_00": 0,
        "mid_px": mid_px,
        "spread_px": spread_px,
        "spread_bps": spread_bps,
        "source": source,
    }


def _normalize_tick_time(raw_time: object) -> datetime:
    if isinstance(raw_time, datetime):
        return _ensure_utc(raw_time)
    if isinstance(raw_time, (int, float)):
        return datetime.fromtimestamp(float(raw_time), tz=timezone.utc)
    raise TypeError(f"Unsupported IBKR tick time type: {type(raw_time)!r}")


def _safe_market_data_number(value: object) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric):
        return None
    return numeric


def fetch_historical_bid_ask_1s_frame(
    ib: IB,
    pair: PairDefinition,
    start: datetime,
    end: datetime,
    use_rth: bool = False,
    fill_missing_seconds: bool = True,
    max_ticks_per_request: int = 1000,
) -> pd.DataFrame:
    resolved = resolve_front_future_contract(ib, pair, asof_date=_ensure_utc(start).date())
    current = _ensure_utc(start)
    finish = _ensure_utc(end)
    rows: list[dict[str, object]] = []

    while current < finish:
        ticks = ib.reqHistoricalTicks(
            resolved.contract,
            startDateTime=_ib_datetime_string(current),
            endDateTime="",
            numberOfTicks=max_ticks_per_request,
            whatToShow="BID_ASK",
            useRth=use_rth,
            ignoreSize=False,
            miscOptions=[],
        )
        if not ticks:
            current += timedelta(minutes=30)
            continue

        in_range_count = 0
        latest_tick_time: datetime | None = None
        for tick in ticks:
            tick_time = _normalize_tick_time(tick.time)
            if tick_time < current:
                continue
            if tick_time >= finish:
                continue
            in_range_count += 1
            latest_tick_time = tick_time
            rows.append(
                _build_snapshot_row(
                    pair=pair,
                    resolved=resolved,
                    timestamp=tick_time.replace(microsecond=0),
                    bid_px=float(tick.priceBid),
                    ask_px=float(tick.priceAsk),
                    bid_sz=float(tick.sizeBid),
                    ask_sz=float(tick.sizeAsk),
                    source="ibkr_historical",
                )
            )

        if latest_tick_time is None:
            break

        current = latest_tick_time + timedelta(seconds=1)
        if in_range_count == 0:
            break

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values(["timestamp", "capture_time_utc"]).groupby("timestamp", as_index=False).last()

    if fill_missing_seconds:
        start_idx = pd.Timestamp(start.astimezone(timezone.utc)).floor("s")
        end_idx = pd.Timestamp(end.astimezone(timezone.utc)).floor("s") - pd.Timedelta(seconds=1)
        if start_idx <= end_idx:
            full_index = pd.date_range(start=start_idx, end=end_idx, freq="1s", tz="UTC")
            frame = frame.set_index("timestamp").reindex(full_index)
            for column in [
                "asset",
                "symbol",
                "local_symbol",
                "exchange",
                "currency",
                "con_id",
                "bid_px_00",
                "ask_px_00",
                "bid_sz_00",
                "ask_sz_00",
                "bid_ct_00",
                "ask_ct_00",
                "mid_px",
                "spread_px",
                "spread_bps",
            ]:
                frame[column] = frame[column].ffill()
            frame["source"] = frame["source"].fillna("ibkr_historical")
            frame["capture_time_utc"] = frame["capture_time_utc"].fillna(_utc_now_iso())
            frame = frame.dropna(subset=["bid_px_00", "ask_px_00"]).reset_index(names="timestamp")

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return frame


def write_ibkr_historical_day(
    ib: IB,
    pair: PairDefinition,
    output_root: Path,
    day_string: str,
    use_rth: bool = False,
    fill_missing_seconds: bool = True,
) -> Path:
    start = datetime.combine(date.fromisoformat(day_string), time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    frame = fetch_historical_bid_ask_1s_frame(
        ib=ib,
        pair=pair,
        start=start,
        end=end,
        use_rth=use_rth,
        fill_missing_seconds=fill_missing_seconds,
    )
    asset_dir = output_root / pair.asset
    asset_dir.mkdir(parents=True, exist_ok=True)
    path = asset_dir / f"{day_string}.csv"
    frame.to_csv(path, index=False)
    return path


def record_ibkr_live_1s(
    ib: IB,
    pairs: Iterable[PairDefinition],
    output_root: Path,
    duration_seconds: float | None = None,
    market_data_type: int = 1,
) -> None:
    ib.reqMarketDataType(market_data_type)
    resolved_pairs: list[tuple[PairDefinition, ResolvedIBKRContract, Ticker]] = []

    for pair in pairs:
        resolved = resolve_front_future_contract(ib, pair)
        ticker = ib.reqMktData(resolved.contract, genericTickList="", snapshot=False, regulatorySnapshot=False)
        resolved_pairs.append((pair, resolved, ticker))

    started = datetime.now(timezone.utc)
    next_tick = math.ceil(datetime.now().timestamp())
    writers: dict[tuple[str, str], tuple[object, object]] = {}
    fieldnames = [
        "timestamp",
        "capture_time_utc",
        "asset",
        "symbol",
        "local_symbol",
        "exchange",
        "currency",
        "con_id",
        "bid_px_00",
        "ask_px_00",
        "bid_sz_00",
        "ask_sz_00",
        "bid_ct_00",
        "ask_ct_00",
        "mid_px",
        "spread_px",
        "spread_bps",
        "source",
    ]

    def ensure_writer(asset: str, day_string: str):
        import csv

        key = (asset, day_string)
        if key in writers:
            return writers[key][1]
        asset_dir = output_root / asset
        asset_dir.mkdir(parents=True, exist_ok=True)
        path = asset_dir / f"{day_string}.csv"
        handle = path.open("a", newline="", encoding="utf-8")
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if path.stat().st_size == 0:
            writer.writeheader()
        writers[key] = (handle, writer)
        return writer

    try:
        while True:
            now_ts = datetime.now().timestamp()
            sleep_for = max(0.0, next_tick - now_ts)
            if sleep_for:
                ib.sleep(sleep_for)

            capture_time = datetime.now(timezone.utc)
            capture_iso = capture_time.isoformat().replace("+00:00", "Z")
            day_string = capture_iso[:10]
            snapshot_time = capture_time.replace(microsecond=0)

            for pair, resolved, ticker in resolved_pairs:
                bid = _safe_market_data_number(ticker.bid)
                ask = _safe_market_data_number(ticker.ask)
                bid_size = _safe_market_data_number(ticker.bidSize)
                ask_size = _safe_market_data_number(ticker.askSize)
                if bid is None or ask is None:
                    continue
                row = _build_snapshot_row(
                    pair=pair,
                    resolved=resolved,
                    timestamp=snapshot_time,
                    bid_px=bid,
                    ask_px=ask,
                    bid_sz=0.0 if bid_size is None else bid_size,
                    ask_sz=0.0 if ask_size is None else ask_size,
                    source="ibkr_live",
                    capture_time_utc=capture_iso,
                )
                writer = ensure_writer(pair.asset, day_string)
                writer.writerow(row)
                writers[(pair.asset, day_string)][0].flush()

            next_tick += 1.0
            if duration_seconds is not None:
                elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                if elapsed >= duration_seconds:
                    return
    finally:
        for _pair, _resolved, ticker in resolved_pairs:
            ib.cancelMktData(ticker.contract)
        for handle, _writer in writers.values():
            handle.close()
