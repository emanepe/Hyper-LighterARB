from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from hyper_tradfi_pairs.config import PairDefinition


HISTORICAL_URL = "https://hist.databento.com/v0/timeseries.get_range"


def utc_day_bounds(day_string: str) -> tuple[str, str]:
    day = date.fromisoformat(day_string)
    start = datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


class DatabentoHistoricalClient:
    def __init__(self, api_key: str | None = None, timeout: int = 120) -> None:
        resolved_api_key = api_key or os.environ.get("DATABENTO_API_KEY")
        if not resolved_api_key:
            raise RuntimeError("Set DATABENTO_API_KEY before downloading Databento data.")
        self.api_key = resolved_api_key
        self.timeout = timeout
        self.session = requests.Session()

    def download_bbo_1s_csv(
        self,
        pair: PairDefinition,
        start: str,
        end: str,
    ) -> str:
        payload = {
            "dataset": pair.tradfi_dataset,
            "symbols": pair.tradfi_symbol,
            "schema": "bbo-1s",
            "stype_in": pair.tradfi_stype_in,
            "start": start,
            "end": end,
            "encoding": "csv",
            "pretty_px": "true",
            "pretty_ts": "true",
            "map_symbols": "true",
            "compression": "none",
        }
        response = self.session.post(
            HISTORICAL_URL,
            data=payload,
            auth=(self.api_key, ""),
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.text

    def download_bbo_1s_to_path(
        self,
        pair: PairDefinition,
        output_root: Path,
        day_string: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> Path:
        if day_string is not None:
            resolved_start, resolved_end = utc_day_bounds(day_string)
            file_name = f"{day_string}.csv"
        elif start is not None and end is not None:
            resolved_start = start
            resolved_end = end
            start_token = start.replace(":", "-")
            end_token = end.replace(":", "-")
            file_name = f"{start_token}__{end_token}.csv"
        else:
            raise ValueError("Provide either day_string or both start and end.")

        text = self.download_bbo_1s_csv(pair=pair, start=resolved_start, end=resolved_end)
        asset_dir = output_root / pair.asset
        asset_dir.mkdir(parents=True, exist_ok=True)
        output_path = asset_dir / file_name
        output_path.write_text(text, encoding="utf-8")
        return output_path

