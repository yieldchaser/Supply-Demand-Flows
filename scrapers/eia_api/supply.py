"""EIA Natural Gas Monthly Supply/Demand Scraper."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from scrapers.base.errors import ScraperError
from scrapers.base.health_writer import HealthWriter
from scrapers.base.safe_writer import StatePreservingWriter, safe_write_json
from scrapers.eia_api.client import EIAClient, load_api_key_from_env

log = logging.getLogger(__name__)

ROUTE = "natural-gas/sum/snd"
SOURCE_NAME = "eia_supply"
RAW_DIR = Path("data/raw/eia_supply")

PROCESS_CODES: dict[str, str] = {
    "FPD": "Dry Production",
    "OVI": "Input Supplemental Fuels",
    "SAI": "Underground Storage Injections",
    "SAW": "Underground Storage Withdrawals",
    "VC0": "Total Consumption",
    "VG4": "Balancing Item",
}


def _get_latest_local_date() -> str | None:
    """Find the most recent downloaded data date based on file names."""
    if not RAW_DIR.exists():
        return None
    files = list(RAW_DIR.rglob("eia_supply_*.json"))
    if not files:
        return None
    return max(p.stem.replace("eia_supply_", "") for p in files)


async def run() -> dict[str, Any]:
    """Fetch latest monthly natural gas supply and demand data from EIA."""
    health = HealthWriter(source_name=SOURCE_NAME)
    try:
        api_key = load_api_key_from_env()
    except RuntimeError as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}

    async with EIAClient(api_key=api_key) as client:
        try:
            latest_api_date = await client.get_latest_date(
                route=ROUTE,
                frequency="monthly",
                facets={
                    "duoarea": ["NUS"],
                    "process": list(PROCESS_CODES.keys()),
                },
            )
        except Exception as exc:
            err = f"API error: {exc}"
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

        if not latest_api_date:
            err = "No data returned from EIA API latest-date check"
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

        latest_local = _get_latest_local_date()
        if latest_local == latest_api_date:
            health.record_skipped(reason=f"no new data since {latest_api_date}")
            return {"status": "skipped", "latest_date": latest_api_date}

        data_to_write: dict[str, Any] | None = None

        async def compute_data() -> dict[str, Any]:
            nonlocal data_to_write
            data_to_write = await client.get_series(
                route=ROUTE,
                frequency="monthly",
                facets={
                    "duoarea": ["NUS"],
                    "process": list(PROCESS_CODES.keys()),
                },
                data_columns=["value"],
                start="2020-01",
                length=5000,
            )
            return data_to_write

        try:
            dt = datetime.strptime(latest_api_date, "%Y-%m")
        except ValueError:
            dt = datetime.now()

        out_path = RAW_DIR / str(dt.year) / f"{dt.month:02d}" / f"eia_supply_{latest_api_date}.json"

        writer = StatePreservingWriter(source_name=SOURCE_NAME, writer=safe_write_json)
        success = await writer.guarded_write(out_path, compute_data)

        if success and data_to_write:
            resp_obj = data_to_write.get("response", {})
            rows = resp_obj.get("data", []) if isinstance(resp_obj, dict) else []
            rows_count = len(rows)

            soft_process_codes = {"VG4"}  # Balancing Item publishes later than others
            found_processes = {row.get("process") for row in rows if isinstance(row, dict)}
            missing_processes = set(PROCESS_CODES.keys()) - found_processes

            hard_empty = [c for c in missing_processes if c not in soft_process_codes]
            soft_empty = [c for c in missing_processes if c in soft_process_codes]

            if hard_empty:
                msg = f"Missing expected process codes: {', '.join(hard_empty)}"
                health.record_failure(error=msg)
                raise ScraperError(msg)

            if soft_empty:
                log.warning(f"Soft-missing (expected, will retry later): {', '.join(soft_empty)}")

            health.record_success(metadata={"latest_date": latest_api_date, "rows": rows_count})
            return {
                "status": "ok",
                "latest_date": latest_api_date,
                "rows": rows_count,
                "path": str(out_path),
            }

        return {"status": "failed"}


if __name__ == "__main__":
    result = asyncio.run(run())
    print(json.dumps(result, indent=2))
