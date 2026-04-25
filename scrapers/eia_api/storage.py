"""EIA Natural Gas Weekly Storage Scraper."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.safe_writer import StatePreservingWriter, safe_write_json
from scrapers.eia_api.client import EIAClient, load_api_key_from_env

log = logging.getLogger(__name__)

ROUTE = "natural-gas/stor/wkly"
SOURCE_NAME = "eia_storage"
RAW_DIR = Path("data/raw/eia_storage")

# 8 years of history gives the dashboard 5+ years for the seasonal envelope.
# EIA returns up to 5000 rows per call; 8y × 52w × 5 regions ≈ 2,080 rows.
START_DATE = "2018-01-01"


def _get_latest_local_date() -> str | None:
    """Find the most recent downloaded data date based on file names."""
    if not RAW_DIR.exists():
        return None
    files = list(RAW_DIR.rglob("eia_storage_*.json"))
    if not files:
        return None
    # Returns the largest timestamp date string
    return max(p.stem.replace("eia_storage_", "") for p in files)


def _get_latest_local_path() -> Path | None:
    """Return the Path of the most recent stored JSON file, or None."""
    if not RAW_DIR.exists():
        return None
    files = list(RAW_DIR.rglob("eia_storage_*.json"))
    if not files:
        return None
    return max(files, key=lambda p: p.stem)


def _count_existing_rows(path: Path | None) -> int:
    """Return row count from the latest stored JSON, or 0 if missing."""
    if not path or not path.exists():
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return len(data.get("response", {}).get("data", []))
    except Exception:
        return 0


async def run() -> dict[str, Any]:
    """Fetch latest weekly natural gas storage data from EIA."""
    health = HealthWriter(source_name=SOURCE_NAME)
    try:
        api_key = load_api_key_from_env()
    except RuntimeError as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}

    async with EIAClient(api_key=api_key) as client:
        try:
            latest_api_date = await client.get_latest_date(route=ROUTE, frequency="weekly")
        except Exception as exc:
            err = f"API error: {exc}"
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

        if not latest_api_date:
            err = "No data returned from EIA API latest-date check"
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

        latest_local = _get_latest_local_date()
        latest_existing_path = _get_latest_local_path()
        existing_rows = _count_existing_rows(latest_existing_path)

        # Staleness gate: skip only when the latest date is unchanged AND the
        # existing row count indicates a full backfill has already been done.
        # The 500-row threshold distinguishes the old 52-week fetch (~416 rows)
        # from the new 8-year backfill (~2,080 rows).
        # - First run post-commit: 416 rows < 500 → bypasses skip → fetches 2,080 rows.
        # - Subsequent same-date runs: rows >= 500 and date matches → skips cleanly.
        if latest_local == latest_api_date and existing_rows >= 500:
            health.record_skipped(reason=f"no new data since {latest_api_date}")
            return {"status": "skipped", "latest_date": latest_api_date}

        if latest_local == latest_api_date and existing_rows < 500:
            # Date unchanged but history is sparse — force a backfill re-fetch.
            log.info(
                "Sparse history detected (%d rows < 500). Re-fetching full backfill.",
                existing_rows,
            )

        data_to_write: dict[str, Any] | None = None

        async def compute_data() -> dict[str, Any]:
            nonlocal data_to_write
            data_to_write = await client.get_series(
                route=ROUTE,
                frequency="weekly",
                start=START_DATE,
                data_columns=["value"],
                length=5000,  # 8y × 52w × 5 regions ≈ 2,080 rows; well within EIA max
            )
            return data_to_write

        try:
            dt = datetime.strptime(latest_api_date, "%Y-%m-%d")
        except ValueError:
            dt = datetime.now()

        out_path = (
            RAW_DIR / str(dt.year) / f"{dt.month:02d}" / f"eia_storage_{latest_api_date}.json"
        )

        writer = StatePreservingWriter(source_name=SOURCE_NAME, writer=safe_write_json)
        success = await writer.guarded_write(out_path, compute_data)

        if success and data_to_write:
            # Overwrite the default health record with metadata
            resp_obj = data_to_write.get("response", {})
            rows_count = len(resp_obj.get("data", [])) if isinstance(resp_obj, dict) else 0

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
