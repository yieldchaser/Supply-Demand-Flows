"""EIA Monthly LNG Exports by Destination Country Scraper.

Route & facet verification (2026-04-30, production API)
--------------------------------------------------------
EIA v2 route: natural-gas/move/expc  (Natural Gas Exports by Country)

Process facet discovery via /v2/natural-gas/move/expc/facet/process/:
    ENG = 'Liquefied Natural Gas Exports'     → US aggregate only (NUS-Z00)
    EVE = 'Exports by Vessel'                 → per-country LNG breakdown ← CORRECT
    ENP = 'Pipeline Exports'                  → Mexico/Canada only
    EVT = 'Exports by Vessel and Truck'       → US aggregate only

Each (country, period) under EVE has TWO rows:
    units=MMCF   → volume (what we want)
    units=$/MCF  → price  (excluded by units filter)

Confirmed schema for process=EVE rows:
    period    → 'YYYY-MM'
    duoarea   → EIA region code e.g. 'NUS-NNL' (destination)
    area-name → 3-letter country code e.g. 'NLD', 'JPN', 'KOR'
    process   → 'EVE'
    value     → volume in MMcf (or price — filter by units='MMCF')
    units     → 'MMCF' (volume) | '$/MCF' (price — skip)

Sample (Feb 2026): NLD = 56,798 MMcf; total US LNG (EVT/ENG) = 493,617 MMcf.
Sum of per-country EVE volumes will reconcile to the ENG US aggregate.

Output JSON schema
------------------
{
  "fetched_at": "...",
  "start_date": "2021-01-01",
  "latest_period": "2026-02",
  "row_count": ~1700,
  "data": [
    {
      "period": "2026-02",
      "destination_code": "NLD",
      "destination_name": "NLD",
      "value_mmcf": 56798.0,
      "process": "EVE"
    }, ...
  ]
}
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.safe_writer import StatePreservingWriter, safe_write_json
from scrapers.eia_api.client import EIAClient, load_api_key_from_env

log = logging.getLogger(__name__)

ROUTE = "natural-gas/move/expc"
SOURCE_NAME = "eia_lng"
RAW_DIR = Path("data/raw/eia_lng")
RAW_PATH = RAW_DIR / "lng_exports.json"

# Verified 2026-04-30 via /v2/natural-gas/move/expc/facet/process/
# EVE = 'Exports by Vessel' — the per-country LNG breakdown in MMcf
PROCESS_FACET = "EVE"

# 5 years of history for seasonal analysis + trend comparisons.
START_DATE = "2021-01-01"

# EIA includes a U.S. national aggregate row under EVE for some series.
# Exclude it — we want per-destination rows only.
AGGREGATE_DUOAREA = "NUS-Z00"


def _read_prior_state() -> tuple[str | None, int]:
    """Return (latest_period, row_count) from the last saved raw file, or (None, 0)."""
    if not RAW_PATH.exists():
        return None, 0
    try:
        payload = json.loads(RAW_PATH.read_text(encoding="utf-8"))
        return payload.get("latest_period"), payload.get("row_count", 0)
    except Exception:
        return None, 0


def _coerce_rows(api_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalise EIA response rows into our canonical output shape.

    EVE schema:
        period    — 'YYYY-MM'
        duoarea   — EIA region code; 'NUS-Z00' = US aggregate (excluded)
        area-name — 3-letter destination country code (NLD, JPN, KOR, ...)
        process   — 'EVE'
        value     — volume in MMcf OR price in $/MCF
        units     — 'MMCF' (volume rows we keep) | '$/MCF' (price rows we skip)
    """
    out: list[dict[str, Any]] = []
    for row in api_rows:
        period = row.get("period")
        duoarea = row.get("duoarea", "")
        area_name = row.get("area-name", "")
        raw_value = row.get("value")
        units = row.get("units", "")
        process = row.get("process", PROCESS_FACET)

        if not period or raw_value is None:
            continue

        # Keep only volume rows — skip price rows ($/MCF)
        if "MCF" not in units.upper() or "$" in units:
            continue

        # Exclude US national aggregate row
        if duoarea == AGGREGATE_DUOAREA or area_name in ("U.S.", "US"):
            continue

        try:
            value_mmcf = float(raw_value)
        except (ValueError, TypeError):
            continue

        if value_mmcf < 0:
            continue

        dest_code = area_name.upper().strip()

        out.append(
            {
                "period": period,
                "destination_code": dest_code,
                "destination_name": area_name,
                "value_mmcf": value_mmcf,
                "process": str(process),
            }
        )
    return out


async def _get_latest_period(client: EIAClient) -> str | None:
    """Get latest available period for LNG exports.

    Tries process=EVE first; falls back to unfiltered if empty
    (defensive against future EIA schema changes).
    """
    resp = await client.get_series(
        route=ROUTE,
        frequency="monthly",
        facets={"process": [PROCESS_FACET]},
        data_columns=["value"],
        length=1,
    )
    data = resp.get("response", {}).get("data", [])
    if data:
        return str(data[0].get("period"))

    log.warning(
        "process=%s probe returned empty; falling back to unfiltered latest-period check.",
        PROCESS_FACET,
    )
    resp_all = await client.get_series(
        route=ROUTE,
        frequency="monthly",
        data_columns=["value"],
        length=1,
    )
    data_all = resp_all.get("response", {}).get("data", [])
    return str(data_all[0].get("period")) if data_all else None


async def _fetch_lng_rows(client: EIAClient) -> list[dict[str, Any]]:
    """Fetch LNG export rows using process=EVE facet.

    Falls back to unfiltered + in-memory filter if the facet returns empty.
    """
    resp = await client.get_series(
        route=ROUTE,
        frequency="monthly",
        start=START_DATE,
        data_columns=["value"],
        facets={"process": [PROCESS_FACET]},
        length=5000,
    )
    api_rows: list[dict[str, Any]] = resp.get("response", {}).get("data", [])
    log.info("LNG facet (process=%s) fetch returned %d API rows.", PROCESS_FACET, len(api_rows))

    if not api_rows:
        log.warning(
            "process=%s returned no data; fetching all exports and filtering to EVE rows.",
            PROCESS_FACET,
        )
        resp_all = await client.get_series(
            route=ROUTE,
            frequency="monthly",
            start=START_DATE,
            data_columns=["value"],
            length=5000,
        )
        api_rows = [
            r for r in resp_all.get("response", {}).get("data", [])
            if r.get("process") == PROCESS_FACET
        ]
        log.info("Fallback filtered to %d EVE rows.", len(api_rows))

    return api_rows


async def run() -> dict[str, Any]:
    """Fetch latest monthly EIA LNG export data by destination country."""
    health = HealthWriter(source_name=SOURCE_NAME)

    try:
        api_key = load_api_key_from_env()
    except RuntimeError as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}

    async with EIAClient(api_key=api_key) as client:
        try:
            latest_api_period = await _get_latest_period(client)
        except Exception as exc:
            err = f"API error fetching latest period: {exc}"
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

        if not latest_api_period:
            err = "No data returned from EIA LNG latest-period check"
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

        prior_period, prior_rows = _read_prior_state()

        # Staleness gate: skip if period unchanged AND we have a meaningful backfill.
        # 300-row threshold: 5y × 12mo × ~5 destinations minimum.
        if prior_period == latest_api_period and prior_rows >= 300:
            health.record_skipped(reason=f"no new data since {latest_api_period}")
            return {"status": "skipped", "latest_period": latest_api_period}

        if prior_period == latest_api_period and prior_rows < 300:
            log.info(
                "Sparse history (%d rows < 300). Re-fetching full backfill.", prior_rows
            )

        data_to_write: dict[str, Any] | None = None

        async def compute_data() -> dict[str, Any]:
            nonlocal data_to_write
            api_rows = await _fetch_lng_rows(client)
            coerced = _coerce_rows(api_rows)

            actual_latest = latest_api_period
            if coerced:
                actual_latest = max(r["period"] for r in coerced)

            data_to_write = {
                "fetched_at": datetime.now(UTC).isoformat(),
                "start_date": START_DATE,
                "latest_period": actual_latest,
                "row_count": len(coerced),
                "data": coerced,
            }
            return data_to_write

        RAW_DIR.mkdir(parents=True, exist_ok=True)
        writer = StatePreservingWriter(source_name=SOURCE_NAME, writer=safe_write_json)
        success = await writer.guarded_write(RAW_PATH, compute_data)

        if success and data_to_write:
            row_count = data_to_write.get("row_count", 0)
            actual_latest = data_to_write.get("latest_period", latest_api_period)
            health.record_success(
                metadata={"latest_period": actual_latest, "rows": row_count}
            )
            log.info(
                "EIA LNG scraper OK: %d rows, latest period %s", row_count, actual_latest
            )
            return {
                "status": "ok",
                "latest_period": actual_latest,
                "rows": row_count,
                "path": str(RAW_PATH),
            }

        return {"status": "failed"}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    result = asyncio.run(run())
    print(json.dumps(result, indent=2))
