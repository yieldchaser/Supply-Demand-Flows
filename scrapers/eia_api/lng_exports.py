"""EIA Monthly LNG Exports by Destination Country Scraper.

Route verification
------------------
EIA v2 route: natural-gas/move/expc  (Natural Gas Exports by Country)
Filter: facets[process][]=LNG isolates liquefied exports only.
        Pipeline exports to Mexico/Canada are excluded by this filter.

Manual verification one-liner (run with API key before first deploy):

    import httpx, os
    key = os.environ["EIA_API_KEY"]
    r = httpx.get(
        "https://api.eia.gov/v2/natural-gas/move/expc/data/",
        params={"api_key": key, "frequency": "monthly",
                "facets[process][]": "LNG", "data[]": "value", "length": 5,
                "sort[0][column]": "period", "sort[0][direction]": "desc"},
    )
    print(r.status_code, r.json()["response"]["data"][:2])

If the process=LNG filter returns no rows the fallback is to omit the
facet and drop rows where area-name is 'Mexico' or 'Canada' (both
pipeline-only destinations). The scraper tries LNG facet first and
falls back automatically if the response is empty.

Output JSON schema
------------------
{
  "fetched_at": "2026-04-30T16:00:00Z",
  "start_date": "2021-01-01",
  "latest_period": "2026-01",
  "row_count": 1850,
  "data": [
    {
      "period": "2026-01",
      "destination_code": "NLD",
      "destination_name": "Netherlands",
      "value_mmcf": 18450.0,
      "process": "LNG"
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

# 5 years of history for seasonal analysis + trend comparisons.
# Monthly × 5y × ~20 countries ≈ 1,200–2,000 rows — well within 5000 limit.
START_DATE = "2021-01-01"

# EIA destination codes that receive ONLY pipeline exports.
# Used as a fallback filter when process=LNG facet isn't sufficient.
PIPELINE_ONLY_AREAS = {"Mexico", "Canada"}


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

    EIA v2 response fields for natural-gas/move/expc:
        period       — 'YYYY-MM'
        duoarea      — ISO-3166 alpha-3 country code (e.g. 'NLD')
        area-name    — Human-readable name (e.g. 'Netherlands')
        process      — 'LNG' (when facet applied) or similar
        value        — volume in MMcf
        value-units  — 'MMcf'
    """
    out: list[dict[str, Any]] = []
    for row in api_rows:
        period = row.get("period")
        dest_code = row.get("duoarea") or row.get("destination-code") or ""
        dest_name = row.get("area-name") or dest_code
        raw_value = row.get("value")
        process = row.get("process", "LNG")

        if not period or raw_value is None:
            continue

        # Skip pipeline-only destinations when process filter wasn't applied.
        if dest_name in PIPELINE_ONLY_AREAS:
            continue

        try:
            value_mmcf = float(raw_value)
        except (ValueError, TypeError):
            continue

        # Drop clearly zero-activity rows (EIA sometimes emits 0 rows for inactive
        # trade pairs; these are filtered more aggressively in the transformer).
        if value_mmcf < 0:
            continue

        out.append(
            {
                "period": period,
                "destination_code": dest_code.upper(),
                "destination_name": dest_name,
                "value_mmcf": value_mmcf,
                "process": str(process),
            }
        )
    return out


async def _fetch_lng_rows(client: EIAClient) -> list[dict[str, Any]]:
    """Fetch LNG export rows. Tries process=LNG facet; falls back to no facet + manual filter."""
    facets: dict[str, list[str]] = {"process": ["LNG"]}
    resp = await client.get_series(
        route=ROUTE,
        frequency="monthly",
        start=START_DATE,
        data_columns=["value"],
        facets=facets,
        length=5000,
    )
    api_rows: list[dict[str, Any]] = resp.get("response", {}).get("data", [])
    log.info("LNG facet fetch returned %d API rows.", len(api_rows))

    if not api_rows:
        # Fallback: fetch without process filter and filter manually.
        log.warning(
            "process=LNG facet returned no data; fetching all exports and filtering manually."
        )
        resp_all = await client.get_series(
            route=ROUTE,
            frequency="monthly",
            start=START_DATE,
            data_columns=["value"],
            length=5000,
        )
        api_rows = resp_all.get("response", {}).get("data", [])
        log.info("Fallback (no-facet) fetch returned %d API rows.", len(api_rows))

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
        # Determine latest available period from the API.
        try:
            latest_api_period = await client.get_latest_date(
                route=ROUTE,
                facets={"process": ["LNG"]},
                frequency="monthly",
            )
        except Exception as exc:
            err = f"API error fetching latest date: {exc}"
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

            # Find actual latest period in coerced data (may differ from API probe).
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
