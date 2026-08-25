"""EIA storage raw JSON → curated long-format Parquet (ACCUMULATING).

The raw API rows carry `duoarea` (R31..R35, R48) and `series`
(NW2_EPG0_S{NO,SO,WO}_R*_BCF). The old code read `area-name` which is 'NA' on
every row — the labels were lost at ingestion. Mapping (verified against the
one retained labeled raw file, 51 weeks):

    NW2_EPG0_SWO_R48_BCF  -> Lower 48   (US total for WNGSR purposes)
    NW2_EPG0_SWO_R31_BCF  -> East
    NW2_EPG0_SWO_R32_BCF  -> Midwest
    NW2_EPG0_SWO_R33_BCF  -> South Central
    NW2_EPG0_SWO_R34_BCF  -> Mountain
    NW2_EPG0_SWO_R35_BCF  -> Pacific
    NW2_EPG0_SNO_R33_BCF  -> South Central Nonsalt
    NW2_EPG0_SSO_R33_BCF  -> South Central Salt

series_id becomes unique per series (e.g. storage_sc_salt), so nothing
collides in merge_into_curated.

WHY ACCUMULATE (sixth silent-data-loss incident, 2026-08-25):
    This transformer once rebuilt curated from thin raw via a direct
    ``safe_write_parquet``. When the regional-relabel fix shipped, retained
    raw held only ONE labeled file (51 weeks) and the nightly transform
    silently truncated curated from 3,600 rows / 450 weeks to 408 rows /
    51 weeks ending 2026-04-10 — an 88% loss invisible to scraper health.
    ``transform()`` now merges through ``merge_into_curated`` (dedup on
    series_id+period keeping freshest ingest, shrink-guard, atomic write)
    so rebuilding from any subset of raw can NEVER erase accumulated
    history again.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from transformers.base.accumulate import merge_into_curated

# series-id stem → canonical region label
_SERIES_REGION: dict[str, str] = {
    "NW2_EPG0_SWO_R48_BCF": "Lower 48",
    "NW2_EPG0_SWO_R31_BCF": "East",
    "NW2_EPG0_SWO_R32_BCF": "Midwest",
    "NW2_EPG0_SWO_R33_BCF": "South Central",
    "NW2_EPG0_SWO_R34_BCF": "Mountain",
    "NW2_EPG0_SWO_R35_BCF": "Pacific",
    "NW2_EPG0_SNO_R33_BCF": "South Central Nonsalt",
    "NW2_EPG0_SSO_R33_BCF": "South Central Salt",
}

# canonical slug used inside series_id
_SLUG: dict[str, str] = {
    "Lower 48": "lower48",
    "East": "east",
    "Midwest": "midwest",
    "South Central": "south_central",
    "Mountain": "mountain",
    "Pacific": "pacific",
    "South Central Nonsalt": "sc_nonsalt",
    "South Central Salt": "sc_salt",
}

_FALLBACK_DUOAREA: dict[str, str] = {
    "R48": "Lower 48",
    "R31": "East",
    "R32": "Midwest",
    "R33": "South Central",
    "R34": "Mountain",
    "R35": "Pacific",
}


def _region_for(row: dict[str, Any]) -> str | None:
    """Resolve the region label from `series`, falling back to `duoarea`+process.

    Why:
        The EIA v2 API's `area-name` is literally "NA" for every row of this
        dataset; the real geography lives in `duoarea` and the series id.
        Salt/nonsalt distinction is encoded by SNO/SSO in the series stem.
    """
    series = str(row.get("series") or "")
    if series in _SERIES_REGION:
        return _SERIES_REGION[series]
    duo = str(row.get("duoarea") or "")
    process = str(row.get("process") or "")
    region = _FALLBACK_DUOAREA.get(duo)
    if region is None:
        return None
    if process == "SNO":
        return f"{region} Nonsalt"
    if process == "SSO":
        return f"{region} Salt"
    return region


def _series_slug(region: str) -> str:
    base = _SLUG.get(region)
    if base:
        return f"storage_{base}"
    slug = re.sub(r"[^a-z0-9]+", "_", region.lower()).strip("_")
    return f"storage_{slug}"


def transform(raw_json_path: Path, curated_parquet_path: Path) -> dict[str, Any]:
    """Transform EIA storage raw JSON and ACCUMULATE into curated Parquet.

    What:
        One row per (series, week); region labels resolved from `series` /
        `duoarea` (NOT `area-name`, which is constant "NA"); series_id is
        unique per region series so accumulation never collides. The batch
        merges into the existing curated history via
        ``merge_into_curated`` — a rebuild from thin raw can no longer
        shrink the artefact (shrink-guard raises instead).

    Returns summary metrics of the transformation.
    """
    with raw_json_path.open("r", encoding="utf-8") as f:
        raw_data = json.load(f)

    rows = []
    ingested_at = datetime.now(UTC).isoformat()

    # EIA v2 API response structure: {"response": {"data": [...]}}
    data = raw_data.get("response", {}).get("data", [])
    skipped = 0
    for row in data:
        # Real keys: period, duoarea, area-name ('NA'), product, process,
        # series, series-description, value, units.
        period = row.get("period")
        region = _region_for(row)
        value = row.get("value")

        if not period or region is None or value is None:
            skipped += 1
            continue

        desc = str(row.get("series-description") or f"Weekly Natural Gas Storage - {region}")
        rows.append(
            {
                "source": "eia",
                "series_id": _series_slug(region),
                "series_name": f"{desc.split('(')[0].strip()} - {region}",
                "period": period,
                "value": float(value),
                "unit": "Bcf",
                "region": region,
                "ingested_at": ingested_at,
            }
        )

    if not rows:
        return {
            "rows": 0,
            "period_range": (None, None),
            "regions": [],
            "skipped_unlabeled": skipped,
        }

    new_df = pd.DataFrame(rows)
    merged = merge_into_curated(new_df, curated_parquet_path)

    return {
        "rows": len(merged),
        "rows_new_batch": len(new_df),
        "period_range": (merged["period"].min(), merged["period"].max()),
        "regions": sorted(merged["region"].unique().tolist()),
        "skipped_unlabeled": skipped,
    }
