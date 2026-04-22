"""
EIA weekly storage raw JSON → curated long-format Parquet.

Schema out:
  source: "eia" | series_id | series_name | period (YYYY-MM-DD) | value (float, Bcf)
  unit: "Bcf" | region (e.g. "Lower 48", "East", "Midwest", etc.) | ingested_at (UTC ISO)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from scrapers.base.safe_writer import safe_write_parquet


def transform(raw_json_path: Path, curated_parquet_path: Path) -> dict:
    """
    Transform EIA storage raw JSON to curated long-format Parquet.

    Returns summary metrics of the transformation.
    """
    with raw_json_path.open("r", encoding="utf-8") as f:
        raw_data = json.load(f)

    rows = []
    ingested_at = datetime.now(UTC).isoformat()

    # EIA v2 API response structure: {"response": {"data": [...]}}
    data = raw_data.get("response", {}).get("data", [])
    for row in data:
        # Expected keys: period, area-name, value, unit
        period = row.get("period")
        region = row.get("area-name")
        value = row.get("value")

        if period and region and value is not None:
            rows.append(
                {
                    "source": "eia",
                    "series_id": "storage",
                    "series_name": f"Weekly Natural Gas Storage - {region}",
                    "period": period,
                    "value": float(value),
                    "unit": "Bcf",
                    "region": region,
                    "ingested_at": ingested_at,
                }
            )

    if not rows:
        return {"rows": 0, "period_range": (None, None), "regions": []}

    df = pd.DataFrame(rows)
    safe_write_parquet(curated_parquet_path, df)

    return {
        "rows": len(df),
        "period_range": (df["period"].min(), df["period"].max()),
        "regions": sorted(df["region"].unique().tolist()),
    }
