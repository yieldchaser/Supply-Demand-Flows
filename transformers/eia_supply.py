"""
EIA Natural Gas Monthly Supply/Demand raw JSON → curated long-format Parquet.

Schema out:
  source: "eia" | series_id | series_name | period (YYYY-MM) | value (float)
  unit: "MMcf" (or as provided) | region: "US48" | ingested_at (UTC ISO)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from scrapers.base.safe_writer import safe_write_parquet
from scrapers.eia_api.supply import PROCESS_CODES


def transform(raw_json_path: Path, curated_parquet_path: Path) -> dict:
    """
    Transform EIA supply raw JSON to curated long-format Parquet.

    Maps process codes to human-readable names.
    """
    with raw_json_path.open("r", encoding="utf-8") as f:
        raw_data = json.load(f)

    rows = []
    ingested_at = datetime.now(UTC).isoformat()

    data = raw_data.get("response", {}).get("data", [])
    for row in data:
        period = row.get("period")
        process_code = row.get("process")
        value = row.get("value")
        unit = row.get("units", "MMcf")  # Default to MMcf if not specified

        if period and process_code and value is not None:
            process_name = PROCESS_CODES.get(process_code, f"Unknown Process ({process_code})")
            rows.append(
                {
                    "source": "eia",
                    "series_id": f"supply_{process_code.lower()}",
                    "series_name": f"Monthly Supply/Demand - {process_name}",
                    "period": period,
                    "value": float(value),
                    "unit": unit,
                    "region": "US48",
                    "ingested_at": ingested_at,
                }
            )

    if not rows:
        return {"rows": 0, "period_range": (None, None), "series": []}

    df = pd.DataFrame(rows)
    safe_write_parquet(curated_parquet_path, df)

    return {
        "rows": len(df),
        "period_range": (df["period"].min(), df["period"].max()),
        "series": sorted(df["series_id"].unique().tolist()),
    }
