"""
GIE AGSI+ European storage raw JSON → curated long-format Parquet.

Why:
    Downstream dashboard panels and bundle builder expect a canonical Parquet
    schema identical to EIA storage: source | series_id | series_name |
    period | value | unit | region | ingested_at.

What:
    Reads data/raw/gie_agsi/european_storage.json. For each row in the
    "data" array, emits up to FOUR output rows — one per metric. Null fields
    are skipped so sparse weekend data doesn't pollute the series with NaN.

    Metrics emitted per country per gas_day:
        gie_storage_{cc}_gas_in_storage  — TWh (gas in storage)
        gie_storage_{cc}_full_pct        — % (percentage full)
        gie_storage_{cc}_injection       — GWh (daily injection flow)
        gie_storage_{cc}_withdrawal      — GWh (daily withdrawal flow)

Failure modes:
    - Raw JSON missing → TransformError raised.
    - Null field for a row/metric combo → that (row, metric) is silently
      skipped; the country/metric combination may have fewer rows than others.
    - Zero output rows after filtering → TransformError.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.base.safe_writer import safe_write_parquet
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_PATH = Path("data/raw/gie_agsi/european_storage.json")
CURATED_PATH = Path("data/curated/gie_agsi.parquet")

# (series_id suffix, source field key, unit string, series name suffix)
SERIES_DEFINITIONS: list[tuple[str, str, str, str]] = [
    ("gas_in_storage", "gas_in_storage_twh", "TWh", "gas in storage"),
    ("full_pct", "full_pct", "%", "% full"),
    ("injection", "injection_gwh", "GWh", "daily injection"),
    ("withdrawal", "withdrawal_gwh", "GWh", "daily withdrawal"),
]


def transform(
    raw_json_path: Path = RAW_PATH,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform GIE AGSI+ raw JSON to curated long-format Parquet.

    Returns a summary dict with row_count, period_range, and series_count.

    Failure modes:
        TransformError if the raw file is missing or produces zero rows.
    """
    if not raw_json_path.exists():
        raise TransformError(
            f"Raw JSON not found: {raw_json_path}. "
            "Run scrapers.gie_agsi.european_storage first."
        )

    with raw_json_path.open("r", encoding="utf-8") as fh:
        payload: dict[str, Any] = json.load(fh)

    raw_rows: list[dict[str, Any]] = payload.get("data", [])
    ingested_at = datetime.now(UTC).isoformat()

    out_rows: list[dict[str, Any]] = []

    for raw in raw_rows:
        country_code: str | None = raw.get("country_code")
        gas_day: str | None = raw.get("gas_day")

        if not country_code or not gas_day:
            continue

        cc = country_code.upper()

        for id_suffix, field_key, unit, name_suffix in SERIES_DEFINITIONS:
            value = raw.get(field_key)
            if value is None:
                # Sparse weekend / partial data — skip this (row, metric).
                continue

            out_rows.append(
                {
                    "source": "gie_agsi",
                    "series_id": f"gie_storage_{cc.lower()}_{id_suffix}",
                    "series_name": f"{cc} {name_suffix}",
                    "period": gas_day,
                    "value": float(value),
                    "unit": unit,
                    "region": cc,
                    "ingested_at": ingested_at,
                }
            )

    if not out_rows:
        raise TransformError(
            "GIE AGSI transformer produced zero rows. "
            f"Raw file had {len(raw_rows)} input rows — all fields may be null."
        )

    df = pd.DataFrame(out_rows)
    safe_write_parquet(curated_parquet_path, df)

    period_min = df["period"].min()
    period_max = df["period"].max()
    series_count = df["series_id"].nunique()

    log.info(
        "GIE AGSI transformer: %d rows, %d series, %s → %s",
        len(df),
        series_count,
        period_min,
        period_max,
    )

    return {
        "rows": len(df),
        "period_range": (period_min, period_max),
        "series_count": series_count,
        "regions": sorted(df["region"].unique().tolist()),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    import json as _json

    result = transform()
    print(_json.dumps(result, indent=2, default=str))
