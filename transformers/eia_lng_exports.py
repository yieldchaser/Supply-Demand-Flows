"""
EIA LNG Exports raw JSON → curated long-format Parquet.

Why:
    Dashboard panels expect canonical Parquet schema:
    source | series_id | series_name | period | value | unit | region | ingested_at

What:
    Reads data/raw/eia_lng/lng_exports.json. For each row emits ONE canonical
    output row (per-country series). Additionally computes:

        1. Per-country series: lng_export_{cc}  (e.g. lng_export_nld)
        2. Derived total:      lng_export_total
        3. Regional aggregates: lng_export_region_europe, _asia, _latam, _other

    Unit conversion: MMcf → Bcf (divide by 1000) for dashboard readability.
    Period: YYYY-MM strings normalised to YYYY-MM-01 month-start dates so
    frontend can parse them as Date objects consistently.

    Active-destination filter: countries with zero LNG volume in the most
    recent 24 months are dropped from per-country series (but still counted
    in total and regional aggregates for the period they had volume).

Failure modes:
    - Raw JSON missing → TransformError raised.
    - Zero output rows → TransformError.
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

RAW_PATH = Path("data/raw/eia_lng/lng_exports.json")
CURATED_PATH = Path("data/curated/eia_lng_exports.parquet")

# ── Regional mappings (ISO-3166 Alpha-3) ─────────────────────────────────────
REGIONS: dict[str, list[str]] = {
    "Europe": [
        "NLD", "GBR", "FRA", "ESP", "ITA", "BEL", "DEU", "PRT", "POL",
        "GRC", "TUR", "FIN", "LTU", "HRV", "SVN", "AUT", "HUN", "CZE",
        "DNK", "SWE", "NOR", "ISL", "IRL", "MLT", "CYP", "SVK", "BGR",
        "ROU", "HRV",
    ],
    "Asia": [
        "JPN", "KOR", "CHN", "IND", "TWN", "THA", "PAK", "BGD", "MYS",
        "SGP", "PHL", "VNM", "IDN",
    ],
    "LatAm": [
        "BRA", "CHL", "ARG", "COL", "DOM", "MEX", "JAM", "PAN", "TTO",
        "CRI", "GTM", "HND", "SLV", "ECU", "PER", "VEN", "BOL",
    ],
}

# EIA sometimes uses different country codes. This map converts them to ISO-3166.
EIA_CODE_REMAP: dict[str, str] = {
    "UK":  "GBR",
    "R1":  "GBR",   # EIA regional code observed in some datasets
    "S48": "USA",   # shouldn't appear but guard anyway
}


def _region_for(country_code: str) -> str:
    """Return the region name for a country code, defaulting to 'Other'."""
    cc = EIA_CODE_REMAP.get(country_code, country_code)
    for region, codes in REGIONS.items():
        if cc in codes:
            return region
    return "Other"


def transform(
    raw_json_path: Path = RAW_PATH,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform EIA LNG exports raw JSON to curated long-format Parquet.

    Returns summary dict with row_count, period_range, and series_count.
    """
    if not raw_json_path.exists():
        raise TransformError(
            f"Raw JSON not found: {raw_json_path}. "
            "Run scrapers.eia_api.lng_exports first."
        )

    with raw_json_path.open("r", encoding="utf-8") as fh:
        payload: dict[str, Any] = json.load(fh)

    raw_rows: list[dict[str, Any]] = payload.get("data", [])
    ingested_at = datetime.now(UTC).isoformat()

    if not raw_rows:
        raise TransformError("EIA LNG exports JSON has no 'data' rows.")

    # ── Build base DataFrame ──────────────────────────────────────────────────
    records: list[dict[str, Any]] = []
    for row in raw_rows:
        period_ym = str(row.get("period", ""))  # 'YYYY-MM'
        dest_code = str(row.get("destination_code", "")).upper()
        dest_name = str(row.get("destination_name", dest_code))
        value_mmcf = row.get("value_mmcf")

        if not period_ym or not dest_code or value_mmcf is None:
            continue

        # Normalise period to month-start ISO date (YYYY-MM-01)
        try:
            pd.Timestamp(f"{period_ym}-01")  # validate format
            period_date = f"{period_ym}-01"
        except Exception:
            log.warning("Skipping row with unparseable period: %s", period_ym)
            continue

        # Remap any non-ISO codes
        canonical_code = EIA_CODE_REMAP.get(dest_code, dest_code)

        records.append(
            {
                "period": period_date,
                "destination_code": canonical_code,
                "destination_name": dest_name,
                "value_bcf": float(value_mmcf) / 1000.0,  # MMcf → Bcf
                "region": _region_for(canonical_code),
            }
        )

    if not records:
        raise TransformError("EIA LNG transformer produced zero valid rows.")

    base_df = pd.DataFrame(records)
    base_df["period"] = pd.to_datetime(base_df["period"])

    # ── Active-destination filter ──────────────────────────────────────────────
    # Keep only countries with non-zero volume in the last 24 months.
    cutoff = base_df["period"].max() - pd.DateOffset(months=24)
    recent = base_df[base_df["period"] >= cutoff]
    active_codes = set(recent[recent["value_bcf"] > 0]["destination_code"].unique())
    log.info("Active destinations (24mo): %d countries", len(active_codes))

    # ── Emit per-country series ───────────────────────────────────────────────
    out_rows: list[dict[str, Any]] = []

    for code in active_codes:
        country_df = base_df[base_df["destination_code"] == code].copy()
        name = country_df["destination_name"].iloc[0]
        for _, r in country_df.iterrows():
            out_rows.append(
                {
                    "source": "eia_lng",
                    "series_id": f"lng_export_{code.lower()}",
                    "series_name": f"US LNG exports to {name}",
                    "period": r["period"].strftime("%Y-%m-%d"),
                    "value": round(r["value_bcf"], 4),
                    "unit": "Bcf",
                    "region": code,
                    "ingested_at": ingested_at,
                }
            )

    # ── Emit total series (all destinations, all periods) ─────────────────────
    total_by_period = (
        base_df.groupby("period")["value_bcf"].sum().reset_index()
    )
    for _, r in total_by_period.iterrows():
        out_rows.append(
            {
                "source": "eia_lng",
                "series_id": "lng_export_total",
                "series_name": "US LNG exports total",
                "period": r["period"].strftime("%Y-%m-%d"),
                "value": round(r["value_bcf"], 4),
                "unit": "Bcf",
                "region": "ALL",
                "ingested_at": ingested_at,
            }
        )

    # ── Emit regional aggregate series ────────────────────────────────────────
    for region in ("Europe", "Asia", "LatAm", "Other"):
        if region == "Other":
            region_df = base_df[~base_df["destination_code"].isin(
                [c for codes in REGIONS.values() for c in codes]
            )].copy()
        else:
            region_df = base_df[base_df["region"] == region].copy()

        if region_df.empty:
            log.info("No rows for region %s — skipping regional series.", region)
            continue

        reg_by_period = region_df.groupby("period")["value_bcf"].sum().reset_index()
        for _, r in reg_by_period.iterrows():
            out_rows.append(
                {
                    "source": "eia_lng",
                    "series_id": f"lng_export_region_{region.lower()}",
                    "series_name": f"US LNG exports to {region}",
                    "period": r["period"].strftime("%Y-%m-%d"),
                    "value": round(r["value_bcf"], 4),
                    "unit": "Bcf",
                    "region": region,
                    "ingested_at": ingested_at,
                }
            )

    if not out_rows:
        raise TransformError("EIA LNG transformer: out_rows is empty after all series.")

    df_out = pd.DataFrame(out_rows)
    safe_write_parquet(curated_parquet_path, df_out)

    period_min = df_out["period"].min()
    period_max = df_out["period"].max()
    series_count = df_out["series_id"].nunique()

    log.info(
        "EIA LNG transformer: %d rows, %d series, %s → %s",
        len(df_out), series_count, period_min, period_max,
    )

    return {
        "rows": len(df_out),
        "period_range": (period_min, period_max),
        "series_count": series_count,
        "active_destinations": sorted(active_codes),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    import json as _json
    result = transform()
    print(_json.dumps(result, indent=2, default=str))
