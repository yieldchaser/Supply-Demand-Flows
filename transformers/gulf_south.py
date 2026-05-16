"""Gulf South Pipeline SQ raw JSON → curated long-format Parquet.

Schema out (canonical Blue Tide):
    source      : "gulf_south"
    series_id   : "gulf_south_sq_{meter_id}_{cycle}"  e.g. gulf_south_sq_24329_timely
    series_name : "Gulf South SQ {location_name} ({cycle})"
    period      : YYYY-MM-DD  (gas_flow_date)
    value       : scheduled quantity in MMcf/d
    unit        : "MMcf/d"
    region      : "US"
    ingested_at : UTC ISO-8601

Unit conversion:
    GasQuest API returns Dth/d (dekatherms per day).
    1 Mcf ≈ HHV_DTH_PER_MCF Dth  (HHV_DTH_PER_MCF = 1.025 for Gulf Coast dry gas)
    MMcf/d = Dth/d / (HHV_DTH_PER_MCF × 1 000)

Deduplication:
    After stacking all raw files we dedupe on (series_id, period), keeping
    the row with the latest ingested_at — handles re-runs and cycle revisions.
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

RAW_DIR = Path("data/raw/gulf_south")
CURATED_PATH = Path("data/curated/gulf_south.parquet")

HHV_DTH_PER_MCF: float = 1.025  # Dth per Mcf; see module docstring
DTH_PER_MMCF: float = HHV_DTH_PER_MCF * 1_000


def _dth_to_mmcfd(dth: float) -> float:
    """Convert Dth/d to MMcf/d using the standard Gulf Coast HHV assumption."""
    return dth / DTH_PER_MMCF


def _parse_raw_file(path: Path, ingested_at: str) -> list[dict[str, Any]]:
    """Read one raw JSON file and return a list of canonical row dicts.

    Failure modes:
        Silently skips rows with missing required fields (location, quantity).
        Returns empty list if the file is malformed JSON.
    """
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Skipping malformed file %s: %s", path, exc)
        return []

    cycle: str = payload.get("cycle", "UNKNOWN").upper()
    gas_day: str | None = payload.get("gas_day")
    raw_rows: list[dict[str, Any]] = payload.get("data", [])

    out: list[dict[str, Any]] = []
    for row in raw_rows:
        location_num = row.get("locationNumber") or row.get("location_number")
        location_name = row.get("locationName") or row.get("location_name") or str(location_num)
        sq_raw = row.get("scheduledQuantity") or row.get("scheduled_quantity")
        period = row.get("gasFlowDate") or row.get("gas_flow_date") or gas_day

        if not location_num or sq_raw is None or not period:
            continue

        try:
            sq_dth = float(sq_raw)
        except (TypeError, ValueError):
            continue

        unit_raw = (row.get("unit") or "DTH").upper()
        value = _dth_to_mmcfd(sq_dth) if unit_raw in ("DTH", "MMBTU", "DEKATHERM") else sq_dth

        out.append(
            {
                "source": "gulf_south",
                "series_id": f"gulf_south_sq_{location_num}_{cycle.lower()}",
                "series_name": f"Gulf South SQ {location_name} ({cycle})",
                "period": period,
                "value": round(value, 4),
                "unit": "MMcf/d",
                "region": "US",
                "ingested_at": ingested_at,
            }
        )

    return out


def transform(
    raw_dir: Path = RAW_DIR,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform all Gulf South SQ raw JSON files to curated Parquet.

    What:
        Scans *raw_dir* recursively for *.json files, parses each one,
        stacks to a DataFrame, deduplicates on (series_id, period) keeping
        latest ingested_at, and writes Parquet atomically.

    Returns:
        Summary dict with rows, series_count, period_range, cycles.

    Failure modes:
        TransformError if no raw files exist or zero valid rows produced.
    """
    if not raw_dir.exists():
        raise TransformError(
            f"Raw directory not found: {raw_dir}. "
            "Run scrapers.energy_transfer.gulf_south first."
        )

    raw_files = sorted(raw_dir.rglob("*.json"))
    if not raw_files:
        raise TransformError(
            f"No raw JSON files in {raw_dir}. "
            "Run scrapers.energy_transfer.gulf_south to populate it."
        )

    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict[str, Any]] = []

    for path in raw_files:
        rows = _parse_raw_file(path, ingested_at)
        all_rows.extend(rows)
        log.debug("%s → %d rows", path.name, len(rows))

    if not all_rows:
        raise TransformError(
            f"Gulf South transformer produced zero rows from {len(raw_files)} file(s). "
            "All rows may have missing location/quantity fields."
        )

    df = pd.DataFrame(all_rows)

    # Dedup: keep the latest ingested_at for each (series_id, period) pair.
    df = (
        df.sort_values("ingested_at")
        .drop_duplicates(subset=["series_id", "period"], keep="last")
        .reset_index(drop=True)
    )

    safe_write_parquet(curated_parquet_path, df)

    period_min = df["period"].min()
    period_max = df["period"].max()
    series_count = df["series_id"].nunique()
    cycles = sorted(
        {sid.rsplit("_", 1)[-1] for sid in df["series_id"].unique()}
    )

    log.info(
        "Gulf South transformer: %d rows, %d series, %s → %s, cycles=%s",
        len(df),
        series_count,
        period_min,
        period_max,
        cycles,
    )

    return {
        "rows": len(df),
        "period_range": (period_min, period_max),
        "series_count": series_count,
        "cycles": cycles,
        "source_files": len(raw_files),
    }


if __name__ == "__main__":
    import json as _json
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    result = transform()
    print(_json.dumps(result, indent=2, default=str))
