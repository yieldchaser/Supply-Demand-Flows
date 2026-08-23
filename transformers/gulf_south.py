"""Gulf South Pipeline OAC raw JSON → curated long-format Parquet.

Schema out (canonical Blue Tide):
    source      : "boardwalk"
    series_id   : "gulf_south_sq_{loc_id}_{cycle}" or "gulf_south_oac_{loc_id}_{cycle}"
    series_name : "Gulf South TSQ {location_name} ({cycle})" or "Gulf South OAC {location_name} ({cycle})"
    period      : YYYY-MM-DD  (Effective Gas Day)
    value       : Raw quantity in Dth/d (Total Scheduled Quantity or Operationally Available Capacity)
    unit        : "Dth/d"
    region      : "US"
    ingested_at : UTC ISO-8601

Deduplication:
    Within a batch, after stacking all raw files, we sort by the posting
    timestamp (Post Date/Time) and keep the last (most recent) value for each
    (series_id, period).  The batch is then accumulated into the curated
    history via ``merge_into_curated``, which re-dedupes on ``ingested_at``
    so re-scrapes update past gas days rather than duplicate them.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from transformers.base.accumulate import merge_into_curated
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/gulf_south")
CURATED_PATH = Path("data/curated/gulf_south.parquet")


def _parse_posting_time(post_time_str: str) -> datetime:
    """Parse Post Date/Time from CSV format (e.g. 20260522 21:53:00) or ISO format."""
    try:
        # Check if format is YYYYMMDD HH:MM:SS
        if " " in post_time_str and ":" in post_time_str and len(post_time_str) >= 17:
            return datetime.strptime(post_time_str, "%Y%m%d %H:%M:%S").replace(tzinfo=UTC)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(post_time_str.replace("Z", "+00:00"))
    except Exception:
        return datetime.min.replace(tzinfo=UTC)


def _parse_raw_file(path: Path, ingested_at: str) -> list[dict[str, Any]]:
    """Read one raw OAC JSON file and return lists of TSQ and OAC row dicts.

    Failure modes:
        Silently skips rows with missing location ID or values.
        Returns empty list if the file is malformed.
    """
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Skipping malformed file %s: %s", path, exc)
        return []

    cycle: str = payload.get("cycle", "UNKNOWN").upper()
    gas_day: str | None = payload.get("gas_day")
    posted_at_raw: str = payload.get("posted_at") or payload.get("fetched_at") or ""
    raw_rows: list[dict[str, Any]] = payload.get("data", [])

    out: list[dict[str, Any]] = []
    for row in raw_rows:
        loc_id = row.get("Loc") or row.get("location_number") or row.get("locationNumber")
        loc_name = (
            row.get("Loc Name")
            or row.get("location_name")
            or row.get("locationName")
            or str(loc_id)
        )

        # Quantity columns
        sq_raw = (
            row.get("Total Scheduled Quantity")
            or row.get("scheduled_quantity")
            or row.get("scheduledQuantity")
        )
        oac_raw = (
            row.get("Operationally Available Capacity")
            or row.get("operationally_available_capacity")
            or row.get("operationallyAvailableCapacity")
        )

        # Period
        period = (
            row.get("Effective Gas Day")
            or row.get("gas_flow_date")
            or row.get("gasFlowDate")
            or gas_day
        )

        if not loc_id or not period:
            continue

        # Format gas day (YYYYMMDD to YYYY-MM-DD if needed)
        period_str = str(period).strip()
        if len(period_str) == 8 and period_str.isdigit():
            period_str = f"{period_str[:4]}-{period_str[4:6]}-{period_str[6:]}"

        # Parse posted timestamp
        post_time_raw = row.get("Post Date/Time") or posted_at_raw
        posted_dt = _parse_posting_time(post_time_raw)

        # Flow direction (R/D) is part of the series identity: a meter may
        # post BOTH legs in one cycle with different quantities.
        flow = (
            str(
                row.get("Flow Ind")
                or row.get("flow_ind")
                or row.get("flowInd")
                or ""
            ).strip().lower()
            or "u"
        )

        # 1. Total Scheduled Quantity (TSQ) series
        if sq_raw is not None:
            try:
                sq_val = float(sq_raw)
                out.append(
                    {
                        "source": "boardwalk",
                        "series_id": f"gulf_south_sq_{loc_id}_{flow}_{cycle.lower()}",
                        "series_name": f"Gulf South TSQ {loc_name} [{flow.upper()}] ({cycle})",
                        "period": period_str,
                        "value": sq_val,
                        "unit": "Dth/d",
                        "region": "US",
                        "ingested_at": ingested_at,
                        "_posted_at": posted_dt,  # temporary for dedup sorting
                    }
                )
            except (TypeError, ValueError):
                pass

        # 2. Operationally Available Capacity (OAC) series
        if oac_raw is not None:
            try:
                oac_val = float(oac_raw)
                out.append(
                    {
                        "source": "boardwalk",
                        "series_id": f"gulf_south_oac_{loc_id}_{flow}_{cycle.lower()}",
                        "series_name": f"Gulf South OAC {loc_name} [{flow.upper()}] ({cycle})",
                        "period": period_str,
                        "value": oac_val,
                        "unit": "Dth/d",
                        "region": "US",
                        "ingested_at": ingested_at,
                        "_posted_at": posted_dt,
                    }
                )
            except (TypeError, ValueError):
                pass

    return out


def transform(
    raw_dir: Path = RAW_DIR,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform all Gulf South OAC raw JSON files to curated Parquet.

    What:
        Scans *raw_dir* recursively, parses each raw file into TSQ & OAC series,
        stacks to a DataFrame, sorts by posted time, dedups on (series_id, period)
        keeping the latest, accumulates the batch into the curated parquet
        history (``merge_into_curated``), and returns stats over the merged frame.
    """
    if not raw_dir.exists():
        raise TransformError(
            f"Raw directory not found: {raw_dir}. Run scrapers.energy_transfer.gulf_south first."
        )

    raw_files = sorted(raw_dir.rglob("*.json"))
    if not raw_files:
        raise TransformError(
            f"No raw JSON files in {raw_dir}. Run scrapers.energy_transfer.gulf_south to populate it."
        )

    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict[str, Any]] = []

    for path in raw_files:
        rows = _parse_raw_file(path, ingested_at)
        all_rows.extend(rows)
        log.debug("%s → %d rows", path.name, len(rows))

    if not all_rows:
        raise TransformError(
            f"Gulf South transformer produced zero rows from {len(raw_files)} file(s)."
        )

    df = pd.DataFrame(all_rows)

    # Dedup: sort by _posted_at and keep the last (most recent) for each (series_id, period)
    df = (
        df.sort_values("_posted_at")
        .drop_duplicates(subset=["series_id", "period"], keep="last")
        .drop(columns=["_posted_at"])
        .reset_index(drop=True)
    )

    merged = merge_into_curated(df, curated_parquet_path)

    period_min = merged["period"].min()
    period_max = merged["period"].max()
    series_count = merged["series_id"].nunique()

    # Extract cycles from series IDs
    cycles = sorted({sid.rsplit("_", 1)[-1] for sid in merged["series_id"].unique()})

    log.info(
        "Gulf South transformer: %d rows, %d series, %s → %s, cycles=%s",
        len(merged),
        series_count,
        period_min,
        period_max,
        cycles,
    )

    return {
        "rows": len(merged),
        "period_range": (period_min, period_max),
        "series_count": series_count,
        "cycles": cycles,
        "source_files": len(raw_files),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    result = transform()
    import json as _json

    print(_json.dumps(result, indent=2, default=str))
