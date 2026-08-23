"""Quorum IPWS raw JSON → curated long-format Parquet.

Schema out (canonical Blue Tide):
    source      : "quorum"
    series_id   : "{prefix}_sq_{loc}_{cycle}" or "{prefix}_oac_{loc}_{cycle}"
                  e.g. "gator_express_sq_vgpqd_id3"
    series_name : "Quorum TSQ {loc_name} ({CYCLE})" or "Quorum OAC {loc_name} ({CYCLE})"
    period      : YYYY-MM-DD  (gas day)
    value       : Raw quantity in Dth/d (Total Scheduled Quantity or
                  Operationally Available Capacity) — never unit-converted
    unit        : "Dth/d"
    region      : "US"
    ingested_at : UTC ISO-8601

Deduplication:
    Within a batch we sort by the posting timestamp (Post Date/Time) and keep
    the last value for each (series_id, period); the batch is then accumulated
    into the curated history via ``merge_into_curated``, which re-dedupes on
    ``ingested_at`` so re-scrapes update rather than duplicate.
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

RAW_DIR = Path("data/raw/quorum")
CURATED_PATH = Path("data/curated/quorum.parquet")


def _parse_posting_time(post_time_str: str) -> datetime:
    """Parse Post Date/Time from Quorum's US format (8/22/2026 10:05:14 PM).

    Failure modes:
        Unrecognised strings fall back to ``datetime.min`` so they sort first
        (oldest) and never win a dedup comparison.
    """
    text = post_time_str.strip()
    if not text:
        return datetime.min.replace(tzinfo=UTC)
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)


def _parse_raw_file(path: Path, ingested_at: str) -> list[dict[str, Any]]:
    """Read one raw ExportToCSV JSON payload into TSQ + OAC row dicts.

    Failure modes:
        Malformed files are skipped with a warning; rows missing Loc or an
        unparseable quantity are dropped silently (matching gulf_south).
    """
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Skipping malformed file %s: %s", path, exc)
        return []

    prefix: str = str(payload.get("prefix") or f"tsp{payload.get('tsp_no', '?')}")
    cycle: str = str(payload.get("cycle", "unknown")).lower()
    gas_day: str | None = payload.get("gas_day")
    raw_rows: list[dict[str, Any]] = payload.get("data", [])

    out: list[dict[str, Any]] = []
    for row in raw_rows:
        loc_id = row.get("Loc")
        loc_name = row.get("Loc Name") or str(loc_id)

        sq_raw = row.get("Total Scheduled Quantity")
        oac_raw = row.get("Operationally Available Capacity")
        if not loc_id or not gas_day:
            continue

        posted_dt = _parse_posting_time(row.get("Post Date/Time") or "")

        # 1. Total Scheduled Quantity (TSQ) series — raw Dth, no conversion.
        if sq_raw:
            try:
                sq_val = float(sq_raw)
                out.append(
                    {
                        "source": "quorum",
                        "series_id": f"{prefix}_sq_{loc_id.lower()}_{cycle}",
                        "series_name": f"Quorum TSQ {loc_name} ({cycle.upper()})",
                        "period": str(gas_day),
                        "value": sq_val,
                        "unit": "Dth/d",
                        "region": "US",
                        "ingested_at": ingested_at,
                        "_posted_at": posted_dt,  # temporary for dedup sorting
                    }
                )
            except (TypeError, ValueError):
                pass

        # 2. Operationally Available Capacity (OAC) parallel series.
        if oac_raw:
            try:
                oac_val = float(oac_raw)
                out.append(
                    {
                        "source": "quorum",
                        "series_id": f"{prefix}_oac_{loc_id.lower()}_{cycle}",
                        "series_name": f"Quorum OAC {loc_name} ({cycle.upper()})",
                        "period": str(gas_day),
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
    """Transform all Quorum raw JSON files to curated Parquet.

    What:
        Scans *raw_dir* recursively (per-pipeline subdirectories), parses each
        raw file into TSQ & OAC series, stacks to a DataFrame, sorts by posting
        time, dedups on (series_id, period) keeping the latest post, then
        accumulates the batch into the curated history via
        ``merge_into_curated`` (shrinkage-guarded, atomic write).

    Failure modes:
        Raises ``TransformError`` when the raw dir is missing/empty or yields
        zero parsable rows; the curated parquet is never touched in that case.
    """
    if not raw_dir.exists():
        raise TransformError(
            f"Raw directory not found: {raw_dir}. Run scrapers.quorum.pipelines first."
        )

    # rglob picks up per-pipeline subdirs; underscore-prefixed bookkeeping
    # files (_backfill_state.json) are excluded.
    raw_files = sorted(p for p in raw_dir.rglob("*.json") if not p.name.startswith("_"))
    if not raw_files:
        raise TransformError(
            f"No raw JSON files in {raw_dir}. Run scrapers.quorum.pipelines to populate it."
        )

    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict[str, Any]] = []
    for path in raw_files:
        rows = _parse_raw_file(path, ingested_at)
        all_rows.extend(rows)
        log.debug("%s → %d rows", path.name, len(rows))

    if not all_rows:
        raise TransformError(
            f"Quorum transformer produced zero rows from {len(raw_files)} file(s)."
        )

    df = pd.DataFrame(all_rows)

    # Within-batch dedup: latest Post Date/Time wins per (series_id, period).
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
    cycles = sorted({sid.rsplit("_", 1)[-1] for sid in merged["series_id"].unique()})
    prefixes = sorted({sid.split("_sq_")[0] for sid in merged["series_id"] if "_sq_" in sid})

    log.info(
        "Quorum transformer: %d rows, %d series, %s → %s, cycles=%s, pipelines=%s",
        len(merged),
        series_count,
        period_min,
        period_max,
        cycles,
        prefixes,
    )

    return {
        "rows": len(merged),
        "period_range": (str(period_min), str(period_max)),
        "series_count": series_count,
        "cycles": cycles,
        "pipelines": prefixes,
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
