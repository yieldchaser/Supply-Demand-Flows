"""BHE GT&S (EGTS) OAC raw JSON → curated long-format Parquet.

Schema out (canonical Blue Tide):
    source      : "bhe"
    series_id   : "egts_sq_{loc}_{cycle}" or "egts_oac_{loc}_{cycle}"
    series_name : "EGTS TSQ {loc_name} ({cycle})" / "EGTS OAC {loc_name} ({cycle})"
    period      : YYYY-MM-DD  (Eff Gas Day from the CSV)
    value       : Raw quantity in Dth/d (MMBtu basis, never converted)
    unit        : "Dth/d"
    region      : "US"
    ingested_at : UTC ISO-8601

Cove Point focus:
    Every EGTS OAC CSV row carries an Interconnect Party Name. The Cove
    Point feedgas meter is Loc 40704 "EGTS - LOUDOUN" with Interconnect
    "COVE POINT LNG LP" and Flow Ind "R". Zeros are legitimate — Cove Point
    is small (~750 MMcf/d) and cargo-driven — and are preserved, not
    dropped.

Deduplication:
    Within a batch, rows are sorted by posting timestamp and the last value
    per (series_id, period) kept; the batch is then accumulated into the
    curated history via ``merge_into_curated``, which re-dedupes on
    ``ingested_at`` so re-scrapes update past gas days rather than duplicate.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.bhe.client import (
    COL_CYCLE,
    COL_GAS_DAY,
    COL_LOC,
    COL_LOC_NAME,
    COL_OAC,
    COL_OP_CAP,
    COL_TSQ,
    is_cove_point_row,
)
from transformers.base.accumulate import merge_into_curated
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/bhe")
CURATED_PATH = Path("data/curated/bhe.parquet")

SERIES_PREFIX = "egts"


def _parse_posted_at(posted_at: str) -> datetime:
    """Parse a posting timestamp for dedup ordering; falls back to epoch.

    Failure modes:
        Unparseable/empty strings yield ``datetime.min`` (UTC) so those rows
        sort first and lose dedup contests.
    """
    if posted_at:
        try:
            return datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=UTC)


def _num(value: Any) -> float | None:
    """Best-effort float parse of a raw CSV quantity string."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _rows_from_file(path: Path, ingested_at: str) -> list[dict[str, Any]]:
    """Extract Cove Point series rows from one raw EGTS payload.

    What:
        Filters to the Cove Point meter rule (Interconnect == COVE POINT LNG
        LP AND Flow Ind == R), then emits up to three series per row: TSQ
        (feedgas), Operationally Available Capacity, and Operating Capacity.
        Zeros are preserved — they mean "no feedgas scheduled that cycle".

    Failure modes:
        Malformed JSON files are skipped with a warning; rows missing loc or
        gas day are dropped individually.
    """
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Skipping malformed file %s: %s", path, exc)
        return []

    cycle_token: str = str(payload.get("cycle") or "UNKNOWN").lower()
    posted_at_raw: str = str(payload.get("posted_at") or "")
    raw_rows: list[dict[str, Any]] = payload.get("data", []) or []

    out: list[dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, dict) or not is_cove_point_row({k: str(v or "") for k, v in row.items()}):
            continue

        loc_id = str(row.get(COL_LOC) or "").strip()
        loc_name = str(row.get(COL_LOC_NAME) or "").strip() or loc_id
        period_raw = str(row.get(COL_GAS_DAY) or "").strip()
        # Eff Gas Day arrives as MM/DD/YYYY in EGTS blobs.
        period = _normalize_gas_day(period_raw) or _normalize_gas_day(str(payload.get("gas_day") or ""))
        csv_cycle = str(row.get(COL_CYCLE) or "").strip()
        if not loc_id or not period:
            continue
        # The CSV's own CycleDesc is authoritative when present; the file's
        # subject-derived cycle token covers legacy single-posting days.
        cycle = _cycle_token(csv_cycle) or cycle_token

        posted_dt = _parse_posted_at(posted_at_raw)

        for kind, col, label in (
            ("sq", COL_TSQ, "TSQ"),
            ("oac", COL_OAC, "OAC"),
            ("opcap", COL_OP_CAP, "Operating Capacity"),
        ):
            val = _num(row.get(col))
            if val is None:
                continue
            out.append(
                {
                    "source": "bhe",
                    "series_id": f"{SERIES_PREFIX}_{kind}_{loc_id}_{cycle}",
                    "series_name": f"EGTS {label} {loc_name} ({cycle.upper()})",
                    "period": period,
                    "value": val,
                    "unit": "Dth/d",
                    "region": "US",
                    "ingested_at": ingested_at,
                    "_posted_at": posted_dt,
                }
            )
    return out


def _normalize_gas_day(raw: str) -> str | None:
    """Normalize MM/DD/YYYY or YYYY-MM-DD to ISO; returns None when unparseable."""
    raw = raw.strip()
    if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
        return raw
    parts = raw.split("/")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        mm, dd, yyyy = parts
        if len(yyyy) == 4:
            return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return None


def _cycle_token(cycle_desc: str) -> str | None:
    """Map a CycleDesc like 'Intraday 3'/'Evening'/'Timely' to id1/id2/id3/evening/timely."""
    c = cycle_desc.strip().upper()
    if c == "TIMELY":
        return "timely"
    if c == "EVENING":
        return "evening"
    if c.startswith("INTRADAY"):
        tail = c.rsplit(" ", 1)[-1]
        if tail in {"1", "2", "3"}:
            return f"id{tail}"
    return None


def transform(
    raw_dir: Path = RAW_DIR,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform all BHE raw JSON files into the curated parquet via accumulation.

    What:
        Scans *raw_dir* recursively, parses each raw file into Cove Point
        series rows, stacks to a DataFrame, sorts by posting time, dedups on
        (series_id, period) keeping the latest, accumulates the batch into
        the curated history (``merge_into_curated``), and returns stats over
        the merged frame.

    Failure modes:
        Raises ``TransformError`` when the raw dir/files are missing or the
        batch produces zero rows; accumulation shrinkage raises
        ``AccumulationShrinkError`` before any write.
    """
    if not raw_dir.exists():
        raise TransformError(
            f"Raw directory not found: {raw_dir}. Run scrapers.bhe.client first."
        )

    raw_files = sorted(raw_dir.rglob("*.json"))
    if not raw_files:
        raise TransformError(f"No raw JSON files in {raw_dir}. Run scrapers.bhe.client first.")

    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict[str, Any]] = []
    for path in raw_files:
        rows = _rows_from_file(path, ingested_at)
        all_rows.extend(rows)

    if not all_rows:
        raise TransformError(f"BHE transformer produced zero rows from {len(raw_files)} file(s).")

    df = pd.DataFrame(all_rows)
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
    zero_tsq = int((merged[merged["series_id"].str.contains("_sq_")]["value"] == 0).sum())

    log.info(
        "BHE transformer: %d rows, %d series, %s → %s, zero-TSQ rows=%d",
        len(merged),
        series_count,
        period_min,
        period_max,
        zero_tsq,
    )

    return {
        "rows": len(merged),
        "period_range": (str(period_min), str(period_max)),
        "series_count": series_count,
        "zero_tsq_rows": zero_tsq,
        "source_files": len(raw_files),
    }


if __name__ == "__main__":
    import json as _json
    import logging as _logging

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    result = transform()
    print(_json.dumps(result, indent=2, default=str))
