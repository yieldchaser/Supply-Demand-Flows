"""Williams/Transco 1Line raw JSON → curated long-format Parquet.

Schema out (canonical Blue Tide):
    source      : "transco"
    series_id   : "transco_{sq|oac|opcap|design}_{loc}_{flow}_{cycle}"
                  flow ∈ r|d|u — flow direction is part of the series key;
                  omitting it caused a silent-overwrite bug across five
                  sources that destroyed 27,015 rows (2026-08 incident).
    series_name : "Transco TSQ {loc_name} [{FLOW}] ({CYCLE})"
    period      : YYYY-MM-DD  (Effective Gas Day)
    value       : Raw quantity in Dth/d — NEVER unit-converted.
    unit        : "Dth/d"
    region      : "US"
    ingested_at : UTC ISO-8601

Watchlist scoping:
    Only rows whose (loc, loc_name) match the scraper's WATCHLIST reach the
    parquet — the scraper already filtered, and this module re-asserts the
    filter defensively so a watchlist regression cannot silently widen the
    dataset.

Deduplication:
    Within a batch, rows are sorted by posting timestamp and the last value
    per (series_id, period) kept; the batch is then accumulated into the
    curated history via ``merge_into_curated`` (shrinkage-guarded), which
    re-dedupes on ``ingested_at`` so re-scrapes update past gas days rather
    than duplicate. BOTH legs of a bidirectional meter are separate series —
    never arbitrated.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.williams.config import SOURCE_NAME, WATCHLIST
from transformers.base.accumulate import merge_into_curated
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/williams")
CURATED_PATH = Path("data/curated/williams.parquet")

SERIES_PREFIX = "transco"

_KIND_LABELS: dict[str, str] = {
    "sq": "TSQ",
    "oac": "OAC",
    "opcap": "Operating Capacity",
    "design": "Design Capacity",
}

#: raw row key → canonical numeric column.
_KIND_COLUMNS: dict[str, str] = {
    "sq": "tsq",
    "oac": "oac",
    "opcap": "operating_cap",
    "design": "design_cap",
}


def _num(value: Any) -> float | None:
    """Best-effort float parse of a raw HTML quantity string.

    What:
        Strips thousands separators ("163,735" → 163735.0) because the JSP
        table renders comma-grouped numbers; the gasnom lesson was that every
        value ≥1,000 failed plain float() and silently dropped 60% of rows.

    Failure modes:
        Returns None for blanks, dashes, and other non-numeric placeholders.
    """
    if value is None:
        return None
    cleaned = str(value).strip().replace(",", "")
    if not cleaned or cleaned in {"-", "--", "OPEN", "N/A"}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_posted_at(raw: str) -> datetime:
    """Parse a posting timestamp for dedup ordering; falls back to epoch.

    Failure modes:
        Unparseable/empty strings yield ``datetime.min`` (UTC) so those rows
        sort first and lose dedup contests.
    """
    raw = (raw or "").strip()
    if not raw:
        return datetime.min.replace(tzinfo=UTC)
    # Posting Date + Posting Time arrive as MM/DD/YYYY and hh:mm:ss AM/PM.
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", raw)
    if not m:
        return datetime.min.replace(tzinfo=UTC)
    for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p", "%m/%d/%Y"):
        try:
            return datetime.strptime(" ".join([raw.split()[0], *raw.split()[1:]]) if False else raw, fmt).replace(
                tzinfo=UTC
            )
        except ValueError:
            continue
    # Date-only fallback when the time half is missing/unparsable.
    mm, dd, yyyy = m.groups()
    try:
        return datetime(int(yyyy), int(mm), int(dd), tzinfo=UTC)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)


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


def _rows_from_file(path: Path, ingested_at: str) -> list[dict[str, Any]]:
    """Extract watchlist series rows from one raw payload.

    What:
        Emits up to four series per watched row — TSQ, OAC, Operating and
        Design capacity — each keyed with the row's own Flow Ind (r/d/u).
        Zeros are preserved: a zero TSQ at an LNG interconnect means "no gas
        scheduled that cycle", which is signal, never noise.

    Failure modes:
        Malformed JSON files are skipped with a warning; rows failing the
        watchlist are dropped silently (that is the scoping contract); rows
        missing loc or gas day are dropped individually.
    """
    import json

    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Skipping malformed file %s: %s", path, exc)
        return []

    cycle_token: str = str(payload.get("cycle") or "UNKNOWN").lower()
    posted_dt = _parse_posted_at(str(payload.get("posted_at") or ""))

    raw_rows: list[dict[str, Any]] = payload.get("data", []) or []
    file_gas_day = _normalize_gas_day(str(payload.get("gas_day") or "")) or ""

    out: list[dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        clean_row = {k: str(v or "") for k, v in row.items()}
        entry = WATCHLIST.match(clean_row.get("loc", ""), clean_row.get("loc_name", ""))
        if entry is None:
            continue

        loc_id = clean_row.get("loc", "").strip()
        loc_name = clean_row.get("loc_name", "").strip() or loc_id
        period = _normalize_gas_day(clean_row.get("_gas_day", "")) or file_gas_day
        if not loc_id or not period:
            continue

        flow = clean_row.get("flow_ind", "").strip().lower()
        if flow not in {"r", "d"}:
            flow = "u"
        cycle = cycle_token

        label = f"{entry.label}:{loc_name}" if entry.label != loc_name.lower() else entry.label

        for kind, col in _KIND_COLUMNS.items():
            val = _num(clean_row.get(col))
            if val is None:
                continue
            out.append(
                {
                    "source": SOURCE_NAME,
                    "series_id": f"{SERIES_PREFIX}_{kind}_{loc_id}_{flow}_{cycle}",
                    "series_name": (
                        f"Transco {_KIND_LABELS[kind]} {loc_name} "
                        f"[{flow.upper()}] ({cycle.upper()})"
                    ),
                    "period": period,
                    "value": val,
                    "unit": "Dth/d",
                    "region": "US",
                    "ingested_at": ingested_at,
                    "_posted_at": posted_dt,
                    "_watch": label,
                }
            )
    return out


def transform(
    raw_dir: Path = RAW_DIR,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform all Williams raw files into the curated parquet via accumulation.

    What:
        Scans *raw_dir* recursively, parses each raw file into watchlist
        series rows, stacks to a DataFrame, sorts by posting time, dedups on
        (series_id, period) keeping the latest, accumulates into curated
        history (``merge_into_curated``, shrinkage-guarded), returns stats.

    Failure modes:
        Raises ``TransformError`` when the raw dir/files are missing or the
        batch produces zero rows; accumulation shrinkage raises
        ``AccumulationShrinkError`` before any write.
    """
    if not raw_dir.exists():
        raise TransformError(
            f"Raw directory not found: {raw_dir}. Run scrapers.williams first."
        )

    raw_files = sorted(raw_dir.rglob("*.json"))
    if not raw_files:
        raise TransformError(f"No raw JSON files in {raw_dir}. Run scrapers.williams first.")

    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict[str, Any]] = []
    for path in raw_files:
        all_rows.extend(_rows_from_file(path, ingested_at))

    if not all_rows:
        raise TransformError(
            f"Williams transformer produced zero rows from {len(raw_files)} file(s)."
        )

    df = pd.DataFrame(all_rows)
    df = (
        df.sort_values("_posted_at")
        .drop_duplicates(subset=["series_id", "period"], keep="last")
        .drop(columns=["_posted_at", "_watch"])
        .reset_index(drop=True)
    )

    merged = merge_into_curated(df, curated_parquet_path)

    period_min = merged["period"].min()
    period_max = merged["period"].max()
    series_count = merged["series_id"].nunique()

    log.info(
        "Williams transformer: %d rows, %d series, %s → %s",
        len(merged),
        series_count,
        period_min,
        period_max,
    )

    return {
        "rows": len(merged),
        "period_range": (str(period_min), str(period_max)),
        "series_count": series_count,
        "source_files": len(raw_files),
    }


if __name__ == "__main__":
    import json as _json
    import logging as _logging

    _logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    result = transform()
    print(_json.dumps(result, indent=2, default=str))
