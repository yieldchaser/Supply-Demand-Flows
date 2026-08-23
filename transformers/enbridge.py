"""Enbridge rtba (Texas Eastern) OAC raw JSON → curated long-format Parquet.

Schema out (canonical Blue Tide, NEW series-key format WITH flow direction —
the dual-leg collision fix; a bi-directional meter posts both a Delivery and
a Receipt row per cycle and the old flow-less key collapsed them):

    source      : "enbridge"
    series_id   : "tetco_{sq|oac}_{loc}_{flow}_{cycle}"
                  e.g. tetco_sq_79999_d_timely / tetco_oac_73568_r_id0901
    series_name : "TETCO TSQ {loc_name} [{flow}] ({cycle})"
    period      : YYYY-MM-DD  (Eff Gas Day)
    value       : Raw quantity in Dth/d (MMBtu basis, never converted)
    unit        : "Dth/d"
    region      : "US"
    ingested_at : UTC ISO-8601

Freeport focus:
    Loc 79999 "STRATTON RIDGE" (Zone STX, Flow=Delivery) is the Freeport
    LNG lateral interconnect. Do NOT match on "LNG" in loc names —
    73568/74568 "Kinder Morgan LNG" and 75866 "CHENIERE LNG, BEAUREGARD"
    are unrelated meters. Meter classification lives in
    ``config/meters/enbridge.json``; this transformer emits ALL meters and
    the config drives panel-level selection downstream.

Deduplication:
    Within a batch, rows are sorted by posting timestamp (Post_Date +
    Post_Time) and the last value per (series_id, period) kept; the batch is
    then accumulated into the curated history via ``merge_into_curated``,
    which re-dedupes on ``ingested_at`` so re-scrapes update past gas days
    rather than duplicate them.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.enbridge.client import (
    COL_CYCLE,
    COL_FLOW_IND,
    COL_GAS_DAY,
    COL_LOC,
    COL_LOC_NAME,
    COL_LOC_ZN,
    COL_OAC,
    COL_POST_DATE,
    COL_POST_TIME,
    COL_TSQ,
    SOURCE_NAME,
)
from transformers.base.accumulate import merge_into_curated
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/enbridge")
CURATED_PATH = Path("data/curated/enbridge.parquet")

SERIES_PREFIX = "tetco"

_FLOW_TOKENS: dict[str, str] = {
    "DELIVERY": "d",
    "RECEIPT": "r",
}


def _parse_posted_at(post_date: str, post_time: str) -> datetime:
    """Parse Post_Date/Post_Time (MM-DD-YYYY, HH:MM) for dedup ordering.

    Failure modes:
        Unparseable/empty parts yield ``datetime.min`` (UTC) so those rows
        sort first and lose dedup contests.
    """
    try:
        base = datetime.strptime(post_date.strip(), "%m-%d-%Y")
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    hh, mm = 0, 0
    if post_time and ":" in post_time:
        try:
            hh_s, mm_s = post_time.split(":", 1)
            hh, mm = int(hh_s), int(mm_s)
        except ValueError:
            pass
    return base.replace(hour=hh, minute=mm, tzinfo=UTC)


def _num(value: Any) -> float | None:
    """Best-effort float parse of a raw CSV quantity string ('1,234' tolerated)."""
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _flow_token(flow_desc: str) -> str | None:
    """Map Flow_Ind_Desc to the single-letter flow segment token."""
    return _FLOW_TOKENS.get(flow_desc.strip().upper())


def _normalize_period(raw: str) -> str | None:
    """Normalize a gas-day cell to ISO ``YYYY-MM-DD``.

    What:
        TETCO CSVs carry ``Eff_Gas_Day`` as ``MM-DD-YYYY``; ISO input passes
        through unchanged.

    Failure modes:
        Returns ``None`` when unparseable (row dropped by caller).
    """
    s = raw.strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    parts = s.split("-")
    if len(parts) == 3 and all(p.isdigit() for p in parts) and len(parts[2]) == 4:
        mm, dd, yyyy = parts
        return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    return None


def _rows_from_file(path: Path, ingested_at: str) -> list[dict[str, Any]]:
    """Extract flow-segmented series rows from one raw TETCO payload.

    What:
        Emits TSQ, OAC, operating-capacity and design-capacity series for
        EVERY meter row — flow direction is part of the series id so
        bi-directional legs never collide. Zeros are preserved (they mean
        "no gas scheduled that cycle").

    Failure modes:
        Malformed JSON files are skipped with a warning; rows missing loc,
        gas day, or an unmappable flow are dropped individually.
    """
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Skipping malformed file %s: %s", path, exc)
        return []

    cycle: str = str(payload.get("cycle") or "").lower()
    raw_rows: list[dict[str, Any]] = payload.get("data", []) or []

    out: list[dict[str, Any]] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        loc_id = str(row.get(COL_LOC) or "").strip()
        loc_name = str(row.get(COL_LOC_NAME) or "").strip() or loc_id
        zone = str(row.get(COL_LOC_ZN) or "").strip()
        period = _normalize_period(str(row.get(COL_GAS_DAY) or ""))
        flow = _flow_token(str(row.get(COL_FLOW_IND) or ""))
        posted_at = _parse_posted_at(
            str(row.get(COL_POST_DATE) or ""), str(row.get(COL_POST_TIME) or "")
        )
        # CSV Cycle_Desc is authoritative; the filename-derived token covers it.
        csv_cycle = resolve_cycle(str(row.get(COL_CYCLE) or ""), cycle)
        if not loc_id or not period or not flow or not csv_cycle:
            continue

        for kind, col, label in (
            ("sq", COL_TSQ, "TSQ"),
            ("oac", COL_OAC, "OAC"),
        ):
            val = _num(row.get(col))
            if val is None:
                continue
            out.append(
                {
                    "source": SOURCE_NAME,
                    "series_id": f"{SERIES_PREFIX}_{kind}_{loc_id}_{flow}_{csv_cycle}",
                    "series_name": f"TETCO {label} {loc_name} [{flow}] ({csv_cycle})",
                    "period": period,
                    "value": val,
                    "unit": "Dth/d",
                    "region": "US",
                    "zone": zone,
                    "ingested_at": ingested_at,
                    "_posted_at": posted_at,
                }
            )
    return out


def resolve_cycle(csv_cycle_desc: str, fallback: str) -> str | None:
    """Prefer the row's own Cycle_Desc; fall back to the filename token."""
    from scrapers.enbridge.client import resolve_cycle_token

    token = resolve_cycle_token(csv_cycle_desc)
    return token or (fallback or None)


def transform(
    raw_dir: Path = RAW_DIR,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform all Enbridge raw JSON files into the curated parquet via accumulation.

    What:
        Scans *raw_dir* recursively, parses each raw file into flow-segmented
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
        raise TransformError(f"Raw directory not found: {raw_dir}. Run scrapers.enbridge.client first.")

    raw_files = sorted(raw_dir.rglob("*.json"))
    raw_files = [f for f in raw_files if f.name != "_backfill_state.json"]
    if not raw_files:
        raise TransformError(f"No raw JSON files in {raw_dir}. Run scrapers.enbridge.client first.")

    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict[str, Any]] = []
    for path in raw_files:
        all_rows.extend(_rows_from_file(path, ingested_at))

    if not all_rows:
        raise TransformError(f"Enbridge transformer produced zero rows from {len(raw_files)} file(s).")

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

    log.info(
        "Enbridge transformer: %d rows, %d series, %s → %s",
        len(merged),
        series_count,
        period_min,
        period_max,
    )

    return {
        "rows": len(merged),
        "period_range": (str(period_min), str(period_max)),
        "series_count": int(series_count),
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
