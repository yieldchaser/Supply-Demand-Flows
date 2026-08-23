"""Cheniere OAC raw JSON → curated long-format Parquet.

Schema out (canonical Blue Tide):
    source      : "cheniere"
    series_id   : "{creole_trail|corpus_christi}_oac_{loc}_{cycle}" for the
                  primary qty_AVAIL signal, plus
                  "{prefix}_sq_{loc}_{cycle}" and "{prefix}_design_{loc}_{cycle}"
    series_name : "Creole Trail OAC {loc_name} ({cycle})" etc.
    period      : YYYY-MM-DD  (avaiL_CAP_EFF_DT_TIME date part)
    value       : Raw Dth/d — never converted; CTPL (~650k design) and CCPL
                  (~35k+ design) scales are kept apart by their prefixes
    unit        : "Dth/d"
    region      : "US"
    ingested_at : UTC ISO-8601

Signal notes:
    Cheniere posts no Scheduled Quantities page anywhere on its connection
    site — OAC is the only surface. ``scheD_QTY`` is captured as ``_sq_``
    series even when 0.0 (unscheduled postings); falling ``qtY_AVAIL``
    against ``desigN_OPER_CAP`` implies the terminal is taking gas.

Deduplication:
    Same pattern as Gulf South/BHE: batch-dedup on latest posting timestamp
    per (series_id, period), then accumulate via ``merge_into_curated``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.cheniere.client import (
    PREFIX_CORPUS_CHRISTI,
    PREFIX_CREOLE_TRAIL,
    TSP_CORPUS_CHRISTI,
    TSP_CREOLE_TRAIL,
)
from transformers.base.accumulate import merge_into_curated
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/cheniere")
CURATED_PATH = Path("data/curated/cheniere.parquet")

_TSP_TO_PREFIX: dict[int, str] = {
    TSP_CREOLE_TRAIL: PREFIX_CREOLE_TRAIL,
    TSP_CORPUS_CHRISTI: PREFIX_CORPUS_CHRISTI,
}
_TSP_LABELS: dict[int, str] = {
    TSP_CREOLE_TRAIL: "Creole Trail",
    TSP_CORPUS_CHRISTI: "Corpus Christi",
}


def _parse_posted_at(posted_at: str) -> datetime:
    """Parse a posting timestamp for dedup ordering; falls back to epoch."""
    if posted_at:
        try:
            return datetime.fromisoformat(posted_at.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.min.replace(tzinfo=UTC)


def _cycle_token(cycle: str) -> str:
    """Map 'Intraday 3' → id3, 'Evening' → evening, 'Timely' → timely."""
    c = cycle.strip().upper()
    if c == "TIMELY":
        return "timely"
    if c == "EVENING":
        return "evening"
    if c.startswith("INTRADAY"):
        tail = c.rsplit(" ", 1)[-1]
        if tail in {"1", "2", "3"}:
            return f"id{tail}"
    return c.lower().replace(" ", "_")


def _rows_from_file(path: Path, ingested_at: str) -> list[dict[str, Any]]:
    """Extract OAC/SQ/design series rows from one raw Cheniere payload.

    Failure modes:
        Malformed JSON files are skipped with a warning; rows with unknown
        tsp numbers or missing loc/period are dropped individually.
    """
    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Skipping malformed file %s: %s", path, exc)
        return []

    tsp_no = payload.get("tsp_no")
    prefix: str | None = None
    if tsp_no is not None:
        try:
            prefix = _TSP_TO_PREFIX.get(int(tsp_no))
        except (TypeError, ValueError):
            prefix = None
    if prefix is None:
        log.warning("Skipping %s: unknown tsp_no %r", path, tsp_no)
        return []
    label = _TSP_LABELS[int(tsp_no)]
    rows_raw: list[dict[str, Any]] = payload.get("rows", []) or []

    out: list[dict[str, Any]] = []
    for row in rows_raw:
        loc = str(row.get("loc") or "").strip()
        period = str(row.get("period") or "").strip()
        cycle_token = _cycle_token(str(row.get("cycle") or ""))
        loc_name = str(row.get("loc_name") or "").strip() or loc
        if not loc or not period:
            continue

        posted_dt = _parse_posted_at(str(row.get("posted_at") or ""))

        series = (
            ("oac", row.get("qty_avail"), "OAC"),
            ("sq", row.get("sched_qty"), "Sched Qty"),
            ("design", row.get("design_oper_cap"), "Design"),
        )
        for kind, raw_val, label_txt in series:
            try:
                val = float(raw_val) if raw_val is not None else None
            except (TypeError, ValueError):
                val = None
            if val is None:
                continue
            out.append(
                {
                    "source": "cheniere",
                    "series_id": f"{prefix}_{kind}_{loc}_{cycle_token}",
                    "series_name": f"{label} {label_txt} {loc_name} ({str(row.get('cycle') or '').strip()})",
                    "period": period,
                    "value": val,
                    "unit": "Dth/d",
                    "region": "US",
                    "ingested_at": ingested_at,
                    "_posted_at": posted_dt,
                }
            )
    return out


def transform(
    raw_dir: Path = RAW_DIR,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform all Cheniere raw JSON files into the curated parquet via accumulation.

    What:
        Scans *raw_dir* recursively, parses each raw file into per-pipeline
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
            f"Raw directory not found: {raw_dir}. Run scrapers.cheniere.client first."
        )

    raw_files = sorted(raw_dir.rglob("*.json"))
    # The checkpoint file lives inside the raw dir; it is not a data file.
    data_files = [p for p in raw_files if p.name != "_backfill_state.json"]
    if not data_files:
        raise TransformError(f"No raw JSON files in {raw_dir}. Run scrapers.cheniere.client first.")

    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict[str, Any]] = []
    for path in data_files:
        all_rows.extend(_rows_from_file(path, ingested_at))

    if not all_rows:
        raise TransformError(
            f"Cheniere transformer produced zero rows from {len(data_files)} file(s)."
        )

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
    prefixes = sorted(merged["series_id"].str.split("_", n=1).str[0].unique())

    log.info(
        "Cheniere transformer: %d rows, %d series, %s → %s, pipelines=%s",
        len(merged),
        series_count,
        period_min,
        period_max,
        prefixes,
    )

    return {
        "rows": len(merged),
        "period_range": (str(period_min), str(period_max)),
        "series_count": series_count,
        "pipelines": prefixes,
        "source_files": len(data_files),
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
