"""GASNom OAC raw JSON → curated long-format Parquet (4 LNG pipelines).

Schema out (canonical Blue Tide):
    source      : "gasnom"
    series_id   : "{prefix}_sq_{loc}_{cycle_code}" or "{prefix}_oac_{loc}_{cycle_code}"
                  (cycle_code ∈ timely/evening/id1/id2/id3 — see
                  scrapers.gasnom.client.cycle_code_from_description)
    series_name : "Golden Pass TSQ Terminal (ID3)" / "Golden Pass OAC Terminal (ID3)"
    period      : YYYY-MM-DD  (Effective Gas Day)
    value       : RAW quantity in Dth/d (TSQ or Operationally Available Capacity)
    unit        : "Dth/d"   ← never converted; Measurement_Basis is DTH on all 4 pipes
    region      : "US"
    ingested_at : UTC ISO-8601

Deduplication:
    Within a batch, rows from every raw file are stacked, sorted by the
    posting timestamp and deduped on (series_id, period) keeping the latest.
    The batch is then accumulated into the curated history via
    ``merge_into_curated`` (shrinkage-guarded, atomic) so re-scrapes update
    past gas days rather than duplicate them.  Overwriting the curated
    parquet is a hard-fail bug; this module must never ``to_parquet`` directly.
"""

from __future__ import annotations

import contextlib
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.gasnom.pipelines import GASNOM_PIPELINES
from transformers.base.accumulate import merge_into_curated
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/gasnom")
CURATED_PATH = Path("data/curated/gasnom.parquet")


def _parse_posting_time(raw: str) -> datetime:
    """Parse a posting timestamp into an aware UTC datetime for ordering.

    Handles the two shapes present in raw payloads:
      * bulk-TSV style: "08/22/2026 09:31:03 PM"
      * HTML-header style: "August 22, 2026 09:31 PM CT"

    Failure modes:
        Unparseable/blank values map to ``datetime.min`` (UTC) so they sort
        earliest and lose dedup contests instead of winning them.
    """
    text = (raw or "").strip()
    if not text:
        return datetime.min.replace(tzinfo=UTC)
    for fmt in (
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%B %d, %Y %I:%M %p",
        "%B %d, %Y %I:%M:%S %p",
    ):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)


def _parse_raw_file(path: Path, ingested_at: str) -> list[dict[str, Any]]:
    """Read one raw GASNom payload and emit TSQ + OAC series row dicts.

    Failure modes:
        Malformed files are skipped with a warning; rows missing a location
        id, gas day, or both quantities contribute nothing.  Values that fail
        float() are dropped rather than coerced to zero.
    """
    import json

    try:
        payload: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("Skipping malformed file %s: %s", path, exc)
        return []

    prefix: str = payload.get("series_prefix") or ""
    if not prefix:
        log.warning("Skipping %s: missing series_prefix", path.name)
        return []
    cycle_code: str = str(payload.get("cycle", "unknown")).lower()
    gas_day: str | None = payload.get("gas_day")
    posted_at_raw: str = str(payload.get("posted_at") or "")

    out: list[dict[str, Any]] = []
    for row in payload.get("data", []):
        loc = str(row.get("loc") or "").strip()
        loc_name = str(row.get("loc_name") or "").strip() or loc
        period = str(gas_day or "").strip()
        if not loc or not period:
            continue

        posted_dt = _parse_posting_time(str(row.get("posting_date_time") or "") or posted_at_raw)

        common = {
            "source": "gasnom",
            "period": period,
            "region": "US",
            "ingested_at": ingested_at,
            "_posted_at": posted_dt,
        }

        tsq_raw = row.get("tsq")
        if tsq_raw not in (None, ""):
            with contextlib.suppress(TypeError, ValueError):
                out.append({
                    **common,
                    "series_id": f"{prefix}_sq_{loc}_{cycle_code}",
                    "series_name": f"{_display_name(prefix)} TSQ {loc_name} ({cycle_code.upper()})",
                    "value": float(tsq_raw),
                    "unit": "Dth/d",
                })

        oac_raw = row.get("oac")
        if oac_raw not in (None, ""):
            with contextlib.suppress(TypeError, ValueError):
                out.append({
                    **common,
                    "series_id": f"{prefix}_oac_{loc}_{cycle_code}",
                    "series_name": f"{_display_name(prefix)} OAC {loc_name} ({cycle_code.upper()})",
                    "value": float(oac_raw),
                    "unit": "Dth/d",
                })

    return out


_PREFIX_DISPLAY: dict[str, str] = {
    cfg.series_prefix: cfg.name.removesuffix(" LLC").removesuffix(", LLC")
    for cfg in GASNOM_PIPELINES.values()
}


def _display_name(prefix: str) -> str:
    """'golden_pass' → 'Golden Pass Pipeline' (for series_name strings)."""
    return _PREFIX_DISPLAY.get(prefix, prefix.replace("_", " ").title())


def transform(
    raw_dir: Path = RAW_DIR,
    curated_parquet_path: Path = CURATED_PATH,
) -> dict[str, Any]:
    """Transform all GASNom raw JSON files into the curated parquet.

    What:
        Scans *raw_dir* recursively, parses each payload into TSQ & OAC
        series rows, stacks them, sorts by posting time, dedupes on
        (series_id, period) keeping the latest posting, then accumulates the
        batch into the curated history via ``merge_into_curated``.

    Failure modes:
        ``TransformError`` when the raw directory is missing/empty or the
        batch produces zero rows.  ``AccumulationShrinkError`` propagates
        from the merge guard if anything attempts to shrink history.
    """
    if not raw_dir.exists():
        raise TransformError(
            f"Raw directory not found: {raw_dir}. Run scrapers.gasnom (or .backfill) first."
        )

    raw_files = sorted(p for p in raw_dir.rglob("*.json") if p.name != "_backfill_state.json")
    if not raw_files:
        raise TransformError(f"No raw JSON files in {raw_dir}.")

    ingested_at = datetime.now(UTC).isoformat()
    all_rows: list[dict[str, Any]] = []
    prefixes_seen: set[str] = set()

    for path in raw_files:
        rows = _parse_raw_file(path, ingested_at)
        for row in rows:
            prefixes_seen.add(str(row["series_id"]).split("_sq_", 1)[0].rsplit("_oac_", 1)[0])
        all_rows.extend(rows)
        log.debug("%s → %d rows", path.name, len(rows))

    if not all_rows:
        raise TransformError(
            f"GASNom transformer produced zero rows from {len(raw_files)} file(s)."
        )

    df = pd.DataFrame(all_rows)

    # Batch-level dedupe: keep the most recently POSTED version of each
    # (series_id, period); merge_into_curated then re-dedupes on ingested_at.
    df = (
        df.sort_values("_posted_at", kind="stable")
        .drop_duplicates(subset=["series_id", "period"], keep="last")
        .drop(columns=["_posted_at"])
        .reset_index(drop=True)
    )

    merged = merge_into_curated(df, curated_parquet_path)

    period_min = str(merged["period"].min())
    period_max = str(merged["period"].max())
    series_count = int(merged["series_id"].nunique())

    per_pipe: dict[str, int] = {}
    for prefix in prefixes_seen:
        per_pipe[prefix] = int(
            merged["series_id"].astype(str).str.startswith(tuple(f"{prefix}_")).sum()
        )

    log.info(
        "GASNom transformer: %d rows total (%d series), %s → %s, pipelines=%s",
        len(merged), series_count, period_min, period_max,
        {k: v for k, v in sorted(per_pipe.items())},
    )

    return {
        "rows": len(merged),
        "new_rows_in_batch": len(df),
        "period_range": (period_min, period_max),
        "series_count": series_count,
        "source_files": len(raw_files),
        "rows_per_pipeline": dict(sorted(per_pipe.items())),
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    result = transform()
    import json as _json

    print(_json.dumps(result, indent=2, default=str))
