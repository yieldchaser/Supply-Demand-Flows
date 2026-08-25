"""Transformer: KM raw payloads -> curated parquet (flow-tokened series)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.kinder_morgan import RAW_DIR
from transformers.base.accumulate import merge_into_curated

log = logging.getLogger(__name__)

SOURCE_NAME = "kinder_morgan"
CURATED_PATH = Path("data/curated/kinder_morgan.parquet")

#: Confirmed LNG-bound meters (Sabine recon 2026-08-24; CI live-fire verified).
CONFIRMED_METERS: dict[str, dict[str, str]] = {
    "3592": {"pipeline": "ngpl", "label": "SABPL/NGPL HENRY HUB VERMILION", "target": "sabine_pass"},
    "49861": {"pipeline": "tgp", "label": "CCCPL/TGP SINTON SAN PATRICIO", "target": "corpus_christi"},
    "47799": {"pipeline": "tgp", "label": "NMP/TGP GILLRINA ROAD NUECES", "target": "corpus_metro"},
    "49524": {"pipeline": "tgp", "label": "GULFSTH/TGP COASTAL BEND LNG", "target": "spl_jv"},
}

#: NOT Sabine — Calcasieu Pass corridor. Kept here to document the exclusion.
EXCLUDED_METERS = {"44337": "Calcasieu Pass area — do not mislabel as Sabine"}

CONV = 1.025 * 1000  # Dth/d -> MMcf/d


def _num(text: str) -> float | None:
    try:
        return float(str(text).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def transform_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Convert one raw payload into curated rows for confirmed meters."""
    prefix = payload.get("pipeline_prefix", "")
    out: list[dict[str, Any]] = []
    fetched = payload.get("fetched_at", "")

    for row in payload.get("data", []):
        loc = str(row.get("loc") or "")
        conf = CONFIRMED_METERS.get(loc)
        if not conf or conf["pipeline"] != prefix:
            continue
        tsq_dth = _num(row.get("total_scheduled_quantity"))
        if tsq_dth is None:
            continue
        # BEST AVAILABLE cycle — no per-cycle pinning yet.
        series_id = f"km_{conf['pipeline']}_sq_{loc}_d_best"
        out.append(
            {
                "source": SOURCE_NAME,
                "series_id": series_id,
                "series_name": f"KM {conf['pipeline'].upper()} TSQ {conf['label']} [d] (best)",
                "period": datetime.now(UTC).date().isoformat(),
                "value": round(tsq_dth / CONV, 1),
                "unit": "MMcf/d",
                "region": "US",
                "zone": row.get("loc_zone", ""),
                "ingested_at": fetched,
            }
        )
    return out


def run(raw_dir: Path = RAW_DIR, curated_parquet_path: Path = CURATED_PATH) -> dict[str, Any]:
    """Accumulate all KM raw payloads into the curated parquet."""
    frames: list[pd.DataFrame] = []
    source_files = 0
    for path in sorted(raw_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Skipping unreadable %s: %s", path.name, exc)
            continue
        rows = transform_payload(payload)
        if rows:
            source_files += 1
            frames.append(pd.DataFrame(rows))

    if not frames:
        raise ValueError(f"No KM raw payloads with confirmed-meter rows under {raw_dir}")

    new_df = pd.concat(frames, ignore_index=True)
    merged = merge_into_curated(new_df, curated_parquet_path)
    log.info(
        "KM transformer: %d rows, %d series from %d files",
        len(merged),
        merged["series_id"].nunique(),
        source_files,
    )
    return {
        "rows": len(merged),
        "series_count": int(merged["series_id"].nunique()),
        "source_files": source_files,
    }


if __name__ == "__main__":
    print(run())
