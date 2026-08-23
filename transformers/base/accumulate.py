"""Source-agnostic accumulation layer for curated Parquet histories.

Why:
    ``data/raw/`` is gitignored and CI runners are ephemeral, so a transformer
    that rebuilds its curated parquet from raw files alone overwrites the
    artefact on every run and destroys gas-day history.  Centralising the
    merge here lets every scraper (current and future EBB sources alike)
    append to one ever-growing curated file instead of clobbering it.

What:
    ``merge_into_curated`` reads the existing curated parquet (treating a
    missing file as an empty history), concatenates the new rows, deduplicates
    on *key_cols* keeping the row with the max ``ingested_at`` (ISO-8601 UTC
    strings compare lexicographically in chronological order, so a re-scrape
    updates rather than duplicates), shrink-guards, sorts by
    (``period``, ``series_id``) for stable diffs, and writes atomically via
    ``scrapers.base.safe_writer.safe_write_parquet``.  Frames are expected to
    carry the canonical Blue Tide columns, including ``period`` and
    ``series_id`` used for the final sort.

Failure modes:
    * ``AccumulationShrinkError`` — raised before any write if the merged
      frame holds fewer rows than the existing history; history can never
      silently shrink.
    * Frames lacking ``ingested_at`` entirely fall back to dropping exact
      duplicate keys, keeping the last occurrence.
    * Corrupt/unreadable existing parquet surfaces as-is; the atomic writer
      leaves the previous artefact untouched on any write failure.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from scrapers.base.safe_writer import safe_write_parquet

log = logging.getLogger(__name__)

_SORT_COLS: tuple[str, ...] = ("period", "series_id")


class AccumulationShrinkError(ValueError):
    """Raised when a merge would reduce the curated history's row count."""


def merge_into_curated(
    new_rows_df: pd.DataFrame,
    curated_path: Path,
    key_cols: tuple[str, ...] = ("series_id", "period"),
) -> pd.DataFrame:
    """Merge *new_rows_df* into the curated parquet at *curated_path*.

    Why:
        Raw inputs are ephemeral across CI runs; accumulating into the curated
        parquet preserves history instead of rebuilding it from scratch.

    What:
        Reads existing history (empty if absent), concatenates the new rows,
        deduplicates on *key_cols* keeping the max ``ingested_at``, refuses to
        shrink, sorts for stable diffs, writes atomically, and returns the
        merged frame.

    Failure modes:
        ``AccumulationShrinkError`` if the merged frame would contain fewer
        rows than the existing history — nothing is written in that case.
    """
    existing: pd.DataFrame = (
        pd.read_parquet(curated_path) if curated_path.exists() else new_rows_df.iloc[0:0]
    )

    if len(existing):
        merged: pd.DataFrame = pd.concat([existing, new_rows_df], ignore_index=True)
    else:
        merged = new_rows_df.copy()

    if "ingested_at" in merged.columns:
        merged = merged.sort_values("ingested_at", kind="stable")
    merged = merged.drop_duplicates(subset=list(key_cols), keep="last")

    n_existing = len(existing)
    n_merged = len(merged)
    if n_merged < n_existing:
        raise AccumulationShrinkError(
            f"Merged frame has {n_merged} rows vs {n_existing} existing rows "
            f"in {curated_path}; refusing to shrink curated history."
        )

    merged = merged.sort_values(list(_SORT_COLS), kind="stable").reset_index(drop=True)

    safe_write_parquet(curated_path, merged)
    log.info(
        "Accumulated %d row(s) into %s (history now %d rows)",
        len(new_rows_df),
        curated_path,
        n_merged,
    )
    return merged
