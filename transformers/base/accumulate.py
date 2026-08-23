"""Source-agnostic accumulation layer for curated Parquet histories.

Why:
    ``data/raw/`` is gitignored and CI runners are ephemeral, so a transformer
    that rebuilds its curated parquet from raw files alone overwrites the
    artefact on every run and destroys gas-day history.  Centralising the
    merge here lets every scraper (current and future EBB sources alike)
    append to one ever-growing curated file instead of clobbering it.

What:
    ``merge_into_curated`` reads the existing curated parquet (treating a
    missing file as an empty history), validates the incoming frame's columns
    against the existing schema, concatenates the new rows, deduplicates on
    *key_cols* keeping the row with the max parsed ``ingested_at``
    (ISO-8601 stamps are parsed timezone-aware before comparing, so mixed
    UTC-offset representations order chronologically), shrink-guards, sorts
    by (``period``, ``series_id``) for stable diffs, and writes atomically
    via ``scrapers.base.safe_writer.safe_write_parquet``.  Frames are
    expected to carry the canonical Blue Tide columns, including ``period``
    and ``series_id`` used for the final sort.

Failure modes:
    * ``SchemaDriftError`` — raised before any write when the incoming
      frame's column set differs from the existing history's (a malformed
      upstream frame must never be silently concatenated onto it).
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
from datetime import UTC
from pathlib import Path

import pandas as pd

from scrapers.base.safe_writer import safe_write_parquet

log = logging.getLogger(__name__)

_SORT_COLS: tuple[str, ...] = ("period", "series_id")


class AccumulationShrinkError(ValueError):
    """Raised when a merge would reduce the curated history's row count."""


class SchemaDriftError(ValueError):
    """Raised when an incoming frame's columns differ from the curated schema."""


def _parse_ingested_at(raw: object) -> pd.Timestamp:
    """Parse one ``ingested_at`` stamp timezone-aware; naive stamps are UTC.

    Why:
        Lexicographic comparison of ISO-8601 strings is only chronological
        when every stamp shares one UTC offset; a scraper writing ``+05:30``
        (or a ``Z`` suffix next to ``+00:00``) would otherwise misorder
        against it.

    What:
        Returns an offset-aware ``pd.Timestamp`` for any ISO-8601 input.

    Failure modes:
        Unparseable values become ``Timestamp.min`` (UTC) so the ordering
        stays total rather than raising mid-merge.
    """
    try:
        ts = pd.Timestamp(raw)
    except (ValueError, TypeError):
        return pd.Timestamp.min.tz_localize(UTC)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts


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
        Reads existing history (empty if absent), gates the incoming frame's
        column set against it, concatenates the new rows, deduplicates on
        *key_cols* keeping the max parsed ``ingested_at``, refuses to shrink,
        sorts for stable diffs, writes atomically, and returns the merged
        frame.

    Failure modes:
        ``SchemaDriftError`` if the incoming columns differ from the existing
        history's — nothing is written in that case.
        ``AccumulationShrinkError`` if the merged frame would contain fewer
        rows than the existing history — nothing is written in that case.
    """
    existing: pd.DataFrame = (
        pd.read_parquet(curated_path) if curated_path.exists() else new_rows_df.iloc[0:0]
    )

    if len(existing):
        # Schema drift gate: a malformed upstream frame (renamed/missing/extra
        # columns) must never be silently concatenated onto curated history.
        incoming_cols = set(new_rows_df.columns)
        existing_cols = set(existing.columns)
        if incoming_cols != existing_cols:
            added = sorted(incoming_cols - existing_cols)
            removed = sorted(existing_cols - incoming_cols)
            raise SchemaDriftError(
                f"Incoming frame columns differ from existing history in "
                f"{curated_path}: unexpected {added}, missing {removed}. "
                f"Existing={sorted(existing_cols)}, incoming={sorted(incoming_cols)}."
            )
        merged: pd.DataFrame = pd.concat([existing, new_rows_df], ignore_index=True)
    else:
        merged = new_rows_df.copy()

    if "ingested_at" in merged.columns:
        if len(existing) and "ingested_at" in new_rows_df.columns:
            # Dedup on parsed timestamps so mixed UTC-offset ISO stamps order
            # chronologically instead of lexicographically.
            key_cols_str = [str(c) for c in key_cols]
            key_mask = merged.duplicated(subset=key_cols_str, keep=False)
            if key_mask.any():
                keep_idx = set(merged.index[~key_mask])
                for _, grp in merged[key_mask].groupby(key_cols_str, dropna=False, sort=False):
                    keep_idx.add(int(grp["ingested_at"].map(_parse_ingested_at).idxmax()))
                merged = merged.loc[merged.index.isin(sorted(keep_idx))]
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
