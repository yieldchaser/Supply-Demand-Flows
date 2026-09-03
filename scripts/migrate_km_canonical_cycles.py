"""One-shot migration script for Kinder Morgan curated parquet canonical cycle tokens.

Prompt W §06 requirement:
Rewrites series_id and series_name in data/curated/kinder_morgan.parquet for the
99 affected rows (evng -> evening, itrd1-3 -> id1-3) while preserving timely and best.

Guards:
1. Refuses to run if row count before and after differ.
2. Refuses to run if the set of (meter, period) pairs changes.
3. Refuses to run if value.sum() moves by more than 1e-6.
4. Prints before/after counts per token.
5. Writes atomically via safe_write_parquet.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from scrapers.base.safe_writer import safe_write_parquet

CURATED_PATH = Path("data/curated/kinder_morgan.parquet")

CYCLE_RENAME_MAP = {
    "evng": "evening",
    "itrd1": "id1",
    "itrd2": "id2",
    "itrd3": "id3",
}

NAME_TOKEN_MAP = {
    "(evng)": "(evening)",
    "(itrd1)": "(id1)",
    "(itrd2)": "(id2)",
    "(itrd3)": "(id3)",
    "(EVNG)": "(EVENING)",
    "(ITRD1)": "(ID1)",
    "(ITRD2)": "(ID2)",
    "(ITRD3)": "(ID3)",
}


def migrate_kinder_morgan_parquet(path: Path = CURATED_PATH) -> pd.DataFrame:
    """Execute guarded migration of KM curated parquet."""
    if not path.exists():
        raise FileNotFoundError(f"Parquet file not found: {path}")

    df_before = pd.read_parquet(path)
    count_before = len(df_before)

    # Extract cycle token (last segment after final underscore)
    def extract_cycle(sid: str) -> str:
        parts = str(sid).split("_")
        return parts[-1] if len(parts) > 1 else ""

    def extract_meter_stem(sid: str) -> str:
        parts = str(sid).rsplit("_", 1)
        return parts[0] if len(parts) > 1 else sid

    cycles_before = df_before["series_id"].apply(extract_cycle)
    tokens_before = cycles_before.value_counts().to_dict()

    # Track meter-period pairs before
    meter_stems_before = df_before["series_id"].apply(extract_meter_stem)
    pairs_before = set(zip(meter_stems_before, df_before["period"], strict=False))
    val_sum_before = float(df_before["value"].sum())

    # Build updated series_id and series_name
    df_after = df_before.copy()

    def update_series_id(sid: str) -> str:
        parts = str(sid).rsplit("_", 1)
        if len(parts) == 2 and parts[1] in CYCLE_RENAME_MAP:
            return f"{parts[0]}_{CYCLE_RENAME_MAP[parts[1]]}"
        return sid

    def update_series_name(name: str) -> str:
        res = str(name)
        for old_tok, new_tok in NAME_TOKEN_MAP.items():
            if old_tok in res:
                res = res.replace(old_tok, new_tok)
        return res

    df_after["series_id"] = df_after["series_id"].apply(update_series_id)
    df_after["series_name"] = df_after["series_name"].apply(update_series_name)

    # Invariant Guard 1: Row count before and after must not differ
    count_after = len(df_after)
    if count_after != count_before:
        raise ValueError(
            f"GUARD FAILURE: Row count changed! Before={count_before}, After={count_after}"
        )

    # Invariant Guard 2: Set of (meter, period) pairs must not change
    meter_stems_after = df_after["series_id"].apply(extract_meter_stem)
    pairs_after = set(zip(meter_stems_after, df_after["period"], strict=False))
    if pairs_after != pairs_before:
        raise ValueError(
            f"GUARD FAILURE: (meter, period) set changed! "
            f"Diff: {pairs_before.symmetric_difference(pairs_after)}"
        )

    # Invariant Guard 3: value.sum() must not move by more than 1e-6
    val_sum_after = float(df_after["value"].sum())
    val_diff = abs(val_sum_after - val_sum_before)
    if val_diff > 1e-6:
        raise ValueError(
            f"GUARD FAILURE: value.sum() moved by {val_diff:.8e} (> 1e-6)! "
            f"Before={val_sum_before}, After={val_sum_after}"
        )

    # Measure after token counts
    cycles_after = df_after["series_id"].apply(extract_cycle)
    tokens_after = cycles_after.value_counts().to_dict()

    print("=" * 60)
    print("KM CANONICAL CYCLES MIGRATION REPORT")
    print("=" * 60)
    print(f"Total rows before: {count_before}")
    print(f"Total rows after:  {count_after}")
    print(f"Value sum before:  {val_sum_before:.4f}")
    print(f"Value sum after:   {val_sum_after:.4f} (diff={val_diff:.2e})")
    print("-" * 60)
    print("Cycle token counts BEFORE migration:")
    for tok in sorted(tokens_before.keys()):
        print(f"  {tok:12s}: {tokens_before[tok]:4d}")
    print("-" * 60)
    print("Cycle token counts AFTER migration:")
    for tok in sorted(tokens_after.keys()):
        print(f"  {tok:12s}: {tokens_after[tok]:4d}")
    print("-" * 60)

    changed_rows = sum(1 for b, a in zip(df_before["series_id"], df_after["series_id"], strict=False) if b != a)
    print(f"Rows migrated:     {changed_rows}")
    print(f"Rows unchanged:    {count_before - changed_rows}")
    print("=" * 60)

    # Atomic write
    safe_write_parquet(path, df_after)
    print(f"Successfully wrote migrated parquet atomically to {path}")
    return df_after


if __name__ == "__main__":
    try:
        migrate_kinder_morgan_parquet()
    except Exception as exc:
        print(f"Migration aborted with error: {exc}", file=sys.stderr)
        sys.exit(1)
