"""Tests for catalogue bug #1 — accumulation overwrite (prompt C-accumulation-overwrite.md).

Three guards:
  1. Every transformer that writes a ``data/curated/*.parquet`` must route through
     ``merge_into_curated`` and must NOT call ``safe_write_parquet`` over a curated
     file (the direct-write path is what truncated five years of DE/PL history).
  2. A transform whose input omits a country/series that already exists in curated
     must NOT shrink the curated file — the accumulated history is preserved.
  3. The restored ``gie_agsi`` curated file carries 40 rows per day across the
     affected range, including the earliest (2021-01-01) and a recent day.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from transformers.base.accumulate import merge_into_curated

TRANSFORMERS_DIR = Path("transformers")

# Modules allowed to use safe_write_parquet legitimately (the atomic writer inside
# the accumulation layer, and anything that does not touch a curated artefact).
_SAFE_WRITER_ALLOWED = {"base/accumulate.py"}


def _transformer_sources() -> list[tuple[str, str]]:
    """Return (relative path, source) for every top-level transformer module."""
    out = []
    for path in sorted(TRANSFORMERS_DIR.glob("*.py")):
        out.append((path.name, path.read_text(encoding="utf-8")))
    return out


def test_every_curated_writer_routes_through_merge_into_curated() -> None:
    """Regression guard: no transformer may direct-write a curated parquet.

    A module that writes ``data/curated/...`` must import and call
    ``merge_into_curated`` and must not call ``safe_write_parquet(`` on a curated
    path. This makes bug #1 impossible to reintroduce.

    Notes:
        * Only actual ``safe_write_parquet(`` *calls* count — mentions inside
          docstrings/comments (e.g. ``eia_storage.py``'s post-mortem, the
          ``gie_agsi.py`` fix note) are not violations.
        * ``safe_write_bytes`` (the Baker Hughes xlsx bridge) is a different
          writer and is out of scope for this guard.
        * ``transformers/base/accumulate.py`` is the legitimate sole caller of
          ``safe_write_parquet`` (the atomic writer inside the merge).
    """
    violations = []
    for name, src in _transformer_sources():
        writes_curated = "data/curated/" in src
        if not writes_curated:
            continue
        # Actual call, not a docstring/comment mention.
        calls_direct_write = "safe_write_parquet(" in src
        calls_bytes = "safe_write_bytes(" in src
        uses_merge = "merge_into_curated(" in src or "merge_into_curated" in src
        if calls_direct_write and name not in _SAFE_WRITER_ALLOWED:
            violations.append(f"{name}: calls safe_write_parquet( over a curated path")
        if not calls_direct_write and not calls_bytes and not uses_merge:
            violations.append(f"{name}: writes curated but does not use merge_into_curated")

    assert not violations, "transformers violating the accumulation rule:\n" + "\n".join(
        f"  - {v}" for v in violations
    )


def test_subset_transform_does_not_shrink_curated(tmp_path: Path) -> None:
    """A run missing a series that exists in curated must leave it untouched.

    Reproduces the bug #1 trigger in miniature: simulate curated holding two
    series (A and B); a transform that only fetched series A must merge so that B
    survives. The shrinkage guard refuses any write that would drop rows.
    """
    curated = tmp_path / "data" / "curated"
    curated.mkdir(parents=True, exist_ok=True)
    curated_path = curated / "sample.parquet"

    # Seed curated with both series A and B across two periods.
    seed = pd.DataFrame(
        [
            {"source": "t", "series_id": "s_a", "series_name": "A", "period": "2026-01-01", "value": 1.0, "unit": "x", "region": "R", "ingested_at": "2026-01-02T00:00:00+00:00"},
            {"source": "t", "series_id": "s_a", "series_name": "A", "period": "2026-01-02", "value": 2.0, "unit": "x", "region": "R", "ingested_at": "2026-01-02T00:00:00+00:00"},
            {"source": "t", "series_id": "s_b", "series_name": "B", "period": "2026-01-01", "value": 3.0, "unit": "x", "region": "R", "ingested_at": "2026-01-02T00:00:00+00:00"},
            {"source": "t", "series_id": "s_b", "series_name": "B", "period": "2026-01-02", "value": 4.0, "unit": "x", "region": "R", "ingested_at": "2026-01-02T00:00:00+00:00"},
        ]
    )
    curated_path.write_bytes(seed.to_parquet())

    # A transform that only fetched series A (B is missing from this run's input).
    batch = pd.DataFrame(
        [
            {"source": "t", "series_id": "s_a", "series_name": "A", "period": "2026-01-02", "value": 2.5, "unit": "x", "region": "R", "ingested_at": "2026-02-01T00:00:00+00:00"},
        ]
    )

    merged = merge_into_curated(batch, curated_path)

    # Series B must still be present (not shrunk away).
    surviving = set(merged["series_id"].unique())
    assert "s_b" in surviving, "series B was dropped by a subset transform — history shrank"
    # Row count must not fall below the seeded history.
    assert len(merged) >= len(seed), "curated shrank below its prior size"
    # The existing B rows are unchanged.
    b_rows = merged[merged["series_id"] == "s_b"]
    assert set(b_rows["period"]) == {"2026-01-01", "2026-01-02"}
    assert b_rows["value"].tolist() == [3.0, 4.0]


def test_restore_gie_agsi_has_full_daily_coverage() -> None:
    """The restored gie_agsi curated file has 40 rows/day across the affected range.

    Includes the earliest affected day (2021-01-01) and a recent one (2026-08-30),
    with all eight DE/PL series present at full daily coverage.
    """
    curated = Path("data/curated/gie_agsi.parquet")
    if not curated.exists():
        pytest.skip("gie_agsi curated parquet not present in this checkout")

    df = pd.read_parquet(curated)
    assert df["series_id"].nunique() == 40, f"expected 40 series, got {df['series_id'].nunique()}"

    affected = [s for s in df["series_id"].unique() if s.startswith("gie_storage_de_") or s.startswith("gie_storage_pl_")]
    assert len(affected) == 8, f"expected 8 DE/PL series, got {len(affected)}"

    for day in ("2021-01-01", "2026-08-30"):
        day_df = df[df["period"] == day]
        assert len(day_df) == 40, f"{day}: expected 40 rows, got {len(day_df)}"
        present = set(day_df["series_id"])
        assert set(affected).issubset(present), f"{day}: DE/PL series missing: {set(affected) - present}"
