"""Tests for the source-agnostic curated-history accumulation layer.

Test strategy:
    - All writes go to pytest tmp_path; no repo data files touched.
    - ingested_at values are ISO-8601 UTC strings with distinct, increasing
      times so lexicographic order == chronological order.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from transformers.base.accumulate import (
    AccumulationShrinkError,
    SchemaDriftError,
    merge_into_curated,
)

_T0 = "2026-08-21T18:00:00+00:00"
_T1 = "2026-08-21T19:30:00+00:00"
_T2 = "2026-08-21T20:00:00+00:00"

_COLUMNS = [
    "source",
    "series_id",
    "series_name",
    "period",
    "value",
    "unit",
    "region",
    "ingested_at",
]


def _row(series_id: str, period: str, value: float, ingested_at: str) -> dict[str, Any]:
    return {
        "source": "test",
        "series_id": series_id,
        "series_name": f"name {series_id}",
        "period": period,
        "value": value,
        "unit": "Dth/d",
        "region": "US",
        "ingested_at": ingested_at,
    }


def _frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=_COLUMNS)


def test_first_run_empty(tmp_path: Path) -> None:
    """Curated parquet absent → merged equals the new rows, sorted."""
    curated = tmp_path / "curated" / "history.parquet"
    new = _frame(
        [
            _row("s_b", "2026-08-21", 1.0, _T0),
            _row("s_a", "2026-08-21", 2.0, _T0),
            _row("s_a", "2026-08-20", 3.0, _T0),
        ]
    )

    merged = merge_into_curated(new, curated)

    assert len(merged) == 3
    assert list(zip(merged["period"], merged["series_id"], strict=True)) == [
        ("2026-08-20", "s_a"),
        ("2026-08-21", "s_a"),
        ("2026-08-21", "s_b"),
    ]
    assert pd.read_parquet(curated).equals(merged)


def test_append_new_day(tmp_path: Path) -> None:
    """New gas day rows are appended; existing history preserved verbatim."""
    curated = tmp_path / "history.parquet"
    merge_into_curated(_frame([_row("s1", "2026-08-20", 10.0, _T1)]), curated)

    day_b = _frame(
        [
            _row("s1", "2026-08-21", 11.0, _T2),
            _row("s2", "2026-08-21", 12.0, _T2),
        ]
    )
    merged = merge_into_curated(day_b, curated)

    assert len(merged) == 3
    on_disk = pd.read_parquet(curated)
    assert len(on_disk) == 3
    old = on_disk[(on_disk["series_id"] == "s1") & (on_disk["period"] == "2026-08-20")]
    assert old.iloc[0]["value"] == 10.0
    assert old.iloc[0]["ingested_at"] == _T1


def test_rescrape_updates_not_duplicates(tmp_path: Path) -> None:
    """A re-scrape of the same key replaces the older row (max ingested_at wins)."""
    curated = tmp_path / "history.parquet"
    merge_into_curated(
        _frame([_row("X", "2026-08-21", 100.0, "2026-08-21T19:30:00+00:00")]),
        curated,
    )

    newer = _frame([_row("X", "2026-08-21", 200.0, "2026-08-21T20:00:00+00:00")])
    merged = merge_into_curated(newer, curated)

    assert len(merged) == 1
    assert merged.iloc[0]["value"] == 200.0
    assert len(pd.read_parquet(curated)) == 1


def test_stale_rescrape_keeps_newer_history(tmp_path: Path) -> None:
    """An older re-scrape arriving after a newer row does not regress history."""
    curated = tmp_path / "history.parquet"
    merge_into_curated(
        _frame([_row("X", "2026-08-21", 200.0, "2026-08-21T20:00:00+00:00")]),
        curated,
    )

    stale = _frame([_row("X", "2026-08-21", 100.0, "2026-08-21T19:30:00+00:00")])
    merged = merge_into_curated(stale, curated)

    assert len(merged) == 1
    assert merged.iloc[0]["value"] == 200.0


def test_shrinkage_guard_raises(tmp_path: Path) -> None:
    """A merge that would drop history raises and leaves the file untouched."""
    curated = tmp_path / "history.parquet"
    # Legacy-style file written outside this layer: 10 rows holding two
    # versions for each of 5 keys.
    rows = [_row("X", f"2026-08-{10 + i:02d}", float(i), t) for i in range(5) for t in (_T0, _T1)]
    assert len(rows) == 10
    curated.parent.mkdir(parents=True, exist_ok=True)
    _frame(rows).to_parquet(curated, engine="pyarrow")
    assert len(pd.read_parquet(curated)) == 10

    # Re-scrape covering only 4 of the 5 known keys, no new keys.
    partial_keys = [(r["series_id"], r["period"]) for r in rows[::2]][:4]
    partial = _frame([_row(sid, per, 999.0, _T2) for sid, per in partial_keys])
    with pytest.raises(AccumulationShrinkError, match=r"5 rows vs 10 existing rows"):
        merge_into_curated(partial, curated)

    assert len(pd.read_parquet(curated)) == 10


def test_custom_key_cols(tmp_path: Path) -> None:
    """key_cols=("a", "b") dedupes on that pair instead of the default keys."""

    def custom_row(a: str, b: str, value: float, ingested_at: str) -> dict[str, Any]:
        return {
            "a": a,
            "b": b,
            "source": "test",
            "series_id": f"{a}_{b}",
            "series_name": f"name {a}{b}",
            "period": "2026-08-21",
            "value": value,
            "unit": "Dth/d",
            "region": "US",
            "ingested_at": ingested_at,
        }

    columns = ["a", "b", *_COLUMNS]
    curated = tmp_path / "custom.parquet"
    first = pd.DataFrame(
        [
            custom_row("a1", "b1", 1.0, _T1),
            custom_row("a1", "b2", 2.0, _T1),
        ],
        columns=columns,
    )
    merge_into_curated(first, curated, key_cols=("a", "b"))

    second = pd.DataFrame([custom_row("a1", "b1", 111.0, _T2)], columns=columns)
    merged = merge_into_curated(second, curated, key_cols=("a", "b"))

    assert len(merged) == 2
    b1 = merged[merged["a"] == "a1"][merged["b"] == "b1"]
    assert b1.iloc[0]["value"] == 111.0
    b2 = merged[merged["a"] == "a1"][merged["b"] == "b2"]
    assert b2.iloc[0]["value"] == 2.0


def test_schema_drift_raises_and_leaves_file_untouched(tmp_path: Path) -> None:
    """An incoming frame whose columns differ from history raises SchemaDriftError."""
    curated = tmp_path / "drift.parquet"
    merge_into_curated(_frame([_row("s1", "2026-08-20", 10.0, _T1)]), curated)

    # Same rows, but the scraper emitted "quantity" instead of "value".
    drifted = _frame([_row("s1", "2026-08-21", 11.0, _T2)]).rename(columns={"value": "quantity"})
    with pytest.raises(SchemaDriftError, match=r"unexpected \['quantity'\], missing \['value'\]"):
        merge_into_curated(drifted, curated)

    on_disk = pd.read_parquet(curated)
    assert len(on_disk) == 1
    assert list(on_disk.columns) == _COLUMNS


def test_schema_drift_extra_column_raises(tmp_path: Path) -> None:
    """An extra unexpected column is drift too — not a silent concat."""
    curated = tmp_path / "extra.parquet"
    merge_into_curated(_frame([_row("s1", "2026-08-20", 10.0, _T1)]), curated)

    extra = _frame([_row("s1", "2026-08-21", 11.0, _T2)])
    extra["bonus"] = 1.0
    with pytest.raises(SchemaDriftError, match=r"unexpected \['bonus'\], missing \[\]"):
        merge_into_curated(extra, curated)
    assert len(pd.read_parquet(curated)) == 1


def test_mixed_offset_ingested_at_dedupes_chronologically(tmp_path: Path) -> None:
    """Dedup compares parsed instants, not raw strings, across UTC offsets."""
    curated = tmp_path / "offsets.parquet"

    # Existing: 18:30 UTC. Incoming stamp is lexically LARGER ("20" > "18")
    # but chronologically EARLIER (20:00+05:30 == 14:30 UTC). The stale
    # lexicographic comparison would have kept the incoming row.
    merge_into_curated(
        _frame([_row("X", "2026-08-21", 100.0, "2026-08-21T18:30:00+00:00")]),
        curated,
    )
    merged = merge_into_curated(
        _frame([_row("X", "2026-08-21", 200.0, "2026-08-21T20:00:00+05:30")]),
        curated,
    )
    assert len(merged) == 1
    assert merged.iloc[0]["value"] == 100.0

    # Reverse direction: existing 18:30+05:30 (== 13:00 UTC); incoming
    # 14:00+00:00 is lexically SMALLER ("14" < "18") but chronologically
    # LATER, so it must win.
    curated2 = tmp_path / "offsets2.parquet"
    merge_into_curated(
        _frame([_row("Y", "2026-08-21", 100.0, "2026-08-21T18:30:00+05:30")]),
        curated2,
    )
    merged2 = merge_into_curated(
        _frame([_row("Y", "2026-08-21", 200.0, "2026-08-21T14:00:00+00:00")]),
        curated2,
    )
    assert len(merged2) == 1
    assert merged2.iloc[0]["value"] == 200.0
