"""Tests for the GIE AGSI+ transformer.

Test strategy:
    - Construct synthetic raw JSON in tmp_path.
    - Call transform() with explicit path arguments.
    - Assert output Parquet has expected shape and values.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from transformers.errors import TransformError
from transformers.gie_agsi import SERIES_DEFINITIONS, transform


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

SAMPLE_RAW_ROW: dict[str, Any] = {
    "country_code": "DE",
    "gas_day": "2026-04-24",
    "gas_in_storage_twh": 76.32,
    "injection_gwh": 184.5,
    "withdrawal_gwh": 12.3,
    "working_gas_volume_twh": 245.81,
    "full_pct": 31.05,
    "trend_twh": 0.65,
    "status": "E",
    "injection_capacity_gwh": 2475.6,
    "withdrawal_capacity_gwh": 2870.4,
}


def _write_raw(path: Path, rows: list[dict[str, Any]]) -> None:
    payload: dict[str, Any] = {
        "fetched_at": "2026-04-25T09:00:00Z",
        "start_date": "2021-01-01",
        "end_date": "2026-04-25",
        "latest_gas_day": "2026-04-24",
        "countries": ["DE", "FR"],
        "row_count": len(rows),
        "data": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_transform_happy_path(tmp_path: Path) -> None:
    """Two countries × one gas_day → 2 × 4 = 8 output rows."""
    raw_path = tmp_path / "european_storage.json"
    out_path = tmp_path / "gie_agsi.parquet"

    rows = [
        SAMPLE_RAW_ROW,
        {**SAMPLE_RAW_ROW, "country_code": "FR", "gas_day": "2026-04-24"},
    ]
    _write_raw(raw_path, rows)

    result = transform(raw_json_path=raw_path, curated_parquet_path=out_path)

    assert result["rows"] == 8  # 2 countries × 4 metrics
    assert result["series_count"] == 8
    assert out_path.exists()

    df = pd.read_parquet(out_path)
    assert len(df) == 8
    assert set(df["source"].unique()) == {"gie_agsi"}
    assert set(df["region"].unique()) == {"DE", "FR"}


def test_transform_schema_correctness(tmp_path: Path) -> None:
    """Output Parquet has the canonical long-format schema columns."""
    raw_path = tmp_path / "european_storage.json"
    out_path = tmp_path / "gie_agsi.parquet"
    _write_raw(raw_path, [SAMPLE_RAW_ROW])

    transform(raw_json_path=raw_path, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    expected_cols = {"source", "series_id", "series_name", "period", "value", "unit", "region", "ingested_at"}
    assert expected_cols.issubset(set(df.columns))


def test_transform_correct_series_ids(tmp_path: Path) -> None:
    """Series IDs follow the gie_storage_{cc}_{metric} pattern."""
    raw_path = tmp_path / "european_storage.json"
    out_path = tmp_path / "gie_agsi.parquet"
    _write_raw(raw_path, [SAMPLE_RAW_ROW])

    transform(raw_json_path=raw_path, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    series_ids = set(df["series_id"].tolist())

    expected = {f"gie_storage_de_{suffix}" for suffix, *_ in SERIES_DEFINITIONS}
    assert expected == series_ids


def test_transform_null_field_skipped(tmp_path: Path) -> None:
    """A null field in the raw data is skipped; other metrics for the same row are kept."""
    raw_path = tmp_path / "european_storage.json"
    out_path = tmp_path / "gie_agsi.parquet"

    # injection_gwh is None → only 3 output rows instead of 4
    row = {**SAMPLE_RAW_ROW, "injection_gwh": None}
    _write_raw(raw_path, [row])

    result = transform(raw_json_path=raw_path, curated_parquet_path=out_path)

    assert result["rows"] == 3  # 4 metrics − 1 null = 3

    df = pd.read_parquet(out_path)
    assert "gie_storage_de_injection" not in df["series_id"].values


def test_transform_all_null_fields_produces_zero_rows(tmp_path: Path) -> None:
    """If ALL tracked fields are null for all rows, TransformError is raised."""
    raw_path = tmp_path / "european_storage.json"
    out_path = tmp_path / "gie_agsi.parquet"

    row = {
        **SAMPLE_RAW_ROW,
        "gas_in_storage_twh": None,
        "full_pct": None,
        "injection_gwh": None,
        "withdrawal_gwh": None,
    }
    _write_raw(raw_path, [row])

    with pytest.raises(TransformError, match="zero rows"):
        transform(raw_json_path=raw_path, curated_parquet_path=out_path)


def test_transform_missing_raw_file_raises(tmp_path: Path) -> None:
    """Missing raw JSON → TransformError with helpful message."""
    raw_path = tmp_path / "does_not_exist.json"
    out_path = tmp_path / "gie_agsi.parquet"

    with pytest.raises(TransformError, match="Raw JSON not found"):
        transform(raw_json_path=raw_path, curated_parquet_path=out_path)


def test_transform_multiple_countries_multiple_days(tmp_path: Path) -> None:
    """Larger fixture: 3 countries × 5 gas_days → 3 × 5 × 4 = 60 rows."""
    raw_path = tmp_path / "european_storage.json"
    out_path = tmp_path / "gie_agsi.parquet"

    rows: list[dict[str, Any]] = []
    for cc in ["DE", "FR", "IT"]:
        for day in range(1, 6):
            rows.append({**SAMPLE_RAW_ROW, "country_code": cc, "gas_day": f"2026-04-{day:02d}"})

    _write_raw(raw_path, rows)
    result = transform(raw_json_path=raw_path, curated_parquet_path=out_path)

    assert result["rows"] == 60
    df = pd.read_parquet(out_path)
    assert df["region"].nunique() == 3
    assert df["period"].nunique() == 5
    # Each country × each metric should appear 5 times (one per day)
    de_storage = df[df["series_id"] == "gie_storage_de_gas_in_storage"]
    assert len(de_storage) == 5


def test_transform_value_types_are_float(tmp_path: Path) -> None:
    """All values in the output Parquet are floats (not ints or strings)."""
    raw_path = tmp_path / "european_storage.json"
    out_path = tmp_path / "gie_agsi.parquet"
    _write_raw(raw_path, [SAMPLE_RAW_ROW])

    transform(raw_json_path=raw_path, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    assert df["value"].dtype == float
