"""Tests for EIA storage transformer."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from transformers.eia_storage import transform


def _raw_row(**overrides: str) -> dict:
    row = {
        "period": "2024-04-12",
        "duoarea": "R31",
        "area-name": "NA",  # the API's area-name is literally "NA" for every row
        "product": "ng",
        "product-name": "Natural Gas",
        "process": "SWO",
        "process-name": "Working Underground Storage",
        "series": "NW2_EPG0_SWO_R31_BCF",
        "series-description": "Weekly East Region Natural Gas Working Underground Storage (Billion Cubic Feet)",
        "value": "300",
        "units": "Bcf",
    }
    row.update(overrides)
    return row


def test_transform_eia_storage(tmp_path: Path):
    """Verify raw JSON → curated Parquet transformation for EIA storage."""
    raw_path = tmp_path / "raw.json"
    out_path = tmp_path / "curated.parquet"

    raw_data = {
        "response": {
            "data": [
                _raw_row(),
                _raw_row(
                    duoarea="R32",
                    series="NW2_EPG0_SWO_R32_BCF",
                    **{
                        "series-description": (
                            "Weekly Midwest Region Natural Gas Working Underground "
                            "Storage (Billion Cubic Feet)"
                        )
                    },
                    value="400",
                ),
            ]
        }
    }
    raw_path.write_text(json.dumps(raw_data))

    result = transform(raw_path, out_path)

    assert result["rows"] == 2
    assert "East" in result["regions"]
    assert "Midwest" in result["regions"]
    assert out_path.exists()

    df = pd.read_parquet(out_path)
    assert len(df) == 2
    assert list(df.columns) == [
        "source",
        "series_id",
        "series_name",
        "period",
        "value",
        "unit",
        "region",
        "ingested_at",
    ]
    east = df[df["region"] == "East"].iloc[0]
    assert east["value"] == 300.0
    assert east["series_id"] == "storage_east"


def test_region_labels_resolved_from_series_not_area_name(tmp_path: Path):
    """area-name is 'NA' on every real row; labels must come from series/duoarea."""
    raw_path = tmp_path / "raw.json"
    out_path = tmp_path / "curated.parquet"

    raw_data = {
        "response": {
            "data": [
                _raw_row(series="NW2_EPG0_SWO_R48_BCF", duoarea="R48", value="2500"),
                _raw_row(series="NW2_EPG0_SWO_R33_BCF", duoarea="R33", value="900"),
                _raw_row(series="NW2_EPG0_SNO_R33_BCF", duoarea="R33", process="SNO", value="600"),
                _raw_row(series="NW2_EPG0_SSO_R33_BCF", duoarea="R33", process="SSO", value="300"),
            ]
        }
    }
    raw_path.write_text(json.dumps(raw_data))

    result = transform(raw_path, out_path)
    regions = set(result["regions"])
    assert regions == {"Lower 48", "South Central", "South Central Nonsalt", "South Central Salt"}
    assert result["skipped_unlabeled"] == 0

    df = pd.read_parquet(out_path).set_index("series_id")
    assert df.loc["storage_lower48", "region"] == "Lower 48"
    assert df.loc["storage_south_central", "region"] == "South Central"
    assert df.loc["storage_sc_nonsalt", "region"] == "South Central Nonsalt"
    assert df.loc["storage_sc_salt", "region"] == "South Central Salt"
    # unique series_id per region — no collisions possible on merge
    assert df.index.is_unique


def test_unknown_geography_is_skipped_not_mislabeled(tmp_path: Path):
    """Rows with an unrecognized series/duoarea are counted and skipped."""
    raw_path = tmp_path / "raw.json"
    out_path = tmp_path / "curated.parquet"

    raw_data = {
        "response": {
            "data": [
                _raw_row(),
                _raw_row(series="NW2_EPG0_SWO_R99_BCF", duoarea="R99", value="111"),
            ]
        }
    }
    raw_path.write_text(json.dumps(raw_data))

    result = transform(raw_path, out_path)
    assert result["rows"] == 1
    assert result["skipped_unlabeled"] == 1
    df = pd.read_parquet(out_path)
    assert set(df["region"]) == {"East"}


@pytest.mark.parametrize(
    ("series", "expected_slug"),
    [
        ("NW2_EPG0_SWO_R48_BCF", "storage_lower48"),
        ("NW2_EPG0_SNO_R33_BCF", "storage_sc_nonsalt"),
        ("NW2_EPG0_SSO_R33_BCF", "storage_sc_salt"),
        ("NW2_EPG0_SWO_R34_BCF", "storage_mountain"),
    ],
)
def test_series_slug_stability(
    tmp_path: Path, series: str, expected_slug: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """series_id slugs are stable and collision-free per region."""
    from transformers import eia_storage as mod

    # exercise through the public transform path for one row of each series
    raw_path = tmp_path / f"raw_{expected_slug}.json"
    out_path = tmp_path / f"out_{expected_slug}.parquet"
    duo = {"storage_lower48": "R48", "storage_sc_nonsalt": "R33", "storage_sc_salt": "R33", "storage_mountain": "R34"}[expected_slug]
    process = {"storage_sc_nonsalt": "SNO", "storage_sc_salt": "SSO"}.get(expected_slug, "SWO")
    raw_data = {"response": {"data": [_raw_row(series=series, duoarea=duo, process=process)]}}
    raw_path.write_text(json.dumps(raw_data))
    transform(raw_path, out_path)
    df = pd.read_parquet(out_path)
    assert df.iloc[0]["series_id"] == expected_slug
