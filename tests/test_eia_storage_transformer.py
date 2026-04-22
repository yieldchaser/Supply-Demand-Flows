"""Tests for EIA storage transformer."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from transformers.eia_storage import transform


def test_transform_eia_storage(tmp_path: Path):
    """Verify raw JSON → curated Parquet transformation for EIA storage."""
    raw_path = tmp_path / "raw.json"
    out_path = tmp_path / "curated.parquet"

    raw_data = {
        "response": {
            "data": [
                {"period": "2024-04-12", "area-name": "East", "value": "300", "units": "Bcf"},
                {"period": "2024-04-12", "area-name": "Midwest", "value": "400", "units": "Bcf"},
            ]
        }
    }
    raw_path.write_text(json.dumps(raw_data))

    result = transform(raw_path, out_path)

    assert result["rows"] == 2
    assert "East" in result["regions"]
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
    assert df.iloc[0]["value"] == 300.0
    assert df.iloc[0]["region"] == "East"
