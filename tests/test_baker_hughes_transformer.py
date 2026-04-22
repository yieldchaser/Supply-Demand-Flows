"""Tests for Baker Hughes transformer."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from transformers.baker_hughes import transform


def test_transform_baker_hughes(tmp_path: Path):
    """Verify Excel → curated Parquet transformation for Baker Hughes."""
    raw_path = tmp_path / "raw.xlsx"
    out_path = tmp_path / "curated.parquet"

    # We mock load_workbook because creating a real .xlsx is heavy
    with patch("transformers.baker_hughes.load_workbook") as mock_load:
        mock_wb = MagicMock()
        mock_wb.sheetnames = ["US Oil & Gas Split", "Canada Oil & Gas Split"]

        # Mock US sheet
        mock_us_ws = MagicMock()
        mock_us_ws.iter_rows.return_value = [
            [
                MagicMock(value="Date"),
                MagicMock(value="Oil"),
                MagicMock(value="Gas"),
                MagicMock(value="Total"),
            ],
            [
                MagicMock(value=datetime(2024, 4, 12)),
                MagicMock(value=500),
                MagicMock(value=100),
                MagicMock(value=600),
            ],
        ]

        # Mock Canada sheet
        mock_ca_ws = MagicMock()
        mock_ca_ws.iter_rows.return_value = [
            [
                MagicMock(value="Date"),
                MagicMock(value="Oil"),
                MagicMock(value="Gas"),
                MagicMock(value="Total"),
            ],
            [
                MagicMock(value=datetime(2024, 4, 12)),
                MagicMock(value=100),
                MagicMock(value=20),
                MagicMock(value=120),
            ],
        ]

        mock_wb.__getitem__.side_effect = lambda name: mock_us_ws if "US" in name else mock_ca_ws
        mock_load.return_value = mock_wb

        result = transform(raw_path, out_path)

        assert result["rows"] > 0
        assert "US" in result["regions"]
        assert "CANADA" in result["regions"]
        assert out_path.exists()

        df = pd.read_parquet(out_path)
        assert "us_oil" in df["series_id"].values
        assert "canada_oil" in df["series_id"].values
