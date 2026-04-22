"""Tests for event detectors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from publishers.events import detect_rig_reversal, detect_storage_print


@pytest.fixture
def mock_curated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Setup mock curated directory."""
    curated_dir = tmp_path / "data" / "curated"
    curated_dir.mkdir(parents=True)

    import publishers.events as events

    monkeypatch.setattr(events, "CURATED_DIR", curated_dir)
    return curated_dir


def test_storage_print_detector_returns_none_when_file_missing(mock_curated):
    """Should return None if Parquet doesn't exist."""
    assert detect_storage_print() is None


def test_storage_print_detector_computes_delta(mock_curated):
    """Verify delta and body formatting for EIA storage."""
    df = pd.DataFrame(
        [
            {"period": "2026-04-10", "region": "Lower 48", "value": 3000.0, "source": "eia"},
            {"period": "2026-04-17", "region": "Lower 48", "value": 3042.0, "source": "eia"},
        ]
    )
    df.to_parquet(mock_curated / "eia_storage.parquet")

    result = detect_storage_print()
    assert result is not None
    assert result["dedup_key"] == "storage_print_2026-04-17"
    assert "Net injection: <code>+42 Bcf</code>" in result["html_body"]
    assert "Total: <code>3,042 Bcf</code>" in result["html_body"]


def test_rig_reversal_detector_fires_on_big_move(mock_curated):
    """Verify rig reversal fires on WoW move >= 20."""
    df = pd.DataFrame(
        [
            {
                "period": "2026-04-10",
                "series_id": "bh_rollup_us_total",
                "value": 600.0,
                "region": "US",
            },
            {
                "period": "2026-04-17",
                "series_id": "bh_rollup_us_total",
                "value": 575.0,
                "region": "US",
            },
            # Add some basin rows for the breakdown
            {
                "period": "2026-04-10",
                "series_id": "bh_rollup_basin_permian",
                "value": 300.0,
                "region": "Permian",
            },
            {
                "period": "2026-04-17",
                "series_id": "bh_rollup_basin_permian",
                "value": 285.0,
                "region": "Permian",
            },
        ]
    )
    df.to_parquet(mock_curated / "baker_hughes_weekly.parquet")

    result = detect_rig_reversal(threshold_rigs=20)
    assert result is not None
    assert "US Total: <code>575 rigs</code> (<code>-25</code> WoW)" in result["html_body"]
    assert "Permian: <code>-15</code>" in result["html_body"]


def test_rig_reversal_detector_quiet_on_small_move(mock_curated):
    """Verify rig reversal stays quiet on move < threshold."""
    df = pd.DataFrame(
        [
            {
                "period": "2026-04-10",
                "series_id": "bh_rollup_us_total",
                "value": 600.0,
                "region": "US",
            },
            {
                "period": "2026-04-17",
                "series_id": "bh_rollup_us_total",
                "value": 605.0,
                "region": "US",
            },
        ]
    )
    df.to_parquet(mock_curated / "baker_hughes_weekly.parquet")

    result = detect_rig_reversal(threshold_rigs=20)
    assert result is None
