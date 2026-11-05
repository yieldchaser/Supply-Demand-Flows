"""Tests for event detectors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from publishers.events import detect_eu_storage_print, detect_rig_reversal, detect_storage_print


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


def test_storage_print_detector_with_salt_breakout(mock_curated):
    """Verify EIA storage print includes SC Salt caverns and TX 527 Bcf benchmark."""
    df = pd.DataFrame(
        [
            {"period": "2026-04-10", "region": "Lower 48", "value": 3000.0, "source": "eia"},
            {"period": "2026-04-17", "region": "Lower 48", "value": 3050.0, "source": "eia"},
            {"period": "2026-04-10", "region": "South Central Salt", "value": 320.0, "source": "eia"},
            {"period": "2026-04-17", "region": "South Central Salt", "value": 331.0, "source": "eia"},
        ]
    )
    df.to_parquet(mock_curated / "eia_storage.parquet")

    result = detect_storage_print()
    assert result is not None
    assert "SC Salt Caverns: <code>331 Bcf (+11 Bcf WoW)</code>" in result["html_body"]
    assert "62.8%</code> of TX 527 Bcf Nameplate" in result["html_body"]


def test_eu_storage_print_detector(mock_curated):
    """Verify daily EU storage print includes stock, net flow, and country breakdown."""
    rows = [
        {"period": "2026-11-02", "series_id": "gie_storage_de_gas_in_storage", "value": 150.0},
        {"period": "2026-11-02", "series_id": "gie_storage_de_full_pct", "value": 60.0},
        {"period": "2026-11-02", "series_id": "gie_storage_de_injection", "value": 200.0},
        {"period": "2026-11-02", "series_id": "gie_storage_de_withdrawal", "value": 1200.0},
        {"period": "2026-11-02", "series_id": "gie_storage_fr_gas_in_storage", "value": 100.0},
        {"period": "2026-11-02", "series_id": "gie_storage_fr_full_pct", "value": 80.0},
        {"period": "2026-11-02", "series_id": "gie_storage_fr_injection", "value": 50.0},
        {"period": "2026-11-02", "series_id": "gie_storage_fr_withdrawal", "value": 500.0},
    ]
    df = pd.DataFrame(rows)
    df.to_parquet(mock_curated / "gie_agsi.parquet")

    result = detect_eu_storage_print()
    assert result is not None
    assert result["dedup_key"] == "eu_storage_print_2026-11-02"
    assert "GIE AGSI+ European Gas Storage — 2026-11-02" in result["html_body"]
    assert "250.0 TWh" in result["html_body"]
    assert "-1,450 GWh/d" in result["html_body"]
    assert "Germany: <code>60.0%</code>" in result["html_body"]


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
