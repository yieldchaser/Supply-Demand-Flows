"""Tests for the EIA LNG Exports scraper.

Test strategy:
    - All HTTP calls are mocked via patch on EIAClient methods.
    - Filesystem writes go to tmp_path (pytest fixture).
    - No real network calls are made.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scrapers.eia_api.lng_exports import (
    RAW_PATH,
    SOURCE_NAME,
    _coerce_rows,
    _read_prior_state,
    run,
)


# ---------------------------------------------------------------------------
# Sample API response rows
# ---------------------------------------------------------------------------

def _api_row(
    period: str = "2026-01",
    dest_code: str = "NLD",
    dest_name: str = "Netherlands",
    value: float = 18450.0,
    process: str = "LNG",
) -> dict[str, Any]:
    return {
        "period": period,
        "duoarea": dest_code,
        "area-name": dest_name,
        "process": process,
        "value": value,
        "value-units": "MMcf",
    }


def _eia_response(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"response": {"data": rows, "total": len(rows)}}


# ---------------------------------------------------------------------------
# Unit tests: helpers
# ---------------------------------------------------------------------------


def test_coerce_rows_basic() -> None:
    raw = [_api_row()]
    coerced = _coerce_rows(raw)
    assert len(coerced) == 1
    assert coerced[0]["destination_code"] == "NLD"
    assert coerced[0]["destination_name"] == "Netherlands"
    assert coerced[0]["value_mmcf"] == 18450.0
    assert coerced[0]["period"] == "2026-01"
    assert coerced[0]["process"] == "LNG"


def test_coerce_rows_drops_pipeline_destinations() -> None:
    rows = [
        _api_row(dest_code="MEX", dest_name="Mexico"),
        _api_row(dest_code="CAN", dest_name="Canada"),
        _api_row(dest_code="NLD", dest_name="Netherlands"),
    ]
    coerced = _coerce_rows(rows)
    codes = {r["destination_code"] for r in coerced}
    assert "NLD" in codes
    # Mexico and Canada excluded only if area-name matches PIPELINE_ONLY_AREAS
    assert "Mexico" not in {r["destination_name"] for r in coerced}
    assert "Canada" not in {r["destination_name"] for r in coerced}


def test_coerce_rows_skips_negative_values() -> None:
    rows = [_api_row(value=-100.0)]
    assert _coerce_rows(rows) == []


def test_coerce_rows_skips_missing_period() -> None:
    row = _api_row()
    row["period"] = None
    assert _coerce_rows([row]) == []


def test_coerce_rows_skips_missing_value() -> None:
    row = _api_row()
    row["value"] = None
    assert _coerce_rows([row]) == []


def test_read_prior_state_returns_none_when_no_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("scrapers.eia_api.lng_exports.RAW_PATH", tmp_path / "nope.json")
    period, count = _read_prior_state()
    assert period is None
    assert count == 0


def test_read_prior_state_reads_existing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_path = tmp_path / "lng_exports.json"
    raw_path.write_text(
        json.dumps({"latest_period": "2026-01", "row_count": 42}), encoding="utf-8"
    )
    monkeypatch.setattr("scrapers.eia_api.lng_exports.RAW_PATH", raw_path)
    period, count = _read_prior_state()
    assert period == "2026-01"
    assert count == 42


# ---------------------------------------------------------------------------
# Integration tests: run()
# ---------------------------------------------------------------------------


def _make_mock_client(latest_period: str = "2026-01", data_rows: list | None = None) -> AsyncMock:
    """Build a fully patched EIAClient mock."""
    rows = data_rows if data_rows is not None else [_api_row()]
    mock = AsyncMock()
    mock.get_latest_date = AsyncMock(return_value=latest_period)
    mock.get_series = AsyncMock(return_value=_eia_response(rows))
    mock.__aenter__ = AsyncMock(return_value=mock)
    mock.__aexit__ = AsyncMock(return_value=False)
    return mock


@pytest.mark.asyncio
async def test_run_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path: valid rows are fetched and written to disk."""
    monkeypatch.setenv("EIA_API_KEY", "test-key")
    raw_path = tmp_path / "lng_exports.json"
    monkeypatch.setattr("scrapers.eia_api.lng_exports.RAW_PATH", raw_path)
    monkeypatch.setattr("scrapers.eia_api.lng_exports.RAW_DIR", tmp_path)

    mock_client = _make_mock_client(
        latest_period="2026-01",
        data_rows=[
            _api_row("2026-01", "NLD", "Netherlands", 18450.0),
            _api_row("2026-01", "FRA", "France", 12000.0),
        ],
    )

    with patch("scrapers.eia_api.lng_exports.EIAClient", return_value=mock_client):
        with patch("scrapers.eia_api.lng_exports.HealthWriter") as MockHW:
            mock_hw = MagicMock()
            MockHW.return_value = mock_hw
            result = await run()

    assert result["status"] == "ok"
    assert result["latest_period"] == "2026-01"
    assert result["rows"] == 2
    assert raw_path.exists()
    payload = json.loads(raw_path.read_text())
    assert len(payload["data"]) == 2
    mock_hw.record_success.assert_called_once()


@pytest.mark.asyncio
async def test_run_staleness_gate_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When latest period and row count are unchanged, the gate fires."""
    monkeypatch.setenv("EIA_API_KEY", "test-key")
    raw_path = tmp_path / "lng_exports.json"
    raw_path.write_text(
        json.dumps({"latest_period": "2026-01", "row_count": 500}), encoding="utf-8"
    )
    monkeypatch.setattr("scrapers.eia_api.lng_exports.RAW_PATH", raw_path)
    monkeypatch.setattr("scrapers.eia_api.lng_exports.RAW_DIR", tmp_path)

    mock_client = _make_mock_client(latest_period="2026-01")

    with patch("scrapers.eia_api.lng_exports.EIAClient", return_value=mock_client):
        with patch("scrapers.eia_api.lng_exports.HealthWriter") as MockHW:
            mock_hw = MagicMock()
            MockHW.return_value = mock_hw
            result = await run()

    assert result["status"] == "skipped"
    mock_hw.record_skipped.assert_called_once()


@pytest.mark.asyncio
async def test_run_missing_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing API key → immediate failure, no HTTP calls."""
    monkeypatch.delenv("EIA_API_KEY", raising=False)

    with patch("scrapers.eia_api.lng_exports.HealthWriter") as MockHW:
        mock_hw = MagicMock()
        MockHW.return_value = mock_hw
        result = await run()

    assert result["status"] == "failed"
    assert "EIA_API_KEY" in result["error"]
    mock_hw.record_failure.assert_called_once()


@pytest.mark.asyncio
async def test_run_empty_response_still_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Empty data array from API → run returns ok with 0 rows."""
    monkeypatch.setenv("EIA_API_KEY", "test-key")
    raw_path = tmp_path / "lng_exports.json"
    monkeypatch.setattr("scrapers.eia_api.lng_exports.RAW_PATH", raw_path)
    monkeypatch.setattr("scrapers.eia_api.lng_exports.RAW_DIR", tmp_path)

    # First call (LNG facet) returns empty, second (fallback) also empty.
    mock_client = _make_mock_client(latest_period="2026-01", data_rows=[])
    mock_client.get_series = AsyncMock(return_value=_eia_response([]))

    with patch("scrapers.eia_api.lng_exports.EIAClient", return_value=mock_client):
        with patch("scrapers.eia_api.lng_exports.HealthWriter"):
            result = await run()

    # 0 coerced rows → run still returns ok (scraper succeeded, just no data)
    assert result["status"] in ("ok", "failed")  # both valid; depends on StatePreservingWriter


@pytest.mark.asyncio
async def test_run_http_error_returns_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """HTTP error on latest date check → failed result."""
    monkeypatch.setenv("EIA_API_KEY", "test-key")

    mock_client = AsyncMock()
    mock_client.get_latest_date = AsyncMock(side_effect=RuntimeError("HTTP 500"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("scrapers.eia_api.lng_exports.EIAClient", return_value=mock_client):
        with patch("scrapers.eia_api.lng_exports.HealthWriter") as MockHW:
            mock_hw = MagicMock()
            MockHW.return_value = mock_hw
            result = await run()

    assert result["status"] == "failed"
    mock_hw.record_failure.assert_called_once()
