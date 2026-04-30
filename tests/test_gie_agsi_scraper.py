"""Tests for the GIE AGSI+ European storage scraper.

Test strategy:
    - All HTTP calls are mocked via pytest monkeypatch on HttpClient.get_json.
    - Filesystem writes go to tmp_path (pytest fixture).
    - No real network calls are made.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scrapers.gie_agsi.european_storage import (
    COUNTRIES,
    RAW_PATH,
    SOURCE_NAME,
    _load_api_key,
    _normalise_row,
    _read_prior_state,
    run,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

SAMPLE_ROW: dict[str, Any] = {
    "name": "DE",
    "code": "DE",
    "url": "/germany",
    "status": "E",
    "gasDayStart": "2026-04-24",
    "gasInStorage": 76.32,
    "consumption": None,
    "consumptionFull": None,
    "injection": 184.5,
    "withdrawal": 12.3,
    "workingGasVolume": 245.81,
    "injectionCapacity": 2475.6,
    "withdrawalCapacity": 2870.4,
    "trend": 0.65,
    "full": 31.05,
    "info": None,
}


def _single_page_response(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap rows in a single-page AGSI response envelope."""
    return {
        "data": rows,
        "last_page": 1,
        "current_page": 1,
        "total": len(rows),
    }


def _two_page_response(rows: list[dict[str, Any]], page: int) -> dict[str, Any]:
    """Return page 1 or 2 of a two-page AGSI response."""
    half = len(rows) // 2
    page_rows = rows[:half] if page == 1 else rows[half:]
    return {
        "data": page_rows,
        "last_page": 2,
        "current_page": page,
        "total": len(rows),
    }


# ---------------------------------------------------------------------------
# Unit tests: helpers
# ---------------------------------------------------------------------------


def test_normalise_row_maps_fields() -> None:
    norm = _normalise_row(SAMPLE_ROW)
    assert norm["country_code"] == "DE"
    assert norm["gas_day"] == "2026-04-24"
    assert norm["gas_in_storage_twh"] == 76.32
    assert norm["injection_gwh"] == 184.5
    assert norm["withdrawal_gwh"] == 12.3
    assert norm["full_pct"] == 31.05
    assert norm["trend_twh"] == 0.65
    assert norm["status"] == "E"


def test_normalise_row_handles_nulls() -> None:
    row = {**SAMPLE_ROW, "injection": None, "withdrawal": None}
    norm = _normalise_row(row)
    assert norm["injection_gwh"] is None
    assert norm["withdrawal_gwh"] is None


def test_read_prior_state_returns_none_when_no_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "scrapers.gie_agsi.european_storage.RAW_PATH",
        tmp_path / "does_not_exist.json",
    )
    gas_day, count = _read_prior_state()
    assert gas_day is None
    assert count == 0


def test_load_api_key_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GIE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="GIE_API_KEY"):
        _load_api_key()


def test_load_api_key_returns_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GIE_API_KEY", "test-key-xyz")
    assert _load_api_key() == "test-key-xyz"


# ---------------------------------------------------------------------------
# Integration tests: run()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_happy_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path: every country returns one row; file is written."""
    monkeypatch.setenv("GIE_API_KEY", "test-key")
    raw_path = tmp_path / "european_storage.json"
    health_dir = tmp_path / "health"
    health_dir.mkdir()

    monkeypatch.setattr("scrapers.gie_agsi.european_storage.RAW_PATH", raw_path)

    # Patch HealthWriter to use tmp health dir
    with patch("scrapers.gie_agsi.european_storage.HealthWriter") as MockHW:
        mock_hw = MagicMock()
        MockHW.return_value = mock_hw

        # Patch HttpClient.get_json to return one row for every country
        with patch(
            "scrapers.gie_agsi.european_storage.HttpClient"
        ) as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.get_json = AsyncMock(
                return_value=_single_page_response([SAMPLE_ROW])
            )
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await run()

    assert result["status"] == "ok"
    assert result["latest_gas_day"] == "2026-04-24"
    assert result["row_count"] == len(COUNTRIES)
    assert raw_path.exists()
    payload = json.loads(raw_path.read_text())
    assert payload["latest_gas_day"] == "2026-04-24"
    assert len(payload["data"]) == len(COUNTRIES)
    mock_hw.record_success.assert_called_once()


@pytest.mark.asyncio
async def test_run_empty_response_skips_country(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When a country returns an empty data array, it's silently skipped."""
    monkeypatch.setenv("GIE_API_KEY", "test-key")
    raw_path = tmp_path / "european_storage.json"
    monkeypatch.setattr("scrapers.gie_agsi.european_storage.RAW_PATH", raw_path)

    # EU returns data; everyone else returns empty
    call_count = 0

    async def _mock_get_json(url: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        country = (params or {}).get("country", "")
        if country == "EU":
            return _single_page_response([{**SAMPLE_ROW, "code": "EU", "name": "EU"}])
        return _single_page_response([])

    with patch("scrapers.gie_agsi.european_storage.HealthWriter"):
        with patch("scrapers.gie_agsi.european_storage.HttpClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.get_json = _mock_get_json
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await run()

    assert result["status"] == "ok"
    assert result["row_count"] == 1
    # All countries returned HTTP 200 (even with empty data[]), so all are "ok"
    assert "EU" in result["countries_ok"]
    assert result["countries_failed"] == []


@pytest.mark.asyncio
async def test_run_bad_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing API key → failed immediately, no HTTP calls."""
    monkeypatch.delenv("GIE_API_KEY", raising=False)

    with patch("scrapers.gie_agsi.european_storage.HealthWriter") as MockHW:
        mock_hw = MagicMock()
        MockHW.return_value = mock_hw

        result = await run()

    assert result["status"] == "failed"
    assert "GIE_API_KEY" in result["error"]
    mock_hw.record_failure.assert_called_once()


@pytest.mark.asyncio
async def test_run_staleness_gate_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Staleness gate fires when latest_gas_day + row count are unchanged."""
    monkeypatch.setenv("GIE_API_KEY", "test-key")
    raw_path = tmp_path / "european_storage.json"

    # Pre-write a prior state with 11 rows and a known gas_day
    prior_payload: dict[str, Any] = {
        "fetched_at": "2026-04-24T09:00:00Z",
        "start_date": "2021-01-01",
        "end_date": "2026-04-24",
        "latest_gas_day": "2026-04-24",
        "countries": COUNTRIES,
        "row_count": 11,
        "data": [{**_normalise_row(SAMPLE_ROW), "country_code": c} for c in COUNTRIES],
    }
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(prior_payload), encoding="utf-8")

    monkeypatch.setattr("scrapers.gie_agsi.european_storage.RAW_PATH", raw_path)

    with patch("scrapers.gie_agsi.european_storage.HealthWriter") as MockHW:
        mock_hw = MagicMock()
        MockHW.return_value = mock_hw

        # API returns the same gas_day + same row count
        with patch("scrapers.gie_agsi.european_storage.HttpClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.get_json = AsyncMock(
                return_value=_single_page_response([SAMPLE_ROW])
            )
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await run()

    assert result["status"] == "skipped"
    mock_hw.record_skipped.assert_called_once()


@pytest.mark.asyncio
async def test_run_pagination_makes_two_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When last_page=2, the scraper makes two page requests per country."""
    monkeypatch.setenv("GIE_API_KEY", "test-key")
    raw_path = tmp_path / "european_storage.json"
    monkeypatch.setattr("scrapers.gie_agsi.european_storage.RAW_PATH", raw_path)

    rows = [
        {**SAMPLE_ROW, "gasDayStart": f"2026-04-{day:02d}"}
        for day in range(1, 3)  # 2 rows → split across 2 pages
    ]
    page_calls: dict[str, int] = {}

    async def _paginated_get(
        url: str, params: dict[str, str] | None = None
    ) -> dict[str, Any]:
        country = (params or {}).get("country", "UNKNOWN")
        page = int((params or {}).get("page", 1))
        page_calls.setdefault(country, 0)
        page_calls[country] += 1
        # Only DE returns two pages; others return single empty page
        if country == "DE":
            return _two_page_response(rows, page)
        return _single_page_response([])

    with patch("scrapers.gie_agsi.european_storage.HealthWriter"):
        with patch("scrapers.gie_agsi.european_storage.HttpClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.get_json = _paginated_get
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await run()

    assert page_calls.get("DE", 0) == 2, "DE should have triggered two page requests"
    assert result["status"] == "ok"
    assert result["row_count"] == len(rows)
