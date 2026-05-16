"""Tests for Gulf South Pipeline SQ scraper and transformer.

Test strategy:
    - All HTTP calls are mocked via unittest.mock.
    - Filesystem writes go to pytest tmp_path.
    - Zero live API calls.

Coverage:
    test_parse_sq_response          — transformer parses raw JSON to canonical schema
    test_unit_conversion            — Dth/d → MMcf/d using HHV_DTH_PER_MCF
    test_meter_filtering            — Freeport meters filtered via lng_meter_map
    test_health_on_failure          — HealthWriter records failure on HTTP error
    test_health_on_missing_token    — HealthWriter records failure with no token
    test_staleness_gate_skips       — existing file → status "skipped"
    test_run_happy_path             — end-to-end fetch + write
    test_transformer_dedup          — duplicate (series_id, period) keeps latest
    test_transformer_missing_dir    — TransformError on missing raw dir
    test_transformer_zero_rows      — TransformError when all rows lack required fields
    test_transformer_canonical_schema — output has exactly the 8 canonical columns
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from scrapers.energy_transfer.gulf_south import (
    DTH_PER_MMCF,
    HHV_DTH_PER_MCF,
    RAW_DIR,
    _load_api_token,
    _raw_path,
    run,
)
from transformers.errors import TransformError
from transformers.gulf_south import _dth_to_mmcfd, transform

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────────────────

GAS_DAY = date(2026, 5, 16)
CYCLE = "TIMELY"

# Sample API row matching inferred GasQuest SQ response schema
_SAMPLE_ROW: dict[str, Any] = {
    "id": 12345,
    "locationNumber": "24329",
    "locationName": "STRATTON RIDGE",
    "locationId": 987,
    "cycleCode": "TIMELY",
    "gasFlowDate": "2026-05-16",
    "scheduledQuantity": 1025000,  # 1,025,000 Dth/d == 1,000 MMcf/d exactly
    "unit": "DTH",
    "flowIndicator": "D",
    "transactionTypeCode": "FTS",
}

_SAMPLE_ROW_2: dict[str, Any] = {
    "id": 12346,
    "locationNumber": "55100",
    "locationName": "MAMOU COMPRESSOR STATION",
    "locationId": 321,
    "cycleCode": "TIMELY",
    "gasFlowDate": "2026-05-16",
    "scheduledQuantity": 512500,  # 500 MMcf/d
    "unit": "DTH",
    "flowIndicator": "R",
    "transactionTypeCode": "FTS",
}


def _api_response(rows: list[dict[str, Any]], total: int | None = None) -> dict[str, Any]:
    """Wrap rows in the expected GasQuest SQ API envelope."""
    return {
        "data": rows,
        "total": total if total is not None else len(rows),
        "pageSize": 500,
        "pageNumber": 1,
    }


def _write_raw(path: Path, rows: list[dict[str, Any]], cycle: str = CYCLE) -> None:
    """Write a synthetic raw JSON file to *path*."""
    payload: dict[str, Any] = {
        "fetched_at": "2026-05-16T12:00:00Z",
        "tsp_id": 1,
        "cycle": cycle,
        "gas_day": "2026-05-16",
        "row_count": len(rows),
        "data": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Unit tests: helpers
# ─────────────────────────────────────────────────────────────────────────────


def test_unit_conversion_exact() -> None:
    """1,025,000 Dth/d == 1,000 MMcf/d when HHV = 1.025."""
    assert _dth_to_mmcfd(1_025_000) == pytest.approx(1_000.0)


def test_unit_conversion_zero() -> None:
    assert _dth_to_mmcfd(0) == pytest.approx(0.0)


def test_hhv_constant_is_documented_value() -> None:
    """HHV constant must match the value documented in the module docstring."""
    assert HHV_DTH_PER_MCF == 1.025
    assert DTH_PER_MMCF == 1025.0


def test_load_api_token_raises_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GASQUEST_API_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="GASQUEST_API_TOKEN"):
        _load_api_token()


def test_load_api_token_returns_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GASQUEST_API_TOKEN", "test-bearer-abc123")
    assert _load_api_token() == "test-bearer-abc123"


def test_raw_path_format() -> None:
    p = _raw_path("TIMELY", date(2026, 5, 16))
    assert p == RAW_DIR / "2026" / "05" / "TIMELY_2026-05-16.json"


# ─────────────────────────────────────────────────────────────────────────────
# Tests: transformer
# ─────────────────────────────────────────────────────────────────────────────


def test_parse_sq_response(tmp_path: Path) -> None:
    """Transformer converts one raw file with 2 rows to 2 canonical output rows."""
    raw_dir = tmp_path / "gulf_south"
    raw_file = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    out_path = tmp_path / "gulf_south.parquet"

    _write_raw(raw_file, [_SAMPLE_ROW, _SAMPLE_ROW_2])
    result = transform(raw_dir=raw_dir, curated_parquet_path=out_path)

    assert result["rows"] == 2
    assert result["series_count"] == 2
    assert out_path.exists()

    df = pd.read_parquet(out_path)
    assert len(df) == 2
    assert set(df["source"].unique()) == {"gulf_south"}
    assert set(df["unit"].unique()) == {"MMcf/d"}
    assert set(df["region"].unique()) == {"US"}


def test_transformer_canonical_schema(tmp_path: Path) -> None:
    """Output Parquet has exactly the 8 canonical columns."""
    raw_dir = tmp_path / "gulf_south"
    raw_file = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    out_path = tmp_path / "gulf_south.parquet"

    _write_raw(raw_file, [_SAMPLE_ROW])
    transform(raw_dir=raw_dir, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    expected = {"source", "series_id", "series_name", "period", "value", "unit", "region", "ingested_at"}
    assert expected == set(df.columns)


def test_transformer_series_id_format(tmp_path: Path) -> None:
    """series_id follows the gulf_south_sq_{meter}_{cycle} pattern."""
    raw_dir = tmp_path / "gulf_south"
    raw_file = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    out_path = tmp_path / "gulf_south.parquet"

    _write_raw(raw_file, [_SAMPLE_ROW])
    transform(raw_dir=raw_dir, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    sid = df["series_id"].iloc[0]
    assert sid == "gulf_south_sq_24329_timely"


def test_transformer_unit_conversion_accuracy(tmp_path: Path) -> None:
    """1,025,000 Dth/d should be stored as 1,000.0 MMcf/d."""
    raw_dir = tmp_path / "gulf_south"
    raw_file = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    out_path = tmp_path / "gulf_south.parquet"

    _write_raw(raw_file, [_SAMPLE_ROW])  # SAMPLE_ROW has scheduledQuantity=1_025_000
    transform(raw_dir=raw_dir, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    assert df["value"].iloc[0] == pytest.approx(1_000.0)


def test_meter_filtering(tmp_path: Path) -> None:
    """Freeport meters (locationNumber 24329) can be filtered via series_id."""
    raw_dir = tmp_path / "gulf_south"
    raw_file = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    out_path = tmp_path / "gulf_south.parquet"

    _write_raw(raw_file, [_SAMPLE_ROW, _SAMPLE_ROW_2])
    transform(raw_dir=raw_dir, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    freeport = df[df["series_id"].str.contains("24329")]
    assert len(freeport) == 1
    assert "STRATTON RIDGE" in freeport["series_name"].iloc[0]


def test_transformer_dedup(tmp_path: Path) -> None:
    """When the same (series_id, period) appears twice, keep the latest ingested."""
    raw_dir = tmp_path / "gulf_south"
    out_path = tmp_path / "gulf_south.parquet"

    # Write two raw files for the same cycle+day (simulates a re-fetch)
    f1 = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    f2 = raw_dir / "2026" / "05" / "TIMELY_2026-05-16_v2.json"

    row_v1 = {**_SAMPLE_ROW, "scheduledQuantity": 500_000}
    row_v2 = {**_SAMPLE_ROW, "scheduledQuantity": 600_000}
    _write_raw(f1, [row_v1])
    _write_raw(f2, [row_v2])

    transform(raw_dir=raw_dir, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    # Both files produce the same series_id + period → deduplicated to 1 row
    assert len(df) == 1


def test_transformer_skips_row_missing_location(tmp_path: Path) -> None:
    """Rows without locationNumber are silently skipped."""
    raw_dir = tmp_path / "gulf_south"
    raw_file = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    out_path = tmp_path / "gulf_south.parquet"

    bad_row = {**_SAMPLE_ROW, "locationNumber": None, "locationName": None}
    good_row = _SAMPLE_ROW_2
    _write_raw(raw_file, [bad_row, good_row])

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_path)
    assert result["rows"] == 1


def test_transformer_missing_dir_raises(tmp_path: Path) -> None:
    with pytest.raises(TransformError, match="not found"):
        transform(
            raw_dir=tmp_path / "does_not_exist",
            curated_parquet_path=tmp_path / "out.parquet",
        )


def test_transformer_zero_rows_raises(tmp_path: Path) -> None:
    """TransformError raised when all raw rows lack required fields."""
    raw_dir = tmp_path / "gulf_south"
    raw_file = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    out_path = tmp_path / "gulf_south.parquet"

    # Row missing location and quantity
    _write_raw(raw_file, [{"cycleCode": "TIMELY", "gasFlowDate": "2026-05-16"}])

    with pytest.raises(TransformError, match="zero rows"):
        transform(raw_dir=raw_dir, curated_parquet_path=out_path)


def test_transformer_multiple_cycles(tmp_path: Path) -> None:
    """Rows from TIMELY and EVENING files produce distinct series_ids."""
    raw_dir = tmp_path / "gulf_south"
    out_path = tmp_path / "gulf_south.parquet"

    for cycle in ("TIMELY", "EVENING"):
        f = raw_dir / "2026" / "05" / f"{cycle}_2026-05-16.json"
        _write_raw(f, [_SAMPLE_ROW], cycle=cycle)

    transform(raw_dir=raw_dir, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    sids = set(df["series_id"].tolist())
    assert "gulf_south_sq_24329_timely" in sids
    assert "gulf_south_sq_24329_evening" in sids


def test_transformer_value_type_is_float(tmp_path: Path) -> None:
    raw_dir = tmp_path / "gulf_south"
    raw_file = raw_dir / "2026" / "05" / "TIMELY_2026-05-16.json"
    out_path = tmp_path / "gulf_south.parquet"

    _write_raw(raw_file, [_SAMPLE_ROW])
    transform(raw_dir=raw_dir, curated_parquet_path=out_path)

    df = pd.read_parquet(out_path)
    assert df["value"].dtype == float


# ─────────────────────────────────────────────────────────────────────────────
# Tests: scraper (mocked HTTP)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_on_missing_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing GASQUEST_API_TOKEN → failed status, no HTTP call."""
    monkeypatch.delenv("GASQUEST_API_TOKEN", raising=False)

    with patch("scrapers.energy_transfer.gulf_south.HealthWriter") as mock_hw_cls:
        mock_hw = MagicMock()
        mock_hw_cls.return_value = mock_hw

        result = await run()

    assert result["status"] == "failed"
    assert "GASQUEST_API_TOKEN" in result["error"]
    mock_hw.record_failure.assert_called_once()


@pytest.mark.asyncio
async def test_health_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HTTP error → HealthWriter.record_failure called, status == 'failed'."""
    monkeypatch.setenv("GASQUEST_API_TOKEN", "bad-token")
    monkeypatch.setattr("scrapers.energy_transfer.gulf_south.RAW_DIR", tmp_path)

    with (
        patch("scrapers.energy_transfer.gulf_south.HealthWriter") as mock_hw_cls,
        patch("scrapers.energy_transfer.gulf_south.HttpClient") as mock_client_cls,
    ):
        mock_hw = MagicMock()
        mock_hw_cls.return_value = mock_hw
        mock_instance = AsyncMock()
        mock_instance.get_json.side_effect = Exception("Connection timeout")
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_instance

        result = await run("TIMELY", GAS_DAY)

    assert result["status"] == "failed"
    assert "Connection timeout" in result["error"]
    mock_hw.record_failure.assert_called_once()


@pytest.mark.asyncio
async def test_staleness_gate_skips(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Existing raw file for cycle + gas_day → status == 'skipped', no HTTP call."""
    monkeypatch.setenv("GASQUEST_API_TOKEN", "test-token")
    monkeypatch.setattr("scrapers.energy_transfer.gulf_south.RAW_DIR", tmp_path)

    # Pre-create the output file
    existing = tmp_path / "2026" / "05" / "TIMELY_2026-05-16.json"
    _write_raw(existing, [_SAMPLE_ROW])

    with patch("scrapers.energy_transfer.gulf_south.HealthWriter") as mock_hw_cls:
        mock_hw = MagicMock()
        mock_hw_cls.return_value = mock_hw

        result = await run("TIMELY", GAS_DAY)

    assert result["status"] == "skipped"
    mock_hw.record_skipped.assert_called_once()


@pytest.mark.asyncio
async def test_run_happy_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: API returns 2 rows → raw JSON written, health ok."""
    monkeypatch.setenv("GASQUEST_API_TOKEN", "test-bearer")
    monkeypatch.setattr("scrapers.energy_transfer.gulf_south.RAW_DIR", tmp_path)

    api_resp = _api_response([_SAMPLE_ROW, _SAMPLE_ROW_2])

    with (
        patch("scrapers.energy_transfer.gulf_south.HealthWriter") as mock_hw_cls,
        patch("scrapers.energy_transfer.gulf_south.HttpClient") as mock_client_cls,
    ):
        mock_hw = MagicMock()
        mock_hw_cls.return_value = mock_hw
        mock_instance = AsyncMock()
        mock_instance.get_json = AsyncMock(return_value=api_resp)
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_instance

        result = await run("TIMELY", GAS_DAY)

    assert result["status"] == "ok"
    assert result["row_count"] == 2
    assert result["cycle"] == "TIMELY"
    assert result["gas_day"] == "2026-05-16"

    # Raw file should be written
    raw_file = tmp_path / "2026" / "05" / "TIMELY_2026-05-16.json"
    assert raw_file.exists()
    written = json.loads(raw_file.read_text())
    assert written["row_count"] == 2
    assert len(written["data"]) == 2

    mock_hw.record_success.assert_called_once()


@pytest.mark.asyncio
async def test_run_pagination_fetches_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When total > page_size, scraper makes multiple page requests."""
    monkeypatch.setenv("GASQUEST_API_TOKEN", "test-bearer")
    monkeypatch.setattr("scrapers.energy_transfer.gulf_south.RAW_DIR", tmp_path)

    call_count = 0

    async def _paginated_get(
        url: str, params: dict[str, str] | None = None
    ) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        page = int((params or {}).get("page_number", 1))
        if page == 1:
            return {"data": [_SAMPLE_ROW], "total": 2, "pageSize": 1, "pageNumber": 1}
        return {"data": [_SAMPLE_ROW_2], "total": 2, "pageSize": 1, "pageNumber": 2}

    with (
        patch("scrapers.energy_transfer.gulf_south.HealthWriter"),
        patch("scrapers.energy_transfer.gulf_south.HttpClient") as mock_client_cls,
    ):
        mock_instance = AsyncMock()
        mock_instance.get_json = _paginated_get
        mock_instance.__aenter__ = AsyncMock(return_value=mock_instance)
        mock_instance.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_instance

        result = await run("TIMELY", GAS_DAY)

    assert result["row_count"] == 2
    assert call_count == 2


@pytest.mark.asyncio
async def test_bearer_token_passed_in_header(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Bearer token must appear in the Authorization header."""
    monkeypatch.setenv("GASQUEST_API_TOKEN", "my-secret-token")
    monkeypatch.setattr("scrapers.energy_transfer.gulf_south.RAW_DIR", tmp_path)

    captured_headers: dict[str, str] = {}

    with (
        patch("scrapers.energy_transfer.gulf_south.HealthWriter"),
        patch("scrapers.energy_transfer.gulf_south.HttpClient") as mock_client_cls,
    ):
        # Capture the headers passed to HttpClient constructor
        def _capture_init(**kwargs: Any) -> AsyncMock:
            nonlocal captured_headers
            captured_headers = kwargs.get("default_headers", {})
            inst = AsyncMock()
            inst.get_json = AsyncMock(return_value=_api_response([_SAMPLE_ROW]))
            inst.__aenter__ = AsyncMock(return_value=inst)
            inst.__aexit__ = AsyncMock(return_value=False)
            return inst

        mock_client_cls.side_effect = _capture_init
        await run("TIMELY", GAS_DAY)

    assert captured_headers.get("Authorization") == "Bearer my-secret-token"
