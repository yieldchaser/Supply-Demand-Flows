"""Tests for scrapers.eia_api.storage — EIA Storage Scraper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from scrapers.base.errors import HttpClientError
from scrapers.eia_api.storage import run as run_storage


@pytest.fixture
def clean_storage_dir(tmp_path: Path) -> None:
    """Fixture to ensure clean RAW_DIR."""
    import scrapers.eia_api.storage as storage_module

    old_dir = storage_module.RAW_DIR
    storage_module.RAW_DIR = tmp_path / "data" / "raw" / "eia_storage"

    # Also redirect health
    import scrapers.base.health_writer as hw

    old_health = hw.HealthWriter

    class MockHealthWriter(hw.HealthWriter):
        def __init__(self, source_name: str, health_dir: Path = tmp_path / "data" / "health"):
            super().__init__(source_name, health_dir)

    hw.HealthWriter = MockHealthWriter  # type: ignore

    yield
    storage_module.RAW_DIR = old_dir
    hw.HealthWriter = old_health  # type: ignore


@pytest.mark.asyncio
async def test_eia_storage_staleness_gate(clean_storage_dir: None) -> None:
    """Test staleness gate skips when local latest matches API latest."""
    from scrapers.eia_api.storage import RAW_DIR

    # Pre-populate directory with a file for 2024-04-19
    dt_str = "2024-04-19"
    target_dir = RAW_DIR / "2024" / "04"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / f"eia_storage_{dt_str}.json").write_text('{"historical": true}')

    class MockEIAClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_latest_date(self, *args, **kwargs) -> str:
            return dt_str

    with (
        patch("scrapers.eia_api.storage.EIAClient", MockEIAClient),
        patch("scrapers.eia_api.storage.load_api_key_from_env", return_value="TEST"),
    ):
        result = await run_storage()

    assert result["status"] == "skipped"
    assert result["latest_date"] == dt_str


@pytest.mark.asyncio
async def test_eia_storage_success_fetch(clean_storage_dir: None) -> None:
    """Test successful fetch writes data atomically."""
    dt_str = "2024-04-19"

    class MockEIAClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_latest_date(self, *args, **kwargs) -> str:
            return dt_str

        async def get_series(self, *args, **kwargs) -> dict:
            return {"response": {"data": [{"period": dt_str, "value": 100}]}}

    with (
        patch("scrapers.eia_api.storage.EIAClient", MockEIAClient),
        patch("scrapers.eia_api.storage.load_api_key_from_env", return_value="TEST"),
    ):
        result = await run_storage()

    assert result["status"] == "ok"
    assert result["latest_date"] == dt_str
    assert result["rows"] == 1
    assert "eia_storage_2024-04-19.json" in str(result.get("path", ""))


@pytest.mark.asyncio
async def test_eia_storage_failure(clean_storage_dir: None) -> None:
    """Test failure during fetch does not write data and records failure."""

    class MockEIAClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_latest_date(self, *args, **kwargs):
            raise HttpClientError(url="test", status=500, attempts=1, elapsed_s=1.0, reason="Fail")

    with (
        patch("scrapers.eia_api.storage.EIAClient", MockEIAClient),
        patch("scrapers.eia_api.storage.load_api_key_from_env", return_value="TEST"),
    ):
        result = await run_storage()

    assert result["status"] == "failed"
    assert "Fail" in result["error"]
