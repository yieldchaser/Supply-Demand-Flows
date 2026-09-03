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
    old_curated = storage_module.CURATED_PATH
    storage_module.RAW_DIR = tmp_path / "data" / "raw" / "eia_storage"
    storage_module.CURATED_PATH = tmp_path / "data" / "curated" / "eia_storage.parquet"

    # Also redirect health
    import scrapers.base.health_writer as hw

    old_health = hw.HealthWriter

    class MockHealthWriter(hw.HealthWriter):
        def __init__(self, source_name: str, health_dir: Path = tmp_path / "data" / "health"):
            super().__init__(source_name, health_dir)

    hw.HealthWriter = MockHealthWriter  # type: ignore

    yield
    storage_module.RAW_DIR = old_dir
    storage_module.CURATED_PATH = old_curated
    hw.HealthWriter = old_health  # type: ignore


@pytest.mark.asyncio
async def test_eia_storage_staleness_gate(clean_storage_dir: None) -> None:
    """Test staleness gate skips when local latest matches API latest AND history is full."""
    from scrapers.eia_api.storage import RAW_DIR

    dt_str = "2024-04-19"
    target_dir = RAW_DIR / "2024" / "04"
    target_dir.mkdir(parents=True, exist_ok=True)

    # Populate with >=500 rows so the row-count gate allows skipping
    fake_data = {"response": {"data": [{"period": dt_str, "value": i} for i in range(600)]}}
    (target_dir / f"eia_storage_{dt_str}.json").write_text(
        __import__("json").dumps(fake_data), encoding="utf-8"
    )

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
    """Test successful fetch writes data atomically and passes START_DATE to get_series."""
    import scrapers.eia_api.storage as storage_module

    dt_str = "2024-04-19"
    captured_kwargs: dict = {}

    class MockEIAClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_latest_date(self, *args, **kwargs) -> str:
            return dt_str

        async def get_series(self, **kwargs) -> dict:
            captured_kwargs.update(kwargs)
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
    # Verify backfill start date and length are passed correctly
    assert captured_kwargs["start"] == storage_module.START_DATE
    assert captured_kwargs["start"] == "2018-01-01"
    assert captured_kwargs["length"] == 5000


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


@pytest.mark.asyncio
async def test_eia_storage_large_row_count(clean_storage_dir: None) -> None:
    """Test that large row counts (post-backfill) are handled correctly."""
    dt_str = "2024-04-19"
    # Simulate ~2080 rows (8y × 52w × 5 regions)
    large_dataset = [{"period": f"2018-01-{i:02d}", "value": i * 10} for i in range(1, 1001)]
    large_dataset += [{"period": f"2020-06-{i:02d}", "value": i * 5} for i in range(1, 1082)]

    class MockEIAClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_latest_date(self, *args, **kwargs) -> str:
            return dt_str

        async def get_series(self, **kwargs) -> dict:
            return {"response": {"data": large_dataset}}

    with (
        patch("scrapers.eia_api.storage.EIAClient", MockEIAClient),
        patch("scrapers.eia_api.storage.load_api_key_from_env", return_value="TEST"),
    ):
        result = await run_storage()

    assert result["status"] == "ok"
    assert result["rows"] > 1000, f"Expected >1000 rows, got {result['rows']}"


@pytest.mark.asyncio
async def test_eia_storage_staleness_gate_uses_payload_content_not_filename(
    clean_storage_dir: None,
) -> None:
    """Staleness gate must inspect payload content, not filename.

    Regression test for Prompt T §02:
    If a file is named eia_storage_2026-08-21.json but its payload only contains
    periods through 2026-08-14, the scraper must NOT skip when the API reports
    latest date 2026-08-21.
    """
    from scrapers.eia_api.storage import RAW_DIR

    target_dir = RAW_DIR / "2026" / "08"
    target_dir.mkdir(parents=True, exist_ok=True)

    # File named for 2026-08-21, but payload only contains periods through 2026-08-14
    fake_data = {
        "response": {
            "data": [{"period": "2026-08-14", "value": i} for i in range(600)]
        }
    }
    (target_dir / "eia_storage_2026-08-21.json").write_text(
        __import__("json").dumps(fake_data), encoding="utf-8"
    )

    fetch_called = False

    class MockEIAClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_latest_date(self, *args, **kwargs) -> str:
            return "2026-08-21"

        async def get_series(self, **kwargs) -> dict:
            nonlocal fetch_called
            fetch_called = True
            return {
                "response": {
                    "data": [{"period": "2026-08-21", "value": 100}]
                }
            }

    with (
        patch("scrapers.eia_api.storage.EIAClient", MockEIAClient),
        patch("scrapers.eia_api.storage.load_api_key_from_env", return_value="TEST"),
    ):
        result = await run_storage()

    # Must NOT have skipped; must have fetched the missing data
    assert result["status"] == "ok"
    assert fetch_called is True
    assert result["latest_date"] == "2026-08-21"


@pytest.mark.asyncio
async def test_eia_storage_no_op_when_dataset_does_not_advance(
    clean_storage_dir: None,
) -> None:
    """When fetch completes but newest period <= curated parquet, record no-op (Prompt U §05)."""
    import pandas as pd

    import scrapers.eia_api.storage as storage_module

    # Pre-populate curated parquet with period 2026-08-21
    curated_file = storage_module.CURATED_PATH
    curated_file.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([{"period": "2026-08-21", "value": 100.0, "region": "US"}])
    df.to_parquet(curated_file)

    class MockEIAClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_latest_date(self, *args, **kwargs) -> str:
            return "2026-08-21"

        async def get_series(self, **kwargs) -> dict:
            return {
                "response": {
                    "data": [{"period": "2026-08-21", "value": 100}]
                }
            }

    with (
        patch("scrapers.eia_api.storage.EIAClient", MockEIAClient),
        patch("scrapers.eia_api.storage.load_api_key_from_env", return_value="TEST"),
    ):
        result = await run_storage()

    # Must record no_op because newest period does not advance beyond curated
    assert result["status"] == "no_op"
    assert result["latest_date"] == "2026-08-21"
    assert result["curated_latest"] == "2026-08-21"
