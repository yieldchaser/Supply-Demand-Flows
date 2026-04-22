"""Tests for scrapers.eia_api.supply — EIA Supply Scraper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from scrapers.base.errors import ScraperError
from scrapers.eia_api.supply import PROCESS_CODES
from scrapers.eia_api.supply import run as run_supply


@pytest.fixture
def clean_supply_dir(tmp_path: Path) -> None:
    import scrapers.eia_api.supply as supply_module

    old_dir = supply_module.RAW_DIR
    supply_module.RAW_DIR = tmp_path / "data" / "raw" / "eia_supply"

    import scrapers.base.health_writer as hw

    old_health = hw.HealthWriter

    class MockHealthWriter(hw.HealthWriter):
        def __init__(self, source_name: str, health_dir: Path = tmp_path / "data" / "health"):
            super().__init__(source_name, health_dir)

    hw.HealthWriter = MockHealthWriter  # type: ignore

    yield
    supply_module.RAW_DIR = old_dir
    hw.HealthWriter = old_health  # type: ignore


@pytest.mark.asyncio
async def test_eia_supply_staleness_gate(clean_supply_dir: None) -> None:
    from scrapers.eia_api.supply import RAW_DIR

    # Pre-populate directory with a file for 2024-04
    dt_str = "2024-04"
    target_dir = RAW_DIR / "2024" / "04"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / f"eia_supply_{dt_str}.json").write_text('{"historical": true}')

    class MockEIAClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def get_latest_date(self, *args, **kwargs) -> str:
            # check dimensions are correct for faceted call
            assert kwargs.get("frequency") == "monthly"
            assert "duoarea" in kwargs.get("facets", {})
            return dt_str

    with (
        patch("scrapers.eia_api.supply.EIAClient", MockEIAClient),
        patch("scrapers.eia_api.supply.load_api_key_from_env", return_value="TEST"),
    ):
        result = await run_supply()

    assert result["status"] == "skipped"
    assert result["latest_date"] == dt_str


@pytest.mark.asyncio
async def test_eia_supply_missing_codes_raise_scraper_error(
    clean_supply_dir: None,
) -> None:
    """Test zero rows for any code raises ScraperError instead of a warning."""
    dt_str = "2024-04"

    # Return 5 processes, missing 'FPD' (Dry Production) which is NOT soft-missing
    mock_data = [
        {"period": dt_str, "process": k, "value": 100} for k in PROCESS_CODES if k != "FPD"
    ]

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
            return {"response": {"data": mock_data}}

    with (
        patch("scrapers.eia_api.supply.EIAClient", MockEIAClient),
        patch("scrapers.eia_api.supply.load_api_key_from_env", return_value="TEST"),
    ):
        with pytest.raises(ScraperError) as exc_info:
            await run_supply()

        assert "Missing expected process codes: FPD" in str(exc_info.value)
