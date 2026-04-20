"""Tests for scrapers.baker_hughes.rigs — Baker Hughes Scraper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from scrapers.baker_hughes.rigs import run as run_rigs


@pytest.fixture
def clean_baker_dir(tmp_path: Path) -> None:
    import scrapers.baker_hughes.rigs as bh_module

    old_dir = bh_module.RAW_DIR
    bh_module.RAW_DIR = tmp_path / "data" / "raw" / "baker_hughes"

    import scrapers.base.health_writer as hw

    old_health = hw.HealthWriter

    class MockHealthWriter(hw.HealthWriter):
        def __init__(self, source_name: str, health_dir: Path = tmp_path / "data" / "health"):
            super().__init__(source_name, health_dir)

    hw.HealthWriter = MockHealthWriter  # type: ignore

    yield
    bh_module.RAW_DIR = old_dir
    hw.HealthWriter = old_health  # type: ignore


@pytest.mark.asyncio
async def test_baker_hughes_selects_current_weekly(clean_baker_dir: None) -> None:
    """Test selects only the current weekly, avoiding archives."""
    req_referer = ""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal req_referer
        url_str = str(request.url)
        if "na-rig-count" in url_str:
            html = Path("tests/fixtures/baker_hughes_page.html").read_bytes()
            return httpx.Response(200, content=html)

        if "static-files" in url_str:
            req_referer = request.headers.get("Referer", "")
            if "f506ff08" not in url_str:
                return httpx.Response(404)
            return httpx.Response(200, content=b"NEW_REPORT_EXCEL")

        return httpx.Response(404)

    from scrapers.base.http_client import HttpClient

    class MockHttpClient(HttpClient):
        def __init__(self, *args, **kwargs):
            # Include custom headers simulation
            super().__init__(
                backoff_base_seconds=0.0, default_headers=kwargs.get("default_headers")
            )
            headers = kwargs.get("default_headers", {})
            self._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), headers=headers)

    with patch("scrapers.baker_hughes.rigs.HttpClient", MockHttpClient):
        result = await run_rigs()

    assert result["status"] == "ok"
    assert result["size"] == len(b"NEW_REPORT_EXCEL")
    assert "f506ff08" in result["url"]
    assert req_referer == "https://rigcount.bakerhughes.com/na-rig-count"


@pytest.mark.asyncio
async def test_baker_hughes_content_hash_staleness_gate(clean_baker_dir: None) -> None:
    """Test staleness gate skips when sha-256 hash matches."""
    from scrapers.baker_hughes.rigs import RAW_DIR

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fake_path = RAW_DIR / "baker_hughes_1999-01-01.xlsx"
    fake_path.write_bytes(b"STALE_BUT_VALID")

    def handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "na-rig-count" in url_str:
            html = Path("tests/fixtures/baker_hughes_page.html").read_bytes()
            return httpx.Response(200, content=html)
        if "f506ff08" in url_str:
            return httpx.Response(200, content=b"STALE_BUT_VALID")
        return httpx.Response(404)

    from scrapers.base.http_client import HttpClient

    class MockHttpClient(HttpClient):
        def __init__(self, *args, **kwargs):
            super().__init__(
                backoff_base_seconds=0.0, default_headers=kwargs.get("default_headers")
            )
            headers = kwargs.get("default_headers", {})
            self._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), headers=headers)

    with patch("scrapers.baker_hughes.rigs.HttpClient", MockHttpClient):
        result = await run_rigs()

    assert result["status"] == "skipped"
    assert result["latest_date"] == "1999-01-01"
