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
async def test_baker_hughes_successful_download(clean_baker_dir: None) -> None:
    """Test successful link extraction and Excel download."""

    def handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "overview" in url_str:
            html = '<html><body><a href="/static/north_america.xlsx">NA Rig Count</a></body></html>'
            return httpx.Response(200, content=html.encode("utf-8"))
        if ".xlsx" in url_str:
            headers = {"Last-Modified": "Fri, 19 Apr 2024 10:00:00 GMT"}
            return httpx.Response(200, content=b"FAKEEXCEL", headers=headers)
        return httpx.Response(404)

    from scrapers.base.http_client import HttpClient

    class MockHttpClient(HttpClient):
        def __init__(self, *args, **kwargs):
            super().__init__(backoff_base_seconds=0.0)
            self._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with patch("scrapers.baker_hughes.rigs.HttpClient", MockHttpClient):
        result = await run_rigs()

    assert result["status"] == "ok"
    assert result["latest_date"] == "2024-04-19"
    assert result["size"] == len(b"FAKEEXCEL")


@pytest.mark.asyncio
async def test_baker_hughes_staleness_gate(clean_baker_dir: None) -> None:
    """Test staleness gate based on size / Last-Modified."""
    from scrapers.baker_hughes.rigs import RAW_DIR

    # Pre-populate directory with a matching file
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fake_path = RAW_DIR / "baker_hughes_2024-04-19.xlsx"
    fake_path.write_bytes(b"FAKEEXCEL")

    def handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "overview" in url_str:
            html = '<html><body><a href="/static/north_america.xlsx">NA Rig Count</a></body></html>'
            return httpx.Response(200, content=html.encode("utf-8"))
        if ".xlsx" in url_str:
            headers = {"Last-Modified": "Fri, 19 Apr 2024 10:00:00 GMT"}
            return httpx.Response(200, content=b"FAKEEXCEL", headers=headers)
        return httpx.Response(404)

    from scrapers.base.http_client import HttpClient

    class MockHttpClient(HttpClient):
        def __init__(self, *args, **kwargs):
            super().__init__(backoff_base_seconds=0.0)
            self._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with patch("scrapers.baker_hughes.rigs.HttpClient", MockHttpClient):
        result = await run_rigs()

    assert result["status"] == "skipped"
    assert result["latest_date"] == "2024-04-19"


@pytest.mark.asyncio
async def test_baker_hughes_fallback_url(clean_baker_dir: None) -> None:
    """Test fallback URL behavior when HTML parse fails."""

    def handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "overview" in url_str:
            # Broken page without links
            return httpx.Response(200, content=b"No links here")
        if ".xlsx" in url_str:
            return httpx.Response(200, content=b"FALLBACKEXCEL")
        return httpx.Response(404)

    from scrapers.base.http_client import HttpClient

    class MockHttpClient(HttpClient):
        def __init__(self, *args, **kwargs):
            super().__init__(backoff_base_seconds=0.0)
            self._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with patch("scrapers.baker_hughes.rigs.HttpClient", MockHttpClient):
        result = await run_rigs()

    assert result["status"] == "ok"
    assert result["size"] == len(b"FALLBACKEXCEL")
