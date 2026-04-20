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
async def test_baker_hughes_successful_download_shortest_link(clean_baker_dir: None) -> None:
    """Test successful link extraction: prefer shortest xlsb matching criteria."""

    def handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "na-rig-count" in url_str:
            html = Path("tests/fixtures/baker_hughes_page.html").read_bytes()
            return httpx.Response(200, content=html)
        if "na_rig_count.xlsb" in url_str:
            headers = {"Last-Modified": "Fri, 19 Apr 2024 10:00:00 GMT"}
            return httpx.Response(200, content=b"FAKE_SHORT_EXCEL", headers=headers)
        if "detailed.xlsb" in url_str:
            return httpx.Response(200, content=b"detailed")
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
    # Should have picked the shorter url content
    assert result["size"] == len(b"FAKE_SHORT_EXCEL")
    assert "na_rig_count.xlsb" in result["url"]


@pytest.mark.asyncio
async def test_baker_hughes_ua_fallback(
    clean_baker_dir: None, caplog: pytest.LogCaptureFixture
) -> None:
    """Test User-Agent fallback loop."""

    call_count = {"403s": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)

        # Check UA attached to request via httpx
        ua = request.headers.get("user-agent", "")

        if "Mozilla" not in ua and call_count["403s"] == 0:
            call_count["403s"] += 1
            return httpx.Response(403, content=b"Forbidden by CDN")

        if "na-rig-count" in url_str:
            html = '<html><body><a href="/static/na_rig_count.xlsb" title="NA rotary rig count">Link</a></body></html>'
            return httpx.Response(200, content=html.encode("utf-8"))
        if ".xlsb" in url_str:
            headers = {"Last-Modified": "Fri, 19 Apr 2024 10:00:00 GMT"}
            return httpx.Response(200, content=b"SUCCESS_WITH_MOZILLA", headers=headers)

        return httpx.Response(404)

    from scrapers.base.http_client import HttpClient

    class MockHttpClient(HttpClient):
        def __init__(self, *args, **kwargs):
            super().__init__(backoff_base_seconds=0.0)
            self._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with patch("scrapers.baker_hughes.rigs.HttpClient", MockHttpClient):
        result = await run_rigs()

    assert result["status"] == "ok"
    assert result["size"] == len(b"SUCCESS_WITH_MOZILLA")
    assert "Standard UA hit 403. Retrying with Mozilla fallback." in caplog.text


@pytest.mark.asyncio
async def test_baker_hughes_missing_link_raises(clean_baker_dir: None) -> None:
    """Test missing valid link raises ScraperError, failing loudly."""

    def handler(request: httpx.Request) -> httpx.Response:
        url_str = str(request.url)
        if "na-rig-count" in url_str:
            html = "<html><body><p>No links here</p></body></html>"
            return httpx.Response(200, content=html.encode("utf-8"))
        return httpx.Response(404)

    from scrapers.base.http_client import HttpClient

    class MockHttpClient(HttpClient):
        def __init__(self, *args, **kwargs):
            super().__init__(backoff_base_seconds=0.0)
            self._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    with patch("scrapers.baker_hughes.rigs.HttpClient", MockHttpClient):
        result = await run_rigs()

    assert result["status"] == "failed"
    assert "No valid .xlsb link found on Baker" in result["error"]
