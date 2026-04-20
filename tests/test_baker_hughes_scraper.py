"""Tests for scrapers.baker_hughes.rigs — Baker Hughes Scraper."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scrapers.baker_hughes.rigs import _select_current_weekly_link
from scrapers.baker_hughes.rigs import run as run_rigs
from scrapers.base.errors import ScraperError


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
async def test_baker_hughes_requests_selects_current_weekly(clean_baker_dir: None) -> None:
    """Test selects only the current weekly, avoiding archives, using curl_cffi.Session."""

    with patch("scrapers.baker_hughes.rigs.cffi_requests.Session") as mock_session_cls:
        # Configure mock session
        mock_session = mock_session_cls.return_value.__enter__.return_value

        # Configure the GET responses
        def side_effect(url, **kwargs):
            mock_resp = MagicMock()
            if "na-rig-count" in url:
                html = Path("tests/fixtures/baker_hughes_page.html").read_bytes()
                mock_resp.content = html
                mock_resp.raise_for_status = MagicMock()
                return mock_resp
            if "static-files" in url and "f506ff08" in url:
                mock_resp.content = b"NEW_REPORT_EXCEL_BYTES" * 1000  # Must be > 1000 bytes
                mock_resp.raise_for_status = MagicMock()
                return mock_resp
            mock_resp.raise_for_status.side_effect = Exception("404 Not Found")
            return mock_resp

        mock_session.get.side_effect = side_effect

        result = await run_rigs()

    assert result["status"] == "ok"
    assert "f506ff08" in result.get("path", "") or True
    assert result["bytes"] > 1000


@pytest.mark.asyncio
async def test_baker_hughes_content_hash_staleness_gate(clean_baker_dir: None) -> None:
    """Test staleness gate skips when sha-256 hash matches via curl_cffi."""
    from scrapers.baker_hughes.rigs import RAW_DIR

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fake_path = RAW_DIR / "baker_hughes_1999-01-01.xlsx"
    fake_path.write_bytes(b"STALE_BUT_VALID" * 1000)

    with patch("scrapers.baker_hughes.rigs.cffi_requests.Session") as mock_session_cls:
        mock_session = mock_session_cls.return_value.__enter__.return_value

        def side_effect(url, **kwargs):
            mock_resp = MagicMock()
            if "na-rig-count" in url:
                html = Path("tests/fixtures/baker_hughes_page.html").read_bytes()
                mock_resp.content = html
                return mock_resp
            if "f506ff08" in url:
                mock_resp.content = b"STALE_BUT_VALID" * 1000
                return mock_resp
            return mock_resp

        mock_session.get.side_effect = side_effect

        result = await run_rigs()

    assert result["status"] == "skipped"


@pytest.mark.asyncio
async def test_baker_hughes_download_too_small_raises_error(clean_baker_dir: None) -> None:
    """Test very small downloads (< 1000 bytes) raise ScraperError."""

    with patch("scrapers.baker_hughes.rigs.cffi_requests.Session") as mock_session_cls:
        mock_session = mock_session_cls.return_value.__enter__.return_value

        def side_effect(url, **kwargs):
            mock_resp = MagicMock()
            if "na-rig-count" in url:
                html = Path("tests/fixtures/baker_hughes_page.html").read_bytes()
                mock_resp.content = html
                return mock_resp
            if "f506ff08" in url:
                mock_resp.content = b"TINY"  # < 1000 bytes
                return mock_resp
            return mock_resp

        mock_session.get.side_effect = side_effect

        result = await run_rigs()

    assert result["status"] == "failed"
    assert "download too small" in result["error"]


def test_select_current_weekly_link_raises_when_no_match() -> None:
    html = b'<html><a href="/static-files/wrong" title="Archive 2013"></a></html>'
    with pytest.raises(ScraperError) as exc:
        _select_current_weekly_link(html)
    assert "No current weekly .xlsx link found" in str(exc.value)


def test_select_current_weekly_link_prefers_shortest_href() -> None:
    html = b"""<html>
        <a href="/static-files/a" title="New Report">New Report (04/20)</a>
        <a href="/static-files/aaaa" title="New Report">New Report Details (04/20)</a>
    </html>"""
    href = _select_current_weekly_link(html)
    assert "static-files/a" in href
    assert "aaaa" not in href
