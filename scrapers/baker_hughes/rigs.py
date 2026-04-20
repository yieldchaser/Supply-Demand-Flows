"""
Baker Hughes NA Rig Count scraper.

Why curl_cffi and not httpx/requests:
  Baker Hughes runs behind Cloudflare with TLS fingerprint-based bot detection
  (JA3/JA4). Python's stdlib ssl + OpenSSL (used by httpx AND requests) has
  well-known fingerprints that Cloudflare throttles to the point of timeout.
  Windows native curl works (via SChannel's distinct fingerprint), and
  curl_cffi wraps libcurl with Chrome/Firefox fingerprint impersonation
  for the same result from Python.

Selector strategy (last verified: 2026-04-20):
  Baker Hughes publishes ~4 static-files links on /na-rig-count, all with
  UUIDs that change weekly. We identify the "current weekly" file by:
    - href contains "static-files"
    - link text contains "New Report"
    - link text does NOT contain year strings "2013", "2011", "2000"
      (these identify the archive files)

  If the selector breaks in future, re-run DevTools recon on /na-rig-count,
  search for the link with the most recent publication date in the table.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path

from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests

from scrapers.base.errors import ScraperError
from scrapers.base.health_writer import HealthWriter
from scrapers.base.safe_writer import safe_write_bytes

logger = logging.getLogger(__name__)

SOURCE_NAME = "baker_hughes_rigs"
RAW_DIR = Path("data/raw/baker_hughes")
LANDING_URL = "https://rigcount.bakerhughes.com/na-rig-count"
BASE_URL = "https://rigcount.bakerhughes.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": LANDING_URL,
}

TIMEOUT_SECONDS = 30.0


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _find_latest_existing(raw_dir: Path) -> Path | None:
    """Return the newest existing .xlsx file in raw_dir, or None."""
    files = sorted(raw_dir.rglob("baker_hughes_*.xlsx"))
    return files[-1] if files else None


def _select_current_weekly_link(html: bytes) -> str:
    soup = BeautifulSoup(html, "html.parser")
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if "static-files" not in href:
            continue
        if "New Report" not in text:
            continue
        if any(year in text for year in ("2013", "2011", "2000")):
            continue
        candidates.append((href, text))

    if not candidates:
        all_static = [
            a["href"] for a in soup.find_all("a", href=True) if "static-files" in a["href"]
        ]
        raise ScraperError(
            f"No current weekly .xlsx link found on Baker Hughes landing page. "
            f"Found {len(all_static)} static-files links total: {all_static[:5]}"
        )

    candidates.sort(key=lambda x: len(x[0]))
    href, text = candidates[0]
    logger.info(f"Selected Baker Hughes link: '{text}' -> {href}")
    return href if href.startswith("http") else BASE_URL + href


def _fetch_and_download() -> bytes:
    """Sync helper - warms session, scrapes landing page, downloads file."""
    with cffi_requests.Session(impersonate="chrome124") as session:
        session.headers.update(HEADERS)

        # Step 1: warm session on landing page
        r = session.get(LANDING_URL, timeout=TIMEOUT_SECONDS)
        r.raise_for_status()

        # Step 2: select current weekly link
        download_url = _select_current_weekly_link(r.content)

        # Step 3: download (same session = cookies preserved + Referer)
        r2 = session.get(download_url, timeout=TIMEOUT_SECONDS)
        r2.raise_for_status()

        if not r2.content or len(r2.content) < 1000:
            raise ScraperError(
                f"Baker Hughes download too small ({len(r2.content)} bytes) - "
                f"likely an error page, not an Excel file."
            )

        return r2.content


async def run() -> dict:
    health = HealthWriter(SOURCE_NAME)

    try:
        content = await asyncio.to_thread(_fetch_and_download)

        current_hash = _sha256_bytes(content)
        latest_existing = _find_latest_existing(RAW_DIR)
        if latest_existing is not None:
            existing_hash = _sha256_bytes(latest_existing.read_bytes())
            if existing_hash == current_hash:
                msg = f"content unchanged since {latest_existing.stem}"
                health.record_skipped(msg)
                return {"status": "skipped", "reason": msg}

        today = datetime.now(UTC).date().isoformat()
        year, month = today[:4], today[5:7]
        out_path = RAW_DIR / year / month / f"baker_hughes_{today}.xlsx"
        safe_write_bytes(out_path, content)

        health.record_success(
            metadata={
                "date": today,
                "bytes": len(content),
                "sha256": current_hash[:16],
            }
        )
        return {
            "status": "ok",
            "date": today,
            "bytes": len(content),
            "path": str(out_path),
        }

    except Exception as e:
        health.record_failure(str(e))
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    import json

    result = asyncio.run(run())
    print(json.dumps(result, indent=2))
