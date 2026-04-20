import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

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
    """
    Selector strategy (last verified: 2026-04-20):
      Baker Hughes publishes ~4 static-files links on /na-rig-count, all with
      UUIDs that change weekly. We identify the "current weekly" file by:
        - href contains "static-files"
        - link text contains "New Report"
        - link text does NOT contain archive year indicators
      If the selector breaks, re-run DevTools recon on /na-rig-count.
    """
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
        all_static = [a["href"] for a in soup.find_all("a", href=True) if "static-files" in a["href"]]
        raise ScraperError(
            f"No current weekly .xlsx link found on Baker Hughes landing page. "
            f"Found {len(all_static)} static-files links total: {all_static[:5]}"
        )

    # If multiple: prefer the shortest href (heuristic: current file has a single stable UUID)
    candidates.sort(key=lambda x: len(x[0]))
    href, text = candidates[0]
    logger.info(f"Selected Baker Hughes link: '{text}' -> {href}")
    return href if href.startswith("http") else BASE_URL + href


def _fetch_and_download() -> bytes:
    """Sync helper - warms session, scrapes landing page, downloads file."""
    with requests.Session() as session:
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
    """
    Why: Baker Hughes publishes NA rotary rig count weekly. First supply-side
    leading indicator for our balance sheet.

    What: scrapes rigcount.bakerhughes.com/na-rig-count for the current
    weekly .xlsx link, downloads with session cookies and Referer header,
    writes to data/raw/baker_hughes/ using content-hash staleness gate.

    Failure modes:
      - Landing page 5xx/timeout -> raises HttpClientError (propagates up)
      - No current weekly link in page HTML -> raises ScraperError
      - Download is too small (< 1 KB) -> raises ScraperError
      - Any failure -> health writer records failure, safe-write prevents
        overwriting last known good file.
    """
    health = HealthWriter(SOURCE_NAME)

    try:
        # Run blocking I/O in a thread so this stays async-compatible
        content = await asyncio.to_thread(_fetch_and_download)

        # Content-hash staleness gate (Drupal Last-Modified is unreliable)
        current_hash = _sha256_bytes(content)
        latest_existing = _find_latest_existing(RAW_DIR)
        if latest_existing is not None:
            existing_hash = _sha256_bytes(latest_existing.read_bytes())
            if existing_hash == current_hash:
                msg = f"content unchanged since {latest_existing.stem}"
                health.record_skipped(msg)
                return {"status": "skipped", "reason": msg}

        # Write new file
        today = datetime.now(timezone.utc).date().isoformat()
        year, month = today[:4], today[5:7]
        out_path = RAW_DIR / year / month / f"baker_hughes_{today}.xlsx"
        safe_write_bytes(out_path, content)

        health.record_success(metadata={
            "date": today,
            "bytes": len(content),
            "sha256": current_hash[:16],  # truncated for readability
        })
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
