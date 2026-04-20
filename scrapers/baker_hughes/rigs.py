# Baker Hughes NA Rig Count scraper
#
# Selector strategy (last verified: 2026-04-20):
#   Baker Hughes publishes ~4 static-files links on /na-rig-count, all with UUIDs
#   that change weekly. We identify the "current weekly" file by:
#     - link href contains "static-files"
#     - link text contains "New Report"
#     - link text does NOT contain year strings "2013", "2011", "2000"
#       (these identify the archive files)
#
#   File is .xlsx format, requires Referer + User-Agent to download.
#   The file at the resolved UUID returns 404 without a Referer header.
#
#   If selector breaks: re-run DevTools recon on /na-rig-count,
#   look for the link dated most recent with a green document icon.
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from scrapers.base.errors import ScraperError
from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import StatePreservingWriter, safe_write_bytes

log = logging.getLogger(__name__)

SOURCE_NAME = "baker_hughes_rigs"
RAW_DIR = Path("data/raw/baker_hughes")
BASE_URL = "https://rigcount.bakerhughes.com"
OVERVIEW_URL = f"{BASE_URL}/na-rig-count"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://rigcount.bakerhughes.com/na-rig-count",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _find_latest_existing(raw_dir: Path) -> Path | None:
    """Return the newest existing .xlsx file in raw_dir, or None."""
    files = sorted(raw_dir.rglob("baker_hughes_*.xlsx"))
    return files[-1] if files else None


def _extract_recent_date(text: str) -> datetime | None:
    matches = re.findall(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})", text)
    if not matches:
        return None
    dates = []
    for m, d, y in matches:
        year = int(y)
        if year < 100:
            year += 2000
        try:
            dates.append(datetime(year, int(m), int(d), tzinfo=UTC))
        except ValueError:
            pass
    return max(dates) if dates else None


async def run() -> dict[str, Any]:
    health = HealthWriter(source_name=SOURCE_NAME)

    async with HttpClient(rate_limit_per_second=1.0, default_headers=HEADERS) as client:
        try:
            # 1. Warm session
            html = await client.get_bytes(OVERVIEW_URL)
            soup = BeautifulSoup(html, "lxml")

            # 2. Link selection
            candidates = []
            links = soup.find_all("a", href=True)
            for a in links:
                href = a["href"]
                text = a.get_text(strip=True)
                if "static-files" in href and "New Report" in text:
                    if not any(yr in text for yr in ["2013", "2011", "2000"]):
                        candidates.append(a)

            if not candidates:
                all_static = sum(1 for tag in links if "static-files" in tag.get("href", ""))
                title = soup.title.string if soup.title else "No Title"
                raise ScraperError(
                    f"Page '{title}', found {all_static} static-files links, but 0 matched requirements."
                )

            selected_a = None
            if len(candidates) == 1:
                selected_a = candidates[0]
            else:
                best_date = None
                for a in candidates:
                    tr = a.find_parent("tr")
                    context = tr.get_text() if tr else a.get_text()
                    d = _extract_recent_date(context)
                    if d:
                        if not best_date or d > best_date:
                            best_date = d
                            selected_a = a
                if not selected_a:
                    selected_a = candidates[0]

            extracted_url = selected_a["href"]
            target_url = (
                BASE_URL + extracted_url if extracted_url.startswith("/") else extracted_url
            )

            # 3. Download
            resp = await client._request("GET", target_url)
            excel_bytes = resp.content
            if not excel_bytes:
                raise RuntimeError("Empty response received from download")

            # 4. Hash checking
            new_hash = _sha256_bytes(excel_bytes)
            latest_file = _find_latest_existing(RAW_DIR)
            if latest_file and latest_file.exists():
                existing_hash = _sha256_bytes(latest_file.read_bytes())
                if existing_hash == new_hash:
                    health.record_skipped(reason=f"content unchanged since {latest_file.name}")
                    return {
                        "status": "skipped",
                        "latest_date": latest_file.stem.replace("baker_hughes_", ""),
                    }

            # 5. Output
            today = datetime.now(UTC)
            iso_date = today.strftime("%Y-%m-%d")
            out_path = (
                RAW_DIR / str(today.year) / f"{today.month:02d}" / f"baker_hughes_{iso_date}.xlsx"
            )

            async def compute_data() -> bytes:
                return excel_bytes

            writer = StatePreservingWriter(source_name=SOURCE_NAME, writer=safe_write_bytes)
            success = await writer.guarded_write(out_path, compute_data)

            if success:
                health.record_success(
                    metadata={
                        "latest_date": iso_date,
                        "size": len(excel_bytes),
                        "hash": new_hash[:8],
                        "url": target_url,
                    }
                )
                return {
                    "status": "ok",
                    "latest_date": iso_date,
                    "size": len(excel_bytes),
                    "path": str(out_path),
                    "url": target_url,
                }

            return {"status": "failed"}

        except Exception as e:
            health.record_failure(error=str(e))
            if isinstance(e, ScraperError):
                raise
            return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    result = asyncio.run(run())
    print(json.dumps(result, indent=2))
