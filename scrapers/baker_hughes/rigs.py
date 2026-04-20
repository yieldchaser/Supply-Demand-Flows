"""Baker Hughes North America Rig Count Scraper."""

from __future__ import annotations

import asyncio
import email.utils
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import StatePreservingWriter, safe_write_bytes

log = logging.getLogger(__name__)

SOURCE_NAME = "baker_hughes_rigs"
RAW_DIR = Path("data/raw/baker_hughes")
BASE_URL = "https://rigcount.bakerhughes.com"
OVERVIEW_URL = f"{BASE_URL}/rig-count-overview"

# Fallback URL pattern if HTML parsing fails.
# e.g., https://bakerhughesrigcount.gcs-web.com/static-files/...
# For testability and robustness, we keep a sensible default structure.
FALLBACK_URL = f"{BASE_URL}/static-files/na_rotary_rig_count.xlsx"


def _get_latest_local_file() -> Path | None:
    """Find the most recent baker hughes file by sorting paths."""
    if not RAW_DIR.exists():
        return None
    files = list(RAW_DIR.rglob("baker_hughes_*.xlsx"))
    if not files:
        return None
    # Sort files by name e.g. baker_hughes_2024-04-19.xlsx
    return max(files, key=lambda f: f.name)


async def run() -> dict[str, Any]:
    """Fetch Baker Hughes North America rig count Excel file."""
    health = HealthWriter(source_name=SOURCE_NAME)

    async with HttpClient(rate_limit_per_second=1.0) as client:
        target_url = FALLBACK_URL
        try:
            html = await client.get_bytes(OVERVIEW_URL)
            soup = BeautifulSoup(html, "lxml")

            candidates: list[str] = []
            for a in soup.find_all("a", href=True):
                href = str(a["href"])
                if href.lower().endswith(".xlsx"):
                    candidates.append(href)

            extracted_url = None
            for pref in ["north_america", "na_rotary", "overview", "weekly"]:
                for c in candidates:
                    if pref in c.lower():
                        extracted_url = c
                        break
                if extracted_url:
                    break

            if not extracted_url and candidates:
                extracted_url = candidates[0]

            if extracted_url:
                if extracted_url.startswith("/"):
                    target_url = BASE_URL + extracted_url
                else:
                    target_url = extracted_url
            else:
                log.warning("Could not find .xlsx link in page, using fallback")

        except Exception as e:
            log.warning("Scrape failed, using fallback URL (%s): %s", FALLBACK_URL, e)

        # Use underlying _request to get both headers and content bytes
        try:
            resp = await client._client.get(target_url)  # bypass retry momentarily or use _request
            # _request is better for retry logic
            resp = await client._request("GET", target_url)
            excel_bytes = resp.content
            if not excel_bytes:
                raise RuntimeError("Empty response received from download")

            lm_header = resp.headers.get("Last-Modified")
            iso_date = datetime.now(UTC).strftime("%Y-%m-%d")

            if lm_header:
                parsed_dt = email.utils.parsedate_to_datetime(lm_header)
                iso_date = parsed_dt.strftime("%Y-%m-%d")
        except Exception as e:
            err_str = f"Download failed: {e}"
            health.record_failure(error=err_str)
            return {"status": "failed", "error": err_str}

        # Staleness check
        latest_file = _get_latest_local_file()
        if latest_file is not None and latest_file.exists() and (
            iso_date in latest_file.name or latest_file.stat().st_size == len(excel_bytes)
        ):
            health.record_skipped(reason="no new data (same size or Last-Modified)")
            return {"status": "skipped", "latest_date": iso_date}

        try:
            dt = datetime.strptime(iso_date, "%Y-%m-%d")
        except ValueError:
            dt = datetime.now()

        out_path = RAW_DIR / str(dt.year) / f"{dt.month:02d}" / f"baker_hughes_{iso_date}.xlsx"

        async def compute_data() -> bytes:
            return excel_bytes

        writer = StatePreservingWriter(source_name=SOURCE_NAME, writer=safe_write_bytes)
        success = await writer.guarded_write(out_path, compute_data)

        if success:
            health.record_success(
                metadata={
                    "latest_date": iso_date,
                    "size": len(excel_bytes),
                    "url": target_url,
                }
            )
            return {
                "status": "ok",
                "latest_date": iso_date,
                "size": len(excel_bytes),
                "path": str(out_path),
            }

        return {"status": "failed"}


if __name__ == "__main__":
    result = asyncio.run(run())
    print(json.dumps(result, indent=2))
