"""Baker Hughes North America Rig Count Scraper (Session-Scoped xlsb)."""

from __future__ import annotations

import asyncio
import email.utils
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from scrapers.base.errors import HttpClientError, ScraperError
from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import StatePreservingWriter, safe_write_bytes

log = logging.getLogger(__name__)

SOURCE_NAME = "baker_hughes_rigs"
RAW_DIR = Path("data/raw/baker_hughes")
BASE_URL = "https://rigcount.bakerhughes.com"
OVERVIEW_URL = f"{BASE_URL}/na-rig-count"

FALLBACK_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def _get_latest_local_file() -> Path | None:
    """Find the most recent baker hughes file by sorting paths."""
    if not RAW_DIR.exists():
        return None
    files = list(RAW_DIR.rglob("baker_hughes_*.xlsb"))
    if not files:
        return None
    return max(files, key=lambda f: f.name)


def _find_best_link(candidates: list[dict[str, str]]) -> str | None:
    """Select the best matching .xlsb link from a list of candidates."""
    matched = []
    for c in candidates:
        href_lower = c["href"].lower()
        title_lower = c.get("title", "").lower()

        # Must end with .xlsb or .xlsx (though we prefer xlsb based on recon)
        if not (href_lower.endswith(".xlsb") or href_lower.endswith(".xlsx")):
            continue

        # Should ideally contain 'rig_count' and 'na' or 'rotary'
        if (
            "rig_count" in href_lower
            or "rig_count" in title_lower
            or "rotary_rig_count" in href_lower
            or "rotary" in title_lower
        ):
            if "na" in href_lower or "na" in title_lower or "north america" in title_lower:
                matched.append(c["href"])

    if not matched:
        # Fallback to broader match if strict fails
        for c in candidates:
            h = c["href"].lower()
            t = c.get("title", "").lower()
            if (h.endswith(".xlsb") or h.endswith(".xlsx")) and ("rig" in h or "rig" in t):
                matched.append(c["href"])

    if not matched:
        return None

    # Sort matching by length (shortest filename usually summary)
    matched.sort(key=len)
    return matched[0]


async def run() -> dict[str, Any]:
    """Fetch Baker Hughes North America rig count session-scoped Excel file."""
    health = HealthWriter(source_name=SOURCE_NAME)

    async def fetch_pipeline(client: HttpClient) -> dict[str, Any] | None:
        html = await client.get_bytes(OVERVIEW_URL)
        soup = BeautifulSoup(html, "lxml")

        candidates = []
        for a in soup.find_all("a", href=True):
            entry = {"href": str(a["href"]), "title": str(a.get("title", ""))}
            candidates.append(entry)

        extracted_url = _find_best_link(candidates)
        if not extracted_url:
            raise ScraperError("No valid .xlsb link found on Baker Hughes landing page.")

        target_url = BASE_URL + extracted_url if extracted_url.startswith("/") else extracted_url

        resp = await client._request("GET", target_url)
        excel_bytes = resp.content
        if not excel_bytes:
            raise RuntimeError("Empty response received from download")

        # Resolve Last-Modified
        lm_header = resp.headers.get("Last-Modified")
        if lm_header:
            parsed_dt = email.utils.parsedate_to_datetime(lm_header)
            iso_date = parsed_dt.strftime("%Y-%m-%d")
            remote_timestamp = parsed_dt.timestamp()
        else:
            iso_date = datetime.now(UTC).strftime("%Y-%m-%d")
            remote_timestamp = datetime.now(UTC).timestamp()

        # Staleness check against local disk
        latest_file = _get_latest_local_file()
        if latest_file is not None and latest_file.exists():
            local_timestamp = latest_file.stat().st_mtime
            if remote_timestamp <= local_timestamp:
                health.record_skipped(reason="Remote file not newer than local")
                return {"status": "skipped", "latest_date": iso_date}

        try:
            dt = datetime.strptime(iso_date, "%Y-%m-%d")
        except ValueError:
            dt = datetime.now()

        # Format output using .xlsb
        suffix = ".xlsb" if target_url.lower().endswith(".xlsb") else ".xlsx"
        out_path = RAW_DIR / str(dt.year) / f"{dt.month:02d}" / f"baker_hughes_{iso_date}{suffix}"

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
                "url": target_url,
            }

        return {"status": "failed"}

    # Attempt with standard UA
    try:
        async with HttpClient(rate_limit_per_second=1.0) as client:
            return await fetch_pipeline(client) or {"status": "failed"}
    except HttpClientError as e:
        status_code = getattr(e, "status", None)
        if status_code != 403:
            health.record_failure(error=str(e))
            return {"status": "failed", "error": str(e)}
        log.warning("Standard UA hit 403. Retrying with Mozilla fallback.")
    except Exception as e:
        health.record_failure(error=str(e))
        return {"status": "failed", "error": str(e)}

    # Attempt with fallback Mozilla UA
    try:
        async with HttpClient(rate_limit_per_second=1.0) as client:
            # Overwrite the default user-agent header
            client._client.headers["User-Agent"] = FALLBACK_UA
            return await fetch_pipeline(client) or {"status": "failed"}
    except Exception as e:
        health.record_failure(error=str(e))
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    result = asyncio.run(run())
    print(json.dumps(result, indent=2))
