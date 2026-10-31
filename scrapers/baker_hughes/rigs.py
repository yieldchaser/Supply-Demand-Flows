"""
Baker Hughes NA Rig Count scraper.

Why curl_cffi and not httpx/requests:
  Baker Hughes runs behind Cloudflare with TLS fingerprint-based bot detection
  (JA3/JA4). Python's stdlib ssl + OpenSSL (used by httpx AND requests) has
  well-known fingerprints that Cloudflare throttles to the point of timeout.
  Windows native curl works (via SChannel's distinct fingerprint), and
  curl_cffi wraps libcurl with Chrome/Firefox fingerprint impersonation
  for the same result from Python.

Why a Safari fingerprint (last verified: 2026-10-31):
  Since ~September 2026 the origin's edge resets every Chrome-fingerprinted
  connection at the HTTP/2 layer:
      curl: (92) HTTP/2 stream 1 was not closed cleanly: INTERNAL_ERROR (err 2)
  This is deterministic (not transient): every chrome*/edge* profile fails,
  and it is not a header problem — overriding or omitting any header does not
  help. The safari17_0 profile passes the same edge and fetches the report.
  IMPORTANT: do not send a Chrome User-Agent string with the Safari profile,
  and do not send headers the browser would not send (e.g. an explicit
  ``Referer`` on the landing-page navigation) — the fingerprint/UA mismatch is
  what trips the reset. curl_cffi already sends a browser-accurate header set
  for the impersonated profile, so we leave navigation headers alone.

  If the Safari profile starts failing too, probe other profiles with
  ``curl_cffi.requests.get(URL, impersonate=<profile>)`` before changing the
  selector — the parse layer is rarely the thing that breaks here.

Selector strategy (last verified: 2026-10-20):
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
import time
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

# Browser profiles tried in order. Safari first — Chrome-family fingerprints
# currently get their HTTP/2 stream reset by the origin's edge (see module doc).
IMPERSONATE_PROFILES = ("safari17_0", "safari17_2_ios", "chrome124")
ATTEMPTS_PER_PROFILE = 2
RETRY_BACKOFF_SECONDS = 5.0

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


def _fetch_once(impersonate: str) -> bytes:
    """One full landing-page + download pass with a single browser profile."""
    with cffi_requests.Session(impersonate=impersonate) as session:
        # Navigation step: no custom headers. curl_cffi already applies the
        # profile's browser-accurate header set; overriding them desyncs the
        # fingerprint and the edge resets the stream.
        r = session.get(LANDING_URL, timeout=TIMEOUT_SECONDS)
        r.raise_for_status()

        download_url = _select_current_weekly_link(r.content)

        # Same session so cookies from the landing page are preserved.
        r2 = session.get(download_url, timeout=TIMEOUT_SECONDS)
        r2.raise_for_status()

        if not r2.content or len(r2.content) < 1000:
            raise ScraperError(
                f"Baker Hughes download too small ({len(r2.content)} bytes) - "
                f"likely an error page, not an Excel file."
            )

        return r2.content


def _fetch_and_download() -> bytes:
    """Sync helper - retries transient transport failures across profiles.

    The origin's edge intermittently resets connections (curl error 92/56/35)
    and occasionally 403s. A single weekly run must not fail on that, so each
    profile is retried a couple of times and then the next profile is tried.
    """
    errors: list[str] = []
    for impersonate in IMPERSONATE_PROFILES:
        for attempt in range(1, ATTEMPTS_PER_PROFILE + 1):
            try:
                content = _fetch_once(impersonate)
                if attempt > 1 or impersonate != IMPERSONATE_PROFILES[0]:
                    logger.info(
                        f"Baker Hughes fetch succeeded with {impersonate} (attempt {attempt})"
                    )
                return content
            except ScraperError:
                # Selector failures will not be fixed by retrying — surface now.
                raise
            except Exception as e:
                msg = f"{impersonate} attempt {attempt}: {type(e).__name__}: {e}"
                logger.warning(f"Baker Hughes fetch failed - {msg}")
                errors.append(msg)
                if attempt < ATTEMPTS_PER_PROFILE:
                    time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise ScraperError(
        "Baker Hughes fetch failed on every profile after retries: " + " | ".join(errors)
    )


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
