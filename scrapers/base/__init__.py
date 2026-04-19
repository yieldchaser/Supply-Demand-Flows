"""scrapers.base — public API surface for all base scraper infrastructure.

Importing from ``scrapers.base`` gives callers access to:

* ``HttpClient`` / ``HttpClientError`` — async HTTP with retry.
* ``PlaywrightClient`` / ``PlaywrightClientError`` — headless browser.
* ``HealthWriter`` — per-source health recording.
* ``safe_write_*`` family — atomic file writes.
* ``StatePreservingWriter`` — guarded write that never clobbers good data.
* ``BlueTideError`` / ``ScraperError`` / ``StalenessGateError`` — exception
  hierarchy.
"""

from scrapers.base.errors import (
    BlueTideError,
    HttpClientError,
    PlaywrightClientError,
    ScraperError,
    StalenessGateError,
)
from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.playwright_client import PlaywrightClient
from scrapers.base.safe_writer import (
    StatePreservingWriter,
    safe_write_bytes,
    safe_write_json,
    safe_write_parquet,
    safe_write_text,
)

__all__ = [
    "BlueTideError",
    "HttpClient",
    "HttpClientError",
    "HealthWriter",
    "PlaywrightClient",
    "PlaywrightClientError",
    "ScraperError",
    "StalenessGateError",
    "StatePreservingWriter",
    "safe_write_bytes",
    "safe_write_json",
    "safe_write_parquet",
    "safe_write_text",
]
