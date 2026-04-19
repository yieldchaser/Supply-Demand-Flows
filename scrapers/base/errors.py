"""Blue Tide exception hierarchy.

Why:
    Typed exceptions let callers distinguish between retriable network hiccups,
    permanent data-source failures, and staleness-gate skips without catching
    bare ``Exception``.

What:
    A single inheritance chain rooted at ``BlueTideError`` so any handler can
    catch broadly or narrowly.

Failure modes:
    None — this module only defines exception types and carries no I/O.
"""

from __future__ import annotations


class BlueTideError(Exception):
    """Base exception for all Blue Tide errors."""


class ScraperError(BlueTideError):
    """Base exception for scraper failures."""


class HttpClientError(ScraperError):
    """HTTP client failure after retries exhausted.

    Attributes:
        url: The request URL that failed.
        status: The last HTTP status code received, or ``None`` if the failure
            was a network/timeout error.
        attempts: Total number of attempts made (initial + retries).
        elapsed_s: Wall-clock seconds from first attempt to final failure.
        reason: Human-readable description of the root cause.
    """

    def __init__(
        self,
        url: str,
        status: int | None,
        attempts: int,
        elapsed_s: float,
        reason: str,
    ) -> None:
        self.url = url
        self.status = status
        self.attempts = attempts
        self.elapsed_s = elapsed_s
        self.reason = reason
        super().__init__(
            f"HTTP failure for {url}: {reason} "
            f"(status={status}, attempts={attempts}, elapsed={elapsed_s:.2f}s)"
        )


class PlaywrightClientError(ScraperError):
    """Playwright failure — browser launch, navigation, or selector timeout."""


class StalenessGateError(ScraperError):
    """Raised when a scraper detects no new data and exits cleanly.

    This is *not* a real error — it is used for control flow so that
    health writers can record a "skipped" status rather than a failure.
    """
