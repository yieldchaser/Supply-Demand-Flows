"""Async HTTP client with deterministic retry and rate limiting.

Why:
    Every external API call in Blue Tide needs consistent retry semantics,
    rate-limit awareness, and structured logging.  Centralising this here
    prevents each scraper from re-inventing the wheel.

What:
    ``HttpClient`` wraps ``httpx.AsyncClient`` with:
    * Deterministic exponential backoff (``delay = base * 2^attempt``, no
      jitter).
    * A simple token-bucket rate limiter (minimum gap between requests).
    * A shared ``User-Agent`` identifying the observatory.
    * Automatic JSON parsing for ``get_json`` / ``post_json``.

Failure modes:
    * Retries on: ``TimeoutException``, ``NetworkError``, HTTP 429/500/502/
      503/504.
    * Does **not** retry 4xx (except 429) — those are caller bugs.
    * After retries are exhausted, raises ``HttpClientError`` carrying the
      URL, final status, attempt count, and elapsed time.
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from scrapers.base.errors import HttpClientError

log = logging.getLogger(__name__)

_DEFAULT_USER_AGENT = "BlueTide/0.1 (+https://github.com/yieldchaser/Supply-Demand-Flows)"

# Status codes that trigger a retry.
_RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})


class HttpClient:
    """Async HTTP client with deterministic retry and optional rate limiting.

    Why:
        See module docstring.

    What:
        Provides ``get_json``, ``get_bytes``, and ``post_json`` with built-in
        retry and rate-limit enforcement.

    Failure modes:
        ``HttpClientError`` on exhausted retries.  ``httpx`` exceptions for
        non-retryable errors (e.g. ``ConnectError`` to a permanently-down
        host is retried, but a 404 is not).
    """

    def __init__(
        self,
        *,
        base_url: str | None = None,
        default_headers: dict[str, str] | None = None,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        backoff_base_seconds: float = 1.0,
        rate_limit_per_second: float | None = None,
        user_agent: str = _DEFAULT_USER_AGENT,
    ) -> None:
        headers = dict(default_headers or {})
        headers.setdefault("User-Agent", user_agent)

        self._max_retries = max_retries
        self._backoff_base = backoff_base_seconds
        self._rate_limit_gap: float | None = (
            1.0 / rate_limit_per_second if rate_limit_per_second else None
        )
        self._last_request_time: float = 0.0

        self._client = httpx.AsyncClient(
            base_url=base_url or "",
            headers=headers,
            timeout=httpx.Timeout(timeout_seconds),
            follow_redirects=True,
        )

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    async def __aenter__(self) -> HttpClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Release the underlying ``httpx.AsyncClient``.

        Why:
            Prevents resource leaks (open sockets/connections).

        What:
            Delegates to ``httpx.AsyncClient.aclose()``.

        Failure modes:
            None significant — safe to call multiple times.
        """
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_json(
        self,
        url: str,
        params: dict[str, str] | None = None,
    ) -> dict | list:  # type: ignore[type-arg]
        """GET *url*, parse response as JSON, return the result.

        Why:
            Most EIA / GIE / ENTSOG endpoints return JSON.

        What:
            Issues a GET with retry, parses the body as JSON.

        Failure modes:
            ``HttpClientError`` on retry exhaustion.
            ``json.JSONDecodeError`` if the body isn't valid JSON.
        """
        response = await self._request("GET", url, params=params)
        return response.json()  # type: ignore[no-any-return]

    async def get_bytes(
        self,
        url: str,
        params: dict[str, str] | None = None,
    ) -> bytes:
        """GET *url* and return the raw response body.

        Why:
            Some sources serve CSV, Excel, or binary blobs.

        What:
            Issues a GET with retry, returns ``response.content``.

        Failure modes:
            ``HttpClientError`` on retry exhaustion.
        """
        response = await self._request("GET", url, params=params)
        return response.content

    async def post_json(
        self,
        url: str,
        payload: dict[str, object],
        params: dict[str, str] | None = None,
    ) -> dict | list:  # type: ignore[type-arg]
        """POST JSON *payload* to *url*, parse and return the response.

        Why:
            A few APIs (e.g. EIA v2) require POST for queries.

        What:
            Issues a POST with retry, parses the body as JSON.

        Failure modes:
            ``HttpClientError`` on retry exhaustion.
            ``json.JSONDecodeError`` if the body isn't valid JSON.
        """
        response = await self._request("GET", url, params=params, json_body=payload)
        return response.json()  # type: ignore[no-any-return]

    # ------------------------------------------------------------------
    # Internal retry / rate-limit engine
    # ------------------------------------------------------------------

    async def _enforce_rate_limit(self) -> None:
        """Sleep if necessary to respect the configured rate limit."""
        if self._rate_limit_gap is None:
            return
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < self._rate_limit_gap:
            await asyncio.sleep(self._rate_limit_gap - elapsed)
        self._last_request_time = time.monotonic()

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, object] | None = None,
    ) -> httpx.Response:
        """Execute an HTTP request with retry and rate-limit enforcement."""
        attempts = 0
        last_status: int | None = None
        last_reason: str = ""
        t0 = time.monotonic()

        while True:
            attempts += 1
            await self._enforce_rate_limit()

            try:
                if json_body is not None:
                    response = await self._client.post(
                        url,
                        params=params,
                        json=json_body,
                    )
                else:
                    response = await self._client.request(
                        method,
                        url,
                        params=params,
                    )

                last_status = response.status_code

                # Success — return immediately.
                if response.is_success:
                    return response

                # Non-retryable 4xx (except 429).
                if 400 <= last_status < 500 and last_status != 429:
                    raise HttpClientError(
                        url=url,
                        status=last_status,
                        attempts=attempts,
                        elapsed_s=time.monotonic() - t0,
                        reason=f"Non-retryable HTTP {last_status}",
                    )

                # Retryable status?
                if last_status in _RETRYABLE_STATUS_CODES:
                    last_reason = f"HTTP {last_status}"
                else:
                    # Unexpected ≥500 not in our set — still fail fast.
                    raise HttpClientError(
                        url=url,
                        status=last_status,
                        attempts=attempts,
                        elapsed_s=time.monotonic() - t0,
                        reason=f"Unexpected HTTP {last_status}",
                    )

            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_status = None
                last_reason = f"{type(exc).__name__}: {exc}"

            # If we've used all retries, bail.
            if attempts > self._max_retries:
                raise HttpClientError(
                    url=url,
                    status=last_status,
                    attempts=attempts,
                    elapsed_s=time.monotonic() - t0,
                    reason=last_reason,
                )

            # Deterministic exponential backoff: 1, 2, 4, 8 …
            delay = self._backoff_base * (2 ** (attempts - 1))
            log.warning(
                "Retry %d/%d for %s — %s — sleeping %.1fs",
                attempts,
                self._max_retries,
                url,
                last_reason,
                delay,
            )
            await asyncio.sleep(delay)
