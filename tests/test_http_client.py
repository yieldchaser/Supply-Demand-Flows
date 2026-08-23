"""Tests for scrapers.base.http_client — HttpClient."""

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest

from scrapers.base.errors import HttpClientError
from scrapers.base.http_client import HttpClient

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client(transport: httpx.MockTransport, **kwargs: Any) -> HttpClient:
    """Build an HttpClient whose internal httpx.AsyncClient uses *transport*."""
    client = HttpClient(
        backoff_base_seconds=0.0,  # speed up tests — no real sleeping
        **kwargs,
    )
    # Swap the real transport for our mock.
    client._client = httpx.AsyncClient(transport=transport)
    return client


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_json_success() -> None:
    """Successful GET returns parsed JSON."""
    expected = {"data": [1, 2, 3]}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=expected)

    async with _make_client(httpx.MockTransport(handler)) as client:
        result = await client.get_json("https://example.com/api")
    assert result == expected


@pytest.mark.asyncio
async def test_retry_then_success() -> None:
    """Mock a server that fails twice (500) then succeeds — verify 3 calls."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(500)
        return httpx.Response(200, json={"ok": True})

    async with _make_client(httpx.MockTransport(handler), max_retries=3) as client:
        result = await client.get_json("https://example.com/retry")
    assert result == {"ok": True}
    assert call_count == 3


@pytest.mark.asyncio
async def test_retry_exhaustion_raises_http_client_error() -> None:
    """After max retries, HttpClientError is raised with correct attributes."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = _make_client(httpx.MockTransport(handler), max_retries=2)
    async with client:
        with pytest.raises(HttpClientError) as exc_info:
            await client.get_json("https://example.com/fail")

    err = exc_info.value
    assert err.url == "https://example.com/fail"
    assert err.status == 503
    assert err.attempts == 3  # initial + 2 retries
    assert err.elapsed_s >= 0


@pytest.mark.asyncio
async def test_4xx_not_retried() -> None:
    """4xx errors (except 429) are NOT retried — fail immediately."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(404)

    client = _make_client(httpx.MockTransport(handler), max_retries=3)
    async with client:
        with pytest.raises(HttpClientError) as exc_info:
            await client.get_json("https://example.com/missing")

    assert call_count == 1  # no retries
    assert exc_info.value.status == 404


@pytest.mark.asyncio
async def test_429_is_retried() -> None:
    """HTTP 429 IS retried (rate-limit response)."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(429)
        return httpx.Response(200, json={"ok": True})

    async with _make_client(httpx.MockTransport(handler), max_retries=3) as client:
        result = await client.get_json("https://example.com/ratelimited")
    assert result == {"ok": True}
    assert call_count == 2


@pytest.mark.asyncio
async def test_rate_limiting_enforced() -> None:
    """With rate_limit_per_second=2, three requests take > 0.9 seconds."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    client = HttpClient(
        rate_limit_per_second=2.0,
        backoff_base_seconds=0.0,
    )
    # Swap transport.
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    t0 = time.monotonic()
    async with client:
        await client.get_json("https://example.com/1")
        await client.get_json("https://example.com/2")
        await client.get_json("https://example.com/3")
    elapsed = time.monotonic() - t0

    # With rate=2/s, gap=0.5s, 3 requests need ≥ 2 gaps = 1.0s
    assert elapsed >= 0.9, f"Expected >= 0.9s, got {elapsed:.2f}s"


@pytest.mark.asyncio
async def test_context_manager_closes_client() -> None:
    """Verify the async context manager calls close (no resource leak)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    client = _make_client(httpx.MockTransport(handler))
    async with client:
        await client.get_json("https://example.com/ctx")

    # After exiting, the underlying client should be closed.
    assert client._client.is_closed


@pytest.mark.asyncio
async def test_403_retried_with_retryable_override() -> None:
    """403 twice then success — with retryable_status_codes={403}, succeeds."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(403)
        return httpx.Response(200, json={"ok": True})

    async with _make_client(
        httpx.MockTransport(handler),
        max_retries=3,
        retryable_status_codes=frozenset({403}),
        backoff_delays=(0.0, 0.0, 0.0),  # injected small delays — no real sleeping
    ) as client:
        result = await client.get_json("https://example.com/waf")

    assert result == {"ok": True}
    assert call_count == 3


@pytest.mark.asyncio
async def test_403_not_retried_by_default() -> None:
    """Without the override, a 403 raises immediately on the first attempt."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(403)

    client = _make_client(httpx.MockTransport(handler), max_retries=3)
    async with client:
        with pytest.raises(HttpClientError) as exc_info:
            await client.get_json("https://example.com/waf")

    assert call_count == 1  # no retries
    assert exc_info.value.status == 403
    assert exc_info.value.attempts == 1


@pytest.mark.asyncio
async def test_backoff_delays_honored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """backoff_delays replaces exponential backoff with literal durations."""
    recorded: list[float] = []

    async def fake_sleep(delay: float) -> None:
        recorded.append(delay)

    monkeypatch.setattr("scrapers.base.http_client.asyncio.sleep", fake_sleep)

    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            return httpx.Response(500)
        return httpx.Response(200, json={"ok": True})

    async with _make_client(
        httpx.MockTransport(handler),
        max_retries=3,
        backoff_delays=(5.0, 15.0, 45.0),
    ) as client:
        result = await client.get_json("https://example.com/backoff")

    assert result == {"ok": True}
    assert recorded == [5.0, 15.0, 45.0]


@pytest.mark.asyncio
async def test_backoff_delays_empty_tuple_means_no_delay() -> None:
    """backoff_delays=() must mean 'no retry delay', not IndexError."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            return httpx.Response(500)
        return httpx.Response(200, json={"ok": True})

    async with _make_client(
        httpx.MockTransport(handler),
        max_retries=3,
        backoff_delays=(),  # empty — previously crashed with IndexError
    ) as client:
        result = await client.get_json("https://example.com/empty-backoff")

    assert result == {"ok": True}
    assert call_count == 4


@pytest.mark.asyncio
async def test_backoff_delays_empty_tuple_exhausts_and_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """With backoff_delays=() every retry is immediate; exhaustion still raises."""

    async def fail_sleep(delay: float) -> None:
        raise AssertionError(f"no sleep expected with empty backoff_delays, got {delay}")

    monkeypatch.setattr("scrapers.base.http_client.asyncio.sleep", fail_sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = _make_client(httpx.MockTransport(handler), max_retries=2, backoff_delays=())
    async with client:
        with pytest.raises(HttpClientError) as exc_info:
            await client.get_json("https://example.com/fail")

    err = exc_info.value
    assert err.status == 503
    assert err.attempts == 3  # initial + 2 immediate retries
