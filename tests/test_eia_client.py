"""Tests for scrapers.eia_api.client — EIAClient."""

from __future__ import annotations

import httpx
import pytest

from scrapers.base.errors import HttpClientError
from scrapers.eia_api.client import EIAClient


@pytest.mark.asyncio
async def test_get_series_formats_query_string() -> None:
    """Test EIAClient.get_series correctly formats query string."""
    req_url = ""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal req_url
        req_url = str(request.url)
        return httpx.Response(200, json={"response": {"data": [{"value": 1}]}})

    client = EIAClient(
        api_key="TEST_KEY",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),  # type: ignore
    )

    # We bypass the standard HttpClient and inject a raw Mock httpx client into it for test
    # Actually, EIAClient expects HttpClient, let's inject transport properly.
    from scrapers.base.http_client import HttpClient

    client.client = HttpClient(backoff_base_seconds=0.0)
    client.client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async with client:
        await client.get_series(
            route="test-route",
            facets={"region": ["R1", "R2"]},
            frequency="weekly",
            start="2024-01-01",
            end=None,
            data_columns=["value"],
        )

    # Output formatting checks
    assert "api_key=TEST_KEY" in req_url
    assert "/test-route/data/" in req_url
    assert "facets%5Bregion%5D%5B%5D=R1" in req_url or "facets[region][]=R1" in req_url
    assert "frequency=weekly" in req_url
    assert "start=2024-01-01" in req_url


@pytest.mark.asyncio
async def test_get_latest_date_extracts_correctly() -> None:
    """Test get_latest_date correctly extracts the most recent period."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": {"data": [{"period": "2024-04-19"}]}})

    # Quick inline mock
    from scrapers.base.http_client import HttpClient

    base_client = HttpClient()
    base_client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    client = EIAClient(api_key="TEST", http_client=base_client)

    async with client:
        latest = await client.get_latest_date(route="test-route")

    assert latest == "2024-04-19"


@pytest.mark.asyncio
async def test_error_envelope_raises() -> None:
    """Test error envelope from EIA raises HttpClientError."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"error": "Invalid API key"})

    from scrapers.base.http_client import HttpClient

    base_client = HttpClient(backoff_base_seconds=0.0)
    base_client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    client = EIAClient(api_key="TEST", http_client=base_client)

    async with client:
        with pytest.raises(HttpClientError) as exc_info:
            await client.get_series("test")

        assert "Invalid API key" in str(exc_info.value)
