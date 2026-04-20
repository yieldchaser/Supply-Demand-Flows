"""EIA v2 API client."""

from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv

from scrapers.base.errors import HttpClientError
from scrapers.base.http_client import HttpClient

load_dotenv()


def load_api_key_from_env() -> str:
    """Load the EIA_API_KEY from environment."""
    key = os.environ.get("EIA_API_KEY")
    if not key:
        raise RuntimeError("EIA_API_KEY not set in environment")
    return key


class EIAClient:
    """High-level EIA v2 API wrapper built on top of HttpClient."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://api.eia.gov/v2",
        http_client: HttpClient | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self._owns_client = http_client is None
        self.client = http_client or HttpClient(rate_limit_per_second=5.0)

    async def __aenter__(self) -> EIAClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP client if we own it."""
        if self._owns_client:
            await self.client.close()

    async def get_series(
        self,
        route: str,
        facets: dict[str, list[str]] | None = None,
        frequency: str | None = None,
        start: str | None = None,
        end: str | None = None,
        data_columns: list[str] | None = None,
        length: int = 5000,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Fetch data from an EIA v2 dataset route."""
        params: dict[str, Any] = {"api_key": self.api_key}
        if frequency:
            params["frequency"] = frequency
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if data_columns:
            params["data[]"] = data_columns
        if facets:
            for k, v in facets.items():
                params[f"facets[{k}][]"] = v
        params["length"] = str(length)
        params["offset"] = str(offset)

        url = f"{self.base_url}/{route.strip('/')}/data/"
        resp = await self.client.get_json(url, params=params)

        if isinstance(resp, dict) and "error" in resp:
            raise HttpClientError(
                url=url,
                status=None,
                attempts=1,
                elapsed_s=0.0,
                reason=str(resp["error"]),
            )
        # mypy requires accurate types, so ensure we return a dict
        if not isinstance(resp, dict):
            raise TypeError("Expected dict response from EIA JSON endpoint")

        return resp

    async def get_latest_date(
        self,
        route: str,
        facets: dict[str, list[str]] | None = None,
        frequency: str | None = None,
    ) -> str | None:
        """Fetch the most recent period from an EIA v2 dataset route."""
        params: dict[str, Any] = {"api_key": self.api_key}
        if frequency:
            params["frequency"] = frequency
        if facets:
            for k, v in facets.items():
                params[f"facets[{k}][]"] = v
        params["length"] = "1"
        params["sort[0][column]"] = "period"
        params["sort[0][direction]"] = "desc"

        url = f"{self.base_url}/{route.strip('/')}/data/"
        resp = await self.client.get_json(url, params=params)

        if isinstance(resp, dict) and "error" in resp:
            raise HttpClientError(
                url=url,
                status=None,
                attempts=1,
                elapsed_s=0.0,
                reason=str(resp["error"]),
            )

        if not isinstance(resp, dict):
            return None

        # Try to parse the response.data object cleanly
        resp_obj = resp.get("response", {})
        if not isinstance(resp_obj, dict):
            return None

        data = resp_obj.get("data", [])
        if not data or not isinstance(data, list):
            return None

        return str(data[0].get("period"))

    async def get_metadata(self, route: str) -> dict[str, Any]:
        """
        Why: EIA v2 routes often surprise us with different frequency/facet requirements.
        Hitting the route without /data/ returns the schema, letting us self-debug 400s.
        What: GETs {base_url}/{route}/ (no /data/ suffix), returns the 'response' envelope.
        Failure modes: propagates HttpClientError on network/401/etc.
        """
        params: dict[str, Any] = {"api_key": self.api_key}
        url = f"{self.base_url}/{route.strip('/')}/"
        resp = await self.client.get_json(url, params=params)

        if isinstance(resp, dict) and "error" in resp:
            raise HttpClientError(
                url=url,
                status=None,
                attempts=1,
                elapsed_s=0.0,
                reason=str(resp["error"]),
            )

        if not isinstance(resp, dict):
            raise TypeError("Expected dict response from EIA JSON endpoint")

        resp_obj = resp.get("response", {})
        if not isinstance(resp_obj, dict):
            return {}
        return resp_obj
