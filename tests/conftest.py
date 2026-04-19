"""Shared pytest fixtures for the Blue Tide test suite."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest


@pytest.fixture()
def tmp_health_dir(tmp_path: Path) -> Path:
    """Return a ``data/health/`` directory under the pytest temp path."""
    health = tmp_path / "data" / "health"
    health.mkdir(parents=True, exist_ok=True)
    return health


@pytest.fixture()
def mock_httpx_client() -> httpx.AsyncClient:
    """Return an ``httpx.AsyncClient`` backed by a no-op mock transport.

    Tests that need specific responses should build their own transport
    using ``httpx.MockTransport`` — this fixture provides the baseline
    wiring.
    """

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(_handler)
    return httpx.AsyncClient(transport=transport)
