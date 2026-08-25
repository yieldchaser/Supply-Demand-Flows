"""Shared pytest fixtures for the Blue Tide test suite."""

from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest

# Real health directory at repo root. Any write to it during a test run is a
# trapped bug — tests must redirect via BLUETIDE_HEALTH_DIR (set below).
REAL_HEALTH_DIR = Path("data/health").resolve()


@pytest.fixture(autouse=True, scope="session")
def _isolate_health_dir(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Redirect ALL HealthWriter output to a session temp dir.

    Why:
        The 2026-08-25 incident: running the full suite from repo root let
        an un-patched HealthWriter overwrite real ``data/health/*.json`` with
        fixture payloads, silently rotting production health state. This
        fixture sets BLUETIDE_HEALTH_DIR so every writer (patched or not)
        lands in a throwaway dir instead — the trap is removed regardless of
        whether a given test remembers to patch.
    """
    health_tmp = tmp_path_factory.mktemp("health")
    REAL_HEALTH_DIR.mkdir(parents=True, exist_ok=True)
    os.environ["BLUETIDE_HEALTH_DIR"] = str(health_tmp)
    yield
    # Leave the env var for any later in-process use; it points at a temp dir.


@pytest.fixture(autouse=True, scope="session")
def _forbid_real_health_writes() -> None:
    """Fail the session if any test writes a health file to repo root.

    Defense in depth: even with the redirect above, assert nothing NEW reaches
    the real directory so a future regression can't hide behind it. We snapshot
    the existing (committed) health files at setup and only flag files created
    or modified *during* this run — legitimate committed health JSON is ignored.
    """
    from scrapers.base import safe_writer  # noqa: F401  (ensure importable)

    REAL_HEALTH_DIR.mkdir(parents=True, exist_ok=True)
    baseline = {
        p.name: p.stat().st_mtime_ns
        for p in REAL_HEALTH_DIR.glob("*.json")
        if p.is_file()
    }
    yield
    # After all tests, any health file created/modified during the run is a
    # violation. Committed files (untouched) are ignored.
    allowed = {"_integrity_state.json", "sent_alerts.json"}
    violations = []
    for p in REAL_HEALTH_DIR.glob("*.json"):
        if not p.is_file():
            continue
        if p.name in allowed:
            continue
        mtime = p.stat().st_mtime_ns
        if p.name not in baseline or mtime != baseline[p.name]:
            violations.append(p.name)
    if violations:
        raise AssertionError(
            f"Tests wrote REAL health files (trap not fully closed): {sorted(violations)}"
        )


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
