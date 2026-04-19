"""Tests for scrapers.base.safe_writer — StatePreservingWriter."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scrapers.base.safe_writer import StatePreservingWriter, safe_write_json


@pytest.mark.asyncio
async def test_guarded_write_does_not_overwrite_on_failure(tmp_path: Path) -> None:
    """When compute_fn raises, the existing file is untouched."""
    target = tmp_path / "data" / "output.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    safe_write_json(target, {"original": True})

    spw = StatePreservingWriter(source_name="test_guard")
    # Override health dir to keep tests isolated.
    spw._health._health_dir = tmp_path / "health"
    spw._health._health_file = spw._health._health_dir / "test_guard.json"
    spw._health._prev_file = spw._health._health_dir / "test_guard.prev.json"

    async def bad_compute() -> dict[str, Any]:
        msg = "network down"
        raise ConnectionError(msg)

    result = await spw.guarded_write(target, bad_compute)

    assert result is False
    loaded = json.loads(target.read_text(encoding="utf-8"))
    assert loaded == {"original": True}


@pytest.mark.asyncio
async def test_guarded_write_writes_on_success(tmp_path: Path) -> None:
    """When compute_fn succeeds, the result is written to path."""
    target = tmp_path / "data" / "success.json"

    spw = StatePreservingWriter(source_name="test_success")
    spw._health._health_dir = tmp_path / "health"
    spw._health._health_file = spw._health._health_dir / "test_success.json"
    spw._health._prev_file = spw._health._health_dir / "test_success.prev.json"

    async def good_compute() -> dict[str, Any]:
        return {"rows": 42}

    result = await spw.guarded_write(target, good_compute)

    assert result is True
    loaded = json.loads(target.read_text(encoding="utf-8"))
    assert loaded == {"rows": 42}


@pytest.mark.asyncio
async def test_health_recorded_on_success(tmp_path: Path) -> None:
    """Health file records 'ok' after a successful guarded write."""
    target = tmp_path / "data" / "health_ok.json"
    health_dir = tmp_path / "health"

    spw = StatePreservingWriter(source_name="health_ok")
    spw._health._health_dir = health_dir
    spw._health._health_file = health_dir / "health_ok.json"
    spw._health._prev_file = health_dir / "health_ok.prev.json"

    async def compute() -> dict[str, Any]:
        return {"val": 1}

    await spw.guarded_write(target, compute)

    health_file = health_dir / "health_ok.json"
    assert health_file.exists()
    health_data = json.loads(health_file.read_text(encoding="utf-8"))
    assert health_data["status"] == "ok"


@pytest.mark.asyncio
async def test_health_recorded_on_failure(tmp_path: Path) -> None:
    """Health file records 'failed' after compute_fn raises."""
    target = tmp_path / "data" / "health_fail.json"
    health_dir = tmp_path / "health"

    spw = StatePreservingWriter(source_name="health_fail")
    spw._health._health_dir = health_dir
    spw._health._health_file = health_dir / "health_fail.json"
    spw._health._prev_file = health_dir / "health_fail.prev.json"

    async def bad_compute() -> dict[str, Any]:
        msg = "oops"
        raise RuntimeError(msg)

    await spw.guarded_write(target, bad_compute)

    health_file = health_dir / "health_fail.json"
    assert health_file.exists()
    health_data = json.loads(health_file.read_text(encoding="utf-8"))
    assert health_data["status"] == "failed"
    assert "RuntimeError" in health_data["error"]


@pytest.mark.asyncio
async def test_empty_result_treated_as_failure(tmp_path: Path) -> None:
    """None or empty dict/list from compute_fn is treated as failure."""
    health_dir = tmp_path / "health"

    for label, empty_val in [("none", None), ("empty_dict", {}), ("empty_list", [])]:
        target = tmp_path / "data" / f"{label}.json"

        spw = StatePreservingWriter(source_name=label)
        spw._health._health_dir = health_dir
        spw._health._health_file = health_dir / f"{label}.json"
        spw._health._prev_file = health_dir / f"{label}.prev.json"

        async def compute(val: Any = empty_val) -> Any:
            return val

        result = await spw.guarded_write(target, compute)
        assert result is False, f"Expected failure for {label}"

        health_file = health_dir / f"{label}.json"
        health_data = json.loads(health_file.read_text(encoding="utf-8"))
        assert health_data["status"] == "failed"
