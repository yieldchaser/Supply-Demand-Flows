"""Tests for scrapers.base.safe_writer — atomic file writing utilities."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from scrapers.base.safe_writer import (
    safe_write_bytes,
    safe_write_json,
)


def test_safe_write_json_creates_correct_content(tmp_path: Path) -> None:
    """safe_write_json creates a file with correctly-formatted JSON."""
    target = tmp_path / "output.json"
    data = {"key": "value", "count": 42}

    safe_write_json(target, data)

    assert target.exists()
    loaded = json.loads(target.read_text(encoding="utf-8"))
    assert loaded == data


def test_mid_write_failure_leaves_original_untouched(tmp_path: Path) -> None:
    """If os.replace raises mid-write, the original file is untouched."""
    target = tmp_path / "existing.json"
    original_data = {"original": True}
    safe_write_json(target, original_data)

    # Now attempt a second write that will fail during os.replace.
    with (
        mock.patch("scrapers.base.safe_writer.os.replace", side_effect=OSError("disk full")),
        pytest.raises(OSError, match="disk full"),
    ):
        safe_write_json(target, {"new": True})

    # Original data must be intact.
    loaded = json.loads(target.read_text(encoding="utf-8"))
    assert loaded == original_data


def test_parent_directory_auto_creation(tmp_path: Path) -> None:
    """safe_write_json creates parent directories if they don't exist."""
    target = tmp_path / "deep" / "nested" / "dir" / "out.json"
    safe_write_json(target, {"nested": True})

    assert target.exists()
    loaded = json.loads(target.read_text(encoding="utf-8"))
    assert loaded == {"nested": True}


def test_safe_write_bytes_roundtrips_binary(tmp_path: Path) -> None:
    """safe_write_bytes correctly round-trips arbitrary binary data."""
    target = tmp_path / "binary.bin"
    data = bytes(range(256))

    safe_write_bytes(target, data)

    assert target.read_bytes() == data


def test_tmpfile_cleaned_up_after_failure(tmp_path: Path) -> None:
    """On failure, the temp file is cleaned up — no leftover .tmp files."""
    target = tmp_path / "cleanup_test.json"

    with (
        mock.patch("scrapers.base.safe_writer.os.replace", side_effect=OSError("fail")),
        pytest.raises(OSError),
    ):
        safe_write_json(target, {"test": True})

    # No .tmp files should remain.
    tmp_files = list(tmp_path.glob("*.tmp.*"))
    assert tmp_files == [], f"Leftover tmp files: {tmp_files}"
