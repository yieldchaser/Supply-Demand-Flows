"""Tests for scrapers.base.health_writer — HealthWriter."""

from __future__ import annotations

import json
from pathlib import Path

from scrapers.base.health_writer import HealthWriter


def test_record_success_writes_correct_schema(tmp_health_dir: Path) -> None:
    """record_success writes JSON with status='ok' and correct fields."""
    hw = HealthWriter(source_name="test_source", health_dir=tmp_health_dir)
    hw.record_success(metadata={"rows_ingested": 100})

    path = tmp_health_dir / "test_source.json"
    assert path.exists()

    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["source"] == "test_source"
    assert data["status"] == "ok"
    assert data["error"] is None
    assert data["metadata"] == {"rows_ingested": 100}
    assert "timestamp_utc" in data
    assert data["timestamp_utc"].endswith("Z")


def test_record_failure_includes_error_string(tmp_health_dir: Path) -> None:
    """record_failure writes JSON with the error message."""
    hw = HealthWriter(source_name="fail_source", health_dir=tmp_health_dir)
    hw.record_failure(error="Connection timed out", metadata={"url": "https://example.com"})

    path = tmp_health_dir / "fail_source.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["status"] == "failed"
    assert data["error"] == "Connection timed out"
    assert data["metadata"] == {"url": "https://example.com"}


def test_record_skipped_has_correct_status(tmp_health_dir: Path) -> None:
    """record_skipped writes JSON with status='skipped'."""
    hw = HealthWriter(source_name="skip_source", health_dir=tmp_health_dir)
    hw.record_skipped(reason="no new data since last run")

    path = tmp_health_dir / "skip_source.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["status"] == "skipped"
    assert data["error"] == "no new data since last run"
    assert data["metadata"] is None


def test_prev_json_created_on_overwrite(tmp_health_dir: Path) -> None:
    """.prev.json is created when overwriting an existing health file."""
    hw = HealthWriter(source_name="rotate_source", health_dir=tmp_health_dir)

    # First write.
    hw.record_success(metadata={"round": 1})
    path = tmp_health_dir / "rotate_source.json"
    json.loads(path.read_text(encoding="utf-8"))

    # Second write — should rotate the first to .prev.json.
    hw.record_success(metadata={"round": 2})
    prev_path = tmp_health_dir / "rotate_source.prev.json"
    assert prev_path.exists()

    prev_data = json.loads(prev_path.read_text(encoding="utf-8"))
    assert prev_data["metadata"] == {"round": 1}

    current_data = json.loads(path.read_text(encoding="utf-8"))
    assert current_data["metadata"] == {"round": 2}


def test_metadata_is_preserved(tmp_health_dir: Path) -> None:
    """Metadata dict is faithfully serialised even with nested structures."""
    hw = HealthWriter(source_name="meta_source", health_dir=tmp_health_dir)
    meta = {
        "rows_ingested": 42,
        "latest_date": "2026-04-19",
        "nested": {"a": 1, "b": [2, 3]},
    }
    hw.record_success(metadata=meta)

    path = tmp_health_dir / "meta_source.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["metadata"] == meta
