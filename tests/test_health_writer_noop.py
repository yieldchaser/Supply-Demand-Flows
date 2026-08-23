"""Tests for the HealthWriter no-op escalation ladder.

Rule under test: a run that processed zero records and wrote nothing must
NOT be stamped ``ok``. First no-op run -> WARN with a reason string; three
or more consecutive no-op runs -> FAIL.
"""

from __future__ import annotations

import json
from pathlib import Path

from scrapers.base.health_writer import HealthWriter


def _read(health_dir: Path, name: str) -> dict:
    return json.loads((health_dir / f"{name}.json").read_text(encoding="utf-8"))


def test_first_noop_run_is_warn(tmp_health_dir: Path) -> None:
    """A single zero-records run records WARN, not ok."""
    hw = HealthWriter(source_name="noop_a", health_dir=tmp_health_dir)
    hw.record_no_op(reason="no postings matched requested cycle/day")

    data = _read(tmp_health_dir, "noop_a")
    assert data["status"] == "warn"
    assert "no postings" in data["error"]
    assert data["metadata"]["consecutive_no_ops"] == 1
    assert "reason" in data["metadata"]


def test_repeated_noops_escalate_to_fail_at_three(tmp_health_dir: Path) -> None:
    """Consecutive no-op runs escalate: warn, warn, fail."""
    hw = HealthWriter(source_name="noop_b", health_dir=tmp_health_dir)
    hw.record_no_op(reason="nothing new")
    hw.record_no_op(reason="nothing new")
    data = _read(tmp_health_dir, "noop_b")
    assert data["status"] == "warn"
    assert data["metadata"]["consecutive_no_ops"] == 2

    hw.record_no_op(reason="nothing new")
    data = _read(tmp_health_dir, "noop_b")
    assert data["status"] == "fail"
    assert data["metadata"]["consecutive_no_ops"] == 3
    assert "3 consecutive no-op" in data["error"]


def test_success_resets_noop_streak(tmp_health_dir: Path) -> None:
    """Any real success clears the no-op streak."""
    hw = HealthWriter(source_name="noop_c", health_dir=tmp_health_dir)
    hw.record_no_op(reason="nothing new")
    hw.record_no_op(reason="nothing new")
    hw.record_success(metadata={"rows_ingested": 5})

    state_path = tmp_health_dir / f"{hw._source_name}.state.json"
    assert not state_path.exists(), "streak state cleaned up after success"
    data = _read(tmp_health_dir, "noop_c")
    assert data["status"] == "ok"

    # Next no-op starts from scratch again.
    hw.record_no_op(reason="quiet day")
    data = _read(tmp_health_dir, "noop_c")
    assert data["status"] == "warn"
    assert data["metadata"]["consecutive_no_ops"] == 1


def test_failure_also_clears_noop_state(tmp_health_dir: Path) -> None:
    """A hard failure resets the streak (the failure itself is louder)."""
    hw = HealthWriter(source_name="noop_d", health_dir=tmp_health_dir)
    hw.record_no_op(reason="nothing new")
    hw.record_failure(error="boom")

    state_path = tmp_health_dir / f"{hw._source_name}.state.json"
    assert not state_path.exists()


def test_corrupt_state_file_tolerated(tmp_health_dir: Path) -> None:
    """An unreadable streak file is treated as no streak, not a crash."""
    hw = HealthWriter(source_name="noop_e", health_dir=tmp_health_dir)
    (tmp_health_dir / "noop_e.state.json").write_text("{not json", encoding="utf-8")
    hw.record_no_op(reason="nothing new")
    data = _read(tmp_health_dir, "noop_e")
    assert data["status"] == "warn"
    assert data["metadata"]["consecutive_no_ops"] == 1


def test_explicit_skips_do_not_count_as_no_ops(tmp_health_dir: Path) -> None:
    """record_skipped stays informational — it does not feed the ladder."""
    hw = HealthWriter(source_name="noop_f", health_dir=tmp_health_dir)
    hw.record_skipped(reason="staleness gate held")
    hw.record_skipped(reason="staleness gate held")
    hw.record_skipped(reason="staleness gate held")
    data = _read(tmp_health_dir, "noop_f")
    assert data["status"] == "skipped"
