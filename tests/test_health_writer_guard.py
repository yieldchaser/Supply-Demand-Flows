"""Tests for the HealthWriter guard-failure escalation ladder.

Rule under test: a guard rejection (identity/tenant mismatch, cycle-pin on a
structurally valid response) is a DISTINCT failure class from infrastructure
errors. It writes status ``guard_failure`` (naming the guard + reason) and
escalates to ``fail`` after three consecutive occurrences. This is the
mechanical answer to the 2026-08 silent-scraper-death incidents, where
identity guards raised on legitimate AJAX deltas / renamed TSPs and the
generic ``failed``/``no_op`` health masked them until data went stale.

Negative-tested both arms: a single guard rejection stays ``guard_failure``
(warn-class, not ``fail``), and the third consecutive rejection escalates to
``fail``. A success or a real infra failure resets the streak.
"""

from __future__ import annotations

import json
from pathlib import Path

from scrapers.base.health_writer import HealthWriter


def _read(health_dir: Path, name: str) -> dict:
    return json.loads((health_dir / f"{name}.json").read_text(encoding="utf-8"))


def test_first_guard_failure_is_distinct_status(tmp_health_dir: Path) -> None:
    """A guard rejection records ``guard_failure`` with guard name + reason."""
    hw = HealthWriter(source_name="guard_a", health_dir=tmp_health_dir)
    hw.record_guard_failure(guard="identity", error="CTPL not found in response")

    data = _read(tmp_health_dir, "guard_a")
    assert data["status"] == "guard_failure"
    assert data["metadata"]["guard"] == "identity"
    assert data["metadata"]["reason"] == "CTPL not found in response"
    assert data["metadata"]["consecutive_guard_failures"] == 1


def test_repeated_guard_failures_escalate_to_fail_at_three(
    tmp_health_dir: Path,
) -> None:
    """Three consecutive guard failures escalate warn -> warn -> fail."""
    hw = HealthWriter(source_name="guard_b", health_dir=tmp_health_dir)
    hw.record_guard_failure(guard="identity", error="run 1")
    hw.record_guard_failure(guard="identity", error="run 2")
    data = _read(tmp_health_dir, "guard_b")
    assert data["status"] == "guard_failure"
    assert data["metadata"]["consecutive_guard_failures"] == 2

    hw.record_guard_failure(guard="identity", error="run 3")
    data = _read(tmp_health_dir, "guard_b")
    assert data["status"] == "fail"
    assert data["metadata"]["consecutive_guard_failures"] == 3
    assert "3 consecutive identity guard failures" in data["error"]


def test_success_resets_guard_streak(tmp_health_dir: Path) -> None:
    """A real success clears the guard-failure streak."""
    hw = HealthWriter(source_name="guard_c", health_dir=tmp_health_dir)
    hw.record_guard_failure(guard="identity", error="boom")
    hw.record_guard_failure(guard="identity", error="boom")
    hw.record_success(metadata={"rows_ingested": 5})

    state_path = tmp_health_dir / f"{hw._source_name}.state.json"
    assert not state_path.exists(), "streak state cleaned up after success"
    data = _read(tmp_health_dir, "guard_c")
    assert data["status"] == "ok"
    # streak cleared: next guard failure starts from scratch again
    hw.record_guard_failure(guard="identity", error="boom")
    data = _read(tmp_health_dir, "guard_c")
    assert data["status"] == "guard_failure"
    assert data["metadata"]["consecutive_guard_failures"] == 1


def test_infra_failure_also_clears_guard_streak(tmp_health_dir: Path) -> None:
    """A hard infra failure resets the guard streak (failure is louder)."""
    hw = HealthWriter(source_name="guard_d", health_dir=tmp_health_dir)
    hw.record_guard_failure(guard="identity", error="boom")
    state_path = tmp_health_dir / "guard_d.state.json"
    assert state_path.exists()  # guard streak recorded
    hw.record_failure(error="network timeout")
    # guard streak state removed after a real failure
    assert not state_path.exists(), "guard streak cleared by record_failure"
    health = _read(tmp_health_dir, "guard_d")
    assert health["status"] == "failed"


def test_guard_failure_distinct_from_no_op(tmp_health_dir: Path) -> None:
    """A guard failure does not feed the no-op ladder (separate counters)."""
    hw = HealthWriter(source_name="guard_e", health_dir=tmp_health_dir)
    hw.record_guard_failure(guard="identity", error="boom")
    hw.record_guard_failure(guard="identity", error="boom")
    hw.record_guard_failure(guard="identity", error="boom")  # -> fail on guard
    # but no no-op streak recorded
    state = json.loads(
        (tmp_health_dir / "guard_e.state.json").read_text(encoding="utf-8")
    )
    assert "consecutive_no_ops" not in state


def test_corrupt_state_tolerated(tmp_health_dir: Path) -> None:
    """An unreadable state file resets the guard streak, not a crash."""
    hw = HealthWriter(source_name="guard_f", health_dir=tmp_health_dir)
    (tmp_health_dir / "guard_f.state.json").write_text("{not json", encoding="utf-8")
    hw.record_guard_failure(guard="identity", error="boom")
    data = _read(tmp_health_dir, "guard_f")
    assert data["status"] == "guard_failure"
    assert data["metadata"]["consecutive_guard_failures"] == 1
