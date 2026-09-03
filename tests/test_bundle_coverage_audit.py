"""Tests for the bundle coverage audit and shrinkage guard.

Verifies:
  1. Active baseline row counts pass the audit cleanly.
  2. Perturbed shrunken row counts (e.g. gasnom dropping to 300 rows) fail with SystemExit.
  3. Empty/zero row counts fail with SystemExit.
"""
from __future__ import annotations

import pytest

from publishers.export_dashboard_json import (
    BUNDLE_ROW_BASELINES,
    _audit_bundle_coverage,
)


def test_bundle_coverage_audit_passes_on_live_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Audit passes when all sources match or exceed baseline rows."""
    monkeypatch.delenv("BLUETIDE_SKIP_COVERAGE_AUDIT", raising=False)
    clean_rows = dict(BUNDLE_ROW_BASELINES)
    _audit_bundle_coverage(clean_rows)


def test_bundle_coverage_audit_rejects_gasnom_shrinkage(monkeypatch: pytest.MonkeyPatch) -> None:
    """Audit fails loudly when gasnom shrinks from 5038 to 300 rows."""
    monkeypatch.delenv("BLUETIDE_SKIP_COVERAGE_AUDIT", raising=False)
    shrunken = dict(BUNDLE_ROW_BASELINES)
    shrunken["gasnom"] = 300  # Severe shrinkage

    with pytest.raises(SystemExit) as exc_info:
        _audit_bundle_coverage(shrunken)

    assert "gasnom: shipped 300 rows in bundle index, falling below shrinkage floor" in str(exc_info.value)


def test_bundle_coverage_audit_rejects_zero_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Audit fails loudly when any source ships zero rows."""
    monkeypatch.delenv("BLUETIDE_SKIP_COVERAGE_AUDIT", raising=False)
    zero_rows = dict(BUNDLE_ROW_BASELINES)
    zero_rows["bhe"] = 0

    with pytest.raises(SystemExit) as exc_info:
        _audit_bundle_coverage(zero_rows)

    assert "bhe: shipped 0 rows in the bundle index" in str(exc_info.value)
