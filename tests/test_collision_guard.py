"""Tests for the collision guard (validators/collision.py)."""

from __future__ import annotations

import pandas as pd

from validators.collision import check_collision


def _frame(rows: list[tuple[str, str, float]]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["series_id", "period", "value"])


def test_clean_frame_passes() -> None:
    df = _frame([("a_id1", "2026-01-01", 1.0), ("b_id1", "2026-01-01", 2.0)])
    res = check_collision(df, {}, {})
    assert res["severity"] == "PASS"
    assert res["details"]["keys"] == 2


def test_identical_duplicates_pass_below_warn_bar() -> None:
    df = _frame(
        [
            ("a_id1", "2026-01-01", 5.0),
            ("a_id1", "2026-01-01", 5.0),
            ("b_id1", "2026-01-01", 2.0),
        ]
    )
    res = check_collision(df, {"collision": {"identical_dup_warn_pct": 90.0}}, {})
    assert res["severity"] == "PASS"


def test_identical_duplicates_warn_above_bar() -> None:
    df = _frame(
        [
            ("a_id1", "2026-01-01", 5.0),
            ("a_id1", "2026-01-01", 5.0),
            ("b_id1", "2026-01-01", 2.0),
        ]
    )
    res = check_collision(df, {"collision": {"identical_dup_warn_pct": 10.0}}, {})
    assert res["severity"] == "WARN"


def test_dual_leg_shape_fails_with_named_keys() -> None:
    """The exact Gulf South bug: R and D legs sharing one series_id."""
    df = _frame(
        [
            ("gulf_south_sq_50202_id3", "2026-08-22", 111071.0),
            ("gulf_south_sq_50202_id3", "2026-08-22", 82548.0),
            ("gulf_south_sq_24329_id3", "2026-08-22", 920149.0),
        ]
    )
    res = check_collision(df, {}, {})
    assert res["severity"] == "FAIL"
    assert res["details"]["colliding_keys"] == 1
    assert any("50202" in ex for ex in res["details"]["examples"])
    assert "SILENT-OVERWRITE" in res["message"]


def test_ignore_globs_exclude_series() -> None:
    df = _frame(
        [
            ("gulf_south_sq_50202_id3", "2026-08-22", 111071.0),
            ("gulf_south_sq_50202_id3", "2026-08-22", 82548.0),
        ]
    )
    cfg = {"collision": {"ignore_series_globs": ["gulf_south_sq_5*"]}}
    res = check_collision(df, cfg, {})
    assert res["severity"] == "SKIPPED" or res["severity"] == "PASS"


def test_missing_columns_skip() -> None:
    df = pd.DataFrame({"foo": [1]})
    assert check_collision(df, {}, {})["severity"] == "SKIPPED"
    assert check_collision(pd.DataFrame(), {}, {})["severity"] == "SKIPPED"
