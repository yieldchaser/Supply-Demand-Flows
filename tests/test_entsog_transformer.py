"""ENTSOG transformer: de-duplication, completeness and route totals."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from transformers.entsog import build, complete_sum, find_duplicates, transform
from transformers.errors import TransformError


def _config(tmp_path: Path) -> Path:
    cfg = {
        "routes": {
            "norway": {"label": "Norway", "points": [["A", "P1", "a"], ["B", "P1", "a twin"], ["C", "P2", "c"]]},
            "lng": {"label": "LNG", "points": [["D", "L1", "terminal 1", "DE"], ["E", "L2", "terminal 2", "FR"]]},
        }
    }
    p = tmp_path / "points.json"
    p.write_text(json.dumps(cfg))
    return p


def _rows(op: str, pt: str, values: list[float | None], start: date = date(2025, 1, 1)) -> list[dict]:
    return [
        {"operator": op, "point": pt, "gas_day": (start + timedelta(days=i)).isoformat(), "kwh": v * 1e6, "unit": "kWh/d"}
        for i, v in enumerate(values)
        if v is not None
    ]


def test_identical_reports_of_one_meter_count_once(tmp_path: Path) -> None:
    n = 60
    a = [100 + i % 7 for i in range(n)]
    rows = _rows("A", "P1", a) + _rows("B", "P1", a) + _rows("C", "P2", [50.0] * n)
    rows += _rows("D", "L1", [10.0] * n) + _rows("E", "L2", [20.0] * n)
    df, report = build(rows, _config(tmp_path))
    assert report["duplicates"] == {"B|P1": "A|P1"}
    norway = df[df.series_id == "entsog_route_norway"].set_index("period")["value"]
    assert norway.iloc[0] == pytest.approx(100 + 50)
    total = df[df.series_id == "entsog_imports_total"].set_index("period")["value"]
    assert total.iloc[0] == pytest.approx(150 + 30)
    fr = df[df.series_id == "entsog_lng_fr"].set_index("period")["value"]
    assert fr.iloc[0] == pytest.approx(20)


def test_complementary_shares_are_both_kept() -> None:
    idx = pd.date_range("2025-01-01", periods=60)
    wide = pd.DataFrame({"x": [100.0 + i for i in range(60)], "y": [80.0 + 2 * i for i in range(60)]}, index=idx)
    assert find_duplicates(wide, ["x", "y"]) == {}


def test_a_missing_report_blanks_the_day_instead_of_understating() -> None:
    idx = pd.date_range("2025-01-01", periods=30)
    a = [10.0] * 30
    b = [5.0] * 30
    for i in range(10, 15):  # a 5-day gap: longer than the interpolation limit
        b[i] = float("nan")
    wide = pd.DataFrame({"a": a, "b": b}, index=idx)
    s = complete_sum(wide, ["a", "b"])
    assert s.iloc[0] == 15
    assert s.iloc[12] != s.iloc[12]  # NaN, not 10


def test_short_gaps_are_interpolated_and_idle_gaps_are_zero(tmp_path: Path) -> None:
    n = 60
    a = [100.0] * n
    a[20] = None  # 1-day reporting gap while flowing -> interpolated
    lng = [0.0] * 10 + [None] * 20 + [0.0] * 30  # silent while idle -> zero
    rows = _rows("A", "P1", a) + _rows("C", "P2", [50.0] * n)
    rows += _rows("D", "L1", lng) + _rows("E", "L2", [20.0] * n)
    df, _ = build(rows, _config(tmp_path))
    total = df[df.series_id == "entsog_imports_total"].set_index("period")["value"]
    assert len(total) == n
    assert total.loc["2025-01-21"] == pytest.approx(170)
    assert total.loc["2025-01-15"] == pytest.approx(170)


def test_a_new_terminal_does_not_blank_the_weeks_before_it_started(tmp_path: Path) -> None:
    n = 60
    rows = _rows("A", "P1", [100.0] * n) + _rows("C", "P2", [50.0] * n) + _rows("D", "L1", [10.0] * n)
    rows += _rows("E", "L2", [None] * 40 + [20.0] * 20)
    df, _ = build(rows, _config(tmp_path))
    lng = df[df.series_id == "entsog_route_lng"].set_index("period")["value"]
    assert lng.loc["2025-01-30"] == pytest.approx(10)
    assert lng.loc["2025-02-20"] == pytest.approx(30)


def test_newest_day_stays_blank_until_every_operator_reports(tmp_path: Path) -> None:
    n = 60
    rows = _rows("A", "P1", [100.0] * n) + _rows("C", "P2", [50.0] * (n - 1))
    rows += _rows("D", "L1", [10.0] * n) + _rows("E", "L2", [20.0] * n)
    df, _ = build(rows, _config(tmp_path))
    norway = df[df.series_id == "entsog_route_norway"]
    assert norway["period"].max() == (date(2025, 1, 1) + timedelta(days=n - 2)).isoformat()


def test_partial_payload_is_refused(tmp_path: Path) -> None:
    raw = tmp_path / "raw.json"
    raw.write_text(json.dumps({"batches_failed": ["2025..:P1+3"], "data": [{"x": 1}]}))
    with pytest.raises(TransformError, match="partial"):
        transform(raw, tmp_path / "out.parquet")


def test_curated_routes_carry_no_identical_series() -> None:
    """Live guard: no two route series in the curated file are the same data."""
    p = Path("data/curated/entsog.parquet")
    if not p.exists():
        pytest.skip("no curated ENTSOG file")
    df = pd.read_parquet(p)
    wide = df[df.series_id.str.startswith("entsog_route_")].pivot_table(index="period", columns="series_id", values="value")
    assert find_duplicates(wide, list(wide.columns)) == {}
    total = df[df.series_id == "entsog_imports_total"].set_index("period")["value"]
    routes = wide.loc[total.index].sum(axis=1)
    assert (routes - total).abs().max() < 0.01
