"""Tests for Baker Hughes transformer."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
from openpyxl import Workbook

from transformers.baker_hughes import EXPECTED_COLUMNS, TransformError, transform


def create_mock_xlsx(
    path: Path,
    sheet_name: str = "NAM Weekly",
    modify_header: bool = False,
    add_bad_date: bool = False,
):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name

    # Rows 1-10 blank
    for _ in range(10):
        ws.append([])

    # Row 11 header
    header = list(EXPECTED_COLUMNS)
    if modify_header:
        header.remove("Rig Count Value")
    ws.append(header)

    if not modify_header:
        # Get column indices
        col_map = {name: idx for idx, name in enumerate(header)}

        def make_row(country, basin, drillfor, location, state, trajectory, date_val, rig_count):
            row = [None] * len(header)
            row[col_map["Country"]] = country
            row[col_map["Basin"]] = basin
            row[col_map["DrillFor"]] = drillfor
            row[col_map["Location"]] = location
            row[col_map["State/Province"]] = state
            row[col_map["Trajectory"]] = trajectory
            row[col_map["US_PublishDate"]] = date_val
            row[col_map["Rig Count Value"]] = rig_count
            return row

        # Period 1: 2024-04-12
        p1 = datetime(2024, 4, 12)
        # Period 2: 2024-04-19
        p2 = "19-04-2024"

        ws.append(
            make_row("UNITED STATES", "Permian", "Oil", "Land", "Texas", "Horizontal", p1, 10)
        )
        ws.append(
            make_row("UNITED STATES", "Permian", "Gas", "Land", "New Mexico", "Directional", p1, 5)
        )
        ws.append(
            make_row(
                "UNITED STATES", "Marcellus", "Gas", "Land", "Pennsylvania", "Horizontal", p1, 20
            )
        )
        ws.append(make_row("CANADA", "Alberta", "Oil", "Land", "Alberta", "Horizontal", p1, 15))

        ws.append(
            make_row("UNITED STATES", "Permian", "Oil", "Land", "Texas", "Horizontal", p2, 12)
        )
        ws.append(make_row("CANADA", "Alberta", "Gas", "Land", "Alberta", "Vertical", p2, 8))

        if add_bad_date:
            ws.append(
                make_row(
                    "UNITED STATES",
                    "Eagle Ford",
                    "Oil",
                    "Land",
                    "Texas",
                    "Horizontal",
                    "invalid_date",
                    50,
                )
            )
            ws.append(
                make_row(
                    "UNITED STATES", "Eagle Ford", "Oil", "Land", "Texas", "Horizontal", None, 50
                )
            )

    wb.save(path)


def test_transform_produces_rollup_and_granular(tmp_path: Path):
    raw_path = tmp_path / "raw.xlsx"
    out_path = tmp_path / "curated.parquet"
    create_mock_xlsx(raw_path)

    res = transform(raw_path, out_path)

    assert res["rows"] > 0
    assert res["rollup_count"] > 0
    assert res["granular_count"] == 6

    df = pd.read_parquet(out_path)
    assert len(df) == res["rows"]

    # Check that both rollup and granular series exist
    rollups = df[df["series_id"].str.startswith("bh_rollup_")]
    granulars = df[df["series_id"].str.startswith("bh_granular_")]

    assert not rollups.empty
    assert not granulars.empty


def test_missing_sheet_raises_error(tmp_path: Path):
    raw_path = tmp_path / "raw.xlsx"
    out_path = tmp_path / "curated.parquet"
    create_mock_xlsx(raw_path, sheet_name="Wrong Sheet")

    with pytest.raises(TransformError, match="Missing expected sheet"):
        transform(raw_path, out_path)


def test_missing_header_column_raises_error(tmp_path: Path):
    raw_path = tmp_path / "raw.xlsx"
    out_path = tmp_path / "curated.parquet"
    create_mock_xlsx(raw_path, modify_header=True)

    with pytest.raises(TransformError, match="missing expected columns"):
        transform(raw_path, out_path)


def test_unparseable_dates_skipped(tmp_path: Path, caplog):
    raw_path = tmp_path / "raw.xlsx"
    out_path = tmp_path / "curated.parquet"
    create_mock_xlsx(raw_path, add_bad_date=True)

    res = transform(raw_path, out_path)

    # Granular count should still be 6 because the bad date rows are skipped
    assert res["granular_count"] == 6
    assert "rows had unparseable US_PublishDate, skipped." in caplog.text


def test_na_total_equals_us_plus_canada(tmp_path: Path):
    raw_path = tmp_path / "raw.xlsx"
    out_path = tmp_path / "curated.parquet"
    create_mock_xlsx(raw_path)

    transform(raw_path, out_path)
    df = pd.read_parquet(out_path)

    periods = df["period"].unique()
    for p in periods:
        us_total = df[(df["period"] == p) & (df["series_id"] == "bh_rollup_us_total")][
            "value"
        ].sum()
        ca_total = df[(df["period"] == p) & (df["series_id"] == "bh_rollup_canada_total")][
            "value"
        ].sum()
        na_total = df[(df["period"] == p) & (df["series_id"] == "bh_rollup_na_total")][
            "value"
        ].sum()

        assert na_total == us_total + ca_total


def test_basin_rollups_only_emitted_when_rig_count_positive(tmp_path: Path):
    raw_path = tmp_path / "raw.xlsx"
    out_path = tmp_path / "curated.parquet"
    create_mock_xlsx(raw_path)

    transform(raw_path, out_path)
    df = pd.read_parquet(out_path)

    # In period 1, Permian and Marcellus have rigs. Eagle Ford does not.
    p1 = pd.Timestamp("2024-04-12").date()
    p1_df = df[df["period"] == p1]

    series_ids = p1_df["series_id"].tolist()
    assert "bh_rollup_basin_permian" in series_ids
    assert "bh_rollup_basin_marcellus" in series_ids
    assert "bh_rollup_basin_eagle_ford" not in series_ids


def test_basin_drill_split_rollups_present_and_sum_to_total(tmp_path: Path):
    """Verify per-basin gas/oil rollups exist and sum equals total rollup."""
    raw_path = tmp_path / "raw.xlsx"
    out_path = tmp_path / "curated.parquet"
    create_mock_xlsx(raw_path)

    transform(raw_path, out_path)
    df = pd.read_parquet(out_path)

    rows = df.to_dict(orient="records")

    permian_total_rows = [r for r in rows if r["series_id"] == "bh_rollup_basin_permian"]
    permian_gas_rows = [r for r in rows if r["series_id"] == "bh_rollup_basin_permian_gas"]
    permian_oil_rows = [r for r in rows if r["series_id"] == "bh_rollup_basin_permian_oil"]

    assert len(permian_gas_rows) > 0
    assert len(permian_oil_rows) > 0

    by_period_total = {str(r["period"]): r["value"] for r in permian_total_rows}
    by_period_gas = {str(r["period"]): r["value"] for r in permian_gas_rows}
    by_period_oil = {str(r["period"]): r["value"] for r in permian_oil_rows}

    for period, total in by_period_total.items():
        gas = by_period_gas.get(period, 0)
        oil = by_period_oil.get(period, 0)
        # Allow small discrepancy for Miscellaneous rigs not split
        assert gas + oil <= total
        assert gas + oil >= total * 0.85
