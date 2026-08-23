"""Tests for Gulf South Pipeline OAC scraper and transformer.

Test strategy:
    - All HTTP calls are mocked via unittest.mock.
    - Filesystem writes go to pytest tmp_path.
    - Zero live API calls.
"""

from __future__ import annotations

import base64
import json
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from scrapers.energy_transfer.gulf_south import (
    extract_csv_tracker_ids,
    fetch_oac_csv,
    parse_oac_csv,
    run,
)
from transformers.gulf_south import transform

GAS_DAY = date(2026, 5, 22)
CYCLE = "TIMELY"

# Sample OAC CSV content matching the actual schema
_SAMPLE_CSV = (
    "\ufeffTSP Name,TSP,Post Date/Time,Effective Gas Day,Effective Time,LineCode,Loc,Loc Name,Loc Zn,Loc Purp Desc,Loc/QTI,Flow Ind,Design Capacity,Operating Capacity,Total Scheduled Quantity,Operationally Available Capacity,IT,All Qty Avail,Quantity Not Available Reason,Meas Basis Desc\n"
    '"Gulf South Pipeline Company, LLC",078444247,20260522 21:53:00,20260522,22:00:00,625,24329,Stratton Ridge (To Freeport Lng),SYS,Delivery Location,DPQ,D,1779129,1779129,920149,858980,,Y,,MMBtu\n'
    '"Gulf South Pipeline Company, LLC",078444247,20260522 21:53:00,20260522,22:00:00,625,24471,Butane Injection Stratton Ridge,SYS,Receipt Location,RPQ,R,7762,7762,3712,4050,,Y,,MMBtu\n'
)

# Base64 encoded version of _SAMPLE_CSV
_SAMPLE_B64 = base64.b64encode(_SAMPLE_CSV.encode("utf-8")).decode("utf-8")

# Sample postings list JSON returned by reporting API
_SAMPLE_POSTINGS = [
    {
        "infoPostName": "Operational Capacity",
        "tspId": 1,
        "datetimePostingEffective": "2026-05-22T21:54:00+00:00",
        "cycleCode": "ID3",
        "description": "05/22/2026 Intraday 3",
        "reportFiles": [
            {
                "infoPostDocumentTypeTitle": "CSV Documents",
                "infoPostTrackerID": 820202,
                "fileName": "OACY-1-20260522-ID3-20260522215400.csv",
                "infoPostDocumentOrder": 2,
            },
            {
                "infoPostDocumentTypeTitle": "Adobe Acrobat PDF",
                "infoPostTrackerID": 820203,
                "fileName": "OACY-1-20260522-ID3-20260522215403.pdf",
                "infoPostDocumentOrder": 1,
            },
        ],
    }
]


def _write_raw(
    path: Path,
    rows: list[dict[str, Any]],
    cycle: str = CYCLE,
    posted_at: str = "2026-05-22T21:54:00Z",
) -> None:
    """Write a synthetic raw JSON file to *path*."""
    payload: dict[str, Any] = {
        "fetched_at": "2026-05-22T22:00:00Z",
        "tsp_id": 1,
        "cycle": cycle,
        "gas_day": "2026-05-22",
        "posted_at": posted_at,
        "row_count": len(rows),
        "data": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


# -----------------------------------------------------------------------------
# Scraper unit tests
# -----------------------------------------------------------------------------


def test_parse_oac_csv() -> None:
    """OAC CSV parsed properly to raw rows; preserving raw MMBtu/Dth quantities."""
    rows = parse_oac_csv(_SAMPLE_CSV)
    assert len(rows) == 2

    # Assert Stratton Ridge fields
    stratton = [r for r in rows if r["Loc"] == "24329"][0]
    assert stratton["Loc Name"] == "Stratton Ridge (To Freeport Lng)"
    assert stratton["Flow Ind"] == "D"
    assert stratton["Total Scheduled Quantity"] == "920149"
    assert stratton["Operationally Available Capacity"] == "858980"
    assert stratton["Meas Basis Desc"] == "MMBtu"


@pytest.mark.asyncio
async def test_base64_bom_decode() -> None:
    """BOM (\ufeff) is cleanly stripped and base64 text is decoded successfully."""
    mock_client = AsyncMock()
    mock_client.get_bytes.return_value = _SAMPLE_B64.encode("utf-8")

    csv_text = await fetch_oac_csv(mock_client, 820202)
    assert csv_text.startswith("TSP Name")
    assert "\ufeff" not in csv_text


def test_extract_csv_tracker_id() -> None:
    """Correct infoPostTrackerID extracted for 'CSV Documents' document type."""
    extracted = extract_csv_tracker_ids(_SAMPLE_POSTINGS)
    assert len(extracted) == 1
    assert extracted[0]["tracker_id"] == 820202
    assert extracted[0]["cycle"] == "ID3"
    assert extracted[0]["gas_day"] == "2026-05-22"
    assert extracted[0]["posted_at"] == "2026-05-22T21:54:00+00:00"


@pytest.mark.asyncio
async def test_cycle_attached_from_posting(tmp_path: Path) -> None:
    """Scraper run correctly extracts cycle code from posting list and writes payload."""
    with (
        patch("scrapers.energy_transfer.gulf_south.HealthWriter"),
        patch("scrapers.energy_transfer.gulf_south.HttpClient") as mock_client_cls,
    ):
        mock_client = AsyncMock()
        mock_client.post_json.return_value = {"postings": _SAMPLE_POSTINGS}
        mock_client.get_bytes.return_value = _SAMPLE_B64.encode("utf-8")
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client_cls.return_value = mock_client

        with patch("scrapers.energy_transfer.gulf_south.RAW_DIR", tmp_path):
            result = await run(cycle="ID3", gas_day=GAS_DAY)

            assert result["status"] == "ok"
            assert result["processed_count"] == 1

            # Verify file exists
            out_file = tmp_path / "2026-05-22_ID3.json"
            assert out_file.exists()

            payload = json.loads(out_file.read_text(encoding="utf-8"))
            assert payload["cycle"] == "ID3"
            assert payload["gas_day"] == "2026-05-22"
            assert len(payload["data"]) == 2


# -----------------------------------------------------------------------------
# Transformer unit tests
# -----------------------------------------------------------------------------


def test_transformer_generates_both_series(tmp_path: Path) -> None:
    """Transformer generates both TSQ and OAC series and preserves raw Dth values."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "curated.parquet"

    # Pre-parse sample csv rows
    rows = parse_oac_csv(_SAMPLE_CSV)
    _write_raw(raw_dir / "2026-05-22_TIMELY.json", rows, cycle="TIMELY")

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)
    assert result["rows"] == 4  # 2 locations * 2 series (sq + oac)
    assert result["series_count"] == 4

    df = pd.read_parquet(out_parquet)
    assert len(df) == 4
    assert df["source"].unique()[0] == "boardwalk"
    assert df["unit"].unique()[0] == "Dth/d"

    # Assert values
    stratton_sq = df[df["series_id"] == "gulf_south_sq_24329_d_timely"].iloc[0]
    assert stratton_sq["value"] == 920149.0
    assert stratton_sq["series_name"] == "Gulf South TSQ Stratton Ridge (To Freeport Lng) [D] (TIMELY)"

    stratton_oac = df[df["series_id"] == "gulf_south_oac_24329_d_timely"].iloc[0]
    assert stratton_oac["value"] == 858980.0
    assert stratton_oac["series_name"] == "Gulf South OAC Stratton Ridge (To Freeport Lng) [D] (TIMELY)"


def test_transformer_dual_leg_yields_two_rows(tmp_path: Path) -> None:
    """A meter posting BOTH R and D in one cycle produces TWO series.

    Regression guard for the 2026-08 dual-leg collision: the leg that
    survived used to depend on which row happened to be seen last.
    """
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "curated.parquet"

    rows = parse_oac_csv(_SAMPLE_CSV)
    # Duplicate Stratton Ridge with the opposite leg + different quantity.
    r_leg = dict(rows[0])
    r_leg["Flow Ind"] = "R"
    r_leg["Total Scheduled Quantity"] = "11111"
    _write_raw(raw_dir / "2026-05-22_TIMELY.json", [*rows, r_leg], cycle="TIMELY")

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)
    # 2 locations × 2 legs × 2 kinds (sq + oac) for loc 24329's two rows +
    # single-leg loc 10128 → the R duplicate of Stratton survives as its own
    # series instead of overwriting the D leg. Fixture: loc1 D (2 series),
    # loc2 D (2), plus our R leg (2) = 6 rows.
    assert result["rows"] == 6

    df = pd.read_parquet(out_parquet)
    stratton_sq = sorted(df[df["series_id"].str.contains("_sq_24329_")]["series_id"])
    assert stratton_sq == [
        "gulf_south_sq_24329_d_timely",
        "gulf_south_sq_24329_r_timely",
    ]
    r_row = df[df["series_id"] == "gulf_south_sq_24329_r_timely"].iloc[0]
    assert r_row["value"] == 11111.0


def test_transformer_dedupe_keeps_latest_posting(tmp_path: Path) -> None:
    """When duplicate (series_id, period) exists, keep the one with the latest Post Date/Time."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "curated.parquet"

    # Row versions (simulating different posts)
    row_v1 = {
        "Loc": "24329",
        "Loc Name": "Stratton Ridge",
        "Flow Ind": "D",
        "Total Scheduled Quantity": "500000",
        "Operationally Available Capacity": "800000",
        "Post Date/Time": "20260522 12:00:00",
    }
    row_v2 = {
        "Loc": "24329",
        "Loc Name": "Stratton Ridge",
        "Flow Ind": "D",
        "Total Scheduled Quantity": "920000",
        "Operationally Available Capacity": "850000",
        "Post Date/Time": "20260522 21:00:00",
    }

    _write_raw(
        raw_dir / "2026-05-22_TIMELY_v1.json",
        [row_v1],
        cycle="TIMELY",
        posted_at="2026-05-22T12:00:00Z",
    )
    _write_raw(
        raw_dir / "2026-05-22_TIMELY_v2.json",
        [row_v2],
        cycle="TIMELY",
        posted_at="2026-05-22T21:00:00Z",
    )

    transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    df = pd.read_parquet(out_parquet)
    # Deduplicated to 1 TSQ and 1 OAC series
    assert len(df) == 2

    sq_row = df[df["series_id"] == "gulf_south_sq_24329_d_timely"].iloc[0]
    assert sq_row["value"] == 920000.0  # Kept v2 (920k) over v1 (500k)
