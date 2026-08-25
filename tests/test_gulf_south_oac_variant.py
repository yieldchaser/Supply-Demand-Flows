"""Regression: the double-space 'Operationally  Available Capacity' header
variant (present in live Boardwalk CSVs) must still emit _oac_ series."""

import json
from pathlib import Path

import pandas as pd

from transformers.gulf_south import transform


def test_double_space_oac_header_still_emits_oac_series(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    rows = [
        {
            "TSP Name": "GULF SOUTH PIPELINE CO, L.P.",
            "TSP": "119107110",
            "Post Date/Time": "08/24/2026 03:11:12 PM",
            "Effective Gas Day": "20260823",
            "Effective Time": "09:00",
            "LineCode": "GS",
            "Loc": "24329",
            "Loc Name": "STRATTON RIDGE",
            "Loc Zn": "SLA",
            "Loc Purp Desc": "Delivery",
            "Loc/QTI": "DPQ",
            "Flow Ind": "D",
            "Design Capacity": "2860000",
            "Operating Capacity": "2860000",
            "Total Scheduled Quantity": "920149",
            # NOTE: two spaces between Operationally and Available — the
            # exact live-CSV variant that silently dropped OAC series.
            "Operationally  Available Capacity": "8617",
            "IT": "N",
            "All Qty Avail": "Y",
            "Quantity Not Available Reason": "",
            "Meas Basis Desc": "BZ",
        }
    ]
    (raw_dir / "2026-08-23_ID3.json").write_text(
        json.dumps(
            {
                "fetched_at": "2026-08-24T20:11:00Z",
                "source": "boardwalk",
                "cycle": "ID3",
                "gas_day": "2026-08-23",
                "posted_at": "08/24/2026 03:11:12 PM",
                "row_count": 1,
                "data": rows,
            }
        ),
        encoding="utf-8",
    )
    curated = tmp_path / "gulf_south.parquet"
    transform(raw_dir=raw_dir, curated_parquet_path=curated)

    df = pd.read_parquet(curated)
    sids = set(df["series_id"])
    assert "gulf_south_sq_24329_d_id3" in sids
    assert "gulf_south_oac_24329_d_id3" in sids
    oac_row = df[df["series_id"] == "gulf_south_oac_24329_d_id3"].iloc[0]
    assert oac_row["value"] == 8617.0
