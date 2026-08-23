"""Tests for Cheniere scraper, backfill plumbing, and transformer.

Test strategy:
    - All HTTP calls are mocked via unittest.mock.
    - Filesystem writes go to pytest tmp_path.
    - Zero live API calls.

Fixtures mirror the real GetCapacity response (verified live: tspNo=200 →
CHENIERE CREOLE TRAIL PIPELINE design 650000; tspNo=400 → CHENIERE CORPUS
CHRISTI PIPELINE design 35000; odd server casing preserved verbatim).
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from scrapers.cheniere.backfill import (
    BackfillConfig,
    CheniereBackfill,
    load_checkpoint,
    load_curated_periods,
)
from scrapers.cheniere.client import (
    TSP_CORPUS_CHRISTI,
    TSP_CREOLE_TRAIL,
    default_headers,
    fetch_capacity,
    parse_capacity_rows,
)
from transformers.cheniere import transform

# -----------------------------------------------------------------------------
# Fixtures — real GetCapacity row shape (odd casing is the server's)
# -----------------------------------------------------------------------------

_CTPL_ROW = {
    "tsp": "800742780",
    "tsP_NAME": "CHENIERE CREOLE TRAIL PIPELINE, L.P. ",
    "cycle": "Intraday 3",
    "postinG_DT_TIME": "2026-08-22T22:15:21.827",
    "avaiL_CAP_EFF_DT_TIME": "2026-08-22T22:00:00",
    "avaiL_CAP_END_DT": "2026-08-23T00:00:00",
    "caP_TYPE_DESC": "Operationally available unscheduled capacity",
    "loc": "CT109413",
    "loC_NAME": "CT109413-GILLIS-TETCO-R",
    "loC_PURP_DESC": "Receipt Location",
    "loC_QTI": "RPQ",
    "meaS_BASIS": "BZ",
    "it": "Y",
    "alL_QTY_AVAIL": "Y",
    "desigN_OPER_CAP": 650000.0,
    "opeR_CAP": 650000.0,
    "scheD_QTY": 37484.0,
    "qtY_AVAIL": 612516.0,
    "floW_IND": "R",
    "row_Number": "Row_Number",
}

_CCPL_ROW = {
    **_CTPL_ROW,
    "tsp": "079841991",
    "tsP_NAME": "CHENIERE CORPUS CHRISTI PIPELINE, L.P.",
    "loc": "CC100221",
    "loC_NAME": "CC100221-CORPUS CHRISTI-CCLIQ-R",
    "desigN_OPER_CAP": 35000.0,
    "opeR_CAP": 35000.0,
    "scheD_QTY": 0.0,
    "qtY_AVAIL": 35000.0,
}

_MALFORMED_ROW = {"loc": "", "cycle": "", "avaiL_CAP_EFF_DT_TIME": ""}


def _write_raw(path: Path, tsp_no: int, rows: list[dict[str, Any]], gas_day: str) -> None:
    """Write a synthetic raw JSON payload matching the scraper's shape."""
    payload: dict[str, Any] = {
        "fetched_at": "2026-08-23T14:00:00Z",
        "source": "cheniere",
        "tsp_no": tsp_no,
        "gas_day": gas_day,
        "cycle_id": None,
        "row_count": len(rows),
        "rows": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


# -----------------------------------------------------------------------------
# Scraper unit tests
# -----------------------------------------------------------------------------


def test_default_headers_carry_origin() -> None:
    """Origin header (mandatory for the API) is present."""
    headers = default_headers()
    assert headers["Origin"] == "https://lngconnection.cheniere.com"


@pytest.mark.asyncio
async def test_fetch_capacity_builds_query() -> None:
    """GetCapacity query uses tspNo/beginDate MM/DD/YYYY/cycleId null/locationId 0."""
    mock_client = AsyncMock()
    mock_client.get_json.return_value = {"report": [_CTPL_ROW], "beginDate": "8/22/2026"}

    payload = await fetch_capacity(mock_client, TSP_CREOLE_TRAIL, date(2026, 8, 22))

    args = mock_client.get_json.call_args
    assert args.args[0] == "/api/Capacity/GetCapacity"
    assert args.kwargs["params"] == {
        "tspNo": "200",
        "beginDate": "08/22/2026",
        "cycleId": "null",
        "locationId": "0",
    }
    assert len(payload["report"]) == 1


def test_parse_capacity_rows_normalises_and_skips_malformed() -> None:
    """Rows normalised to snake_case; malformed rows skipped; numbers coerced."""
    rows = parse_capacity_rows({"report": [_CTPL_ROW, _MALFORMED_ROW]})
    assert len(rows) == 1
    row = rows[0]
    assert row["loc"] == "CT109413"
    assert row["period"] == "2026-08-22"
    assert row["design_oper_cap"] == 650000.0
    assert row["sched_qty"] == 37484.0
    assert row["qty_avail"] == 612516.0
    assert row["flow_ind"] == "R"
    # Non-dict garbage never raises.
    assert parse_capacity_rows({"report": [None, 42]}) == []


# -----------------------------------------------------------------------------
# Transformer unit tests
# -----------------------------------------------------------------------------


def test_transformer_emits_all_series_per_pipeline(tmp_path: Path) -> None:
    """Both pipelines → oac + sq + design series; sched_QTY 0 still emitted."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "curated.parquet"

    ctpl_parsed = parse_capacity_rows({"report": [_CTPL_ROW]})[0]
    ccpl_parsed = parse_capacity_rows({"report": [_CCPL_ROW]})[0]
    _write_raw(raw_dir / "2026-08-22_tsp200.json", TSP_CREOLE_TRAIL, [ctpl_parsed], "2026-08-22")
    _write_raw(raw_dir / "2026-08-22_tsp400.json", TSP_CORPUS_CHRISTI, [ccpl_parsed], "2026-08-22")

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    df = pd.read_parquet(out_parquet)
    assert result["rows"] == 6  # 2 pipelines × 3 series
    flow = "r" if ctpl_parsed["flow_ind"].upper() == "R" else "d"
    cc_flow = "r" if ccpl_parsed["flow_ind"].upper() == "R" else "d"
    assert set(df["series_id"]) == {
        f"creole_trail_oac_CT109413_{flow}_id3",
        f"creole_trail_sq_CT109413_{flow}_id3",
        f"creole_trail_design_CT109413_{flow}_id3",
        f"corpus_christi_oac_CC100221_{cc_flow}_id3",
        f"corpus_christi_sq_CC100221_{cc_flow}_id3",
        f"corpus_christi_design_CC100221_{cc_flow}_id3",
    }
    assert df["source"].unique()[0] == "cheniere"
    assert df["unit"].unique()[0] == "Dth/d"

    # Scales stay apart — no cross-pipeline normalisation.
    ctpl_oac = df[df["series_id"] == f"creole_trail_oac_CT109413_{flow}_id3"].iloc[0]
    ccpl_oac = df[df["series_id"] == f"corpus_christi_oac_CC100221_{cc_flow}_id3"].iloc[0]
    assert float(ctpl_oac["value"]) == 612516.0
    assert float(ccpl_oac["value"]) == 35000.0

    # Zero scheduled quantity on CCPL unscheduled posting is data, not absence.
    ccpl_sq = df[df["series_id"] == f"corpus_christi_sq_CC100221_{cc_flow}_id3"].iloc[0]
    assert float(ccpl_sq["value"]) == 0.0


def test_transformer_dual_leg_yields_two_rows(tmp_path: Path) -> None:
    """Same loc posting R and D rows in one cycle → both legs kept."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "curated.parquet"

    ctpl_parsed = parse_capacity_rows({"report": [_CTPL_ROW]})[0]
    d_leg = dict(ctpl_parsed)
    d_leg["flow_ind"] = "D"
    d_leg["sched_qty"] = "999999.0"
    _write_raw(raw_dir / "2026-08-22_tsp200.json", TSP_CREOLE_TRAIL,
               [ctpl_parsed, d_leg], "2026-08-22")

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)
    # one loc × 2 flows × 3 kinds = 6 rows — NOT 3 (no leg overwrite).
    assert result["rows"] == 6

    df = pd.read_parquet(out_parquet)
    sq_ids = sorted(
        sid for sid in df["series_id"] if sid.startswith("creole_trail_sq_CT109413_")
    )
    assert sq_ids == [
        "creole_trail_sq_CT109413_d_id3",
        "creole_trail_sq_CT109413_r_id3",
    ]


def test_transformer_accumulates_without_overwriting(tmp_path: Path) -> None:
    """Second run merges into history instead of clobbering it."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "curated.parquet"

    ctpl_parsed = [parse_capacity_rows({"report": [_CTPL_ROW]})[0]]
    _write_raw(raw_dir / "2026-08-22_tsp200.json", TSP_CREOLE_TRAIL, ctpl_parsed, "2026-08-22")
    transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)
    first = pd.read_parquet(out_parquet)
    assert len(first) == 3

    newer_row = {
        **_CTPL_ROW,
        "cycle": "Timely",
        "avaiL_CAP_EFF_DT_TIME": "2026-08-23T14:00:00",
        "postinG_DT_TIME": "2026-08-23T14:10:00.000",
    }
    newer_parsed = [parse_capacity_rows({"report": [newer_row]})[0]]
    _write_raw(
        raw_dir / "2026-08-23_tsp200.json", TSP_CREOLE_TRAIL, newer_parsed, "2026-08-23"
    )
    transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    merged = pd.read_parquet(out_parquet)
    assert len(merged) == 6
    assert set(merged["period"]) == {"2026-08-22", "2026-08-23"}


# -----------------------------------------------------------------------------
# Backfill unit tests
# -----------------------------------------------------------------------------


def test_load_curated_periods(tmp_path: Path) -> None:
    """Curated periods derived from the parquet; missing file → empty set."""
    parquet = tmp_path / "cheniere.parquet"
    pd.DataFrame({"period": ["2026-08-22T00:00:00", "2026-08-23"]}).to_parquet(parquet)
    periods = load_curated_periods(parquet)
    assert periods == {"2026-08-22", "2026-08-23"}
    assert load_curated_periods(tmp_path / "missing.parquet") == set()


def test_load_checkpoint_since_mismatch(tmp_path: Path) -> None:
    """Checkpoint with a different --since is ignored."""
    state_path = tmp_path / "_backfill_state.json"
    state_path.write_text(
        json.dumps({"since": "2026-01-01", "next_day": "2026-01-05"}), encoding="utf-8"
    )
    assert load_checkpoint(state_path, "2026-02-01") is None
    assert load_checkpoint(state_path, "2026-01-01") is not None
    assert load_checkpoint(tmp_path / "nope.json", "2026-01-01") is None


@pytest.mark.asyncio
async def test_backfill_walk_writes_raw_and_checkpoints(tmp_path: Path) -> None:
    """Walk fetches missing days and checkpoints past today after finishing."""
    raw_dir = tmp_path / "raw"
    since = date.today() - timedelta(days=2)
    config = BackfillConfig(
        since=since,
        raw_dir=raw_dir,
        curated_path=tmp_path / "curated.parquet",
        request_gap_seconds=0.0,
    )
    mock_client = AsyncMock()

    runner = CheniereBackfill(config=config, client=mock_client)
    with patch(
        "scrapers.cheniere.backfill.fetch_capacity",
        new=AsyncMock(return_value={"report": [_CTPL_ROW]}),
    ):
        summary = await runner.run(fresh=False)

    today = date.today()
    assert summary["days_seen"] == 3
    assert summary["fetched"] == 3
    # Two pipeline payloads per day.
    assert len(list(raw_dir.glob("*_tsp200.json"))) == 3
    assert len(list(raw_dir.glob("*_tsp400.json"))) == 3
    state = json.loads((raw_dir / "_backfill_state.json").read_text(encoding="utf-8"))
    assert state["next_day"] == (today + timedelta(days=1)).isoformat()

    # Resume with a finished checkpoint starts fresh at since (cursor > today).
    runner2 = CheniereBackfill(config=config, client=mock_client)
    with patch(
        "scrapers.cheniere.backfill.fetch_capacity",
        new=AsyncMock(return_value={"report": []}),
    ) as mock_fetch:
        summary2 = await runner2.run(fresh=False)
    assert summary2["days_seen"] == 0
    mock_fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_backfill_resumes_from_checkpoint_cursor(tmp_path: Path) -> None:
    """A mid-walk checkpoint resumes at next_day without re-fetching earlier days."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    since = date.today() - timedelta(days=2)
    mid_walk = date.today() - timedelta(days=1)
    state = {
        "since": since.isoformat(),
        "next_day": mid_walk.isoformat(),
        "fetched": 0,
        "errors_http": 0,
        "updated_at": "2026-08-01T00:00:00Z",
    }
    (raw_dir / "_backfill_state.json").write_text(json.dumps(state), encoding="utf-8")

    config = BackfillConfig(
        since=since,
        raw_dir=raw_dir,
        curated_path=tmp_path / "curated.parquet",
        request_gap_seconds=0.0,
    )
    mock_client = AsyncMock()

    runner = CheniereBackfill(config=config, client=mock_client)
    with patch(
        "scrapers.cheniere.backfill.fetch_capacity",
        new=AsyncMock(return_value={"report": [_CTPL_ROW]}),
    ) as mock_fetch:
        summary = await runner.run(fresh=False)

    # Only mid_walk and today are fetched — the earlier day is behind the cursor.
    assert summary["fetched"] == 2
    assert mock_fetch.await_count == 4  # 2 days × 2 pipelines
    final_state = json.loads((raw_dir / "_backfill_state.json").read_text(encoding="utf-8"))
    assert final_state["next_day"] == (date.today() + timedelta(days=1)).isoformat()
