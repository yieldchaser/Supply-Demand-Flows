"""Tests for BHE GT&S (EGTS) scraper, backfill plumbing, and transformer.

Test strategy:
    - All HTTP calls are mocked via unittest.mock.
    - Filesystem writes go to pytest tmp_path.
    - Zero live API calls.

Fixtures are synthesised from the real EGTS OAC blob schema (verified live
against notice 1011665, gas day 2026-05-16: Loc 40704 "EGTS - LOUDOUN",
Operating Capacity 741280, Cove Point R-row TSQ 0).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from scrapers.bhe.backfill import (
    BackfillConfig,
    BheBackfill,
    load_checkpoint,
    load_curated_gas_day_cycles,
    month_windows,
)
from scrapers.bhe.client import (
    build_search_payload,
    extract_csv_postings,
    fetch_postings,
    is_cove_point_row,
    parse_oac_csv,
)
from transformers.bhe import transform

# -----------------------------------------------------------------------------
# Fixtures — real EGTS OAC CSV shape (subset of columns, real anchor values)
# -----------------------------------------------------------------------------

_SAMPLE_CSV_HEADER = (
    "Posting Date,Posting Time,TSP,TSP Name,CycleDesc,Eff Gas Day,Eff Time,"
    "Meas Basis Desc,Loc Purp Desc,Loc/QTI Desc,Interconnect Party Name,OIA,"
    "Loc,Loc Name,Operating Capacity,Total Scheduled Quantity,"
    "Operationally Available Capacity,Design Capacity,"
    "All Quantities Available Indicator,Quantity Not Available Reason,Flow Ind,IT"
)

# Gas day 05/16/2026 Intraday 3 — the Cove Point anchor row carries TSQ 0.
_SAMPLE_CSV = (
    _SAMPLE_CSV_HEADER
    + "\n"
    + "05/16/2026,21:38,072888858,Eastern Gas Transmission and Storage,Intraday 3,"
    "05/16/2026,22:00:00,MMBtu,Receipt Location,RPQ,COVE POINT LNG LP,,40704,EGTS - LOUDOUN,"
    "741280,0,741280,777600,,Y,R,Y\n"
    + "05/16/2026,21:38,072888858,Eastern Gas Transmission and Storage,Intraday 3,"
    "05/16/2026,22:00:00,MMBtu,Delivery Location,DPQ,COVE POINT LNG LP,,40704,EGTS - LOUDOUN,"
    "200000,153050,46950,200000,,Y,D,Y\n"
    + "05/16/2026,21:38,072888858,Eastern Gas Transmission and Storage,Intraday 3,"
    "05/16/2026,22:00:00,MMBtu,Receipt Location,RPQ,SOME OTHER SHIPPER,,41010,EGTS - OTHER,"
    "500000,123456,376544,500000,,Y,R,Y\n"
)

_SAMPLE_POSTING = {
    "category": "Capacity",
    "subcategory": "Operationally Available",
    "contents": [
        {
            "blobPath": "egts/postings/1011665/0/egts-1011665.csv",
            "blobUrl": "https://infopost.bhegts.com/docs/egts/postings/1011665/0/egts-1011665.csv",
            "type": "csv",
        },
        {
            "blobPath": "egts/postings/1011665/0/egts-1011665.pdf",
            "blobUrl": "https://infopost.bhegts.com/docs/egts/postings/1011665/0/egts-1011665.pdf",
            "type": "pdf",
        },
    ],
    "effectiveDate": "2026-05-17T03:00:00",
    "id": "6a1cb16ce979b2134ec8b3a1",
    "noticeId": 1011665,
    "postedDate": "2026-05-16T21:38:00.000",
    "revision": 0,
    "subject": "MAY Capacity Available 05/16/2026 Intraday 3",
}

_NON_OAC_POSTING = {
    "category": "Capacity",
    "subcategory": "Unsubscribed",
    "noticeId": 999,
    "subject": "MAY Capacity Unsubscribed 05/16/2026",
    "postedDate": "2026-05-16T21:38:00.000",
    "effectiveDate": "2026-05-17T03:00:00",
    "contents": [{"blobUrl": "https://x/y.csv", "type": "csv"}],
}


def _write_raw(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    cycle: str = "ID3",
    gas_day: str = "2026-05-16",
    posted_at: str = "2026-05-16T21:38:00.000",
) -> None:
    """Write a synthetic raw JSON payload matching the scraper's shape."""
    payload: dict[str, Any] = {
        "fetched_at": "2026-08-23T14:00:00Z",
        "source": "bhe",
        "tsp": "egts",
        "notice_id": 1011665,
        "cycle": cycle,
        "gas_day": gas_day,
        "posted_at": posted_at,
        "row_count": len(rows),
        "data": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


# -----------------------------------------------------------------------------
# Scraper unit tests
# -----------------------------------------------------------------------------


def test_build_search_payload_dates_iso_or_none() -> None:
    """Search body matches the frontend contract: yyyy-MM-dd strings or nulls."""
    assert build_search_payload(date(2026, 5, 1), date(2026, 5, 31)) == {
        "category": "Capacity",
        "subcategory": "Operationally Available",
        "beginDate": "2026-05-01",
        "endDate": "2026-05-31",
    }
    assert build_search_payload(None, None) == {
        "category": "Capacity",
        "subcategory": "Operationally Available",
        "beginDate": None,
        "endDate": None,
    }


def test_extract_csv_postings_filters_oac_and_non_csv() -> None:
    """OAC CSV postings kept; other subcategories and PDF-only postings dropped."""
    postings = extract_csv_postings({"postings": [_SAMPLE_POSTING, _NON_OAC_POSTING]})
    assert len(postings) == 1
    assert postings[0]["notice_id"] == 1011665
    assert postings[0]["gas_day"] == "2026-05-17"  # effective-date bookkeeping value
    assert postings[0]["csv_url"].endswith(".csv")

    # Bare-list payloads normalise too.
    assert len(extract_csv_postings([_SAMPLE_POSTING])) == 1
    # Garbage shapes never raise.
    assert extract_csv_postings({"postings": [None, 42, {}]}) == []
    assert extract_csv_postings("nope") == []


def test_parse_oac_csv_strips_and_drops_placeholder_rows() -> None:
    """CSV parsed to clean dicts; rows without Loc dropped."""
    noisy = _SAMPLE_CSV + "\n,,,,,,,,,,,,,,,,,,,,,\n"
    rows = parse_oac_csv(noisy)
    assert len(rows) == 3
    cp_receipt = [r for r in rows if r["Flow Ind"] == "R" and r["Loc"] == "40704"][0]
    assert cp_receipt["Interconnect Party Name"] == "COVE POINT LNG LP"
    assert cp_receipt["Total Scheduled Quantity"] == "0"
    assert cp_receipt["Operating Capacity"] == "741280"


def test_is_cove_point_row_rule() -> None:
    """Filter rule: Interconnect COVE POINT LNG LP AND Flow Ind R."""
    rows = parse_oac_csv(_SAMPLE_CSV)
    by_flow = {r["Flow Ind"]: r for r in rows if r["Loc"] == "40704"}
    assert is_cove_point_row(by_flow["R"]) is True
    assert is_cove_point_row(by_flow["D"]) is False  # right shipper, wrong direction
    other = [r for r in rows if r["Loc"] == "41010"][0]
    assert is_cove_point_row(other) is False  # right direction, wrong shipper


@pytest.mark.asyncio
async def test_fetch_postings_posts_and_normalises() -> None:
    """fetch_postings POSTs the search payload and returns filtered postings."""
    mock_client = AsyncMock()
    mock_client.post_json.return_value = {"postings": [_SAMPLE_POSTING]}

    postings = await fetch_postings(mock_client, date(2026, 5, 1), date(2026, 5, 31))

    assert len(postings) == 1
    args = mock_client.post_json.call_args
    assert args.args[0] == "/api/egts/postings/searchHistoricalData"
    assert args.args[1]["beginDate"] == "2026-05-01"


# -----------------------------------------------------------------------------
# Transformer unit tests
# -----------------------------------------------------------------------------


def test_transformer_emits_series_and_preserves_zero(tmp_path: Path) -> None:
    """Cove Point rows → sq/oac/opcap series; TSQ 0 preserved, not dropped."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "curated.parquet"

    rows = [
        {k: v for k, v in r.items()}
        for r in parse_oac_csv(_SAMPLE_CSV)
    ]
    _write_raw(raw_dir / "2026-05-16_ID3_1011665.json", rows)

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    df = pd.read_parquet(out_parquet)
    # Only the R-direction Cove Point row passes the filter → 3 series.
    assert result["rows"] == 3
    assert set(df["series_id"]) == {
        "egts_sq_40704_id3",
        "egts_oac_40704_id3",
        "egts_opcap_40704_id3",
    }
    assert df["source"].unique()[0] == "bhe"
    assert df["unit"].unique()[0] == "Dth/d"

    sq = df[df["series_id"] == "egts_sq_40704_id3"].iloc[0]
    assert float(sq["value"]) == 0.0  # zero is data
    assert sq["period"] == "2026-05-16"

    oac = df[df["series_id"] == "egts_oac_40704_id3"].iloc[0]
    assert float(oac["value"]) == 741280.0
    assert oac["series_name"] == "EGTS OAC EGTS - LOUDOUN (ID3)"


def test_transformer_accumulates_without_overwriting(tmp_path: Path) -> None:
    """Second transform run merges into history instead of clobbering it."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "curated.parquet"

    rows = parse_oac_csv(_SAMPLE_CSV)
    _write_raw(raw_dir / "2026-05-16_ID3_1011665.json", rows)
    transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    first = pd.read_parquet(out_parquet)
    assert len(first) == 3

    # A second posting for a different cycle joins history; nothing shrinks.
    rows_timely = [
        {**r, "CycleDesc": "Timely", "Eff Gas Day": "05/17/2026", "Total Scheduled Quantity": "650000"}
        for r in rows
    ]
    _write_raw(
        raw_dir / "2026-05-17_TIMELY_1011781.json",
        rows_timely,
        cycle="TIMELY",
        gas_day="2026-05-17",
    )
    transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    merged = pd.read_parquet(out_parquet)
    assert len(merged) == 6  # 3 ID3 + 3 TIMELY
    timelier_sq = merged[merged["series_id"] == "egts_sq_40704_timely"].iloc[0]
    assert float(timelier_sq["value"]) == 650000.0
    assert len(pd.read_parquet(out_parquet)) >= len(first)


# -----------------------------------------------------------------------------
# Backfill unit tests
# -----------------------------------------------------------------------------


def test_month_windows_splits_ranges() -> None:
    """Window helper covers [since, until] without gaps or overlaps."""
    windows = month_windows(date(2026, 1, 1), date(2026, 3, 15))
    assert windows[0][0] == date(2026, 1, 1)
    assert windows[-1][1] == date(2026, 3, 15)
    flat = [d for w in windows for d in w]
    assert flat == sorted(flat)
    for (_, end_a), (start_b, _) in zip(windows, windows[1:], strict=False):
        assert (start_b - end_a).days == 1


def test_load_curated_gas_day_cycles(tmp_path: Path) -> None:
    """Curated skip keys derived from series ids (case-insensitive cycles)."""
    parquet = tmp_path / "bhe.parquet"
    pd.DataFrame(
        {
            "series_id": ["egts_sq_40704_id3", "egts_oac_40704_evening"],
            "period": ["2026-05-16", "2026-05-17"],
        }
    ).to_parquet(parquet)
    keys = load_curated_gas_day_cycles(parquet)
    assert ("2026-05-16", "ID3") in keys
    assert ("2026-05-17", "EVENING") in keys
    assert load_curated_gas_day_cycles(tmp_path / "missing.parquet") == set()


def test_load_checkpoint_since_mismatch(tmp_path: Path) -> None:
    """Checkpoint with a different --since is ignored."""
    state_path = tmp_path / "_backfill_state.json"
    state_path.write_text(json.dumps({"since": "2026-01-01", "next_window": 3}), encoding="utf-8")
    assert load_checkpoint(state_path, "2026-02-01") is None
    assert load_checkpoint(state_path, "2026-01-01") == {"since": "2026-01-01", "next_window": 3}
    assert load_checkpoint(tmp_path / "nope.json", "2026-01-01") is None


@pytest.mark.asyncio
async def test_backfill_walk_writes_raw_and_checkpoints(tmp_path: Path) -> None:
    """Walk fetches missing postings, skips existing, checkpoints past the last window."""
    raw_dir = tmp_path / "raw"
    config = BackfillConfig(
        since=date(2026, 5, 1),
        raw_dir=raw_dir,
        curated_path=tmp_path / "curated.parquet",
        download_gap_seconds=0.0,
    )
    mock_client = AsyncMock()
    runner = BheBackfill(config=config, client=mock_client)

    posting_b = {
        "notice_id": 1011665,
        "subject": "MAY Capacity Available 05/16/2026 Intraday 3",
        "posted_at": "2026-05-16T21:38:00.000",
        "gas_day": "2026-05-16",
        "csv_url": "https://infopost.bhegts.com/docs/egts/postings/1011665/0/egts-1011665.csv",
    }  # will be fetched
    posting_a = {**posting_b, "notice_id": 1011653}  # raw file pre-exists

    with (
        patch(
            "scrapers.bhe.backfill.fetch_postings", new=AsyncMock(return_value=[posting_a, posting_b])
        ),
        patch("scrapers.bhe.backfill.parse_oac_csv", return_value=parse_oac_csv(_SAMPLE_CSV)),
        patch("scrapers.bhe.backfill.download_posting_csv", new=AsyncMock()) as _mock_dl,
    ):
        # Pre-seed one raw file so posting_a hits the raw-file skip.
        rows = parse_oac_csv(_SAMPLE_CSV)
        _write_raw(raw_dir / "2026-05-16_ID3_1011653.json", rows, posted_at="2026-05-15T21:30:00.000")

        summary = await runner.run(fresh=False)

    assert summary["fetched"] == 1
    assert summary["skipped_raw_file"] == 1
    assert (raw_dir / "2026-05-16_ID3_1011665.json").exists()
    assert (raw_dir / "_backfill_state.json").exists()

    # The walk finished every window: a resume starts fresh (cursor past the end).
    runner2 = BheBackfill(config=config, client=mock_client)
    with (
        patch("scrapers.bhe.backfill.fetch_postings", new=AsyncMock(return_value=[])) as mock_list,
    ):
        summary2 = await runner2.run(fresh=False)
    assert summary2["postings_seen"] == 0
    mock_list.assert_not_awaited()
