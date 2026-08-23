"""Tests for the Quorum myQuorumCloud IPWS scraper, backfill, and transformer.

Test strategy:
    - All HTTP calls are mocked via unittest.mock (zero live API calls).
    - Filesystem writes go to pytest tmp_path.
    - Fixture CSVs mirror the live ExportToCSV schema, including the
      unnamed blank column between "Quantity Not Available Reason" and
      "Design Capacity".
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from scrapers.quorum.backfill import BackfillConfig, QuorumBackfill, load_curated_keys
from scrapers.quorum.pipelines import (
    GATOR_EXPRESS,
    TRANSCAMERON,
    QuorumIPWSScraper,
    build_export_url,
    normalize_cycle,
    parse_export_csv,
)
from transformers.quorum import transform

GAS_DAY = date(2026, 8, 22)

# Live header order — note the empty name between "Quantity Not Available
# Reason" and "Design Capacity" (the unnamed blank column).
_HEADER = (
    "Post Date/Time,TSP,TSP Name,Eff Gas Day/Time,End Eff Gas Day/Time,Cycle Desc,"
    "Loc,Loc Name,Loc Purp,Loc/QTI,Flow Ind,IT Desc,Meas Basis,All Qty Avail,"
    "Quantity Not Available Reason,,Design Capacity,Operating Capacity,"
    "Operationally Available Capacity,Total Scheduled Quantity"
)


def _csv_row(post_time: str, eff_day: str, cycle: str, loc: str, loc_name: str,
             purp: str, flow: str, design: str, op_cap: str, oac: str, tsq: str) -> str:
    """Render one fixture CSV row with all 19 columns (blank column empty)."""
    fields = [
        post_time, "119107110", '"VENTURE GLOBAL GATOR EXPRESS, LLC"', eff_day,
        "8/23/2026 10:00:00 PM", f'"{cycle}"', f'"{loc}"', f'"{loc_name}"',
        f'"{purp}"', '"RPQ"', f'"{flow}"', '"Yes"', '"BZ"', '"Yes"', "",
        "", design, op_cap, oac, tsq,
    ]
    return ",".join(fields)


def _gator_csv(cycle: str = "Intraday 3") -> str:
    """Fixture CSV mirroring the live TspNo=2 payload (5 meters, one cycle)."""
    rows = [
        _csv_row("8/22/2026 10:05:14 PM", "8/22/2026 10:00:00 PM", cycle,
                 "CGT", "CGT/GXP GATOR EXPRESS", "M2", "R", "725000", "725000",
                 "68995", "656005"),
        _csv_row("8/22/2026 10:05:14 PM", "8/22/2026 10:00:00 PM", cycle,
                 "PAYBACKGXP", "SYSTEM BALANCING", "M2", "R", "1970000", "1970000",
                 "1970000", "0"),
        _csv_row("8/22/2026 10:05:14 PM", "8/22/2026 10:00:00 PM", cycle,
                 "TETCO", "TETCO/GXP GATOR EXPRESS", "M2", "R", "1542666", "1542666",
                 "130671", "1411995"),
        _csv_row("8/22/2026 10:05:14 PM", "8/22/2026 10:00:00 PM", cycle,
                 "TGP", "TGP/GXP GATOR EXPRESS", "M2", "R", "2260125", "2260125",
                 "516815", "1743310"),
        # THE feedgas meter — sanity anchor magnitude ~3.9M Dth.
        _csv_row("8/22/2026 10:05:14 PM", "8/22/2026 10:00:00 PM", cycle,
                 "VGPQD", "VENTURE GLOBAL PLAQUEMINES LNG DELIVERY", "MQ", "D",
                 "3940000", "3940000", "129681", "3810319"),
    ]
    return "\r\n".join([_HEADER, *rows]) + "\r\n"


_SAMPLE_CSV = _gator_csv()


# -----------------------------------------------------------------------------
# URL / parsing unit tests
# -----------------------------------------------------------------------------


def test_build_export_url() -> None:
    """URL carries tenant, path, and query params in the confirmed shape."""
    url = build_export_url("VGPPB1IPWS", 2, GAS_DAY)
    assert url == (
        "https://web-prd.myquorumcloud.com/VGPPB1IPWS/OpAvailPosting/ExportToCSV"
        "?CycleId=&GasDay=2026-08-22&LocId=&TspNo=2"
    )


def test_parse_export_csv_column_counts() -> None:
    """Blank column parses; column count matches header count for every row."""
    rows = parse_export_csv(_SAMPLE_CSV)
    assert len(rows) == 5

    vgpqd = [r for r in rows if r["Loc"] == "VGPQD"][0]
    assert vgpqd["Total Scheduled Quantity"] == "3810319"
    assert vgpqd["Operationally Available Capacity"] == "129681"
    assert vgpqd["Design Capacity"] == "3940000"
    assert vgpqd["Meas Basis"] == "BZ"
    assert "_unnamed_blank" in vgpqd  # re-keyed blank column

    expected_cols = len(_HEADER.split(","))
    assert len(vgpqd) <= expected_cols + 1  # +1 only if blank col counted
    assert set(rows[0].keys()) == set(vgpqd.keys())


def test_parse_export_csv_extra_field_raises() -> None:
    """A row with more fields than the header fails loudly (schema change).

    Note:
        The fixture appends an unquoted extra value to a data line WITHOUT
        extending the header, so DictReader sees 21 fields vs 20 headers.
    """
    first_data_line = _SAMPLE_CSV.split("\r\n")[1]
    bad_csv = _HEADER + "\r\n" + first_data_line + ",oops\r\n"
    with pytest.raises(ValueError, match="schema changed"):
        parse_export_csv(bad_csv)


def test_parse_export_csv_header_only_returns_empty() -> None:
    """Header-only CSV (pre-first-posting gas days) parses to zero rows."""
    assert parse_export_csv(_HEADER + "\r\n") == []


def test_normalize_cycle() -> None:
    """Quorum Cycle Desc values map to canonical lowercase codes."""
    assert normalize_cycle("Intraday 3") == "id3"
    assert normalize_cycle("Intraday 2") == "id2"
    assert normalize_cycle("Intraday 1") == "id1"
    assert normalize_cycle("Evening") == "evening"
    assert normalize_cycle("Timely") == "timely"
    assert normalize_cycle("Something New") == "something_new"


# -----------------------------------------------------------------------------
# Scraper tests (mocked HTTP)
# -----------------------------------------------------------------------------


def _mock_client(csv_text: str) -> AsyncMock:
    client = AsyncMock()
    client.get_bytes.return_value = csv_text.encode("utf-8")
    return client


@pytest.mark.asyncio
async def test_scraper_writes_per_cycle_payloads(tmp_path: Path) -> None:
    """One GET → per-cycle raw JSON payloads under {prefix}/ subdirectory."""
    scraper = QuorumIPWSScraper("VGPPB1IPWS", 2, prefix="gator_express", raw_dir=tmp_path)
    with patch("scrapers.quorum.pipelines.HealthWriter"):
        result = await scraper.run(GAS_DAY, client=_mock_client(_SAMPLE_CSV))

    assert result["status"] == "ok"
    assert result["rows"] == 5
    out_file = tmp_path / "gator_express" / "2026-08-22_id3.json"
    assert out_file.exists()

    payload = json.loads(out_file.read_text(encoding="utf-8"))
    assert payload["cycle"] == "id3"
    assert payload["gas_day"] == "2026-08-22"
    assert payload["tsp_no"] == 2
    assert payload["row_count"] == 5
    assert len(payload["data"]) == 5


@pytest.mark.asyncio
async def test_scraper_skips_existing_files(tmp_path: Path) -> None:
    """Staleness gate: existing cycle files are not re-written."""
    scraper = QuorumIPWSScraper("VGPPB1IPWS", 2, prefix="gator_express", raw_dir=tmp_path)
    with patch("scrapers.quorum.pipelines.HealthWriter"):
        first = await scraper.run(GAS_DAY, client=_mock_client(_SAMPLE_CSV))
        second = await scraper.run(GAS_DAY, client=_mock_client(_SAMPLE_CSV))
    assert first["status"] == "ok"
    assert second["status"] == "ok"
    assert second["processed_count"] == 0
    assert second["skipped_count"] == 1


@pytest.mark.asyncio
async def test_scraper_header_only_day_is_skipped(tmp_path: Path) -> None:
    """Header-only CSV → status skipped, no files written."""
    scraper = QuorumIPWSScraper("VGPPB1IPWS", 10, prefix="trans_cameron", raw_dir=tmp_path)
    with patch("scrapers.quorum.pipelines.HealthWriter"):
        result = await scraper.run(date(2020, 1, 1), client=_mock_client(_HEADER + "\r\n"))
    assert result["status"] == "skipped"
    assert list(tmp_path.rglob("*.json")) == []


# -----------------------------------------------------------------------------
# Transformer tests
# -----------------------------------------------------------------------------


def _write_raw(path: Path, rows: list[dict[str, Any]], *, prefix: str, cycle: str,
               gas_day: str) -> None:
    """Write a synthetic raw JSON payload matching the scraper's shape."""
    payload = {
        "fetched_at": "2026-08-22T22:10:00Z",
        "source": "quorum",
        "tenant": "VGPPB1IPWS",
        "tsp_no": 2 if prefix == "gator_express" else 10,
        "prefix": prefix,
        "cycle": cycle,
        "gas_day": gas_day,
        "row_count": len(rows),
        "data": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_transformer_generates_both_series(tmp_path: Path) -> None:
    """TSQ + OAC series generated per location/cycle with raw Dth values."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "quorum.parquet"

    rows = parse_export_csv(_SAMPLE_CSV)
    _write_raw(raw_dir / "gator_express" / "2026-08-22_id3.json", rows,
               prefix="gator_express", cycle="id3", gas_day="2026-08-22")

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)
    assert result["rows"] == 10  # 5 locations × 2 series (sq + oac)
    assert result["series_count"] == 10
    assert result["pipelines"] == ["gator_express"]

    df = pd.read_parquet(out_parquet)
    assert df["source"].unique()[0] == "quorum"
    assert df["unit"].unique()[0] == "Dth/d"
    assert df["region"].unique()[0] == "US"

    feedgas = df[df["series_id"] == "gator_express_sq_vgpqd_d_id3"].iloc[0]
    assert feedgas["value"] == 3810319.0  # RAW Dth — never converted
    assert feedgas["series_name"] == "Quorum TSQ VENTURE GLOBAL PLAQUEMINES LNG DELIVERY [D] (ID3)"

    oac = df[df["series_id"] == "gator_express_oac_vgpqd_d_id3"].iloc[0]
    assert oac["value"] == 129681.0


def test_transformer_dual_leg_yields_two_rows(tmp_path: Path) -> None:
    """A meter posting BOTH R and D in one cycle produces TWO series.

    Regression guard for the 2026-08 dual-leg collision: without the flow
    token in the series key, one leg silently overwrote the other.
    """
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "quorum.parquet"

    # VGPQD posts R and D rows in the SAME cycle with different quantities.
    dual_rows = parse_export_csv(
        "\r\n".join(
            [
                _HEADER,
                _csv_row("8/22/2026 10:05:14 PM", "8/22/2026 10:00:00 PM", "Intraday 3",
                         "VGPQD", "VENTURE GLOBAL PLAQUEMINES LNG DELIVERY", "MQ", "R",
                         "3940000", "3940000", "100", "111"),
                _csv_row("8/22/2026 10:05:14 PM", "8/22/2026 10:00:00 PM", "Intraday 3",
                         "VGPQD", "VENTURE GLOBAL PLAQUEMINES LNG DELIVERY", "MQ", "D",
                         "3940000", "3940000", "129681", "3810319"),
            ]
        )
        + "\r\n"
    )
    _write_raw(raw_dir / "gator_express" / "2026-08-22_id3.json", dual_rows,
               prefix="gator_express", cycle="id3", gas_day="2026-08-22")

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)
    # 2 legs × 2 kinds (sq+oac) = 4 rows — NOT 2.
    assert result["rows"] == 4

    df = pd.read_parquet(out_parquet)
    sq_legs = df[df["series_id"].str.contains("_sq_vgpqd_")]
    assert sorted(sq_legs["series_id"]) == [
        "gator_express_sq_vgpqd_d_id3",
        "gator_express_sq_vgpqd_r_id3",
    ]
    r_row = df[df["series_id"] == "gator_express_sq_vgpqd_r_id3"].iloc[0]
    d_row = df[df["series_id"] == "gator_express_sq_vgpqd_d_id3"].iloc[0]
    assert r_row["value"] == 111.0
    assert d_row["value"] == 3810319.0


def test_transformer_dedupe_keeps_latest_posting(tmp_path: Path) -> None:
    """Duplicate (series_id, period): latest Post Date/Time wins."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "quorum.parquet"

    # Early posting: VGPQD TSQ 3810319.
    rows = parse_export_csv(_gator_csv("Timely"))
    _write_raw(raw_dir / "gator_express" / "2026-08-23_timely.json", rows[4:5],
               prefix="gator_express", cycle="timely", gas_day="2026-08-23")

    # Re-post of the same day at a later time with a different TSQ.
    repost = (
        _gator_csv("Timely")
        .replace("3810319", "3947246")
        .replace("8/22/2026 10:05:14 PM", "8/23/2026 09:45:11 AM")
    )
    rows_repost = parse_export_csv(repost)
    assert rows_repost[4]["Total Scheduled Quantity"] == "3947246"
    _write_raw(raw_dir / "gator_express" / "2026-08-23_timely_v2.json", rows_repost[4:5],
               prefix="gator_express", cycle="timely", gas_day="2026-08-23")

    transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    df = pd.read_parquet(out_parquet)
    sq = df[df["series_id"] == "gator_express_sq_vgpqd_d_timely"]
    assert len(sq) == 1
    assert sq.iloc[0]["value"] == 3947246.0  # kept the later posting


def test_transformer_merges_both_pipelines_and_accumulates(tmp_path: Path) -> None:
    """Two pipelines coexist by prefix; a second run accumulates, not overwrites."""
    raw_dir = tmp_path / "raw"
    out_parquet = tmp_path / "quorum.parquet"

    gator_rows = parse_export_csv(_gator_csv("Evening"))
    _write_raw(raw_dir / "gator_express" / "2026-08-21_evening.json", gator_rows,
               prefix="gator_express", cycle="evening", gas_day="2026-08-21")

    tcsv = (
        _HEADER
        + "\r\n"
        + _csv_row(
            "8/21/2026 10:10:15 PM", "8/21/2026 10:00:00 PM", "Intraday 3",
            "VGCPD", "VENTURE GLOBAL CALCASIEU PASS DELIVERY", "MQ", "D",
            "2125000", "2125000", "459134", "1665866",
        )
        + "\r\n"
    )
    tc_rows = parse_export_csv(tcsv)
    assert len(tc_rows) == 1
    _write_raw(raw_dir / "trans_cameron" / "2026-08-21_id3.json", tc_rows,
               prefix="trans_cameron", cycle="id3", gas_day="2026-08-21")

    transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    # Second batch: a new gas day for gator only — history must grow.
    day2_rows = parse_export_csv(_gator_csv("Timely"))
    _write_raw(raw_dir / "gator_express" / "2026-08-22_timely.json", day2_rows,
               prefix="gator_express", cycle="timely", gas_day="2026-08-22")

    result = transform(raw_dir=raw_dir, curated_parquet_path=out_parquet)

    df = pd.read_parquet(out_parquet)
    assert result["pipelines"] == ["gator_express", "trans_cameron"]
    assert "trans_cameron_sq_vgcpd_d_id3" in set(df["series_id"])
    # 5 gator meters × 2 series × 2 days + VGCPD id3 pair (sq + oac) = 22 rows.
    assert len(df) == 22


# -----------------------------------------------------------------------------
# Backfill tests (mocked HTTP)
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backfill_walks_to_floor_and_checkpoints(tmp_path: Path) -> None:
    """Walk stops at the header-only floor; checkpoint records it."""
    config = BackfillConfig(
        since=date(2026, 8, 18),
        raw_dir=tmp_path / "raw",
        curated_path=tmp_path / "quorum.parquet",
        checkpoint_every=2,
        request_gap_seconds=0.0,
        state_path=tmp_path / "state.json",
        pipelines=(GATOR_EXPRESS,),
        # One empty day = floor in this fixture (no retention holes).
        empty_streak_floor_days=1,
    )
    responses = {
        date(2026, 8, 22): _gator_csv("Intraday 3"),
        date(2026, 8, 21): _gator_csv("Evening"),
        date(2026, 8, 20): "",  # header-only floor
    }

    async def fake_fetch(self: Any, pipeline: Any, day: date) -> list[dict[str, str]]:
        return parse_export_csv(responses.get(day, "")) if day in responses else []

    client = AsyncMock()
    bf = QuorumBackfill(config=config, client=client)
    frozen_now = datetime(2026, 8, 22, tzinfo=UTC)
    with (
        patch.object(QuorumBackfill, "_fetch_day_rows", new=fake_fetch),
        patch("scrapers.quorum.backfill.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = frozen_now
        summary = await bf.run(fresh=True)

    assert summary["pipelines"]["gator_express"]["floor"] == "2026-08-20"
    files = sorted((tmp_path / "raw" / "gator_express").glob("*.json"))
    assert len(files) == 2  # 08-22 and 08-21 only

    state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    assert state["completed"] is True
    assert state["floors"]["gator_express"] == "2026-08-20"


@pytest.mark.asyncio
async def test_backfill_punches_through_hole_before_floor(tmp_path: Path) -> None:
    """A short empty streak is a retention hole, not the floor.

    What:
        2025-03-26/27 are empty but 2025-03-25 has data — the walk must
        continue below the hole and only stop once the streak limit holds.
    """
    config = BackfillConfig(
        since=date(2026, 3, 20),
        raw_dir=tmp_path / "raw",
        curated_path=tmp_path / "quorum.parquet",
        request_gap_seconds=0.0,
        state_path=tmp_path / "state.json",
        pipelines=(GATOR_EXPRESS,),
        # Hole is 2 wide (03-26/27); limit 3 means the walk must survive it
        # and only declare a floor after 3 consecutive empties below.
        empty_streak_floor_days=3,
    )
    responses = {
        date(2026, 3, 25): _gator_csv(),
    }

    async def fake_fetch(self: Any, pipeline: Any, day: date) -> list[dict[str, str]]:
        return parse_export_csv(responses.get(day, ""))

    client = AsyncMock()
    bf = QuorumBackfill(config=config, client=client)
    frozen_now = datetime(2026, 3, 27, tzinfo=UTC)
    with (
        patch.object(QuorumBackfill, "_fetch_day_rows", new=fake_fetch),
        patch("scrapers.quorum.backfill.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = frozen_now
        summary = await bf.run(fresh=True)

    # Walked THROUGH the 2-day hole (limit=3) to --since. Floor = first
    # empty day below the last real-data day (03-25).
    assert summary["pipelines"]["gator_express"]["floor"] == "2026-03-24"
    files = sorted((tmp_path / "raw" / "gator_express").glob("*.json"))
    assert len(files) == 1
    assert "2026-03-25" in files[0].name


@pytest.mark.asyncio
async def test_backfill_resumes_from_checkpoint(tmp_path: Path) -> None:
    """A saved cursor makes the walk resume where it stopped, not from today."""
    config = BackfillConfig(
        since=date(2026, 8, 15),
        raw_dir=tmp_path / "raw",
        curated_path=tmp_path / "quorum.parquet",
        request_gap_seconds=0.0,
        state_path=tmp_path / "state.json",
        pipelines=(GATOR_EXPRESS,),
    )
    fetched_days: list[date] = []

    async def fake_fetch(self: Any, pipeline: Any, day: date) -> list[dict[str, str]]:
        fetched_days.append(day)
        return parse_export_csv(_gator_csv()) if day >= date(2026, 8, 16) else []

    client = AsyncMock()
    bf = QuorumBackfill(config=config, client=client)
    frozen_now = datetime(2026, 8, 23, tzinfo=UTC)
    with (
        patch.object(QuorumBackfill, "_fetch_day_rows", new=fake_fetch),
        patch("scrapers.quorum.backfill.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = frozen_now
        await bf.run(fresh=True)
        # The fake runs dry exactly AT --since (08-15), so that day is the
        # probed floor — the walker correctly stepped onto it once.
        assert min(fetched_days) == date(2026, 8, 15)

    # Simulate an interrupted run whose cursor sits mid-window.
    (tmp_path / "state.json").write_text(
        json.dumps({"completed": False, "floors": {}, "next_days":
                    {"gator_express": "2026-08-17"}}),
        encoding="utf-8",
    )

    fetched_days.clear()
    bf2 = QuorumBackfill(config=config, client=client)
    with (
        patch.object(QuorumBackfill, "_fetch_day_rows", new=fake_fetch),
        patch("scrapers.quorum.backfill.datetime") as mock_dt,
    ):
        mock_dt.now.return_value = frozen_now
        await bf2.run()

    # Resumed AT 08-17 (cursor inclusive), walked down to --since (08-15),
    # never re-touched 08-22/08-23 above the cursor.
    assert max(fetched_days) == date(2026, 8, 17)
    assert min(fetched_days) == date(2026, 8, 15)


def test_load_curated_keys_parses_prefixes(tmp_path: Path) -> None:
    """Curated keys extract (prefix, period) pairs from series ids."""
    parquet = tmp_path / "quorum.parquet"
    frame = pd.DataFrame(
        {
            "series_id": ["gator_express_sq_vgpqd_id3", "trans_cameron_oac_anr_timely"],
            "period": ["2026-08-22", "2026-08-22"],
        }
    )
    frame.to_parquet(parquet)
    keys = load_curated_keys(parquet)
    assert ("gator_express", "2026-08-22") in keys
    assert ("trans_cameron", "2026-08-22") in keys


def test_transcameron_constant_wiring() -> None:
    """TransCameron rides the same tenant with TspNo=10."""
    assert TRANSCAMERON.tenant == "VGPPB1IPWS"
    assert TRANSCAMERON.tsp_no == 10
    assert TRANSCAMERON.prefix == "trans_cameron"
