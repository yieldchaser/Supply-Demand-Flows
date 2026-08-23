"""Tests for the Enbridge rtba (TETCO) scraper, backfill plumbing, and transformer.

Test strategy:
    - All HTTP calls are mocked via httpx.MockTransport / unittest.mock.
    - Filesystem writes go to pytest tmp_path.
    - Zero live API calls.

Fixtures are synthesised from the real TE OAC MLC blob schema (verified live
2026-08-23: gas day 2026-08-21, Loc 79999 "STRATTON RIDGE" STX Delivery,
TIMELY TSQ 246706 / INTRADAY TSQ 297640 against 322000 design capacity).
"""

from __future__ import annotations

import io
import json
import zipfile
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from scrapers.enbridge.backfill import (
    BackfillConfig,
    EnbridgeBackfill,
    curated_gas_days,
    load_checkpoint,
    six_month_windows,
)
from scrapers.enbridge.client import (
    build_download_params,
    fetch_oac_zip_bytes,
    from_api_date,
    parse_csv_filename,
    parse_oac_zip,
    resolve_cycle_token,
    to_api_date,
)
from transformers.enbridge import transform

# -----------------------------------------------------------------------------
# Fixtures — real TE_OA_MLC CSV shape (anchor values verified live 2026-08-23)
# -----------------------------------------------------------------------------

_SAMPLE_HEADER = (
    "Cycle_Desc,Post_Date,Eff_Gas_Day,Cap_Type_Desc,Post_Time,Eff_Time,Loc,Loc_Name,"
    "Loc_Zn,Flow_Ind_Desc,Loc_Purp_Desc,Loc_QTI_Desc,Meas_Basis_Desc,IT,All_Qty_Avail,"
    "Total_Design_Capacity,Operating_Capacity,Total_Scheduled_Quantity,"
    "Operationally_Available_Capacity,TSP_Name,TSP"
)

_TIMELY_ROW = (
    "TIMELY_2026-08-20_1511,08-20-2026,08-21-2026,Operational Capacity,15:11,09:00,"
    '79999,STRATTON RIDGE,STX,Delivery,Delivery Location,Delivery Point Quantity,MMBtu,N,Y,'
    '"322,000","322,000","246,706","75,294",TX EAST TRAN,007932908'
)

_INTRADAY_ROW = (
    "INTRDY_2026-08-22_0901,08-22-2026,08-21-2026,Operational Capacity,09:01,09:00,"
    '79999,STRATTON RIDGE,STX,Delivery,Delivery Location,Delivery Point Quantity,MMBtu,N,Y,'
    '"322,000","322,000","297,640","24,360",TX EAST TRAN,007932908'
)

# Bi-directional pair (Cheniere Beauregard) — proves flow-segmented series ids.
_BIDIR_ROWS = (
    "INTRDY_2026-08-22_0901,08-22-2026,08-21-2026,Operational Capacity,09:01,09:00,"
    '75866,"CHENIERE LNG, BEAUREGARD PA,LA (D-73866/R-73566)",WLA,Receipt,Receipt Location,'
    'Receipt Point Quantity,MMBtu,N,Y,"559,739","559,739",0,"559,739",TX EAST TRAN,007932908\n'
    "INTRDY_2026-08-22_0901,08-22-2026,08-21-2026,Operational Capacity,09:01,09:00,"
    '75866,"CHENIERE LNG, BEAUREGARD PA,LA (D-73866/R-73566)",WLA,Delivery,Delivery Location,'
    'Delivery Point Quantity,MMBtu,N,Y,"712,396","712,396",0,"712,396",TX EAST TRAN,007932908'
)


def _csv_bytes(*rows: str) -> bytes:
    return ("\n".join((_SAMPLE_HEADER,) + rows) + "\n").encode("utf-8")


def _fixture_zip_bytes() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("TE_OA_MLC_2026-08-21_TIMELY_2026-08-20_1511.csv", _csv_bytes(_TIMELY_ROW))
        zf.writestr("TE_OA_MLC_2026-08-21_INTRDY_2026-08-22_0901.csv", _csv_bytes(_INTRADAY_ROW, _BIDIR_ROWS))
        zf.writestr("readme.txt", b"not a csv")
    return buf.getvalue()


@pytest.fixture()
def fixture_zip() -> bytes:
    return _fixture_zip_bytes()


def _write_raw_files(raw_dir: Path) -> None:
    payloads = parse_oac_zip(_fixture_zip_bytes(), "2026-08-23T12:00:00Z")
    raw_dir.mkdir(parents=True, exist_ok=True)
    for p in payloads:
        stem = str(p["archive_member"]).rsplit(".", 1)[0]
        (raw_dir / f"{p['gas_day']}_{p['cycle']}_{stem}.json").write_text(
            json.dumps(p), encoding="utf-8"
        )


# -----------------------------------------------------------------------------
# Date + cycle helpers
# -----------------------------------------------------------------------------


def test_to_and_from_api_date_roundtrip() -> None:
    d = date(2026, 8, 21)
    assert to_api_date(d) == "08-21-2026"
    assert from_api_date("08-21-2026") == d


def test_resolve_cycle_tokens() -> None:
    assert resolve_cycle_token("TIMELY_2026-08-20_1511") == "timely"
    assert resolve_cycle_token("EVENING") == "evening"
    # Intraday flavors bucket to the posting hour (minute jitter collapses).
    assert resolve_cycle_token("INTRDY_2026-08-22_0901") == "id0900"
    assert resolve_cycle_token("INTRDYC_2026-08-21_2135", "2136") == "id2100"
    assert resolve_cycle_token("INTRADAY 2", "") == "intraday"
    assert resolve_cycle_token("") is None


def test_parse_csv_filename_variants(fixture_zip: bytes) -> None:
    assert parse_csv_filename("TE_OA_MLC_2026-08-21_TIMELY_2026-08-20_1511.csv") == (
        "timely",
        "2026-08-21",
    )
    assert parse_csv_filename("TE_OA_MLC_2026-08-21_INTRDY_2026-08-22_0901.csv") == (
        "id0900",
        "2026-08-21",
    )
    assert parse_csv_filename("junk.txt") is None
    assert parse_csv_filename("BAD_2026-13-99_X_Y_Z_W.csv") is None


# -----------------------------------------------------------------------------
# ZIP parsing
# -----------------------------------------------------------------------------


def test_parse_oac_zip_payloads(fixture_zip: bytes) -> None:
    payloads = parse_oac_zip(fixture_zip, "2026-08-23T12:00:00Z")
    assert len(payloads) == 2
    by_cycle = {p["cycle"]: p for p in payloads}
    assert set(by_cycle) == {"timely", "id0900"}
    timely = by_cycle["timely"]
    assert timely["gas_day"] == "2026-08-21"
    assert timely["row_count"] == 1
    row = timely["data"][0]
    assert row["Loc"] == "79999"
    assert row["Total_Scheduled_Quantity"] == "246,706"


def test_parse_oac_zip_max_files(fixture_zip: bytes) -> None:
    assert len(parse_oac_zip(fixture_zip, "x", max_files=1)) == 1


# -----------------------------------------------------------------------------
# Download chain over a mocked transport
# -----------------------------------------------------------------------------


class _FakePageMethods:
    """Simulates the rtba page-method state machine on one GUID."""

    def __init__(self) -> None:
        self.filename = "guid-1234"

    def handle(self, url: str, body: dict[str, Any]) -> dict[str, Any]:
        method = url.rsplit("/", 1)[-1]
        if method == "StartFile":
            return {"d": self.filename}
        if method == "AddToFile":
            # One gas day inside the window, then past the end.
            if body["currentDate"] == "08-19-2026":
                return {"d": "08-20-2026"}
            return {"d": "08-22-2026"}
        if method == "ZipFile":
            return {"d": ""}
        raise AssertionError(f"unexpected method {url}")


def test_fetch_oac_zip_chain() -> None:
    fake = _FakePageMethods()
    zip_bytes = _fixture_zip_bytes()

    import asyncio

    import httpx

    from scrapers.enbridge.client import HttpClient

    def route(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("FileHandler.ashx"):
            form = request.content.decode()
            assert "fileName=guid-1234" in form
            assert "postingAbbreviation=OA" in form
            return httpx.Response(200, content=zip_bytes)
        body = json.loads(request.content.decode())
        assert request.headers["Content-Type"].startswith("application/json")
        return httpx.Response(200, json=fake.handle(request.url.path, body))

    transport = httpx.MockTransport(route)
    client = HttpClient(base_url="https://rtba.enbridge.com/InformationalPosting/")

    async def _run() -> bytes:
        client._client = httpx.AsyncClient(  # noqa: SLF001 - test injects transport
            base_url=client._client.base_url,  # noqa: SLF001
            transport=transport,
        )
        try:
            return await fetch_oac_zip_bytes(
                client, date(2026, 8, 19), date(2026, 8, 21), inter_call_sleep_seconds=0.0
            )
        finally:
            await client.close()

    result = asyncio.run(_run())
    assert result == zip_bytes


# -----------------------------------------------------------------------------
# Backfill plumbing
# -----------------------------------------------------------------------------


def test_six_month_windows_span_three_years() -> None:
    windows = six_month_windows(date(2023, 8, 23), date(2026, 8, 23))
    assert windows[0][0] == date(2023, 8, 23)
    assert windows[-1][1] == date(2026, 8, 23)
    # No gaps, ascending, each ≤ WINDOW_DAYS long.
    for (_, end), (nxt_begin, _) in zip(windows, windows[1:], strict=False):
        assert (nxt_begin - end).days == 1
    assert all((end - begin).days < 183 for begin, end in windows)


def test_load_checkpoint_mismatch(tmp_path: Path) -> None:
    assert load_checkpoint(tmp_path / "missing.json", "2023-08-23") is None
    (tmp_path / "state.json").write_text(json.dumps({"since": "2020-01-01"}), encoding="utf-8")
    assert load_checkpoint(tmp_path / "state.json", "2023-08-23") is None
    (tmp_path / "state.json").write_text(json.dumps({"since": "2023-08-23", "next_window": 2}), encoding="utf-8")
    assert load_checkpoint(tmp_path / "state.json", "2023-08-23") == {
        "since": "2023-08-23",
        "next_window": 2,
    }


def test_backfill_walk_with_mocked_chain(tmp_path: Path) -> None:
    import asyncio


    raw_dir = tmp_path / "raw"
    config = BackfillConfig(since=date(2026, 8, 19), raw_dir=raw_dir, chain_gap_seconds=0.0)
    runner = EnbridgeBackfill(config=config, client=AsyncMock())

    async def fake_chain(client: Any, begin: date, end: date, **_: Any) -> bytes:
        assert begin == date(2026, 8, 19)
        return _fixture_zip_bytes()

    async def _run() -> dict[str, Any]:
        with patch("scrapers.enbridge.backfill.fetch_oac_zip_bytes", side_effect=fake_chain):
            return await runner.run()

    summary = asyncio.run(_run())
    assert summary["windows_walked"] >= 1
    assert summary["fetched"] == 2
    files = sorted(raw_dir.glob("*.json"))
    assert len([f for f in files if f.name != "_backfill_state.json"]) == 2

    # Second walk resumes past the final window (checkpoint cursor), so the
    # cumulative counters hold steady and no new files appear.
    async def _rerun() -> dict[str, Any]:
        with patch("scrapers.enbridge.backfill.fetch_oac_zip_bytes", side_effect=fake_chain):
            return await runner.run()

    again = asyncio.run(_rerun())
    assert again["fetched"] == 2  # cumulative counter holds steady
    assert again["windows_walked"] == summary["windows_walked"]  # nothing re-walked
    assert len([f for f in raw_dir.glob("*.json") if f.name != "_backfill_state.json"]) == 2


def test_curated_gas_days_missing_file(tmp_path: Path) -> None:
    assert curated_gas_days(tmp_path / "nope.parquet") == set()


def test_build_download_params_defaults() -> None:
    params = build_download_params(start_gas_date="01-01-2026", end_gas_date="06-30-2026")
    assert params["businessUnitAbbreviation"] == "TE"
    assert params["postingType"] == "OA"
    assert params["postingSubType"] == "MLC"
    assert params["cycle"] == "All"


# -----------------------------------------------------------------------------
# Transformer — flow-segmented series keys + accumulation
# -----------------------------------------------------------------------------


def test_transform_flow_segmented_series(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    curated = tmp_path / "curated" / "enbridge.parquet"
    _write_raw_files(raw_dir)

    result = transform(raw_dir=raw_dir, curated_parquet_path=curated)
    df = pd.read_parquet(curated)

    # Freeport anchor: delivery leg only → *_d_* segment in every 79999 id.
    anchor = df[df["series_id"].str.startswith("tetco_sq_79999_")]
    assert set(anchor["series_id"]) == {"tetco_sq_79999_d_timely", "tetco_sq_79999_d_id0900"}
    assert sorted(anchor["value"].tolist()) == [246706.0, 297640.0]
    assert set(anchor["unit"]) == {"Dth/d"}
    assert result["rows"] == len(df)

    # Bi-directional meter produces BOTH legs under distinct ids — no collision.
    legs = set(df[df["series_id"].str.contains("tetco_sq_75866_")]["series_id"])
    assert legs == {"tetco_sq_75866_r_id0900", "tetco_sq_75866_d_id0900"}

    # Periods are ISO-normalized from the MM-DD-YYYY CSV cells.
    assert set(df["period"]) == {"2026-08-21"}

    # Re-running the same batch must not duplicate history (shrinkage guard +
    # ingested_at dedup keep it stable).
    before = len(df)
    transform(raw_dir=raw_dir, curated_parquet_path=curated)
    after = len(pd.read_parquet(curated))
    assert after == before
