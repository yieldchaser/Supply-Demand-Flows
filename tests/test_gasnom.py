"""Tests for the GASNom ESG-Latitude scraper, backfill, and transformer.

Test strategy:
    - All HTTP calls are mocked (no socket is ever opened — zero live calls).
    - Filesystem writes go to pytest tmp_path.
    - HTML/TSV fixtures mirror the real gasnom.com response shapes captured
      2026-08-23.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from scrapers.gasnom import run as scraper_run
from scrapers.gasnom.backfill import GasnomBackfill, _chunked_window
from scrapers.gasnom.client import (
    GasnomHeader,
    cycle_code_from_description,
    parse_bulk_tsv,
    parse_header_date,
    parse_html_oac,
)
from scrapers.gasnom.pipelines import GASNOM_PIPELINES
from transformers.base.accumulate import AccumulationShrinkError
from transformers.errors import TransformError
from transformers.gasnom import transform

GAS_DAY = date(2026, 8, 22)

# -----------------------------------------------------------------------------
# Fixtures — shapes mirror live captures (goldenpass, 2026-08-22, ID3)
# -----------------------------------------------------------------------------

_SAMPLE_HTML = """\
<html><head><style>.hdrlabel{}</style></head><body>
<div><div class="header">Operationally Available Capacity</div><br />
<div class="header2">
<table class="hdr" align="center">
    <tr>
        <td class="hdrlabel">TSP Name:</td>
        <td>Golden Pass Pipeline LLC</td>
    </tr>
    <tr>
        <td class="hdrlabel">TSP:</td>
        <td>809259778</td>
    </tr>
    <tr>
        <td class="hdrlabel">Posting Date/Time:</td>
        <td>August 22, 2026 09:31 PM CT</td>
    </tr>
    <tr>
        <td class="hdrlabel">Eff Gas Day/Time:</td>
        <td>August 22, 2026 10:00 PM CT</td>
    </tr>
    <tr>
        <td class="hdrlabel">Cycle Indicator Description:</td>
        <td>Intraday 3</td>
    </tr>
    <tr>
        <td class="hdrlabel">Capacity Type Description</td>
        <td>Operationally available capacity</td>
    </tr>
</table>
</div>
<div class="row" style="margin-top:15px;">
<table border="0" align="center" cellspacing="0" width="750">
<tr class="hdr">
    <td class="hdr1">Location Name</td>
    <td class="hdrc">Loc</td>
    <td class="hdrc">Loc Zn</td>
    <td class="hdr1">Loc Purp<br />Desc</td>
    <td class="hdrc">Loc/QTI</td>
    <td class="hdr1">Flow Ind</td>
    <td class="hdrc">All Qty<br />Avail</td>
    <td class="hdrc">Design<br />Capacity</td>
    <td class="hdrc">Operating<br />Capacity</td>
    <td class="hdrc">TSQ</td>
    <td class="hdrc">OAC</td>
    <td class="hdrc">IT<br />Indicator</td>
    <td class="hdrc">Qty<br />Reason</td>
</tr>
<tr><td colspan="12" style="height:5px;"></td></tr>
<tr class="alt1">
    <td class="datacellleft">Terminal</td>
    <td class="datacellcenter">1097217</td>
    <td class="datacellcenter">1</td>
    <td class="datacellleft">DQ</td>
    <td class="datacellcenter">DPQ</td>
    <td class="datacellleft">R</td>
    <td class="datacellcenter">Y</td>
    <td class="datacellright">2600910</td>
    <td class="datacellright">2500000</td>
    <td class="datacellright">0</td>
    <td class="datacellright">2913027</td>
    <td class="datacellcenter">N</td>
    <td class="datacellleft"></td>
</tr>
<tr class="alt2">
    <td class="datacellleft">Florida Gas</td>
    <td class="datacellcenter">746935</td>
    <td class="datacellcenter">1</td>
    <td class="datacellleft">MQ</td>
    <td class="datacellcenter">RDQ</td>
    <td class="datacellleft">D</td>
    <td class="datacellcenter">Y</td>
    <td class="datacellright">263250</td>
    <td class="datacellright">250000</td>
    <td class="datacellright">185339</td>
    <td class="datacellright">2609561</td>
    <td class="datacellcenter">N</td>
    <td class="datacellleft"></td>
</tr>
</table>
</div>
</div>
<script type="text/javascript" src="/_Incapsula_Resource?SWJIYLWA=719d34d31c8e3a6e6fffd425f7e032f3&ns=2&cb=529299772" async></script></body>
</html>"""

_SAMPLE_EMPTY_HTML = """\
<html><head></head><body>
<div><div class="header">Operationally Available Capacity</div><br />
<div class="header2">
<table class="hdr" align="center">
    <tr>
        <td class="hdrlabel">TSP Name:</td>
        <td>Port Arthur Pipeline, LLC</td>
    </tr>
    <tr>
        <td class="hdrlabel">Posting Date/Time:</td>
        <td>&nbsp;</td>
    </tr>
    <tr>
        <td class="hdrlabel">Eff Gas Day/Time:</td>
        <td>&nbsp;</td>
    </tr>
</table>
</div>
</div>
</body></html>"""

_SAMPLE_TSV = (
    "TSP Name\tTSP\tLocation\tLocation_Name\tLocation_Zone\tLoc_Purp\tAll_Qty_Avail\t"
    "Loc/QTI\tFlow_Ind\tIT\tDesign_Capacity\tOperational_Capacity\tTSQ\tOAC\t"
    "Eff_Gas_Day/Time\tCycleDesc\tPosting_Date/Time\tMeasurement_Basis\tPressure_Base\n"
    "Golden Pass Pipeline LLC\t809259778\t1097217\tTerminal\t1\tDQ\tY\tDPQ\tR\tN\t2600910\t2500000\t0\t2913027\t"
    "08/22/2026 10:00:00 PM\tIntraday 3\t08/22/2026 09:31:03 PM\tMillion BTUs (DTH)\t14.73 psia\n"
    "Golden Pass Pipeline LLC\t809259778\t1097217\tTerminal\t1\tDQ\tY\tDPQ\tD\tN\t2600910\t2600910\t185339\t2609561\t"
    "08/22/2026 10:00:00 PM\tIntraday 3\t08/22/2026 09:31:03 PM\tMillion BTUs (DTH)\t14.73 psia\n"
    "Golden Pass Pipeline LLC\t809259778\t1097217\tTerminal\t1\tDQ\tY\tDPQ\tD\tN\t2600910\t2600910\t262729\t2440710\t"
    "08/22/2026 10:00:00 PM\tIntraday 2\t08/22/2026 03:12:44 PM\tMillion BTUs (DTH)\t14.73 psia\n"
)


class GasnomHeaderStub(GasnomHeader):
    """Keeps the import meaningful for type checkers; fixtures build real ones."""


def _fake_client_factory(**kwargs: Any) -> Any:
    """Build an offline stand-in for GasnomClient serving canned fixtures."""

    class _FakeGasnomClient:
        def __init__(self, pipeline: Any) -> None:
            self.pipeline = pipeline
            self.html_body: str = kwargs.get("html_body", _SAMPLE_HTML)

        def __enter__(self) -> _FakeGasnomClient:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            return None

        def fetch_oac_html(self, gas_day: date) -> tuple[GasnomHeader, list[dict[str, str]]]:
            if kwargs.get("raise_waf"):
                from scrapers.gasnom.client import GasnomWafError

                raise GasnomWafError(
                    url="https://www.gasnom.com/", status=403, attempts=2,
                    elapsed_s=0.0,
                    reason="Imperva/Incapsula challenge persisted through chrome124 impersonation",
                )
            header, rows = parse_html_oac(self.html_body)
            if kwargs.get("waf_then_clean") and not getattr(self, "_retried", False):
                self._retried = True  # first attempt "trips"; retry serves fixture
                self._retried_marker = kwargs.get("waf_then_clean")
            return header, rows

    return _FakeGasnomClient


def _tsv_backfill_client(tsv_bodies: list[str]) -> Any:
    """Offline stand-in whose fetch_bulk_tsv pops canned TSV bodies."""

    class _FakeBackfillClient:
        def __init__(self, pipeline: Any) -> None:
            self.pipeline = pipeline
            self.bodies = list(tsv_bodies)

        def __enter__(self) -> _FakeBackfillClient:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            return None

        def fetch_bulk_tsv(self, start: date, end: date) -> str:
            if not self.bodies:
                return ""
            return self.bodies.pop(0)

    return _FakeBackfillClient


# -----------------------------------------------------------------------------
# Parser tests
# -----------------------------------------------------------------------------


def test_parse_html_header() -> None:
    """Page header yields TSP name, posting time, gas day, and cycle."""
    header, rows = parse_html_oac(_SAMPLE_HTML)
    assert header.tsp_name == "Golden Pass Pipeline LLC"
    assert header.posting_time == "August 22, 2026 09:31 PM CT"
    assert header.gas_day_time == "August 22, 2026 10:00 PM CT"
    assert header.cycle_desc == "Intraday 3"
    assert len(rows) == 2


def test_parse_html_columns() -> None:
    """Fixed column order maps positionally even with trailing extra cells."""
    _, rows = parse_html_oac(_SAMPLE_HTML)
    terminal = [r for r in rows if r["Loc"] == "1097217"][0]
    assert terminal["Loc Name"] == "Terminal"
    assert terminal["Flow Ind"] == "R"
    assert terminal["Design Capacity"] == "2600910"
    assert terminal["Operating Capacity"] == "2500000"
    assert terminal["TSQ"] == "0"
    assert terminal["OAC"] == "2913027"


def test_parse_html_empty_gas_day() -> None:
    """An unposted gas day parses to blank header + zero rows (not an error)."""
    header, rows = parse_html_oac(_SAMPLE_EMPTY_HTML)
    assert rows == []
    assert header.tsp_name == "Port Arthur Pipeline, LLC"
    assert header.cycle_desc == ""


def test_parse_header_date_variants() -> None:
    assert parse_header_date("August 22, 2026 09:31 PM CT") == GAS_DAY
    assert parse_header_date("") is None
    assert parse_header_date("n/a") is None


def test_cycle_code_from_description() -> None:
    assert cycle_code_from_description("Timely") == "timely"
    assert cycle_code_from_description("Evening") == "evening"
    assert cycle_code_from_description("Intraday 1") == "id1"
    assert cycle_code_from_description("Intraday 2") == "id2"
    assert cycle_code_from_description("Intraday 3") == "id3"
    assert cycle_code_from_description("") == "unknown"


def test_parse_bulk_tsv() -> None:
    """Bulk TSV parses to clean dicts preserving raw Dth quantities."""
    rows = parse_bulk_tsv(_SAMPLE_TSV)
    assert len(rows) == 3
    id3_d = [
        r for r in rows
        if r["CycleDesc"] == "Intraday 3" and r["Flow_Ind"] == "D"
    ][0]
    assert id3_d["Location"] == "1097217"
    assert id3_d["TSQ"] == "185339"
    assert id3_d["Measurement_Basis"] == "Million BTUs (DTH)"
    assert id3_d["Posting_Date/Time"] == "08/22/2026 09:31:03 PM"


def test_pipeline_registry_frozen_values() -> None:
    """Slug registry matches the frozen spec exactly (incl. uppercase SABINE)."""
    assert set(GASNOM_PIPELINES) == {
        "goldenpass", "cameron", "SABINE", "portarthurpipeline",
    }
    sabine = GASNOM_PIPELINES["SABINE"]
    assert sabine.slug == "SABINE"  # case-sensitive on purpose
    assert sabine.series_prefix == "sabine_pipe_line"
    assert sabine.nameplate_dth == 4500
    assert GASNOM_PIPELINES["goldenpass"].nameplate_dth == 2600
    assert GASNOM_PIPELINES["cameron"].series_prefix == "cameron_interstate"
    assert GASNOM_PIPELINES["portarthurpipeline"].terminal == "port_arthur_lng"


def test_chunked_window_respects_90_day_cap() -> None:
    """Windows longer than 90 days split into site-compatible chunks."""
    chunks = _chunked_window(date(2026, 1, 1), date(2026, 8, 22))
    assert len(chunks) >= 2
    for start, end in chunks:
        assert (end - start).days <= 89
        assert start <= end
    for (_, prev_end), (next_start, _) in zip(chunks, chunks[1:], strict=False):
        assert (next_start - prev_end).days == 1


# -----------------------------------------------------------------------------
# Scraper orchestrator tests
# -----------------------------------------------------------------------------


def test_run_scrapes_and_writes_payload(tmp_path: Path) -> None:
    """run() writes a canonical payload via the fake client (no network)."""
    result = scraper_run(
        "goldenpass",
        gas_day=GAS_DAY,
        raw_dir=tmp_path,
        client_cls=_fake_client_factory(),
    )
    assert result["status"] == "ok"
    out_file = tmp_path / "goldenpass_2026-08-22_id3.json"
    assert out_file.exists()
    payload = json.loads(out_file.read_text(encoding="utf-8"))
    assert payload["cycle"] == "id3"
    assert payload["gas_day"] == "2026-08-22"
    assert payload["series_prefix"] == "golden_pass"
    assert payload["posted_at"] == "August 22, 2026 09:31 PM CT"
    assert len(payload["data"]) == 2
    terminal = [r for r in payload["data"] if r["loc"] == "1097217"][0]
    assert terminal["tsq"] == "0"
    assert terminal["oac"] == "2913027"


def test_run_unposted_gas_day_is_empty_not_crash(tmp_path: Path) -> None:
    """Port-Arthur-style empty page returns status 'empty' without raising."""
    result = scraper_run(
        "portarthurpipeline",
        gas_day=date(2026, 5, 1),
        raw_dir=tmp_path,
        client_cls=_fake_client_factory(html_body=_SAMPLE_EMPTY_HTML),
    )
    assert result["status"] == "empty"
    assert result["rows"] == 0
    assert not list(tmp_path.glob("*.json"))


def test_run_staleness_gate_skips_existing_raw(tmp_path: Path) -> None:
    """An existing raw file short-circuits the scrape."""
    existing = tmp_path / "goldenpass_2026-08-22_id3.json"
    existing.write_text("{}", encoding="utf-8")
    result = scraper_run(
        "goldenpass",
        gas_day=GAS_DAY,
        raw_dir=tmp_path,
        client_cls=_fake_client_factory(),
    )
    assert result["status"] == "skipped"


def test_run_records_health_failure_on_waf(tmp_path: Path) -> None:
    """A persistent WAF challenge surfaces as status 'failed' + health record."""
    from unittest.mock import MagicMock, patch

    health_instance = MagicMock()
    with patch("scrapers.gasnom.HealthWriter", return_value=health_instance):
        result = scraper_run(
            "goldenpass",
            gas_day=GAS_DAY,
            raw_dir=tmp_path,
            client_cls=_fake_client_factory(raise_waf=True),
        )
    assert result["status"] == "failed"
    failure_kwargs = health_instance.record_failure.call_args.kwargs
    assert "Imperva" in str(failure_kwargs)


# -----------------------------------------------------------------------------
# Backfill tests
# -----------------------------------------------------------------------------


def test_backfill_groups_tsv_into_per_cycle_payloads(tmp_path: Path) -> None:
    """One bulk TSV becomes one raw file per (gas day, cycle); all cycles kept."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    backfill = GasnomBackfill(raw_dir=tmp_path, download_gap_seconds=0.0)
    pipeline = GASNOM_PIPELINES["goldenpass"]

    original_cls = backfill.client_cls
    backfill.client_cls = _tsv_backfill_client([_SAMPLE_TSV])
    try:
        summary = backfill.run_slug(pipeline, date(2026, 8, 20), GAS_DAY)
    finally:
        backfill.client_cls = original_cls

    assert summary["errors"] == []
    assert summary["fetched_files"] >= 2  # ID3 + ID2 present in fixture
    assert summary["cycles"] == ["id2", "id3"]

    id3 = json.loads(
        (tmp_path / "goldenpass_2026-08-22_id3.json").read_text(encoding="utf-8")
    )
    assert id3["cycle"] == "id3"
    assert len(id3["data"]) == 2  # R and D rows
    d_row = [r for r in id3["data"] if r["flow_ind"] == "D"][0]
    assert d_row["tsq"] == "185339"
    assert id3["posted_at"] == "08/22/2026 09:31:03 PM"

    # Re-run against the SAME TSV skips everything (staleness gate + checkpoint).
    backfill2 = GasnomBackfill(raw_dir=tmp_path, download_gap_seconds=0.0)
    original_cls2 = backfill2.client_cls
    backfill2.client_cls = _tsv_backfill_client([_SAMPLE_TSV])
    try:
        summary2 = backfill2.run_slug(pipeline, date(2026, 8, 20), GAS_DAY)
    finally:
        backfill2.client_cls = original_cls2
    assert summary2["errors"] == []
    assert summary2["fetched_files"] == 0
    assert summary2["skipped_existing"] >= 2


def test_backfill_empty_result_does_not_crash(tmp_path: Path) -> None:
    """A slug whose window predates postings reports zeros without erroring."""
    backfill = GasnomBackfill(raw_dir=tmp_path, download_gap_seconds=0.0)
    pipeline = GASNOM_PIPELINES["portarthurpipeline"]
    original_cls = backfill.client_cls
    backfill.client_cls = _tsv_backfill_client([""])  # server returns empty body
    try:
        summary = backfill.run_slug(pipeline, date(2026, 5, 25), date(2026, 6, 7))
    finally:
        backfill.client_cls = original_cls

    assert summary["errors"] == []
    assert summary["fetched_files"] == 0
    assert summary["rows_total"] == 0
    assert summary["oldest_gas_day"] is None


# -----------------------------------------------------------------------------
# Transformer tests
# -----------------------------------------------------------------------------


_TERMINAL_D_ROW = {
    "loc_name": "Terminal", "loc": "1097217", "loc_zone": "1",
    "loc_purp": "DQ", "loc_qti": "DPQ", "flow_ind": "D",
    "all_qty_avail": "Y", "design_cap": "2600910",
    "operating_cap": "2600910", "tsq": "587431", "oac": "2013479",
    "it_indicator": "N",
}


def _write_raw(path: Path, payload_mod: dict[str, Any] | None = None) -> None:
    """Write one canonical raw payload (Golden Pass Terminal, timely, D row)."""
    payload: dict[str, Any] = {
        "fetched_at": "2026-08-22T21:35:00Z",
        "source_slug": "goldenpass",
        "series_prefix": "golden_pass",
        "terminal": "golden_pass_lng",
        "tsp_name": "Golden Pass Pipeline LLC",
        "gas_day": "2026-08-22",
        "cycle": "timely",
        "cycle_desc": "Timely",
        "posted_at": "August 22, 2026 02:11:04 PM CT",
        "row_count": 1,
        "data": [dict(_TERMINAL_D_ROW)],
    }
    if payload_mod:
        payload.update(payload_mod)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_transformer_emits_both_series_raw_dth(tmp_path: Path) -> None:
    """TSQ+OAC series per location; values RAW in Dth/d; unit never converted."""
    raw_dir = tmp_path / "raw"
    curated = tmp_path / "curated.parquet"
    _write_raw(raw_dir / "goldenpass_2026-08-22_timely.json")

    transform(raw_dir=raw_dir, curated_parquet_path=curated)

    df = pd.read_parquet(curated)
    assert set(df["source"].unique()) == {"gasnom"}
    assert set(df["unit"].unique()) == {"Dth/d"}
    assert set(df["region"].unique()) == {"US"}

    sq = df[df["series_id"] == "golden_pass_sq_1097217_timely"].iloc[0]
    assert sq["value"] == 587431.0  # RAW — no MMcf conversion anywhere
    assert sq["period"] == "2026-08-22"

    oac = df[df["series_id"] == "golden_pass_oac_1097217_timely"].iloc[0]
    assert oac["value"] == 2013479.0


def test_transformer_multi_pipeline_prefixes(tmp_path: Path) -> None:
    """Each slug's files land under its own series prefix."""
    raw_dir = tmp_path / "raw"
    curated = tmp_path / "curated.parquet"
    _write_raw(raw_dir / "goldenpass_2026-08-22_timely.json")
    _write_raw(
        raw_dir / "SABINE_2026-08-22_timely.json",
        {
            "source_slug": "SABINE",
            "series_prefix": "sabine_pipe_line",
            "terminal": "sabine_pass_lng",
            "tsp_name": "Sabine Pipe Line, LLC",
            "data": [
                {
                    "loc_name": "TransCameron Pipeline", "loc": "278925",
                    "loc_zone": "1", "loc_purp": "MQ", "loc_qti": "RDQ",
                    "flow_ind": "D", "all_qty_avail": "Y",
                    "design_cap": "500000", "operating_cap": "500000",
                    "tsq": "500000", "oac": "0", "it_indicator": "N",
                }
            ],
            "row_count": 1,
        },
    )

    transform(raw_dir=raw_dir, curated_parquet_path=curated)
    df = pd.read_parquet(curated)
    prefixes = {
        sid.rsplit("_sq_", 1)[0].rsplit("_oac_", 1)[0] for sid in df["series_id"]
    }
    assert prefixes == {"golden_pass", "sabine_pipe_line"}
    sabine_sq = df[df["series_id"] == "sabine_pipe_line_sq_278925_timely"].iloc[0]
    assert sabine_sq["value"] == 500000.0


def test_transformer_batch_dedupe_keeps_latest_posting(tmp_path: Path) -> None:
    """Duplicate (series_id, period): latest Posting_Date/Time wins."""
    raw_dir = tmp_path / "raw"
    curated = tmp_path / "curated.parquet"

    earlier_row = dict(_TERMINAL_D_ROW, tsq="100000")
    later_row = dict(_TERMINAL_D_ROW, tsq="587431")
    _write_raw(
        raw_dir / "a_evening.json",
        {
            "cycle": "evening",
            "posted_at": "",
            "data": [earlier_row],
        },
    )
    _write_raw(
        raw_dir / "b_evening.json",
        {
            "cycle": "evening",
            "posted_at": "August 22, 2026 09:31:03 PM CT",
            "data": [later_row],
        },
    )

    transform(raw_dir=raw_dir, curated_parquet_path=curated)
    df = pd.read_parquet(curated)
    ev = df[df["series_id"] == "golden_pass_sq_1097217_evening"]
    assert len(ev) == 1
    assert float(ev.iloc[0]["value"]) == 587431.0


def test_transformer_accumulates_without_overwrite(tmp_path: Path) -> None:
    """Second run APPENDS a new gas day instead of clobbering history."""
    raw_dir = tmp_path / "raw"
    curated = tmp_path / "curated.parquet"

    _write_raw(raw_dir / "day1.json", {"gas_day": "2026-08-21"})
    transform(raw_dir=raw_dir, curated_parquet_path=curated)
    first = pd.read_parquet(curated)
    assert len(first) == 2

    (raw_dir / "day1.json").unlink()
    _write_raw(raw_dir / "day2.json", {"gas_day": "2026-08-22"})
    transform(raw_dir=raw_dir, curated_parquet_path=curated)
    second = pd.read_parquet(curated)
    assert len(second) == 4
    assert set(second["period"]) == {"2026-08-21", "2026-08-22"}


def test_transformer_reingest_same_day_updates_not_duplicates(tmp_path: Path) -> None:
    """Re-scraping the same day replaces values via ingested_at dedupe."""
    raw_dir = tmp_path / "raw"
    curated = tmp_path / "curated.parquet"

    _write_raw(raw_dir / "day.json")
    transform(raw_dir=raw_dir, curated_parquet_path=curated)
    n_first = len(pd.read_parquet(curated))

    _write_raw(
        raw_dir / "day.json",
        {"data": [dict(_TERMINAL_D_ROW, tsq="600000")]},
    )
    transform(raw_dir=raw_dir, curated_parquet_path=curated)
    df = pd.read_parquet(curated)
    assert len(df) == n_first  # no duplicates
    sq = df[df["series_id"] == "golden_pass_sq_1097217_timely"].iloc[0]
    assert float(sq["value"]) == 600000.0


def test_shrinkage_guard_blocks_history_loss(tmp_path: Path) -> None:
    """merge_into_curated refuses when a merge would shrink the history.

    The guard fires when legacy rows sharing one (series_id, period) key
    would collapse to a single row on re-merge — i.e. the new batch's keys
    no longer cover the distinct keys the existing history was built with.
    A plain subset merge cannot shrink (dedupe is by key, not by row), so we
    reproduce the real collapse: an existing frame carrying two rows for the
    same key merges against a batch holding only that key.
    """
    curated = tmp_path / "curated.parquet"

    def _row(series_id: str, value: float) -> dict[str, Any]:
        return {
            "source": "gasnom", "series_id": series_id,
            "series_name": f"GASNom TSQ {series_id}", "period": "2026-08-22",
            "value": value, "unit": "Dth/d", "region": "US",
            "ingested_at": "2026-08-22T21:35:00+00:00",
        }

    # Legacy history: two rows that today would dedupe into ONE key.
    legacy = pd.DataFrame([
        _row("golden_pass_sq_1097217_timely", 1.0),
        _row("golden_pass_sq_1097217_timely", 2.0),
    ])
    from transformers.base.accumulate import merge_into_curated as _merge

    merged_legacy = _merge(legacy, curated)
    assert len(merged_legacy) == 1  # deduped on write

    # Rebuild the on-disk state the guard protects: simulate a parquet that
    # legitimately holds more rows than a fresh merge would produce by
    # writing two DISTINCT-key rows plus a duplicate-key pair via raw concat.
    bloated = pd.DataFrame([
        _row("golden_pass_sq_1097217_timely", 1.0),
        _row("golden_pass_oac_1097217_timely", 3.0),
    ])
    # Manually craft an existing parquet whose key set collapses under the
    # next merge: two rows for the same (series_id, period), as could happen
    # if an older writer predated the dedupe rule.
    import shutil

    shutil.copy(curated, tmp_path / "keep.parquet")
    collapse_history = pd.concat(
        [
            pd.DataFrame([_row("golden_pass_sq_1097217_timely", 5.0)]).assign(
                ingested_at="2026-08-20T00:00:00+00:00"
            ),
            bloated,
        ],
        ignore_index=True,
    )
    collapse_history.to_parquet(tmp_path / "collapse.parquet")

    # New batch carries ONLY the sq key → merged frame loses the oac row.
    new_batch = pd.DataFrame([_row("golden_pass_sq_1097217_timely", 9.0)])
    existing = pd.read_parquet(tmp_path / "collapse.parquet")
    n_existing = len(existing)
    assert n_existing == 3

    with pytest.raises(AccumulationShrinkError):
        _merge(new_batch, tmp_path / "collapse.parquet")

    after = pd.read_parquet(tmp_path / "collapse.parquet")
    assert len(after) == n_existing  # artefact survived untouched


def test_transformer_missing_dir_raises(tmp_path: Path) -> None:
    with pytest.raises(TransformError):
        transform(
            raw_dir=tmp_path / "nope",
            curated_parquet_path=tmp_path / "c.parquet",
        )


def test_transformer_ignores_checkpoint_file(tmp_path: Path) -> None:
    """The backfill checkpoint state file is not ingested as data."""
    raw_dir = tmp_path / "raw"
    curated = tmp_path / "curated.parquet"
    raw_dir.mkdir(parents=True)
    (raw_dir / "_backfill_state.json").write_text('{"processed_files": []}', encoding="utf-8")

    with pytest.raises(TransformError):
        # Only the (ignored) checkpoint exists → zero usable raw files.
        transform(raw_dir=raw_dir, curated_parquet_path=curated)
