"""Tests for the Williams/Transco 1Line scraper.

Strategy:
    ZERO live network calls. The report HTML fixture below reproduces the
    OACreport.jsp structure proven from live-era Wayback captures (header
    label/value pairs + positional data grid with a Total Scheduled Quantity
    column). Parsers, watchlist scoping, transformer emission, accumulation
    semantics, and the orchestrator's cycle/skip/empty paths are all tested
    against fixtures and a fake client.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# Williams is SHELVED (endpoints dead since 2026-08-25). These tests exercise
# the parser/orchestrator against fixtures + a fake client (zero live calls),
# but they are integration-style tests for a shelved scraper, so they are
# marked `network` and excluded from the default local run per the 2026-08-26
# pytest-hang fix. CI can opt back in with `-m network`.
pytestmark = pytest.mark.network

from scrapers.williams.client import (
    TranscoHeader,
    TranscoWafError,
    cycle_code_from_value,
    parse_oac_table,
)
from scrapers.williams.config import WATCHLIST
from transformers.williams import _num, transform

# ---------------------------------------------------------------------------
# Fixtures — faithful to the JSP rendering (thousands separators included).
# ---------------------------------------------------------------------------

_REPORT_HTML = """
<html><body>
<table>
 <tr>
  <th align="right"> TSP: </th><td>TRANSCONTINENTAL GAS PIPE LINE COMPANY, LLC</td>
  <th align="right"> Cycle Desc: </th><td>Intraday 3</td>
  <th align="right"> Effective Gas Day: </th><td>08/22/2026</td>
  <th align="right"> Posting Date: </th><td>08/23/2026</td>
  <th align="right"> Posting Time: </th><td>04:12:45 PM</td>
  <th align="right"> Meas Basis Desc: </th><td>Vol</td>
 </tr>
</table>
<table>
 <tr><th>Loc</th><th>Loc Prop</th><th>Loc Purp Desc1</th><th>Flow Ind2</th>
     <th>Loc Name</th><th>Design5 Capacity</th><th>Operating8 Capacity</th>
     <th>Total Scheduled Quantity</th><th>Operationally Available Capacity</th>
     <th>IT4 Indicator</th></tr>
 <tr>
  <td>1001297</td><td>Delivery</td><td>LNG Delivery</td><td>D</td>
  <td>SABINE PASS LNG TERMINAL</td><td>1,200,000</td><td>1,150,000</td>
  <td>2,100,000</td><td>-950,000</td><td>N</td>
 </tr>
 <tr>
  <td>1003045</td><td>Receipt</td><td>Storage Injection</td><td>R</td>
  <td>EMINENCE STORAGE</td><td>500,000</td><td>480,000</td>
  <td>0</td><td>480,000</td><td>N</td>
 </tr>
 <tr>
  <td>9007422</td><td>Delivery</td><td>Mkt Area Delivery</td><td>D</td>
  <td>STATION 207 LOWER BAY LATERAL MP 8.57</td><td>300,000</td><td>280,000</td>
  <td>250,000</td><td>30,000</td><td>N</td>
 </tr>
 <tr>
  <td>1006175</td><td>Receipt</td><td>Receipt</td><td>R</td>
  <td>GULF SOUTH-MAGNOLIA M4150</td><td>90,000</td><td>85,000</td>
  <td>60,000</td><td>25,000</td><td>N</td>
 </tr>
</table>
</body></html>
"""


def _fake_header(**overrides: str) -> TranscoHeader:
    values = {
        "tsp_name": "TRANSCONTINENTAL GAS PIPE LINE COMPANY, LLC",
        "cycle_value": "8",
        "cycle_desc": "Intraday 3",
        "gas_day": "08/22/2026",
        "posting_date": "08/23/2026",
        "posting_time": "04:12:45 PM",
        "meas_basis": "Vol",
    }
    values.update(overrides)
    return TranscoHeader(**values)


class FakeClient:
    """Offline stand-in for TranscoClient returning canned reports."""

    instances: list[FakeClient] = []

    def __init__(self) -> None:
        self.header = _fake_header()
        self.rows: list[dict[str, str]] = []
        FakeClient.instances.append(self)

    def __enter__(self) -> FakeClient:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass

    def fetch_oac(
        self, cycle: str, gas_day: date
    ) -> tuple[TranscoHeader, list[dict[str, str]]]:
        if not self.rows:
            return TranscoHeader(
                tsp_name="", cycle_value="", cycle_desc="",
                gas_day="", posting_date="", posting_time="", meas_basis="",
            ), []
        rows = [dict(r) for r in self.rows]
        for r in rows:
            r["_gas_day"] = gas_day.strftime("%m/%d/%Y")
        return self.header, rows


@pytest.fixture(autouse=True)
def _reset_fake(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeClient.instances.clear()


# ---------------------------------------------------------------------------
# Parser.
# ---------------------------------------------------------------------------


def test_parse_oac_table_header_and_rows() -> None:
    header, rows = parse_oac_table(_REPORT_HTML)
    assert header.tsp_name.startswith("TRANSCONTINENTAL")
    assert header.cycle_desc == "Intraday 3"
    assert header.cycle_value == "8"  # id3 radio value recovered from desc
    assert header.gas_day == "08/22/2026"
    assert header.posting_time == "04:12:45 PM"
    assert header.meas_basis == "Vol"
    # All four data rows parsed regardless of watchlist membership.
    assert len(rows) == 4
    sabine = next(r for r in rows if r["loc"] == "1001297")
    assert sabine["loc_name"] == "SABINE PASS LNG TERMINAL"
    assert sabine["tsq"] == "2,100,000"  # raw text kept; commas stripped later
    assert sabine["flow_ind"] == "D"


def test_parse_empty_report() -> None:
    html = (
        "<table><tr><th>TSP:</th><td></td><th>Cycle Desc:</th>"
        "<td></td></tr></table><table></table>"
    )
    header, rows = parse_oac_table(html)
    assert rows == []
    assert header.cycle_desc == ""


def test_cycle_code_roundtrip() -> None:
    assert cycle_code_from_value("8") == "id3"
    assert cycle_code_from_value("1") == "timely"
    assert cycle_code_from_value("99") == "unknown"


# ---------------------------------------------------------------------------
# Watchlist scoping.
# ---------------------------------------------------------------------------


def test_watchlist_matches_terminal_names() -> None:
    hit = WATCHLIST.match("1001297", "SABINE PASS LNG TERMINAL")
    assert hit is not None and hit.label == "sabine_pass_lng"

    hit_gp = WATCHLIST.match("9999991", "GOLDEN PASS LATERAL")
    assert hit_gp is not None and hit_gp.label == "golden_pass_lng"

    hit_cp = WATCHLIST.match("9999992", "COVE POINT SUPPLY")
    assert hit_cp is not None and hit_cp.label == "cove_point_lng"


def test_watchlist_does_not_regex_bare_lng() -> None:
    """The TETCO lesson: 'KINDER MORGAN LNG RECEIPT' is NOT terminal feedgas."""
    assert WATCHLIST.match("1234567", "KINDER MORGAN LNG RECEIPT (WLA)") is None


def test_watchlist_storage_requires_suffix() -> None:
    assert WATCHLIST.match("1003045", "EMINENCE STORAGE") is not None
    # Bare city name must never match storage rules.
    assert WATCHLIST.match("1003046", "WASHINGTON DC METRO") is None


# ---------------------------------------------------------------------------
# Transformer.
# ---------------------------------------------------------------------------


def test_num_strips_thousands_separators() -> None:
    assert _num("163,735") == 163_735.0
    assert _num("-950,000") == -950_000.0
    assert _num("0") == 0.0
    assert _num("") is None
    assert _num("OPEN") is None


def _write_raw(tmp_path: Path) -> Path:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    payload = {
        "fetched_at": "2026-08-23T00:00:00Z",
        "source_slug": "transco",
        "series_prefix": "transco",
        "tsp_name": "TRANSCONTINENTAL GAS PIPE LINE COMPANY, LLC",
        "gas_day": "2026-08-22",
        "cycle": "id3",
        "cycle_desc": "Intraday 3",
        "posted_at": "08/23/2026 04:12:45 PM",
        "row_count": 3,
        "data": [
            {"loc": "1001297", "loc_name": "SABINE PASS LNG TERMINAL",
             "flow_ind": "D", "tsq": "2,100,000", "oac": "-950,000",
             "operating_cap": "1,150,000", "design_cap": "1,200,000"},
            {"loc": "1003045", "loc_name": "EMINENCE STORAGE",
             "flow_ind": "R", "tsq": "0", "oac": "480,000",
             "operating_cap": "480,000", "design_cap": "500,000"},
            {"loc": "1006175", "loc_name": "GULF SOUTH-MAGNOLIA M4150",
             "flow_ind": "R", "tsq": "60,000", "oac": "25,000",
             "operating_cap": "85,000", "design_cap": "90,000"},
        ],
    }
    (raw_dir / "transco_2026-08-22_id3.json").write_text(json.dumps(payload))
    return raw_dir


def test_transform_emits_flow_token_series(tmp_path: Path) -> None:
    curated = tmp_path / "williams.parquet"
    stats = transform(_write_raw(tmp_path), curated)
    df = pd.read_parquet(curated)

    # Gulf South-Magnolia is NOT on the watchlist → excluded.
    assert not df["series_id"].str.contains("1006175").any()

    # Sabine Pass: both legs would be distinct series; here only D exists.
    sq_d = df[df["series_id"] == "transco_sq_1001297_d_id3"]
    assert len(sq_d) == 1 and float(sq_d.iloc[0]["value"]) == 2_100_000.0

    # Storage zero TSQ preserved as signal.
    zero = df[df["series_id"] == "transco_sq_1003045_r_id3"]
    assert len(zero) == 1 and float(zero.iloc[0]["value"]) == 0.0

    # Canonical schema columns only.
    assert set(df.columns) == {
        "source", "series_id", "series_name", "period", "value",
        "unit", "region", "ingested_at",
    }
    assert set(df["unit"]) == {"Dth/d"}
    assert set(df["source"]) == {"transco"}
    assert set(df["region"]) == {"US"}
    assert stats["rows"] == len(df)


def test_transform_accumulates_without_overwrite(tmp_path: Path) -> None:
    curated = tmp_path / "williams.parquet"
    raw_dir = _write_raw(tmp_path)

    first = transform(raw_dir, curated)
    n_first = first["rows"]

    # Second run with an UPDATED value for the same key must merge, not clobber.
    payload_path = raw_dir / "transco_2026-08-22_id3.json"
    payload = json.loads(payload_path.read_text())
    payload["data"][0]["tsq"] = "2,150,000"
    payload_path.write_text(json.dumps(payload))

    second = transform(raw_dir, curated)
    assert second["rows"] >= n_first

    df = pd.read_parquet(curated)
    vals = df[df["series_id"] == "transco_sq_1001297_d_id3"]
    assert len(vals) == 1  # deduped on (series_id, period)
    assert float(vals.iloc[0]["value"]) == 2_150_000.0  # newer ingested_at won


def test_transform_zero_rows_raises(tmp_path: Path) -> None:
    from transformers.errors import TransformError

    raw_dir = tmp_path / "empty_raw"
    raw_dir.mkdir()
    with pytest.raises(TransformError):
        transform(raw_dir, tmp_path / "out.parquet")


# ---------------------------------------------------------------------------
# Orchestrator (run) with fake client.
# ---------------------------------------------------------------------------


def _seed_fake(rows: list[dict[str, str]], **header_overrides: str) -> None:
    client = FakeClient()

    def factory(*args: Any, **kwargs: Any) -> FakeClient:
        return client

    def run_factory(**kwargs: Any) -> FakeClient:
        return client

    client.rows = rows or []
    for k, v in header_overrides.items():
        setattr(client, k, v)
    FakeClient.__enter__ = lambda self: self  # type: ignore[method-assign]
    return client


def test_run_writes_watchlist_payload(tmp_path: Path) -> None:
    import scrapers.williams as pkg

    class WatchClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.rows = [
                {"loc": "1001297", "loc_prop": "Delivery", "loc_purp": "LNG Delivery",
                 "flow_ind": "D", "loc_name": "SABINE PASS LNG TERMINAL",
                 "design_cap": "1,200,000", "operating_cap": "1,150,000",
                 "tsq": "2,100,000", "oac": "-950,000", "it_indicator": "N"},
                {"loc": "1006175", "loc_prop": "Receipt", "loc_purp": "Receipt",
                 "flow_ind": "R", "loc_name": "GULF SOUTH-MAGNOLIA M4150",
                 "design_cap": "90,000", "operating_cap": "85,000",
                 "tsq": "60,000", "oac": "25,000", "it_indicator": "N"},
            ]

    result = pkg.run(
        "id3", date(2026, 8, 22),
        raw_dir=tmp_path / "raw", client_cls=WatchClient,
    )
    assert result["status"] == "ok"
    assert result["system_rows"] == 2  # full system seen
    out_file = Path(result["path"])
    payload = json.loads(out_file.read_text())
    names = [r["loc_name"] for r in payload["data"]]
    assert names == ["SABINE PASS LNG TERMINAL"]  # watchlist filtered


def test_run_cycle_mismatch_is_skipped(tmp_path: Path) -> None:
    import scrapers.williams as pkg

    class MismatchClient(FakeClient):
        def fetch_oac(self, cycle: str, gas_day: date):  # type: ignore[override]
            return _fake_header(cycle_value="4"), [{"loc": "1001297"}]

    result = pkg.run(
        "id3", date(2026, 8, 22),
        raw_dir=tmp_path / "raw", client_cls=MismatchClient,
    )
    assert result["status"] == "skipped"


def test_run_unposted_day_is_empty_not_crash(tmp_path: Path) -> None:
    """Port Arthur lesson: an unposted day is data, not an error."""
    import scrapers.williams as pkg

    class EmptyClient(FakeClient):
        def fetch_oac(self, cycle: str, gas_day: date):  # type: ignore[override]
            return TranscoHeader(
                tsp_name="", cycle_value="", cycle_desc="",
                gas_day="", posting_date="", posting_time="", meas_basis="",
            ), []

    result = pkg.run(
        "timely", date(2026, 1, 1),
        raw_dir=tmp_path / "raw", client_cls=EmptyClient,
    )
    assert result["status"] == "empty"
    assert result["rows"] == 0


def test_run_records_health_failure_on_block(tmp_path: Path) -> None:
    import scrapers.williams as pkg

    class BlockedClient(FakeClient):
        def fetch_oac(self, cycle: str, gas_day: date):  # type: ignore[override]
            raise TranscoWafError(
                url="https://www.1line.williams.com/x", status=403,
                attempts=2, elapsed_s=0.0, reason="gateway block",
            )

    result = pkg.run(
        "evening", date(2026, 8, 22),
        raw_dir=tmp_path / "raw", client_cls=BlockedClient,
    )
    assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# Backfill windowing.
# ---------------------------------------------------------------------------


def test_month_windows_cover_range() -> None:
    from scrapers.williams.backfill import _month_windows

    windows = _month_windows(date(2026, 7, 15), date(2026, 9, 10))
    assert windows[0] == (date(2026, 7, 15), date(2026, 7, 31))
    assert windows[1] == (date(2026, 8, 1), date(2026, 8, 31))
    assert windows[-1] == (date(2026, 9, 1), date(2026, 9, 10))


def test_backfill_walks_cycles_with_checkpoint(tmp_path: Path) -> None:
    from scrapers.williams.backfill import WilliamsBackfill

    raw_dir = tmp_path / "raw"
    checkpoint = tmp_path / "state.json"

    class BackfillFake(FakeClient):
        fetched: list[tuple[date, str]] = []

        def fetch_oac(self, cycle: str, gas_day: date):  # type: ignore[override]
            self.fetched.append((gas_day, cycle))  # type: ignore[attr-defined]
            hdr = _fake_header(gas_day=gas_day.strftime("%m/%d/%Y"))
            rows = [{
                "loc": "1001297", "loc_name": "SABINE PASS LNG TERMINAL",
                "flow_ind": "D", "tsq": "100,000", "oac": "50,000",
                "operating_cap": "90,000", "design_cap": "120,000",
                "_gas_day": gas_day.strftime("%m/%d/%Y"),
            }]
            return hdr, rows

    walker = WilliamsBackfill(
        client=BackfillFake(),  # type: ignore[arg-type]
        raw_dir=raw_dir,
        checkpoint_path=checkpoint,
        cycles=("timely",),
    )
    stats = walker.run(date(2026, 8, 20), date(2026, 8, 21))
    assert stats["fetched"] == 2
    files = sorted(p.name for p in raw_dir.glob("*.json"))
    assert files == [
        "transco_2026-08-20_timely.json",
        "transco_2026-08-21_timely.json",
    ]
    # Checkpoint advanced to the end.
    state = json.loads(checkpoint.read_text())
    assert state["next_window"] == stats["windows"]
