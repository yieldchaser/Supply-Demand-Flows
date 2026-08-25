"""Update legacy transformer tests for cycle-tokened series ids."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from scrapers.base.identity import TenantFallbackError
from scrapers.kinder_morgan import parse_opavail_grid, scrape_tenant_best_available
from transformers.kinder_morgan import transform_payload

# Real grid shape (live-fire verified 2026-08-25 from CI runners).
_GRID_HTML = """
<html><head><title>Informational Postings :: Natural Gas Pipeline Company of America LLC</title></head>
<body><table>
<tr><th>Loc</th><th>Loc Name</th></tr>
<tr><td><a href="x">View</a></td><td>3592</td><td>SABPL/NGPL HENRY HUB VERMILION</td>
<td>05</td><td>24</td><td>500,000</td><td>500,000</td><td>472,702</td><td>27,298</td>
<td>N</td><td>BD</td><td>Y</td></tr>
<tr><td>View</td><td>99999</td><td>SOME OTHER METER</td><td>05</td><td>24</td>
<td>100,000</td><td>100,000</td><td>50,000</td><td>50,000</td><td>N</td><td>BD</td><td>Y</td></tr>
<tr><td>View</td><td>44337</td><td>SABPL/KMLP CALCA</td><td>01</td><td>120</td>
<td>443,017</td><td>443,017</td><td>0</td><td>443,017</td><td>N</td><td>BD</td><td>Y</td></tr>
</table></body></html>
"""

_TGP_FALLBACK_HTML = (
    "<html><head><title>Informational Postings :: Tennessee Gas Pipeline Company"
    ", L.L.C.</title></head><body></body></html>"
)


class TestParseOpavailGrid:
    def test_parses_confirmed_meters(self) -> None:
        rows = parse_opavail_grid(_GRID_HTML)
        locs = {r["loc"] for r in rows}
        assert {"3592", "99999", "44337"} <= locs

    def test_row_shape(self) -> None:
        rows = parse_opavail_grid(_GRID_HTML)
        r3592 = next(r for r in rows if r["loc"] == "3592")
        assert r3592["loc_name"] == "SABPL/NGPL HENRY HUB VERMILION"
        assert r3592["total_scheduled_quantity"] == "472,702"
        assert r3592["flow_ind"] == "BD"

    def test_header_rows_skipped(self) -> None:
        rows = parse_opavail_grid(_GRID_HTML)
        # header row has <th> only — never appears as a data row
        assert all(r["loc"].isdigit() for r in rows)


class TestTenantFallbackTrap:
    def test_scrape_raises_when_tgp_fallback_served(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """THE TRAP: NGPL requested, TGP page served (HTTP 200) must raise."""
        import scrapers.kinder_morgan as km

        monkeypatch.setattr(km, "httpx", _FakeHttpxModule)
        with pytest.raises(TenantFallbackError):
            scrape_tenant_best_available("NGPL")


class _FakeClientFactory:
    """Minimal context-manager client stub returning the fallback page."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        pass

    def __enter__(self) -> _FakeClientFactory:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def get(self, url: str, **kwargs: object) -> FakeResp:
        return FakeResp(_TGP_FALLBACK_HTML)

    def post(self, url: str, **kwargs: object) -> FakeResp:
        return FakeResp(_TGP_FALLBACK_HTML)


class FakeResp:
    status_code = 200
    headers: dict[str, str] = {}

    def __init__(self, text: str) -> None:
        self.text = text


class _FakeHttpxModule:
    """Stand-in for the httpx module so the test patches the module object
    rather than reaching through ``km.httpx`` (which strict mypy flags as a
    non-exported attribute)."""

    Client = _FakeClientFactory


class TestTransformPayload:
    def test_only_confirmed_meters_emitted(self) -> None:
        payload = {
            "pipeline_prefix": "ngpl",
            "cycle": "BEST_AVAILABLE",
            "gas_day_requested": "2026-08-25",
            "fetched_at": "2026-08-25T00:00:00Z",
            "data": [
                {
                    "loc": "3592",
                    "total_scheduled_quantity": "472,702",
                    "loc_zone": "05",
                },
                {
                    "loc": "99999",
                    "total_scheduled_quantity": "10,000",
                },
            ],
        }
        rows = transform_payload(payload)
        assert len(rows) == 1
        # Cycle token lowercased with underscore; flow leg d.
        assert rows[0]["series_id"] == "km_ngpl_sq_3592_d_best_available"
        # RAW Dth/d — conversion to MMcf happens only in the frontend.
        assert rows[0]["value"] == 472702
        assert rows[0]["unit"] == "Dth/d"

    def test_kmlp_44337_never_emitted(self) -> None:
        payload = {
            "pipeline_prefix": "kmlp",
            "data": [{"loc": "44337", "total_scheduled_quantity": "5,000"}],
        }
        assert transform_payload(payload) == []

    def test_tgp_meter_maps_to_corpus(self) -> None:
        payload = {
            "pipeline_prefix": "tgp",
            "cycle": "BEST_AVAILABLE",
            "data": [{"loc": "49861", "total_scheduled_quantity": "169,489"}],
        }
        rows = transform_payload(payload)
        assert rows[0]["series_id"] == "km_tgp_sq_49861_d_best_available"


def test_raw_payload_roundtrip(tmp_path: Path) -> None:
    """A written raw payload transforms into curated-shaped rows."""
    payload = {
        "fetched_at": "2026-08-25T01:00:00Z",
        "source": "kinder_morgan",
        "tenant_code": "TGP",
        "pipeline_prefix": "tgp",
        "gas_day_requested": "2026-08-24",
        "cycle": "BEST_AVAILABLE",
        "row_count": 1,
        "data": [
            {
                "loc": "49861",
                "total_scheduled_quantity": "169,489",
                "loc_zone": "00",
            }
        ],
    }
    out = tmp_path / "raw.json"
    out.write_text(json.dumps(payload), encoding="utf-8")
    loaded = json.loads(out.read_text(encoding="utf-8"))
    rows = transform_payload(loaded)
    df = pd.DataFrame(rows)
    assert list(df.columns)[:4] == ["source", "series_id", "series_name", "period"]
