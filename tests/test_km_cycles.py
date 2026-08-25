"""Tests for KM cycle pinning — scraper + transformer. Zero live calls.

The clientState fixture strings are the exact serializations captured from a
real browser session on pipeline2.kindermorgan.com (TGP tenant, 2026-08-25)
and replayed successfully through httpx (probe42-43).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scrapers.kinder_morgan import (
    CYCLES,
    _harvest_hidden_inputs,
    build_cycle_client_state,
    parse_cycle_label,
    parse_opavail_grid,
)
from transformers.kinder_morgan import transform_payload

# ---------------------------------------------------------------------------
# Fixtures — real page shapes from pipeline2.kindermorgan.com (TGP)
# ---------------------------------------------------------------------------

_PAGE_SHELL = """
<html><head><title>Informational Postings :: Tennessee Gas Pipeline Company, L.L.C.</title></head>
<body><form>
<input type="hidden" name="__VIEWSTATE" value="/qJcuv9J77e8lFs7XxZD5DEEb2YzccBmWNkuraP9rMBBWIWnF8jQywRtvv6mVmZdO" />
<input type="hidden" name="__VIEWSTATEGENERATOR" value="D16F03EC" />
<input type="hidden" name="__EVENTVALIDATION" value="XV0NRQS/Wf77PwqtMR3KNxu8Gaer9x8fN9zA01w02v/9KAJq/I1rEjJIraMx" />
<input type="hidden" id="WebSplitter1_tmpl1_ContentPlaceHolder1_ddlCycleDD_clientState"
       name="WebSplitter1_tmpl1_ContentPlaceHolder1_ddlCycleDD_clientState" value="" />
<table><tr>
<td>View</td><td>49861</td><td>CCCPL/TGP SINTON SAN PATRICIO</td><td>00</td><td>101</td>
<td>768,750</td><td>768,750</td><td>169,489</td><td>599,261</td><td>N</td><td>BD</td><td>Y</td>
</tr></table>
</form></body></html>
"""

_GRID_TIMELY = """
<span id="WebSplitter1_tmpl1_ContentPlaceHolder1_lCycle">CycleDesc:  TIMELY</span>
<table><tr>
<td>View</td><td>49861</td><td>CCCPL/TGP SINTON SAN PATRICIO</td><td>00</td><td>101</td>
<td>768,750</td><td>768,750</td><td>169,489</td><td>599,261</td><td>N</td><td>BD</td><td>Y</td>
</tr></table>
"""

_GRID_ITRD1 = _GRID_TIMELY.replace("CycleDesc:  TIMELY", "CycleDesc:  INTRADAY 1").replace(
    "<td>169,489</td>", "<td>241,314</td>"
)

_WRONG_TENANT = '<html><head><title>Informational Postings :: Elba Express Company, L.L.C.</title></head></html>'


# ---------------------------------------------------------------------------
# clientState builder — must match the browser serialization byte-for-byte
# ---------------------------------------------------------------------------

_BROWSER_CAPTURED_TIMELY = (
    '|0|TIMELY&tilda;1||[[[[null,null,null,null,null,null,null,-1,null,null,null,'
    'null,null,null,null,null,null,null,null,null,null,null,null,"BEST%20AVAILABLE",null,'
    'null,null,null,null,null,null,null,null,null,null,null,null,0,null,null,null,-1,null,'
    'null,null,null,null,null,null,null]],[],null],[{"0":[7,1],"1":[23,"TIMELY"]},'
    '[{"0":[0,7,0],"1":[1,7,1]}]],null]'
)


@pytest.mark.parametrize("cycle", list(CYCLES))
def test_build_cycle_client_state_structure(cycle: str) -> None:
    """Every cycle builds a pipe-delta with its code, index and change log."""
    cs = build_cycle_client_state(cycle)
    idx = CYCLES[cycle][0]
    assert cs.startswith(f"|0|{cycle}&tilda;{idx}||")
    assert f'"0":[7,{idx}]' in cs
    assert f'"1":[23,"{cycle}"]' in cs
    # initial blob still carries BEST AVAILABLE as the loaded value
    assert "BEST%20AVAILABLE" in cs
    # JSON blob tail parses
    json_part = cs.split("||", 1)[1]
    parsed = json.loads(json_part)
    assert isinstance(parsed, list) and len(parsed) == 3


def test_build_cycle_client_state_matches_browser_capture() -> None:
    """TIMELY serialization equals what a real browser posted (probe43)."""
    assert build_cycle_client_state("TIMELY") == _BROWSER_CAPTURED_TIMELY


def test_build_cycle_client_state_unknown_cycle_raises() -> None:
    with pytest.raises(KeyError):
        build_cycle_client_state("NOTACYCLE")


# ---------------------------------------------------------------------------
# Cycle-label parsing + pin guard
# ---------------------------------------------------------------------------


def test_parse_cycle_label_timely() -> None:
    assert parse_cycle_label(_GRID_TIMELY) == "CycleDesc:  TIMELY"


def test_parse_cycle_label_missing_returns_empty() -> None:
    assert parse_cycle_label(_WRONG_TENANT) == ""


# ---------------------------------------------------------------------------
# Grid parsing
# ---------------------------------------------------------------------------


def test_parse_opavail_grid_49861() -> None:
    rows = parse_opavail_grid(_GRID_TIMELY)
    assert len(rows) == 1
    row = rows[0]
    assert row["loc"] == "49861"
    assert row["total_scheduled_quantity"] == "169,489"
    assert row["operating_capacity"] == "768,750"
    assert row["flow_ind"] == "BD"


def test_harvest_hidden_inputs() -> None:
    fields = _harvest_hidden_inputs(_PAGE_SHELL)
    assert "__VIEWSTATE" in fields
    assert fields["__VIEWSTATEGENERATOR"] == "D16F03EC"


# ---------------------------------------------------------------------------
# Transformer — flow-tokened series keys with real cycle tokens
# ---------------------------------------------------------------------------


def _raw_payload(cycle: str, tsq: str, gas_day: str) -> dict[str, Any]:
    return {
        "fetched_at": "2026-08-25T12:00:00+00:00",
        "source": "kinder_morgan",
        "tenant_code": "TGP",
        "pipeline_prefix": "tgp",
        "gas_day_requested": gas_day,
        "cycle": cycle,
        "row_count": 1,
        "data": [
            {
                "loc": "49861",
                "loc_name": "CCCPL/TGP SINTON SAN PATRICIO",
                "loc_zone": "00",
                "loc_segment": "101",
                "design_capacity": "768,750",
                "operating_capacity": "768,750",
                "total_scheduled_quantity": tsq,
                "operationally_available_capacity": "599,261",
                "it": "N",
                "flow_ind": "BD",
                "all_qty_avail": "Y",
            }
        ],
    }


def test_transform_emits_flow_and_cycle_tokens(tmp_path: Path) -> None:
    payload = _raw_payload("ITRD1", "241,314", "2026-08-24")
    rows = transform_payload(payload)
    assert len(rows) == 1
    r = rows[0]
    # {prefix}_{sq}_{loc}_{flow}_{cycle}
    assert r["series_id"] == "km_tgp_sq_49861_d_itrd1"
    assert r["series_id"].count("_") >= 5
    assert r["unit"] == "MMcf/d"


def test_transform_best_available_still_tokened(tmp_path: Path) -> None:
    payload = _raw_payload("BEST_AVAILABLE", "169,489", "2026-08-24")
    payload["cycle"] = "BEST_AVAILABLE"
    rows = transform_payload(payload)
    assert len(rows) == 1
    assert rows[0]["series_id"] == "km_tgp_sq_49861_d_best_available"


def test_transform_unconfirmed_meters_skipped() -> None:
    payload = _raw_payload("ITRD1", "999,999", "2026-08-24")
    payload["data"][0]["loc"] = "12345"
    rows = transform_payload(payload)
    assert rows == []
