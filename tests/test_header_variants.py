"""Header-variant hardening tests — every CSV/TSV parser survives header noise.

Background: a double-space variant ("Operationally  Available Capacity")
silently dropped every Gulf South OAC series for months (~205,000 rows).
These tests pin the contract that spacing/case/BOM variants still parse,
while genuinely renamed columns raise HeaderMismatchError instead of
silently returning None.
"""

from __future__ import annotations

import pytest

from scrapers.base.headers import (
    HeaderMismatchError,
    normalize_header,
    rename_keys,
    resolve_columns,
)

# ---------------------------------------------------------------------------
# scrapers.base.headers unit behavior
# ---------------------------------------------------------------------------


def test_normalize_header_collapses_whitespace_and_case() -> None:
    assert normalize_header("Operationally  Available Capacity") == (
        "operationally available capacity"
    )
    assert normalize_header("  TOTAL Scheduled\tquantity ") == (
        "total scheduled quantity"
    )
    assert normalize_header("OAC") == normalize_header(" oac ")


def test_normalize_header_strips_bom() -> None:
    assert normalize_header("\ufeffTSP Name") == "tsp name"


def test_resolve_columns_maps_variants_to_canonical() -> None:
    mapping = resolve_columns(
        ["Loc", "Operationally Available Capacity"],
        ["Loc", "Operationally  Available Capacity"],
        source="unit",
    )
    assert mapping["Operationally Available Capacity"] == (
        "Operationally  Available Capacity"
    )


def test_resolve_columns_raises_naming_every_unmatched() -> None:
    with pytest.raises(HeaderMismatchError) as exc:
        resolve_columns(
            ["Loc", "Total Scheduled Quantity", "Mystery Column"],
            ["Loc", "Totall Schedulled Quantity"],
            source="unit-test",
        )
    msg = str(exc.value)
    assert "Mystery Column" in msg
    assert "unit-test" in msg


def test_resolve_columns_optional_never_fails() -> None:
    mapping = resolve_columns(
        ["Loc"],
        ["loc"],
        optional=["Quantity Not Available Reason"],
        source="unit",
    )
    assert mapping == {"Loc": "loc"}


def test_rename_keys_roundtrip() -> None:
    colmap = resolve_columns(
        ["Loc Name", "Flow Ind"],
        ["Loc Name ", "FLOW IND"],
        source="unit",
    )
    row = {"Loc Name ": "Stratton Ridge", "FLOW IND": "D", "Extra": "x"}
    out = rename_keys(row, colmap)
    assert out == {"Loc Name": "Stratton Ridge", "Flow Ind": "D"}


# ---------------------------------------------------------------------------
# Per-source parser variants
# ---------------------------------------------------------------------------


_GS_HEADER = (
    "\ufeffTSP Name,TSP,Post Date/Time,Effective Gas Day,Effective Time,LineCode,"
    "Loc,Loc Name,Loc Zn,Loc Purp Desc,Loc/QTI,Flow Ind,Design Capacity,"
    "Operating Capacity,Total Scheduled Quantity,Operationally  Available Capacity,"
    "IT,All Qty Avail,Quantity Not Available Reason,Meas Basis Desc\n"
)
_GS_ROW = (
    '"Gulf South Pipeline Company, LLC",078444247,20260522 21:53:00,20260522,'
    '22:00:00,625,24329,Stratton Ridge (To Freeport Lng),SYS,Delivery Location,'
    'DPQ,D,1779129,1779129,920149,858980,,Y,,MMBtu\n'
)


def test_gulf_south_double_space_oac_variant_parses() -> None:
    """THE regression: double-space 'Operationally  Available Capacity'."""
    from scrapers.energy_transfer.gulf_south import parse_oac_csv

    rows = parse_oac_csv(_GS_HEADER + _GS_ROW)
    assert len(rows) == 1
    assert rows[0]["Operationally Available Capacity"] == "858980"
    assert rows[0]["Total Scheduled Quantity"] == "920149"


def test_gulf_south_trailing_space_and_case_variant_parses() -> None:
    from scrapers.energy_transfer.gulf_south import parse_oac_csv

    noisy = _GS_HEADER.replace(
        "Total Scheduled Quantity", " total scheduled quantity "
    ).replace("Loc Name", "LOC NAME")
    rows = parse_oac_csv(noisy + _GS_ROW)
    assert rows[0]["Total Scheduled Quantity"] == "920149"


def test_gulf_south_renamed_column_raises_loudly() -> None:
    from scrapers.energy_transfer.gulf_south import parse_oac_csv

    broken = _GS_HEADER.replace(
        "Operationally  Available Capacity", "Capacity Operationally Available"
    )
    with pytest.raises(HeaderMismatchError):
        parse_oac_csv(broken + _GS_ROW)


_QUORUM_HEADER = (
    "Post Date/Time,TSP ,TSP Name,Eff Gas Day/Time,End Eff Gas Day/Time,Cycle Desc,"
    "Loc,LOC NAME,Loc Purp,Loc/QTI,Flow Ind,IT Desc,Meas Basis,All Qty Avail,"
    "Quantity Not Available Reason,,Design Capacity,Operating Capacity,"
    "Operationally Available Capacity,Total Scheduled Quantity\n"
)
_QUORUM_ROW = (
    '2026-08-22 22:00:00,2,VENTURE GLOBAL PLAQ,8/22/2026 10:00:00 PM,'
    '8/23/2026 10:00:00 AM,Intraday 3,VGPQD,VGPQD,LNG,D,QCP,FIRM,N/A,Yes,,'
    ',3940000,3940000,1200000,2700000\n'
)


def test_quorum_spacing_case_variants_parse() -> None:
    from scrapers.quorum.pipelines import _UNNAMED_COLUMN_KEY, parse_export_csv

    rows = parse_export_csv(_QUORUM_HEADER + _QUORUM_ROW)
    assert len(rows) == 1
    r = rows[0]
    assert r["Total Scheduled Quantity"] == "2700000"
    assert r["Operationally Available Capacity"] == "1200000"
    assert r["Loc Name"] == "VGPQD"
    # positional blank column placeholder survives re-keying
    assert _UNNAMED_COLUMN_KEY in r


def test_quorum_renamed_column_raises_loudly() -> None:
    from scrapers.quorum.pipelines import parse_export_csv

    broken = _QUORUM_HEADER.replace(
        "Total Scheduled Quantity", "Scheduled Quantity Total"
    )
    with pytest.raises(HeaderMismatchError):
        parse_export_csv(broken + _QUORUM_ROW)


_BHE_ROW = (
    '"08/24/2026","10:45 AM","EGTS","BHE GT&S - ENERGY TRANSFER GAS SYSTEM",'
    '"Intraday 1","2026-08-25","","MMBtu","Receipt","LNG","COVE POINT LNG LP",'
    '"Y","40704","EGTS - LOUDOUN","R",100000,50000,25000,75000,"N","","R"\n'
)
_BHE_HEADER = (
    "Posting Date,Posting Time,TSP ,TSP Name,CycleDesc,Eff Gas Day,Eff Time,"
    "Meas Basis Desc,Loc Purp Desc,Loc/QTI Desc,Interconnect Party Name,OIA,"
    "Loc,Loc Name,Flow Ind,Design Capacity,Operating Capacity,"
    "Total Scheduled Quantity,Operationally  Available Capacity,"
    "All Quantities Available Indicator,Quantity Not Available Reason,IT\n"
)


def test_bhe_double_space_variant_parses() -> None:
    from scrapers.bhe.client import parse_oac_csv

    rows = parse_oac_csv(_BHE_HEADER + _BHE_ROW)
    assert len(rows) == 1
    r = rows[0]
    assert r["Operationally Available Capacity"] == "75000"
    assert r["Total Scheduled Quantity"] == "25000"
    assert r["Interconnect Party Name"] == "COVE POINT LNG LP"


def test_bhe_renamed_column_raises_loudly() -> None:
    from scrapers.bhe.client import parse_oac_csv

    broken = _BHE_HEADER.replace("Operationally", "Operationallyyy")
    with pytest.raises(HeaderMismatchError):
        parse_oac_csv(broken + _BHE_ROW)


_TETCO_HEADER = (
    "Cycle_Desc, Post_Date, Eff_Gas_Day,Cap_Type_Desc,Post_Time,Eff_Time,Loc,"
    "Loc_Name, Loc_Zn,Flow_Ind_Desc,Loc_Purp_Desc,Loc_QTI_Desc,Meas_Basis_Desc,"
    "IT,All_Qty_Avail,Total_Design_Capacity,Operating_Capacity,"
    "total_scheduled_quantity ,Operationally_Available_Capacity,TSP_Name,TSP\n"
)
_TETCO_ROW = (
    "Timely,08/24/2026 06:00:00 PM,08/25/2026,PWR,06:00:00 PM,,79999,"
    "STRATTON RIDGE,,DELIVERY,DPQ,ISRM4,MMBtu,N,Y,1500000,1500000,700000,"
    "800000,TX EAST TRANSMISSION LP,TE\n"
)


def test_enbridge_spacing_case_variants_parse() -> None:
    from scrapers.enbridge.client import parse_capacity_csv

    rows = parse_capacity_csv(_TETCO_HEADER + _TETCO_ROW)
    assert len(rows) == 1
    r = rows[0]
    assert r["Total_Scheduled_Quantity"] == "700000"
    assert r["Operationally_Available_Capacity"] == "800000"
    assert r["Loc"] == "79999"


def test_enbridge_renamed_column_raises_loudly() -> None:
    from scrapers.enbridge.client import parse_capacity_csv

    broken = _TETCO_HEADER.replace(
        "Operationally_Available_Capacity", "Available_Operationally_Capacity"
    )
    with pytest.raises(HeaderMismatchError):
        parse_capacity_csv(broken + _TETCO_ROW)


_GASNOM_ROWS = [
    {
        "TSP Name": "GOLDEN PASS PIPELINE",
        "Location": "1097217",
        "Location_Name": "Golden Pass Terminal",
        "Location_Zone": "LOCZN",
        "Loc_Purp": "D",
        "Loc/QTI": "LNG",
        "All_Qty_Avail": "Y",
        "Flow_Ind": "D",
        "IT": "F",
        "Design_Capacity": "2600000",
        "Operational_Capacity": "2600000",
        "TSQ": "1400000",
        "OAC": "1200000",
        "Eff_Gas_Day/Time": "08/25/2026",
        "CycleDesc": "TIMELY",
        "Posting_Date/Time": "08/24/2026 11:05",
        "Measurement_Basis": "MMBtu",
        "Pressure_Base": "PSIG",
    }
]


def test_gasnom_case_variant_keys_normalize() -> None:
    """Bulk-TSV key maps tolerate case/spacing noise in upstream headers."""
    from scrapers.gasnom import _normalize_tsv_rows

    noisy = [dict(_GASNOM_ROWS[0])]
    noisy[0][" tsq "] = noisy[0].pop("TSQ")  # spacing/case variant
    noisy[0]["oac"] = noisy[0].pop("OAC")  # lowercase key
    normalized = _normalize_tsv_rows(noisy)
    assert len(normalized) == 1
    assert normalized[0]["tsq"] == "1400000"
    assert normalized[0]["oac"] == "1200000"


def test_gasnom_renamed_column_raises_loudly() -> None:
    from scrapers.gasnom import _normalize_tsv_rows

    noisy = [dict(_GASNOM_ROWS[0])]
    noisy[0]["Total_Sched_Qty"] = noisy[0].pop("TSQ")
    with pytest.raises(HeaderMismatchError):
        _normalize_tsv_rows(noisy)


def test_km_grid_mixed_cell_counts_raise() -> None:
    """KM has no header row; its guard is positional cell-count consistency."""
    from scrapers.kinder_morgan import parse_opavail_grid

    good = (
        "<tr><td>View</td><td>49861</td><td>CCCPL/TGP SINTON</td><td>00</td>"
        "<td>101</td><td>768,750</td><td>768,750</td><td>169,489</td><td>599,261</td>"
        "<td>N</td><td>BD</td><td>Y</td></tr>"
    )
    shifted = good.replace("<td>N</td>", "<td>N</td><td>NEWCOL</td>")
    with pytest.raises(HeaderMismatchError):
        parse_opavail_grid(good + shifted)


def test_km_grid_uniform_shape_parses() -> None:
    from scrapers.kinder_morgan import parse_opavail_grid

    row = (
        "<tr><td>View</td><td>49861</td><td>CCCPL/TGP SINTON</td><td>00</td>"
        "<td>101</td><td>768,750</td><td>768,750</td><td>169,489</td><td>599,261</td>"
        "<td>N</td><td>BD</td><td>Y</td></tr>"
    )
    rows = parse_opavail_grid(row + row)
    assert len(rows) == 2
    assert all(r["loc"] == "49861" for r in rows)
