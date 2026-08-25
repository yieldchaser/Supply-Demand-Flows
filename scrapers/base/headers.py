"""Header normalization + column resolution for scraped tabular payloads.

Why:
    Exact-match lookups against upstream column headers are systemically
    fragile. A DOUBLE-SPACE header variant ("Operationally  Available
    Capacity") silently dropped every Gulf South OAC series for months —
    205,000 rows — because ``row.get("Operationally Available Capacity")``
    returned None and the transformer treated None as "column absent". That
    was the fifth silent-data-loss bug of this family; every one of them
    lived at exactly this lookup boundary.

What:
    ``normalize_header`` collapses internal whitespace and casefolds so all
    spacing/case variants compare equal; ``resolve_columns`` maps a parser's
    EXPECTED column names onto whatever the payload ACTUALLY carries via
    normalized comparison, raising :class:`HeaderMismatchError` naming every
    unmatched expected column instead of silently returning None.

Failure modes:
    ``resolve_columns`` NEVER returns a partial mapping silently: any
    expected column with no normalized match raises with the full diff
    (expected vs actual headers), so schema drift fails loudly at the
    parsing boundary where it can be diagnosed — not months later as a
    hole in curated history.
"""

from __future__ import annotations

import re

_WHITESPACE_RE = re.compile(r"\s+")

#: Gulf South (Boardwalk) OAC CSV columns, exactly as the live export spells
#: them. resolve_columns() tolerates whitespace/case variants of these.
GULF_SOUTH_CSV_COLUMNS: tuple[str, ...] = (
    "TSP Name",
    "TSP",
    "Post Date/Time",
    "Effective Gas Day",
    "Effective Time",
    "LineCode",
    "Loc",
    "Loc Name",
    "Loc Zn",
    "Loc Purp Desc",
    "Loc/QTI",
    "Flow Ind",
    "Design Capacity",
    "Operating Capacity",
    "Total Scheduled Quantity",
    "Operationally Available Capacity",
    "IT",
    "All Qty Avail",
    "Quantity Not Available Reason",
    "Meas Basis Desc",
)

#: BHE GT&S (EGTS) OAC CSV columns — exact spellings from real
#: searchHistoricalData blobs. Note: NO Loc Zn column, and the All-Qty
#: indicator is spelled out in full.
BHE_CSV_COLUMNS: tuple[str, ...] = (
    "Posting Date",
    "Posting Time",
    "TSP",
    "TSP Name",
    "CycleDesc",
    "Eff Gas Day",
    "Eff Time",
    "Meas Basis Desc",
    "Loc Purp Desc",
    "Loc/QTI Desc",
    "Interconnect Party Name",
    "OIA",
    "Loc",
    "Loc Name",
    "Operating Capacity",
    "Total Scheduled Quantity",
    "Operationally Available Capacity",
    "Design Capacity",
    "All Quantities Available Indicator",
    "Quantity Not Available Reason",
    "Flow Ind",
    "IT",
)

#: Enbridge rtba TE_OA_MLC_* CSV columns (TETCO et al.) — mirrors COL_* in
#: scrapers.enbridge.client.
ENBRIDGE_CSV_COLUMNS: tuple[str, ...] = (
    "Cycle_Desc",
    "Post_Date",
    "Eff_Gas_Day",
    "Cap_Type_Desc",
    "Post_Time",
    "Eff_Time",
    "Loc",
    "Loc_Name",
    "Loc_Zn",
    "Flow_Ind_Desc",
    "Loc_Purp_Desc",
    "Loc_QTI_Desc",
    "Meas_Basis_Desc",
    "IT",
    "All_Qty_Avail",
    "Total_Design_Capacity",
    "Operating_Capacity",
    "Total_Scheduled_Quantity",
    "Operationally_Available_Capacity",
    "TSP_Name",
    "TSP",
)

#: Quorum ExportToCSV columns (live header spelling). The unnamed blank
#: column between "Quantity Not Available Reason" and "Design Capacity" is
#: handled by the parser's placeholder logic, not here.
QUORUM_CSV_COLUMNS: tuple[str, ...] = (
    "Post Date/Time",
    "TSP",
    "TSP Name",
    "Eff Gas Day/Time",
    "End Eff Gas Day/Time",
    "Cycle Desc",
    "Loc",
    "Loc Name",
    "Loc Purp",
    "Loc/QTI",
    "Flow Ind",
    "IT Desc",
    "Meas Basis",
    "All Qty Avail",
    "Quantity Not Available Reason",
    "Design Capacity",
    "Operating Capacity",
    "Operationally Available Capacity",
    "Total Scheduled Quantity",
)


class HeaderMismatchError(ValueError):
    """Raised when expected columns cannot be resolved in an actual header.

    Attributes:
        unmatched: Expected columns that resolved to nothing.
        normalized_actual: The normalized forms of the payload's headers,
            included so on-call can see near-misses without re-fetching.
    """

    def __init__(
        self,
        source: str,
        unmatched: list[str],
        actual: list[str],
    ) -> None:
        self.unmatched = unmatched
        self.normalized_actual = [normalize_header(a) for a in actual]
        super().__init__(
            f"{source}: expected column(s) {unmatched} not resolvable in "
            f"actual header {actual} (normalized: {self.normalized_actual}). "
            "Refusing to parse — upstream header changed?"
        )


def normalize_header(raw: str) -> str:
    """Normalize one column header for comparison.

    What:
        Strips the ends, removes BOM characters, lowercases (casefold), and
        collapses every internal whitespace run — spaces, tabs, NBSP — to a
        single space. All of ``"OAC"``, ``" oac "``, ``"Oac"``,
        ``"Operationally  Available"`` style variants collapse onto one
        comparable form.
    """
    cleaned = str(raw).lstrip("﻿")
    return _WHITESPACE_RE.sub(" ", cleaned).strip().casefold()


def resolve_columns(
    expected: list[str] | tuple[str, ...],
    actual: list[str] | tuple[str, ...],
    *,
    optional: list[str] | tuple[str, ...] = (),
    source: str = "payload",
) -> dict[str, str]:
    """Map expected column names onto the payload's actual headers.

    What:
        Builds a normalized-name -> actual-header index over *actual* and
        resolves every name in *expected* through it. Returns
        ``{expected_name: actual_header}`` — feed rows through this mapping
        (or rename keys with :func:`rename_keys`) instead of trusting the
        upstream spelling. Names in *optional* resolve when present but never
        fail the match (columns a payload legitimately omits cycle-to-cycle).

    Failure modes:
        Raises :class:`HeaderMismatchError` listing EVERY unresolved required
        column plus the normalized actual headers when anything is missing.
        Duplicate normalized actuals resolve to the first occurrence — a
        genuine duplicate column is an upstream schema smell the caller's
        extra-field guards will surface.
    """
    index: dict[str, str] = {}
    for header in actual:
        norm = normalize_header(header)
        if norm and norm not in index:
            index[norm] = header
    resolved: dict[str, str] = {}
    unmatched: list[str] = []
    seen: set[str] = set()
    for want in (*expected, *optional):
        if want in seen:
            continue
        seen.add(want)
        hit = index.get(normalize_header(want))
        if hit is None:
            if want in expected:
                unmatched.append(want)
            continue
        resolved[want] = hit
    if unmatched:
        raise HeaderMismatchError(source, unmatched, list(actual))
    return resolved


def rename_keys(
    row: dict[str, str],
    mapping: dict[str, str],
) -> dict[str, str]:
    """Rename a parsed row's keys from actual headers to expected names.

    What:
        Inverse of :func:`resolve_columns`: given its ``{expected: actual}``
        output, produce a copy of *row* keyed by expected names. Matching is
        NORMALIZED on both sides — parsers commonly strip whitespace from
        row keys after the mapping was built from raw fieldnames, so exact
        matching would silently drop columns. Unmapped keys are dropped;
        expected names whose actual header is absent map to ``""``.
    """
    inverse: dict[str, str] = {}
    for expected, actual in mapping.items():
        inverse[normalize_header(actual)] = expected
    out: dict[str, str] = {}
    seen_norms: set[str] = set()
    for key, value in row.items():
        norm = normalize_header(key)
        hit = inverse.get(norm)
        if hit is not None:
            out[hit] = value
            seen_norms.add(norm)
    for expected, actual in mapping.items():
        if normalize_header(actual) not in seen_norms and expected not in out:
            out[expected] = ""
    return out
