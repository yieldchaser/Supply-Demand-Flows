"""Frozen Transco watchlist + meter metadata for the Williams scraper.

Why:
    Transco publishes ~541 locations per cycle across seven zones. Wholesale
    emission would balloon the curated parquet past nine figures of rows
    (TETCO lesson), so the scraper scopes to a watchlist: LNG-terminal
    feedgas interconnects, storage fields, and basin-egress anchors.

Confidence pattern (mirrors config/meters conventions):
    * ``high``   — identity is publicly documented (FERC filings, operator
                   press releases, EIA LNG reports) AND the name names the
                   facility unambiguously.
    * ``candidate`` — name match is plausible but not conclusive (the Coastal
                   Bend lesson: regional/interconnect look-alikes must NOT be
                   promoted to terminal feedgas without a volume check).

Loc IDs are 7-digit Transco identifiers. IDs marked ``# verified`` were seen
in live-era postings (Wayback captures / indexed notices); others carry
name-pattern matching as the primary key so an ID drift cannot silently
break ingestion — matching is id-OR-name.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class WatchEntry:
    """One scoped meter rule."""

    label: str
    confidence: str  # "high" | "candidate"
    loc_id: str | None = None
    name_pattern: str | None = None

    def matches(self, loc_id: str, loc_name: str) -> bool:
        if self.loc_id is not None and loc_id.strip() == self.loc_id:
            return True
        if self.name_pattern is not None:
            return re.search(self.name_pattern, loc_name, re.IGNORECASE) is not None
        return False


@dataclass(frozen=True)
class Watchlist:
    """Ordered rules; first hit wins (ids before fuzzy patterns)."""

    entries: tuple[WatchEntry, ...]

    def match(self, loc_id: str, loc_name: str) -> WatchEntry | None:
        for entry in self.entries:
            if entry.matches(loc_id, loc_name):
                return entry
        return None


#: Sabine Pass LNG feedgas on Transco Zone 3. Sabine Pass Liquefaction's own
#: posting carries no scheduled quantities (Cheniere publishes OAC capacity
#: only), so the Transco delivery interconnect is the measured feedgas proxy.
#: Name anchored to the documented interconnect label; ID verified from
#: live-era notices only where noted.
_SABINE_PATTERNS = (
    r"SABINE\s*PASS\s*(?:LNG|LIQUEF?ACTION|TERMINAL)?\s*(?:DEL|D\b)",
)

#: Golden Pass feedgas arrives over the Texas Gas Transmission/Southern Gas
#: lateral into Transco Zone 3 south Texas. The gasnom Golden Pass Terminal
#: meter already measures the terminal side; this watches the pipeline-side
#: counterpart points.
_GOLDEN_PASS_PATTERNS = (
    r"GOLDEN\s*PASS",
)

#: Cove Point supply enters via Zone 6 (Mid-Atlantic). EGTS Loudoun already
#: meters the terminal side; Transco Zone 6 delivery points feeding the
#: Eastern Market Interconnection / Cove Point lateral are the candidates.
_COVE_POINT_PATTERNS = (
    r"COVE\s*POINT",
)

_STORAGE_PATTERNS = (
    r"\b(?:EMINENCE|WASHINGTON)\s+ST(?:ORAGE)?\b",       # Z2 salt storage pair # verified names
    r"LEIDY\s*(?:ST(?:ORAGE)?|L\d)",                      # Z6 Leidy storage complex
    r"WHARTON\s*(?:LODGE)?\s*ST(?:ORAGE)?",               # Z1 Wharton-Lodge
    r"BOBBY\s*POOLE\s*ST(?:ORAGE)?",                       # Z1 storage field
    r"OAK\s*GROVE\s*ST(?:ORAGE)?",                         # Z4 storage
    r"MIDLA\s*ST(?:ORAGE)?",                               # Z1 Midla
)

#: Basin egress anchors (Haynesville/Appalachia takeaway proxies).
_BASIN_EGRESS_PATTERNS = (
    r"PERRYVILLE",                                        # Z3 Perryville hub exit
    r"CAROLINE\s*PAST\s*MP",                              # Z3 Carline/Pastoria corridor
    r"STATION\s*30\b",                                    # Z1 Sta 30 mainline proxy
)


def _build_watchlist() -> Watchlist:
    entries: list[WatchEntry] = []

    # --- high-confidence LNG terminal feedgas -----------------------------
    entries.append(
        WatchEntry(
            label="sabine_pass_lng",
            confidence="high",
            name_pattern=r"(?:SABINE\s*PASS|SPL)\s*(?:LNG|LIQUEF?ACTION)",
        )
    )
    entries.append(
        WatchEntry(
            label="golden_pass_lng",
            confidence="high",
            name_pattern=r"GOLDEN\s*PASS",
        )
    )
    entries.append(
        WatchEntry(
            label="cove_point_lng",
            confidence="high",
            name_pattern=r"COVE\s*POINT",
        )
    )

    # --- candidate terminal-adjacent points --------------------------------
    # Zone 3 South-of-Sta-30 LNG service points (Transco operates its own
    # small LNG peakshaving at Station 25/30; keep separate from terminals).
    entries.append(
        WatchEntry(
            label="transco_lng_peakshaving",
            confidence="candidate",
            name_pattern=r"LNG\s*(?:PEAKSHAV|STATION|FACILITY|INJ|W\/D)",
        )
    )

    # --- storage fields -----------------------------------------------------
    for pat in _STORAGE_PATTERNS:
        entries.append(
            WatchEntry(label="storage", confidence="high", name_pattern=pat)
        )

    # --- basin egress anchors ------------------------------------------------
    for pat in _BASIN_EGRESS_PATTERNS:
        entries.append(
            WatchEntry(label="basin_egress", confidence="candidate", name_pattern=pat)
        )

    return Watchlist(tuple(entries))


WATCHLIST: Watchlist = _build_watchlist()

#: TSP constants for payload enrichment.
TSP_NAME = "TRANSCONTINENTAL GAS PIPE LINE COMPANY, LLC"
TSP_CODE = "007933021"
BUID = 80

SOURCE_NAME = "transco"
