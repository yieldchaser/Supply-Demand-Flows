"""Gulf South physical location inventory and Freeport LNG meter matching.

Identifies Freeport LNG feedgas meters based on naming heuristics and seed IDs.
Generates the standard config/lng_meter_map.json.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from scrapers.energy_transfer.gulf_south import _CYCLES, RAW_DIR

log = logging.getLogger(__name__)

MAP_PATH = Path("config/lng_meter_map.json")


def identify_freeport_meters(locations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Match locations against Freeport LNG naming heuristics and seed IDs.

    Name matches (case-insensitive):
        - High confidence: contains "FREEPORT" or "STRATTON RIDGE" or "FLNG"
        - Candidate: contains "COASTAL BEND" (regional transfer point)
    """
    matched = []
    for loc in locations:
        loc_id = loc.get("loc_id")
        name = str(loc.get("loc_name") or "").upper()
        flow_ind = str(loc.get("flow_ind") or "").upper()

        # We are only interested in delivery points for LNG feedgas
        if flow_ind != "D":
            continue

        confidence = None
        note = ""

        # Stratton Ridge (24329) is a known seed, but also check names
        if loc_id == 24329 or "FREEPORT" in name or "STRATTON RIDGE" in name or "FLNG" in name:
            confidence = "high"

        if confidence:
            matched_item = {
                "loc_id": int(loc_id) if loc_id is not None else 0,
                "loc_name": loc.get("loc_name"),
                "flow_ind": flow_ind,
                "confidence": confidence,
                "source": "oac_csv",
            }
            if note:
                matched_item["note"] = note
            matched.append(matched_item)

    return matched


def newest_gas_day_on_disk(raw_dir: Path | None = None) -> date:
    """Return the newest gas day actually present in the raw directory.

    Why:
        The inventory's job is "describe the meters we just pulled", so its
        input must be *what was pulled*, never a wall-clock guess. Boardwalk
        posts each gas day's cycles hours after the fact, so ``date.today()``
        routinely names a gas day that has not been posted yet. The inventory
        then raised ``FileNotFoundError`` for a day that never existed, and
        because that step gated the commit it discarded an already-computed
        curated parquet (gas day 2026-08-27 was lost this way).

        This is the same discipline the Kinder Morgan scraper applies via
        ``parse_posting_stamp()``/``derive_gas_day()`` — derive the gas day
        from what the source served, never from the wall clock. Those two
        functions are deliberately NOT imported here: they parse NAESB posting
        stamps out of KM's HTML (``CycleDesc: EVENING | Post Date: 08/24/2026``)
        and apply KM's tariff roll-forward rules. Gulf South has no such stamp
        in its postings payload; its served effective gas day is already
        present per-row as the ``Effective Gas Day`` CSV column, so the raw
        filenames written by ``_raw_path`` are the served truth on disk.

    What:
        Scans *raw_dir* for ``{YYYY-MM-DD}_{CYCLE}.json`` and returns the
        greatest parseable gas day. Only cycles listed in ``_CYCLES`` are
        recognised, so unrelated files cannot contribute a date.

    Failure modes:
        Raises ``FileNotFoundError`` when *raw_dir* is missing or holds no
        parseable gas-day files. That is a genuinely empty state — distinct
        from "we guessed the wrong day" — and must stay loud rather than
        falling back to the wall clock.
    """
    target = RAW_DIR if raw_dir is None else raw_dir
    if not target.exists():
        raise FileNotFoundError(
            f"Raw directory {target} does not exist — nothing has been scraped. "
            f"Run scrapers.energy_transfer.gulf_south first."
        )

    known_cycles = {c.upper() for c in _CYCLES}
    found: set[date] = set()
    for path in target.iterdir():
        if not path.is_file() or path.suffix != ".json":
            continue
        day_str, sep, cycle_str = path.stem.partition("_")
        if not sep or cycle_str.upper() not in known_cycles:
            continue
        try:
            found.add(date.fromisoformat(day_str))
        except ValueError:
            continue

    if not found:
        raise FileNotFoundError(
            f"No gas-day raw files matching {{YYYY-MM-DD}}_{{CYCLE}}.json found in "
            f"{target}. Nothing has been scraped, so there is no inventory to "
            f"describe. Run scrapers.energy_transfer.gulf_south first."
        )
    return max(found)


def run_inventory(gas_day: date, raw_dir: Path | None = None) -> dict[str, Any]:
    """Orchestrate the inventory run for a given gas day.

    Finds the latest available cycle file, parses all delivery meters,
    runs the Freeport matching logic, and returns the full map structure.

    *raw_dir* defaults to :data:`RAW_DIR` (the module constant from
    ``scrapers.energy_transfer.gulf_south``). Tests pass a temporary dir so
    the inventory never touches the real ``data/raw`` tree.
    """
    base = RAW_DIR if raw_dir is None else raw_dir

    # Find latest available cycle raw file for this gas day
    target_file = None
    for cycle in reversed(_CYCLES):
        p = base / f"{gas_day.isoformat()}_{cycle}.json"
        if p.exists():
            target_file = p
            break

    if not target_file:
        raise FileNotFoundError(f"No raw JSON files found for gas day {gas_day} in any cycle.")

    log.info("Running inventory using raw file: %s", target_file)
    payload = json.loads(target_file.read_text(encoding="utf-8"))
    raw_rows = payload.get("data", [])

    # Deduplicate locations and extract volume (scheduled quantity)
    unique_locations: dict[tuple[int, str], dict[str, Any]] = {}
    for row in raw_rows:
        loc_id_str = row.get("Loc") or row.get("location_number") or row.get("locationNumber")
        if not loc_id_str:
            continue
        try:
            loc_id = int(loc_id_str)
        except ValueError:
            continue

        name = (
            row.get("Loc Name") or row.get("location_name") or row.get("locationName") or ""
        ).strip()
        flow_ind = (
            row.get("Flow Ind") or row.get("flow_indicator") or row.get("flowIndicator") or ""
        ).strip()
        sq_str = (
            row.get("Total Scheduled Quantity")
            or row.get("scheduled_quantity")
            or row.get("scheduledQuantity")
            or "0"
        )
        try:
            sq = float(sq_str)
        except ValueError:
            sq = 0.0

        key = (loc_id, flow_ind)
        unique_locations[key] = {
            "loc_id": loc_id,
            "loc_name": name,
            "flow_ind": flow_ind,
            "volume": sq,
        }

    # Match locations
    locations_list = list(unique_locations.values())
    matched_meters = identify_freeport_meters(locations_list)

    freeport_lng = [m for m in matched_meters if m["confidence"] == "high"]
    freeport_lng_candidates = [m for m in matched_meters if m["confidence"] == "candidate"]

    # Gather unmatched delivery meters for review
    matched_ids = {m["loc_id"] for m in matched_meters}
    unmatched_delivery = []
    for loc in locations_list:
        if loc["flow_ind"] == "D" and loc["loc_id"] not in matched_ids:
            unmatched_delivery.append(loc)

    # Sort by volume descending and take top 10
    unmatched_sorted = sorted(unmatched_delivery, key=lambda x: x["volume"], reverse=True)
    top_unmatched = [
        {
            "loc_id": u["loc_id"],
            "loc_name": u["loc_name"],
            "flow_ind": u["flow_ind"],
            "scheduled_quantity_dth": u["volume"],
        }
        for u in unmatched_sorted[:10]
    ]

    return {
        "gulf_south": {
            "freeport_lng": freeport_lng,
            "freeport_lng_candidates": freeport_lng_candidates,
        },
        "_meta": {
            "schema_version": 1,
            "last_inventory_run": datetime.now(UTC)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "total_locations_seen": len(unique_locations),
            "unmatched_high_volume_delivery_meters": top_unmatched,
        },
    }


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) >= 2:
        # Explicit argument: manual/backfill use. Honoured verbatim.
        try:
            _gas_day = date.fromisoformat(sys.argv[1])
        except ValueError:
            print(f"Invalid gas_day '{sys.argv[1]}'. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)
    else:
        # No argument: anchor to what the scraper actually pulled, not to the
        # wall clock. See newest_gas_day_on_disk() for why date.today() was
        # wrong here (it gates nothing now, but it loses data silently).
        try:
            _gas_day = newest_gas_day_on_disk()
        except FileNotFoundError as exc:
            print(f"Error: {exc}", file=sys.stderr)
            sys.exit(1)

    log.info("Inventory gas day resolved from disk: %s", _gas_day)

    try:
        result = run_inventory(_gas_day)
        # Write to map file
        MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
        MAP_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"Successfully populated {MAP_PATH}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
