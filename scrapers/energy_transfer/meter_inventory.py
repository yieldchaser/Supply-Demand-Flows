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

from scrapers.energy_transfer.gulf_south import _CYCLES, _raw_path

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
        elif "COASTAL BEND" in name:
            confidence = "candidate"
            note = "regional transfer point, may aggregate non-Freeport demand"

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


def run_inventory(gas_day: date) -> dict[str, Any]:
    """Orchestrate the inventory run for a given gas day.

    Finds the latest available cycle file, parses all delivery meters,
    runs the Freeport matching logic, and returns the full map structure.
    """
    # Find latest available cycle raw file for this gas day
    target_file = None
    for cycle in reversed(_CYCLES):
        p = _raw_path(cycle, gas_day)
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

    _gas_day = date.today()
    if len(sys.argv) >= 2:
        try:
            _gas_day = date.fromisoformat(sys.argv[1])
        except ValueError:
            print(f"Invalid gas_day '{sys.argv[1]}'. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    try:
        result = run_inventory(_gas_day)
        # Write to map file
        MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
        MAP_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(f"Successfully populated {MAP_PATH}")
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
