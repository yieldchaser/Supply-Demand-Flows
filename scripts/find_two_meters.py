"""Identify the two meters that changed Gulf South universe from 717 to 719.

Prompt R §07 requirement:
Find the two meters. scripts/classify_meters.py::build_universe produces the set;
diff it against whatever produced 717 (config/meters/classification.json)
and identify what appeared.
"""
from __future__ import annotations

import json
from pathlib import Path

from scripts.classify_meters import build_universe

REPO_ROOT = Path(__file__).resolve().parent.parent


def identify_new_meters() -> list[dict[str, object]]:
    class_path = REPO_ROOT / "config" / "meters" / "classification.json"
    if not class_path.exists():
        print("classification.json not found")
        return []

    old_doc = json.loads(class_path.read_text(encoding="utf-8"))
    old_gs_meters = old_doc.get("gulf_south", {})
    old_loc_ids = set(old_gs_meters.keys())

    universe = build_universe()
    current_gs_meters = universe.get("gulf_south", [])
    current_by_loc = {m["loc_id"]: m for m in current_gs_meters}
    current_loc_ids = set(current_by_loc.keys())

    new_loc_ids = sorted(current_loc_ids - old_loc_ids)
    print(f"Old Gulf South meters (2026-08-23 classification.json): {len(old_loc_ids)}")
    print(f"Current Gulf South meters (data/curated/gulf_south.parquet): {len(current_loc_ids)}")
    print(f"Delta: {len(new_loc_ids)} new meter(s): {new_loc_ids}")

    results = []
    for loc_id in new_loc_ids:
        m = current_by_loc[loc_id]
        print(f"  - Loc {loc_id}: {m['loc_name']} | Flow: {m['flow_ind']} | Class: {m['class']} | Conf: {m['confidence']}")
        print(f"    Evidence: {m['evidence']}")
        print(f"    Stats: {m['stats']}")
        results.append(m)

    return results


if __name__ == "__main__":
    identify_new_meters()
