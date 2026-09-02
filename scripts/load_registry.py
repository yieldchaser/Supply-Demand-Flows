"""Load LNG terminal registry metadata directly from docs/js/util/lng-terminals.js.

Ensures Python tests and validators read the single source of truth without
requiring hand-maintained JSON duplicates that can rot.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

REGISTRY_JS_PATH = Path("docs/js/util/lng-terminals.js")
REGISTRY_JSON_PATH = Path("config/terminals_registry.json")


def load_terminal_registry(js_path: Path = REGISTRY_JS_PATH) -> dict[str, dict[str, Any]]:
    """Parse structured metadata for all terminals from lng-terminals.js.
    
    Extracts: id, nameplate, expectedCoveragePct, expectedMedianMmcf,
    coverageTolerancePct, and operational flag.
    """
    if not js_path.exists():
        raise FileNotFoundError(f"Registry JS not found at {js_path}")

    content = js_path.read_text(encoding="utf-8")

    # Match each terminal definition block inside LNG_TERMINALS
    # e.g. "freeport: {\n ... \n },"
    term_pattern = re.compile(
        r"^\s*([a-z_]+):\s*\{([^}]+(?:\{[^}]*\}[^}]*)*)\}",
        re.MULTILINE
    )

    registry: dict[str, dict[str, Any]] = {}

    for match in term_pattern.finditer(content):
        term_key = match.group(1)
        block = match.group(2)

        # Extract structured numeric fields
        nameplate_m = re.search(r"nameplate:\s*(\d+(?:\.\d+)?)", block)
        expected_cov_m = re.search(r"expectedCoveragePct:\s*(\d+(?:\.\d+)?)", block)
        expected_med_m = re.search(r"expectedMedianMmcf:\s*(\d+(?:\.\d+)?)", block)
        tolerance_m = re.search(r"coverageTolerancePct:\s*(\d+(?:\.\d+)?)", block)
        operational_m = re.search(r"operational:\s*(true|false)", block)

        if not nameplate_m:
            continue

        nameplate = float(nameplate_m.group(1))
        expected_cov = float(expected_cov_m.group(1)) if expected_cov_m else 0.0
        expected_med = float(expected_med_m.group(1)) if expected_med_m else 0.0
        tolerance = float(tolerance_m.group(1)) if tolerance_m else 0.0
        operational = operational_m.group(1) != "false" if operational_m else True

        registry[term_key] = {
            "id": term_key,
            "nameplate": nameplate,
            "expectedCoveragePct": expected_cov,
            "expectedMedianMmcf": expected_med,
            "coverageTolerancePct": tolerance,
            "operational": operational,
        }

    return registry


def export_registry_json(dest_path: Path = REGISTRY_JSON_PATH) -> Path:
    """Generate a machine-readable JSON sidecar from the single-source JS file."""
    registry = load_terminal_registry()
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_path, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)
    return dest_path


if __name__ == "__main__":
    reg = load_terminal_registry()
    print(f"Loaded {len(reg)} terminals from {REGISTRY_JS_PATH}:")
    for k, v in reg.items():
        print(f"  {k:15s}: np={v['nameplate']:4.0f} MMcf/d | exp={v['expectedCoveragePct']:5.1f}% ± {v['coverageTolerancePct']:4.1f}%")
    out = export_registry_json()
    print(f"Exported JSON sidecar to {out}")
