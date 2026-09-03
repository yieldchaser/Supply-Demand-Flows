"""Prune superseded bundle generations from docs/data/ (Prompt S §03).

Keeps manifest.json, bundle.json, active live hash from manifest.json,
and KEEP_PREVIOUS prior hashes for rollback safety.

Usage:
    python scripts/prune_bundles.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "docs" / "data"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from publishers.export_dashboard_json import KEEP_PREVIOUS, _prune_stale_bundles


def prune_bundles(keep_previous: int = KEEP_PREVIOUS) -> dict[str, object]:
    manifest_path = DATA_DIR / "manifest.json"
    if not manifest_path.exists():
        print(f"Error: {manifest_path} not found.")
        return {"status": "error", "error": "manifest.json missing"}

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    current_hash = manifest.get("hash", "")
    if not current_hash:
        print("Error: 'hash' missing from manifest.json.")
        return {"status": "error", "error": "hash missing in manifest"}

    files_before = list(DATA_DIR.glob("*.json"))
    bytes_before = sum(f.stat().st_size for f in files_before)

    print(f"Before prune: {len(files_before)} files ({bytes_before / (1024 * 1024):.1f} MB)")
    print(f"Active live hash: {current_hash} (keeping current + {keep_previous} previous generations)")

    pruned_count = _prune_stale_bundles(DATA_DIR, current_hash=current_hash, keep_previous=keep_previous)

    files_after = list(DATA_DIR.glob("*.json"))
    bytes_after = sum(f.stat().st_size for f in files_after)
    bytes_saved = bytes_before - bytes_after

    # Determine retained hashes
    retained_hashes = set()
    for f in files_after:
        parts = f.name.split(".")
        if len(parts) >= 3 and len(parts[-2]) == 8:
            retained_hashes.add(parts[-2])

    print(f"Pruned {pruned_count} files ({bytes_saved / (1024 * 1024):.1f} MB saved).")
    print(f"After prune: {len(files_after)} files ({bytes_after / (1024 * 1024):.1f} MB)")
    print(f"Retained hashes: {sorted(retained_hashes)}")

    return {
        "status": "ok",
        "current_hash": current_hash,
        "keep_previous": keep_previous,
        "files_before": len(files_before),
        "bytes_before": bytes_before,
        "files_after": len(files_after),
        "bytes_after": bytes_after,
        "bytes_saved": bytes_saved,
        "pruned_count": pruned_count,
        "retained_hashes": sorted(retained_hashes),
    }


if __name__ == "__main__":
    prune_bundles()
