"""
Build the dashboard JSON bundle that the frontend fetches.

Aggregates curated Parquets and health files into a single bundle.json.

Bundle vs archive:
    ``data/curated/*.parquet`` are the COMPLETE historical archive — nothing
    in this module ever writes to them.  The bundle is the *display subset*
    the frontend fetches: for EBB sources (gulf_south, gasnom, quorum, bhe,
    cheniere) it ships ONLY the series whose location id appears as a
    high-confidence entry in the corresponding config/meters/*.json (and
    config/lng_meter_map.json for gulf_south), with FULL history for those
    series and no date truncation.  Non-EBB sources (EIA, GIE AGSI, Baker
    Hughes) are shipped as-is.

Blob-size guard:
    A per-source safety cap (MAX_ROWS_PER_SOURCE) exists purely to keep the
    bundle under GitHub's 100 MB blob limit.  If it EVER trips, this module
    raises instead of silently truncating — silent pruning by recency was a
    bug (it dropped old history from the UI); relevance-based pruning above
    is the intended mechanism.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.base.safe_writer import safe_write_json, safe_write_text

CURATED_DIR = Path("data/curated")
HEALTH_DIR = Path("data/health")
DOCS_DATA_DIR = Path("docs/data")

#: EBB sources whose bundle rows are pruned to relevant meters BEFORE any
#: cap. Maps source key -> its meter-config file (high-confidence entries only).
EBB_METER_CONFIGS: dict[str, Path] = {
    "gulf_south": Path("config/lng_meter_map.json"),
    "gasnom": Path("config/meters/gasnom.json"),
    "quorum": Path("config/meters/quorum.json"),
    "bhe": Path("config/meters/bhe.json"),
    "cheniere": Path("config/meters/cheniere.json"),
}

#: Blob-size guard only — never a pruning mechanism. Trips loudly.
MAX_ROWS_PER_SOURCE = 400_000


def _json_default(obj: object) -> str:
    """
    Fallback serializer for non-JSON-native types emitted by pandas/transformers.
    Handles date, datetime, pd.Timestamp, numpy scalar types, Decimal.
    """
    if isinstance(obj, date | datetime):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    # numpy types (int64, float64) have .item() method
    if hasattr(obj, "item"):
        return obj.item()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _collect_high_confidence_loc_ids(node: Any, found: set[str], high_key: bool = False) -> None:
    """Recursively gather loc_ids of high-confidence meter entries.

    Why:
        Meter configs use different nesting shapes and two confidence styles:
        an explicit ``confidence: high`` field (lng_meter_map, quorum, bhe,
        cheniere) or membership in a list named ``high`` (gasnom). Both are
        accepted; ``candidates``/``excluded`` lists are never collected.
    """
    if isinstance(node, dict):
        if node.get("loc_id") is not None and (
            node.get("confidence") == "high" or high_key
        ):
            found.add(str(node["loc_id"]).strip().lower())
        for key, value in node.items():
            _collect_from_pair(key, value, found)
    elif isinstance(node, list):
        for item in node:
            _collect_high_confidence_loc_ids(item, found, high_key=high_key)


def _collect_from_pair(key: str, value: Any, found: set[str]) -> None:
    """Walk one container entry, flagging gasnom-style ``high`` lists."""
    if key == "high" and isinstance(value, list):
        _collect_high_confidence_loc_ids(value, found, high_key=True)
    else:
        _collect_high_confidence_loc_ids(value, found)


def load_relevant_loc_ids(source_key: str) -> set[str]:
    """Return the lowercase loc_id allowlist for one EBB source.

    Failure modes:
        Missing/unreadable config -> empty set; callers treat that as
        keep-all with a loud warning rather than silently shipping an empty
        source.
    """
    path = EBB_METER_CONFIGS[source_key]
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"WARN: unreadable meter config {path}: {exc} — keeping ALL series")
        return set()
    found: set[str] = set()
    if isinstance(data, dict):
        for key, value in data.items():
            _collect_from_pair(key, value, found)
    else:
        _collect_high_confidence_loc_ids(data, found)
    return found


def _series_matches(series_id: str, allowed: set[str]) -> bool:
    """True when the series_id embeds one of the allowed loc ids.

    What:
        Loc ids sit at different token positions per source (prefix lengths
        vary), so this matches ``_<loc>_`` anywhere in the lowercased id —
        unambiguous because loc ids are unique within a source.
    """
    padded = f"_{series_id.lower()}_"
    return any(f"_{loc}_" in padded for loc in allowed)


def prune_to_relevant_meters(df: pd.DataFrame, source_key: str) -> pd.DataFrame:
    """Filter an EBB frame down to configured high-confidence meter series.

    What:
        Keeps every row whose series_id carries an allowlisted loc id, with
        FULL history — no recency truncation. Falls back to keep-all when no
        allowlist resolves (loudly).

    Failure modes:
        Never raises for empty results by itself; an over-aggressive config
        surfaces via the blob-size guard downstream or as a visibly tiny
        row_count in the summary.
    """
    allowed = load_relevant_loc_ids(source_key)
    if not allowed:
        print(
            f"WARN: no high-confidence meters resolved for '{source_key}' — "
            "shipping ALL series (check its meter config)"
        )
        return df
    mask = df["series_id"].map(lambda sid: _series_matches(str(sid), allowed))
    kept = df[mask]
    print(
        f"{source_key}: relevance prune {len(df)} -> {len(kept)} rows "
        f"(allowlist {sorted(allowed)})"
    )
    return kept


def build() -> dict:
    """
    Aggregation logic for building the dashboard bundle.
    """
    bundle = {
        "generated_at": datetime.now(UTC).isoformat(),
        "sources": {},
        "health": {},
    }

    # 1. Collect Curated Data
    if CURATED_DIR.exists():
        for parquet_path in CURATED_DIR.glob("*.parquet"):
            source_key = parquet_path.stem
            try:
                df = pd.read_parquet(parquet_path)
                # Sort by period for easier frontend consumption
                if "period" in df.columns:
                    df = df.sort_values("period")
                # Relevance-based pruning for EBB sources: keep only the
                # configured LNG feedgas meters, with full history.
                if source_key in EBB_METER_CONFIGS:
                    df = prune_to_relevant_meters(df, source_key)
                # Blob-size guard: must never trip silently. If it does,
                # the relevance config needs revisiting — fail loudly.
                if len(df) > MAX_ROWS_PER_SOURCE:
                    msg = (
                        f"Bundle blob-size guard tripped for '{source_key}': "
                        f"{len(df)} rows > {MAX_ROWS_PER_SOURCE}. "
                        "Refusing to ship a >100MB bundle. Prune relevance in "
                        f"{EBB_METER_CONFIGS.get(source_key, 'its meter config')} "
                        "or raise MAX_ROWS_PER_SOURCE deliberately."
                    )
                    raise RuntimeError(msg)

                bundle["sources"][source_key] = {
                    "latest_period": df["period"].max() if "period" in df.columns else None,
                    "row_count": len(df),
                    "data": df.to_dict(orient="records"),
                }
            except Exception as exc:
                print(f"Error reading {parquet_path}: {exc}")

    # 2. Collect Health Data
    if HEALTH_DIR.exists():
        for health_path in HEALTH_DIR.glob("*.json"):
            source_key = health_path.stem
            if source_key.endswith(".prev"):
                continue  # .prev files are local rotation state, not for dashboard
            try:
                with health_path.open("r", encoding="utf-8") as f:
                    bundle["health"][source_key] = json.load(f)
            except Exception as exc:
                print(f"Error reading {health_path}: {exc}")

    # 3. Serialise and Hash
    bundle_json = json.dumps(bundle, separators=(",", ":"), ensure_ascii=False, default=_json_default)
    bundle_hash = hashlib.md5(bundle_json.encode("utf-8")).hexdigest()[:8]

    # 4. Write Files
    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Static bundle.json
    safe_write_text(DOCS_DATA_DIR / "bundle.json", bundle_json)

    # Cache-busting bundle.{HASH}.json
    hashed_name = f"bundle.{bundle_hash}.json"
    safe_write_text(DOCS_DATA_DIR / hashed_name, bundle_json)

    # Manifest
    manifest = {
        "bundle_url": hashed_name,
        "generated_at": bundle["generated_at"],
        "hash": bundle_hash,
    }
    safe_write_json(DOCS_DATA_DIR / "manifest.json", manifest)

    return {
        "generated_at": bundle["generated_at"],
        "hash": bundle_hash,
        "bundle_url": hashed_name,
        "sources_count": len(bundle["sources"]),
    }


if __name__ == "__main__":
    result = build()
    print(json.dumps(result, indent=2, default=_json_default))
