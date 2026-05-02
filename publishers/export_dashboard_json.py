"""
Build the dashboard JSON bundle that the frontend fetches.

Aggregates curated Parquets and health files into a single bundle.json.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

from scrapers.base.safe_writer import safe_write_json, safe_write_text

CURATED_DIR = Path("data/curated")
HEALTH_DIR = Path("data/health")
DOCS_DATA_DIR = Path("docs/data")


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
