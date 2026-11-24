"""Eurostat monthly gas consumption raw JSON -> curated parquet.

Series: eurostat_gas_consumption_<geo> (geo lower-case; 'eu' for EU-27),
value = monthly inland consumption in TWh (GCV; TJ / 3,600), period YYYY-MM.
The Eurostat status flag is kept in series_name-independent form by writing
provisional/estimated months with region '<GEO>:<flag>' so the panel can mark
them; final months carry the bare country code.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from transformers.base.accumulate import merge_into_curated
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_PATH = Path("data/raw/eurostat/gas_consumption.json")
CURATED_PATH = Path("data/curated/eurostat_gas.parquet")


def transform(raw_path: Path = RAW_PATH, curated_path: Path = CURATED_PATH) -> dict[str, Any]:
    if not raw_path.exists():
        raise TransformError(f"Raw file not found: {raw_path}")
    rows = json.loads(raw_path.read_text(encoding="utf-8")).get("data", [])
    if not rows:
        raise TransformError("Eurostat raw payload has no rows")
    ingested_at = datetime.now(UTC).isoformat()
    recs = []
    for r in rows:
        geo = "eu" if r["geo"].startswith("EU27") else r["geo"].lower()
        flag = (r.get("status") or "").strip()
        recs.append({
            "source": "eurostat_gas",
            "series_id": f"eurostat_gas_consumption_{geo}",
            "series_name": f"{r['geo']} natural gas inland consumption (observed)",
            "period": r["month"],
            "value": round(r["tj_gcv"] / 3600.0, 4),
            "unit": "TWh",
            "region": f"{r['geo']}:{flag}" if flag else r["geo"],
            "ingested_at": ingested_at,
        })
    merged = merge_into_curated(pd.DataFrame(recs), curated_path)
    return {"rows": len(merged), "series": int(merged["series_id"].nunique()),
            "period_range": (merged["period"].min(), merged["period"].max())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(transform(), indent=2, default=str))
