"""EIA trade and sector consumption raw JSON -> curated parquet.

Series (MMcf per month, period YYYY-MM, same unit as eia_supply):
    trade_import_total | _pipeline | _lng            (all partners)
    trade_import_canada | trade_import_mexico         (total by partner)
    trade_export_total | _pipeline | _lng
    trade_export_canada | trade_export_mexico
    trade_export_vessel                               (EIA 'exports by vessel', US total)
    cons_residential | cons_commercial | cons_industrial | cons_electric_power
    cons_lease_plant | cons_pipeline | cons_vehicle | cons_total

Only the partner totals (IM0/EEX) are kept for Canada and Mexico: trade with
both is almost entirely by pipeline, and the mode split per partner would
add rows without adding information the panel uses.
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

RAW_PATH = Path("data/raw/eia_flows/eia_flows.json")
CURATED_PATH = Path("data/curated/eia_flows.parquet")

TRADE_MODE = {"IM0": "total", "IRP": "pipeline", "IML": "lng", "EEX": "total", "ENP": "pipeline", "ENG": "lng", "EVE": "vessel"}
PARTNER = {"NUS-NCA": "canada", "NUS-NMX": "mexico"}
CONS = {
    "VRS": "residential", "VCS": "commercial", "VIN": "industrial", "VEU": "electric_power",
    "VGL": "lease_plant", "VGP": "pipeline", "VDV": "vehicle", "VC0": "total",
}


def series_id_for(row: dict[str, Any]) -> str | None:
    kind, proc, area = row.get("kind"), row.get("process"), row.get("duoarea")
    if kind in ("import", "export"):
        if area == "NUS-Z00" and proc in TRADE_MODE:
            return f"trade_{kind}_{TRADE_MODE[proc]}"
        if area in PARTNER and TRADE_MODE.get(proc) in ("total", "pipeline"):
            # EIA reports Canada/Mexico trade under the pipeline codes; the
            # partner total is used when present (see transform's de-dup).
            return f"trade_{kind}_{PARTNER[area]}_{TRADE_MODE[proc]}"
        return None
    if kind == "cons" and area == "NUS" and proc in CONS:
        return f"cons_{CONS[proc]}"
    return None


def transform(raw_path: Path = RAW_PATH, curated_path: Path = CURATED_PATH) -> dict[str, Any]:
    if not raw_path.exists():
        raise TransformError(f"Raw file not found: {raw_path}")
    rows = json.loads(raw_path.read_text(encoding="utf-8")).get("data", [])
    ingested_at = datetime.now(UTC).isoformat()
    recs = []
    for r in rows:
        sid = series_id_for(r)
        if not sid or r.get("value") in (None, "") or not r.get("period"):
            continue
        units = str(r.get("units") or "").upper()
        if units not in ("MMCF", "MILLION CUBIC FEET"):
            continue  # price and per-day rows share some routes; keep volumes only
        recs.append({
            "source": "eia_flows",
            "series_id": sid,
            "series_name": r.get("description") or sid,
            "period": r["period"],
            "value": float(r["value"]),
            "unit": "MMcf",
            "region": "US",
            "ingested_at": ingested_at,
        })
    if not recs:
        raise TransformError("EIA flows transformer produced zero rows")
    df = pd.DataFrame(recs).drop_duplicates(subset=["series_id", "period"], keep="last")
    # Partner series: prefer the partner total, fall back to its pipeline
    # figure, and publish one series per partner (trade_<kind>_<partner>).
    out = []
    for (kind, partner) in [(k, p) for k in ("import", "export") for p in PARTNER.values()]:
        tot = df[df.series_id == f"trade_{kind}_{partner}_total"].set_index("period")
        pipe = df[df.series_id == f"trade_{kind}_{partner}_pipeline"].set_index("period")
        merged_p = tot.combine_first(pipe) if not tot.empty else pipe
        if not merged_p.empty:
            merged_p = merged_p.reset_index()
            merged_p["series_id"] = f"trade_{kind}_{partner}"
            out.append(merged_p)
    df = pd.concat([df[~df.series_id.str.contains(r"_(canada|mexico)_")]] + out, ignore_index=True)
    merged = merge_into_curated(df, curated_path)
    return {"rows": len(merged), "series": sorted(merged["series_id"].unique().tolist()),
            "period_range": (merged["period"].min(), merged["period"].max())}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(json.dumps(transform(), indent=2, default=str))
