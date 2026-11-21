"""Eurostat nrg_cb_gasm: monthly natural gas inland consumption by country.

Why:
    Storage in TWh means little without the demand it serves. Monthly
    consumption lets the Europe panel express stocks as days of winter cover
    and as a share of annual demand, per country and for the EU.

What:
    Fetches 'Inland consumption - observed' (IC_OBS) of natural gas (G3000)
    in TJ (gross calorific value) for the EU-27 and the ten AGSI countries
    since 2019-01, and writes data/raw/eurostat/gas_consumption.json with one
    record per (geo, month) including Eurostat's status flag ('p' provisional,
    'e' estimated, ...). Public API, no key.

Failure modes:
    HTTP errors propagate as a failed run (health records them); an empty
    response is a failure, not a success.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json

log = logging.getLogger(__name__)

SOURCE_NAME = "eurostat_gas"
BASE_URL = "https://ec.europa.eu"
PATH = "/eurostat/api/dissemination/statistics/1.0/data/nrg_cb_gasm"
RAW_PATH = Path("data/raw/eurostat/gas_consumption.json")
GEOS = ["EU27_2020", "DE", "FR", "IT", "NL", "ES", "AT", "BE", "CZ", "HU", "PL"]
SINCE = "2019-01"


def parse_jsonstat(d: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten a single-measure JSON-stat response (geo x time) to records."""
    dims = d["id"]
    sizes = d["size"]
    if any(s != 1 for dim, s in zip(dims, sizes, strict=True) if dim not in ("geo", "time")):
        raise ValueError(f"expected one value per geo/time, got sizes {dict(zip(dims, sizes, strict=True))}")
    geo = {v: k for k, v in d["dimension"]["geo"]["category"]["index"].items()}
    time = {v: k for k, v in d["dimension"]["time"]["category"]["index"].items()}
    nt = len(time)
    status = d.get("status", {})
    out = []
    for k, v in d.get("value", {}).items():
        i = int(k)
        out.append({"geo": geo[i // nt], "month": time[i % nt], "tj_gcv": float(v), "status": status.get(k, "")})
    return out


async def run() -> dict[str, Any]:
    health = HealthWriter(source_name=SOURCE_NAME)
    params: list[tuple[str, str]] = [
        ("format", "JSON"), ("lang", "en"), ("nrg_bal", "IC_OBS"), ("siec", "G3000"),
        ("unit", "TJ_GCV"), ("sinceTimePeriod", SINCE),
    ] + [("geo", g) for g in GEOS]
    try:
        async with HttpClient(base_url=BASE_URL, timeout_seconds=120.0, max_retries=3, backoff_base_seconds=3.0) as client:
            # HttpClient takes a dict; repeated 'geo' keys go in the query string.
            query = "&".join(f"{k}={v}" for k, v in params)
            d = await client.get_json(f"{PATH}?{query}")
        rows = parse_jsonstat(d)
    except Exception as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}
    if not rows:
        health.record_failure(error="empty response")
        return {"status": "failed", "error": "empty response"}
    latest = max(r["month"] for r in rows)
    safe_write_json(RAW_PATH, {
        "fetched_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "dataset_updated": d.get("updated"),
        "latest_month": latest,
        "data": rows,
    })
    meta = {"latest_month": latest, "row_count": len(rows), "dataset_updated": d.get("updated")}
    health.record_success(metadata=meta)
    return {"status": "ok", **meta}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    print(json.dumps(asyncio.run(run()), indent=2))
