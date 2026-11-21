"""EIA monthly natural gas trade and consumption by sector.

Why:
    The balance panel solved net trade from the other Table 1 legs because the
    bundle had no trade data. EIA publishes imports and exports by partner and
    mode (pipeline / LNG) and consumption by sector; with them the identity is
    closed by measurement, net pipeline trade with Canada and Mexico is
    visible on its own, and demand can be read by who consumes it.

What:
    Three EIA v2 requests (monthly, US totals and partners, since 2019):
      natural-gas/move/impc   IM0 total, IRP pipeline, IML LNG
                              at NUS-Z00 (all partners), NUS-NCA, NUS-NMX
      natural-gas/move/expc   EEX total, ENP pipeline, ENG LNG
                              at NUS-Z00, NUS-NCA, NUS-NMX
      natural-gas/cons/sum    VRS residential, VCS commercial, VIN industrial,
                              VEU electric power, VGL lease & plant fuel,
                              VGP pipeline & distribution, VDV vehicle, VC0 total
    Codes were read from the routes' own facet listings. Raw rows are written
    to data/raw/eia_flows/eia_flows.json; values are MMcf per month.

Failure modes:
    Missing key -> failed. Any request error -> failed (nothing written).
    A request returning no rows -> failed: the three must arrive together or
    the balance would mix vintages.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.safe_writer import safe_write_json
from scrapers.eia_api.client import EIAClient, load_api_key_from_env

log = logging.getLogger(__name__)

SOURCE_NAME = "eia_flows"
RAW_PATH = Path("data/raw/eia_flows/eia_flows.json")
START = "2019-01"
PARTNERS = ["NUS-Z00", "NUS-NCA", "NUS-NMX"]

REQUESTS: list[dict[str, Any]] = [
    {"key": "import", "route": "natural-gas/move/impc", "process": ["IM0", "IRP", "IML"], "duoarea": PARTNERS},
    {"key": "export", "route": "natural-gas/move/expc", "process": ["EEX", "ENP", "ENG"], "duoarea": PARTNERS},
    {"key": "cons", "route": "natural-gas/cons/sum", "process": ["VRS", "VCS", "VIN", "VEU", "VGL", "VGP", "VDV", "VC0"], "duoarea": ["NUS"]},
]


async def _fetch_all(client: EIAClient, req: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    while True:
        resp = await client.get_series(
            route=req["route"],
            frequency="monthly",
            facets={"process": req["process"], "duoarea": req["duoarea"]},
            data_columns=["value"],
            start=START,
            length=5000,
            offset=offset,
        )
        body = resp.get("response", {})
        page = body.get("data", []) if isinstance(body, dict) else []
        rows.extend(page)
        total = int(body.get("total", len(rows))) if isinstance(body, dict) else len(rows)
        offset += len(page)
        if not page or offset >= total:
            return rows


async def run() -> dict[str, Any]:
    health = HealthWriter(source_name=SOURCE_NAME)
    try:
        api_key = load_api_key_from_env()
    except RuntimeError as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}

    out: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    try:
        async with EIAClient(api_key=api_key) as client:
            for req in REQUESTS:
                rows = await _fetch_all(client, req)
                if not rows:
                    raise RuntimeError(f"no rows for {req['route']}")
                counts[req["key"]] = len(rows)
                for r in rows:
                    out.append({
                        "kind": req["key"],
                        "process": r.get("process"),
                        "duoarea": r.get("duoarea"),
                        "period": r.get("period"),
                        "value": r.get("value"),
                        "units": r.get("units"),
                        "description": r.get("series-description"),
                    })
    except Exception as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}

    latest = max(r["period"] for r in out if r.get("period") and r.get("value") is not None)
    safe_write_json(RAW_PATH, {"fetched_at": datetime.now(UTC).replace(microsecond=0).isoformat(), "latest_period": latest, "data": out})
    meta = {"latest_period": latest, "rows": len(out), "by_request": counts}
    health.record_success(metadata=meta)
    return {"status": "ok", **meta}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    print(json.dumps(asyncio.run(run()), indent=2))
