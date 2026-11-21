"""Temporary (branch-only): list EIA v2 facet values for trade/consumption/production
routes so the scrapers can request exact codes. Prints only metadata and a few
sample rows; writes nothing. Delete before merge."""
import asyncio
import json
import os

import httpx

KEY = os.environ["EIA_API_KEY"]
BASE = "https://api.eia.gov/v2"
ROUTES = ["natural-gas/sum/snd", "natural-gas/move/impc", "natural-gas/move/expc",
          "natural-gas/cons/sum", "natural-gas/prod/sum", "natural-gas/stor/cap"]


async def main() -> None:
    async with httpx.AsyncClient(timeout=60) as c:
        for r in ROUTES:
            meta = (await c.get(f"{BASE}/{r}/", params={"api_key": KEY})).json().get("response", {})
            print("== ROUTE", r, "| freq", [f.get("id") for f in meta.get("frequency", [])], "| facets", [f.get("id") for f in meta.get("facets", [])])
            for facet in ("process", "duoarea", "product"):
                if facet not in [f.get("id") for f in meta.get("facets", [])]:
                    continue
                fv = (await c.get(f"{BASE}/{r}/facet/{facet}", params={"api_key": KEY})).json().get("response", {})
                vals = fv.get("facets", [])
                shown = vals if facet != "duoarea" else [v for v in vals if v.get("id", "").startswith(("NUS", "R"))][:40]
                print(f"   facet {facet} ({len(vals)}):", json.dumps([(v.get('id'), v.get('name')) for v in shown])[:3000])
            params = {"api_key": KEY, "frequency": "monthly", "data[]": "value", "facets[duoarea][]": "NUS",
                      "sort[0][column]": "period", "sort[0][direction]": "desc", "length": "60"}
            d = (await c.get(f"{BASE}/{r}/data/", params=params)).json().get("response", {})
            rows = d.get("data", [])
            seen = {}
            for x in rows:
                seen.setdefault((x.get("process"), x.get("series-description")), (x.get("period"), x.get("value"), x.get("units")))
            for k, v in list(seen.items())[:40]:
                print("   sample", k, v)


asyncio.run(main())
