"""ENTSOG Transparency: EU gas imports by supply route (daily physical flows).

Why:
    European storage only tells half of the winter story: whether stocks fall
    faster than normal depends on where the gas comes from. ENTSOG publishes
    daily physical flows at every interconnection point, which lets the
    dashboard show EU imports by route (Norway, LNG, Russia by corridor, North
    Africa, Azerbaijan, UK) next to the storage it feeds, and set EU LNG
    send-out against US LNG exports.

What:
    For each point in ``config/entsog_points.json`` fetches the 'Physical
    Flow' indicator (kWh/d, gas day, CET) between ``start`` and ``end`` and
    writes ``data/raw/entsog/flows.json``: one record per (operator, point,
    day). Route assignment and de-duplication happen in the transformer.

    No API key is needed. Requests are batched (several points per call) and
    split by calendar year so no single response is very large.

Failure modes:
    - ENTSOG answers ``{"message": "No result found"}`` for a batch with no
      data in the window; that is recorded as zero rows, not an error.
    - Network/HTTP errors on a batch are retried by HttpClient; a batch that
      still fails is listed in health under ``batches_failed`` and the run is
      'partial'. A run with no rows at all is 'failed'.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient, HttpClientError
from scrapers.base.safe_writer import safe_write_json

log = logging.getLogger(__name__)

SOURCE_NAME = "entsog"
BASE_URL = "https://transparency.entsog.eu"
CONFIG_PATH = Path("config/entsog_points.json")
RAW_PATH = Path("data/raw/entsog/flows.json")
CURATED_PATH = Path("data/curated/entsog.parquet")
HISTORY_START = date(2021, 1, 1)
BATCH_SIZE = 10


def load_points(config_path: Path = CONFIG_PATH) -> list[dict[str, str]]:
    """Flatten the route config into one dict per point, in config order."""
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    out: list[dict[str, str]] = []
    for route, spec in cfg["routes"].items():
        for entry in spec["points"]:
            operator, point, label = entry[0], entry[1], entry[2]
            country = entry[3] if len(entry) > 3 else ""
            out.append({"route": route, "operator": operator, "point": point, "label": label, "country": country})
    return out


def point_direction(p: dict[str, str]) -> str:
    """ENTSOG's ``pointDirection`` token: operator + point + direction, lower-case."""
    return f"{p['operator'].lower()}{p['point'].lower()}entry"


def year_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into calendar-year windows."""
    out = []
    cur = start
    while cur <= end:
        stop = min(date(cur.year, 12, 31), end)
        out.append((cur, stop))
        cur = stop + timedelta(days=1)
    return out


def default_start() -> date:
    """Re-fetch the last 45 days on an incremental run (ENTSOG revises recent
    days); the full history when there is no curated file yet."""
    if CURATED_PATH.exists():
        return date.today() - timedelta(days=45)
    return HISTORY_START


async def fetch(start: date, end: date, points: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    failed: list[str] = []
    batches = [points[i : i + BATCH_SIZE] for i in range(0, len(points), BATCH_SIZE)]
    async with HttpClient(base_url=BASE_URL, timeout_seconds=180.0, max_retries=3, backoff_base_seconds=3.0) as client:
        for w0, w1 in year_windows(start, end):
            for batch in batches:
                params = {
                    "pointDirection": ",".join(point_direction(p) for p in batch),
                    "indicator": "Physical Flow",
                    "periodType": "day",
                    "from": w0.isoformat(),
                    "to": w1.isoformat(),
                    "timezone": "CET",
                    "limit": "-1",
                }
                tag = f"{w0}..{w1}:{batch[0]['point']}+{len(batch) - 1}"
                try:
                    payload = await client.get_json("/api/v1/operationalData", params=params)
                except HttpClientError as exc:
                    # ENTSOG answers 404 {"message": "No result found"} when a
                    # batch simply has no data in the window (a dormant route).
                    if getattr(exc, "status", None) == 404:
                        continue
                    log.error("batch %s failed: %s", tag, exc)
                    failed.append(tag)
                    continue
                except Exception as exc:  # recorded, not fatal
                    log.error("batch %s failed: %s", tag, exc)
                    failed.append(tag)
                    continue
                data = payload.get("operationalData", []) if isinstance(payload, dict) else []
                for r in data:
                    if r.get("directionKey") != "entry":
                        continue
                    try:
                        value = float(r.get("value"))
                    except (TypeError, ValueError):
                        continue  # blank value = not reported, not zero
                    rows.append({
                        "operator": r["operatorKey"],
                        "point": r["pointKey"],
                        "gas_day": str(r["periodFrom"])[:10],
                        "kwh": value,
                        "unit": r.get("unit"),
                    })
                await asyncio.sleep(0.5)
    return rows, failed


async def run(start: date | None = None, end: date | None = None) -> dict[str, Any]:
    health = HealthWriter(source_name=SOURCE_NAME)
    start = start or default_start()
    end = end or date.today()
    points = load_points()
    rows, failed = await fetch(start, end, points)
    if not rows:
        err = f"no rows for {start}..{end} ({len(failed)} batches failed)"
        health.record_failure(error=err, metadata={"batches_failed": failed})
        return {"status": "failed", "error": err}
    units = sorted({r["unit"] for r in rows if r.get("unit")})
    if units != ["kWh/d"]:
        err = f"unexpected units {units}"
        health.record_failure(error=err)
        return {"status": "failed", "error": err}
    latest = max(r["gas_day"] for r in rows)
    payload = {
        "fetched_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "latest_gas_day": latest,
        "row_count": len(rows),
        # The transformer refuses a payload with failed batches: a route total
        # summed over a partial set of points would be silently understated.
        "batches_failed": failed,
        "data": rows,
    }
    safe_write_json(RAW_PATH, payload)
    meta = {"latest_gas_day": latest, "row_count": len(rows), "points": len(points), "batches_failed": failed}
    health.record_success(metadata=meta)
    return {"status": "partial" if failed else "ok", **meta}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=date.fromisoformat)
    ap.add_argument("--end", type=date.fromisoformat)
    args = ap.parse_args()
    print(json.dumps(asyncio.run(run(args.start, args.end)), indent=2))


if __name__ == "__main__":
    main()
