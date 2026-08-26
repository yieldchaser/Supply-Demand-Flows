"""Cheniere lngconnection Capacity API client — Creole Trail + Corpus Christi Pipeline.

Why:
    Cheniere does NOT post Scheduled Quantities anywhere on its connection
    site (confirmed via their GetPages manifest — no SQ page exists). The
    Operational Capacity report is the only surface. ``GetCapacity`` does
    carry a ``scheD_QTY`` field (the terminal's scheduled volume at the
    interconnect); it is captured, but ``qtY_AVAIL`` falling against
    ``desigN_OPER_CAP`` is the primary "terminal is taking gas" signal.

What:
    Talks to the public, tokenless API at ``lngconnectionapi.cheniere.com``
    (an ``Origin: https://lngconnection.cheniere.com`` header is required):
    * ``fetch_capacity(tsp_no, gas_day)`` — GET ``/api/Capacity/GetCapacity``
      with ``tspNo={200|400}&beginDate=MM/DD/YYYY&cycleId=null&locationId=0``.
      A per-date call returns the rows of the most recently *posted* cycle
      for that gas day; pass an explicit cycle id to pin one cycle.
    * ``fetch_cycles()`` — the NAESB cycle catalogue.
    * ``fetch_locations(tsp_no)`` — location ids/names per pipeline.

Failure modes:
    Retries 403/429 alongside the HttpClient defaults; raises
    ``HttpClientError`` after retries are exhausted. Rows missing any of
    loc/cycle/gas-day fields are skipped by ``parse_capacity_rows`` rather
    than raised. Dates before 2026-05-25 return an empty ``report`` — that
    is the server-side history floor, not an error.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from scrapers.base.http_client import HttpClient
from scrapers.base.identity import assert_response_identity

log = logging.getLogger(__name__)

SOURCE_NAME = "cheniere"
RAW_DIR = "data/raw/cheniere"

API_BASE_URL = "https://lngconnectionapi.cheniere.com"
ORIGIN = "https://lngconnection.cheniere.com"

GET_CAPACITY_PATH = "/api/Capacity/GetCapacity"
GET_CYCLES_PATH = "/api/Capacity/GetCycles"
GET_LOCATIONS_PATH = "/api/Capacity/GetLocations"

# tspNo 200 → Creole Trail Pipeline (Sabine Pass) · 400 → Corpus Christi Pipeline
TSP_CREOLE_TRAIL = 200
TSP_CORPUS_CHRISTI = 400

# Canonical series prefixes (series_id f"{prefix}_oac_{loc}_{cycle}").
PREFIX_CREOLE_TRAIL = "creole_trail"
PREFIX_CORPUS_CHRISTI = "corpus_christi"


def default_headers() -> dict[str, str]:
    """Return the mandatory Origin/Referer/Accept headers for the API."""
    return {
        "Origin": ORIGIN,
        "Referer": f"{ORIGIN}/",
        "Accept": "application/json",
    }


def _format_begin_date(gas_day: date) -> str:
    """Format a gas day as the API's ``MM/DD/YYYY`` beginDate."""
    return gas_day.strftime("%m/%d/%Y")


async def fetch_capacity(
    client: HttpClient,
    tsp_no: int,
    gas_day: date,
    cycle_id: int | None = None,
) -> dict[str, Any]:
    """GET one GetCapacity report for *tsp_no* on *gas_day*.

    What:
        Returns the raw JSON dict (keys ``report`` and ``beginDate``).
        ``cycle_id=None`` sends ``null`` → every row of the most recent
        posted cycle for that day.

    Failure modes:
        ``HttpClientError`` propagates after retry exhaustion. Dates older
        than the server floor (~2026-05-25) come back as an empty report.
    """
    params = {
        "tspNo": str(tsp_no),
        "beginDate": _format_begin_date(gas_day),
        "cycleId": str(cycle_id) if cycle_id is not None else "null",
        "locationId": "0",
    }
    payload = await client.get_json(GET_CAPACITY_PATH, params=params)
    if not isinstance(payload, dict):
        log.warning("GetCapacity returned non-object payload for tsp=%s — ignoring.", tsp_no)
        return {"report": []}
    return payload


async def fetch_cycles(client: HttpClient) -> list[dict[str, Any]]:
    """Return the cycle catalogue (e.g. cyclE_ID 1 Timely … 166 Intraday 3)."""
    payload = await client.get_json(GET_CYCLES_PATH)
    return payload if isinstance(payload, list) else []


async def fetch_locations(client: HttpClient, tsp_no: int, gas_day: date | None = None) -> list[dict[str, Any]]:
    """Return the location inventory for a pipeline."""
    params: dict[str, str] = {"tspNo": str(tsp_no)}
    if gas_day is not None:
        params["beginDate"] = _format_begin_date(gas_day)
        params["endDate"] = _format_begin_date(date.today())
    payload = await client.get_json(GET_LOCATIONS_PATH, params=params)
    return payload if isinstance(payload, list) else []


def parse_capacity_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise one GetCapacity report into clean per-row dicts.

    What:
        Keeps the odd server casing under canonical snake_case keys and adds
        ``period`` (YYYY-MM-DD from ``avaiL_CAP_EFF_DT_TIME``) plus the
        posting timestamp for dedup bookkeeping.

    Failure modes:
        Rows missing loc/cycle/effective-time are skipped with a warning;
        numeric fields that fail ``float()`` become ``None`` so the
        transformer can drop them individually.
    """
    rows_out: list[dict[str, Any]] = []
    for raw in payload.get("report", []) or []:
        if not isinstance(raw, dict):
            continue
        loc = str(raw.get("loc") or "").strip()
        cycle = str(raw.get("cycle") or "").strip()
        eff_dt = str(raw.get("avaiL_CAP_EFF_DT_TIME") or "").strip()
        period = eff_dt.split("T")[0] if eff_dt else ""
        if not loc or not cycle or not period:
            log.warning(
                "Skipping malformed capacity row (loc=%r cycle=%r eff=%r)", loc, cycle, eff_dt
            )
            continue

        def _num(key: str, row: dict[str, Any] = raw) -> float | None:
            value = row.get(key)
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        rows_out.append(
            {
                "loc": loc,
                "loc_name": str(raw.get("loC_NAME") or "").strip(),
                "loc_purpose": str(raw.get("loC_PURP_DESC") or "").strip(),
                "loc_qti": str(raw.get("loc_QTI") or "").strip(),
                "flow_ind": str(raw.get("floW_IND") or "").strip(),
                "cap_type": str(raw.get("caP_TYPE_DESC") or "").strip(),
                "meas_basis": str(raw.get("meaS_BASIS") or "").strip(),
                "it": str(raw.get("it") or "").strip(),
                "all_qty_avail": str(raw.get("alL_QTY_AVAIL") or "").strip(),
                "cycle": cycle,
                "posted_at": str(raw.get("postinG_DT_TIME") or "").strip(),
                "period": period,
                "design_oper_cap": _num("desigN_OPER_CAP"),
                "oper_cap": _num("opeR_CAP"),
                "sched_qty": _num("scheD_QTY"),
                "qty_avail": _num("qtY_AVAIL"),
            }
        )
    return rows_out


async def run(
    gas_day: date | None = None,
    cycle_id: int | None = None,
) -> dict[str, Any]:
    """Scrape OAC for both pipelines into raw payloads for one gas day.

    Why:
        Orchestrator entry point. Fetches CTPL (tspNo=200) and CCPL
        (tspNo=400), normalises rows, and writes one raw JSON per pipeline
        unless the file already exists (staleness gate).

    What:
        Raw payload shape: ``fetched_at``, ``tsp_no``, ``gas_day``,
        ``cycle_id``, ``row_count``, ``rows`` (normalised dicts).

    Failure modes:
        Per-pipeline HTTP faults fail the run (both pipes are the point);
        health is recorded either way.
    """
    from datetime import UTC, datetime
    from pathlib import Path

    from scrapers.base.health_writer import HealthWriter
    from scrapers.base.safe_writer import safe_write_json

    health = HealthWriter(source_name=SOURCE_NAME)
    target_day = gas_day or datetime.now(UTC).date()
    raw_dir = Path(RAW_DIR)

    try:
        async with HttpClient(
            base_url=API_BASE_URL,
            default_headers=default_headers(),
            timeout_seconds=30.0,
            max_retries=3,
            backoff_base_seconds=1.0,
            rate_limit_per_second=1.0,
            retryable_status_codes=frozenset({403}),
        ) as client:
            processed_count = 0
            skipped_count = 0
            per_tsp_rows: dict[int, int] = {}

            for tsp_no in (TSP_CREOLE_TRAIL, TSP_CORPUS_CHRISTI):
                out_path = raw_dir / f"{target_day.isoformat()}_tsp{tsp_no}.json"
                if out_path.exists():
                    skipped_count += 1
                    continue

                payload = await fetch_capacity(client, tsp_no, target_day, cycle_id=cycle_id)
                # Tenant-fallback guard (KM pipeline2 lesson): the GetCapacity
                # API must return rows whose pipeline identity matches the
                # requested tspNo. Creole Trail rows carry CTPL loc prefixes;
                # Corpus Christi rows CCPL. Verify before parsing. The API's
                # tsP_NAME field is the stable identity ("CHENIERE CREOLE TRAIL
                # PIPELINE, L." / "CHENIERE CORPUS CHRISTI PIPELINE, L."), so
                # match on the distinctive pipeline words, not the legacy
                # "CTPL"/"CCPL" ticker that no longer appears in the payload.
                expected_marker = (
                    "CREOLE TRAIL" if tsp_no == TSP_CREOLE_TRAIL else "CORPUS CHRISTI"
                )
                blob = json.dumps(payload)
                assert_response_identity(
                    expected=expected_marker,
                    response_text=blob,
                    context=f"cheniere/tsp{tsp_no}",
                )
                rows = parse_capacity_rows(payload)
                per_tsp_rows[tsp_no] = len(rows)

                body = {
                    "fetched_at": datetime.now(UTC)
                    .replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "source": SOURCE_NAME,
                    "tsp_no": tsp_no,
                    "gas_day": target_day.isoformat(),
                    "cycle_id": cycle_id,
                    "row_count": len(rows),
                    "rows": rows,
                }
                safe_write_json(out_path, body)
                log.info(
                    "Fetched %d rows for tsp=%s %s → %s",
                    len(rows),
                    tsp_no,
                    target_day,
                    out_path,
                )
                processed_count += 1

        status = "ok" if processed_count else "skipped"
        run_metadata = {
            "processed_count": processed_count,
            "skipped_count": skipped_count,
            "per_tsp_rows": per_tsp_rows,
            "gas_day": target_day.isoformat(),
        }
        if processed_count:
            health.record_success(metadata=run_metadata)
        else:
            health.record_no_op(
                reason="no postings matched the requested gas day",
                metadata=run_metadata,
            )
        return {
            "status": status,
            "processed_count": processed_count,
            "skipped_count": skipped_count,
            "per_tsp_rows": per_tsp_rows,
        }

    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.error("Cheniere scraper failed: %s", err)
        health.record_failure(error=err)
        return {"status": "failed", "error": err}


if __name__ == "__main__":
    import asyncio as _asyncio
    import json as _json
    import logging as _logging
    import sys as _sys
    from datetime import date as _date

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    _gas_day: _date | None = None
    if len(_sys.argv) >= 2:
        try:
            _gas_day = _date.fromisoformat(_sys.argv[1])
        except ValueError:
            print(f"Invalid gas_day '{_sys.argv[1]}'. Use YYYY-MM-DD.", file=_sys.stderr)
            _sys.exit(1)

    _result = _asyncio.run(run(_gas_day))
    print(_json.dumps(_result, indent=2, default=str))
