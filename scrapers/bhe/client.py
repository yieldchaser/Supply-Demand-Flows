"""BHE GT&S infopost client — EGTS Operationally Available postings.

Why:
    Cove Point LNG's own infopost is thin; its feedgas TSQ is visible inside
    the larger EGTS (Eastern Gas Transmission & Storage) OAC CSV at
    Loc 40704 "EGTS - LOUDOUN" with Interconnect Party Name
    "COVE POINT LNG LP". One scrape covers both.

What:
    Talks to the public, tokenless infopost JSON API:
    * ``search_historical_data`` — POSTs ``{category, subcategory, beginDate,
      endDate}`` to ``/api/{tsp}/postings/searchHistoricalData``. The bare GET
    form of this URL answers HTTP 500; the POST body is the contract the
    infopost frontend itself uses (dates are ``yyyy-MM-dd`` strings or null;
    the UI allows at most a 39-month lookback). Returns the posting list.
    * ``download_posting_csv`` — GETs a posting's CSV ``blobUrl``.

Failure modes:
    Retries 403/429 alongside the HttpClient defaults (the infopost WAF
    intermittently serves transient 403s to datacenter IPs); raises
    ``HttpClientError`` once retries are exhausted. Malformed posting
    payloads are skipped by ``extract_csv_postings`` rather than raised.
"""

from __future__ import annotations

import asyncio
import csv
import logging
from datetime import date
from io import StringIO
from typing import Any, Literal

from scrapers.base.errors import HttpClientError
from scrapers.base.headers import BHE_CSV_COLUMNS, rename_keys, resolve_columns
from scrapers.base.http_client import HttpClient
from scrapers.base.identity import assert_response_identity

log = logging.getLogger(__name__)

SOURCE_NAME = "bhe"
#: BHE GT&S infopost hosts several pipelines under one API family:
#: ``egts`` (Eastern Gas Transmission & Storage — carries Cove Point
#: feedgas interconnects) and ``cpl`` (Cove Point LNG LP's OWN postings,
#: which enumerate receipts from ALL feeders: Transco Pleasant Valley,
#: Columbia/TCO Loudoun, EGTS itself, and CPL storage). Both are scraped
#: since 2026-08-26; raw filenames carry the tsp so they never collide.
TSP_SLUG = "egts"
CPL_TSP_SLUG = "cpl"
RAW_DIR = "data/raw/bhe"

INFOPOST_BASE_URL = "https://infopost.bhegts.com"
SEARCH_HISTORICAL_PATH = "/api/{tsp}/postings/searchHistoricalData"

CATEGORY = "Capacity"
SUBCATEGORY = "Operationally Available"

Cycle = Literal["TIMELY", "EVENING", "ID1", "ID2", "ID3"]
_CYCLES: list[Cycle] = ["TIMELY", "EVENING", "ID1", "ID2", "ID3"]

# Loc 40704 "EGTS - LOUDOUN" — the Cove Point feedgas anchor on EGTS.
COVE_POINT_LOC = 40704
COVE_POINT_INTERCONNECT = "COVE POINT LNG LP"
COVE_POINT_FLOW_IND = "R"

# CSV column names (exact, from EGTS OAC blobs).
COL_INTERCONNECT = "Interconnect Party Name"
COL_FLOW_IND = "Flow Ind"
COL_LOC = "Loc"
COL_LOC_NAME = "Loc Name"
COL_GAS_DAY = "Eff Gas Day"
COL_CYCLE = "CycleDesc"
COL_OP_CAP = "Operating Capacity"
COL_TSQ = "Total Scheduled Quantity"
COL_OAC = "Operationally Available Capacity"


def build_search_payload(
    begin_date: date | None,
    end_date: date | None,
) -> dict[str, object]:
    """Return the JSON body the infopost frontend POSTs for a date window.

    What:
        ``{"category": ..., "subcategory": ..., "beginDate": yyyy-MM-dd|null,
        "endDate": yyyy-MM-dd|null}`` — the exact contract observed in the
        infopost page bundle (dates formatted ``yyyy-MM-dd``, nullable).
    """
    return {
        "category": CATEGORY,
        "subcategory": SUBCATEGORY,
        "beginDate": begin_date.isoformat() if begin_date else None,
        "endDate": end_date.isoformat() if end_date else None,
    }


def extract_csv_postings(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    """Filter a searchHistoricalData response to Capacity/Operationally-Available CSV postings.

    What:
        Normalises the response (either ``{"postings": [...]}`` or a bare
        list), keeps postings whose category/subcategory match the OAC report
        and that carry a CSV content blob, and derives ``gas_day`` from the
        CSV's own effective-date semantics via the posting ``effectiveDate``
        (the CSV's ``Eff Gas Day`` column remains the authoritative period at
        parse time; this value only drives skip/checkpoint bookkeeping).

    Failure modes:
        Non-dict postings, missing fields, or postings without a CSV blob are
        skipped; never raises on payload shape surprises.
    """
    if isinstance(payload, dict):
        postings = payload.get("postings", [])
    elif isinstance(payload, list):
        postings = payload
    else:
        return []

    out: list[dict[str, Any]] = []
    for p in postings:
        if not isinstance(p, dict):
            continue
        if p.get("category") != CATEGORY or p.get("subcategory") != SUBCATEGORY:
            continue
        contents = p.get("contents") or []
        csv_url = next(
            (c.get("blobUrl") for c in contents if isinstance(c, dict) and c.get("type") == "csv"),
            None,
        )
        if not csv_url:
            continue
        effective = str(p.get("effectiveDate") or "")
        out.append(
            {
                "notice_id": p.get("noticeId"),
                "subject": str(p.get("subject") or ""),
                "posted_at": str(p.get("postedDate") or ""),
                # Effective gas day implied by the posting (local ET calendar
                # day); the CSV's Eff Gas Day column governs at parse time.
                "gas_day": effective.split("T")[0] if effective else "",
                "csv_url": str(csv_url),
            }
        )
    return out


def parse_oac_csv(csv_text: str, tsp: str = TSP_SLUG) -> list[dict[str, str]]:
    """Parse a BHE infopost OAC CSV blob into clean dicts, stripping whitespace.

    What:
        Mirrors the Gulf South parser: strips keys/values, drops rows without
        a ``Loc`` so header/placeholder rows never enter the raw payloads.

        Tenant-fallback guard (KM pipeline2 lesson): the CSV's ``TSP Name``
        column must identify the *requested* pipeline before any row is
        returned — infopost serves multiple pipelines from the same API
        family, and a wrong-pipe response parses cleanly but mislabels every
        row. ``egts`` matches its own name; ``cpl`` blobs are served with
        ``TSP Name = COVE POINT LNG LP`` (the terminal operator itself).

    Failure modes:
        Rows with missing ``Loc`` are skipped; a malformed header surfaces as
        an empty list rather than an exception. A response whose TSP Name
        does not match the requested tsp raises :class:`TenantFallbackError`.
    """
    expected_name = "COVE POINT LNG LP" if tsp == CPL_TSP_SLUG else "EGTS"
    assert_response_identity(
        expected=expected_name,
        response_text=csv_text,
        context=f"bhe/{tsp} OAC CSV",
    )
    reader = csv.DictReader(StringIO(csv_text))
    if reader.fieldnames is None:
        return []
    colmap = resolve_columns(
        BHE_CSV_COLUMNS,
        [f for f in reader.fieldnames if f],
        source="bhe/egts OAC CSV",
    )
    rows: list[dict[str, str]] = []
    for row in reader:
        clean = {
            (k.strip() if k else ""): (v.strip() if v else "")
            for k, v in row.items()
            if k is not None
        }
        clean = rename_keys(clean, colmap)
        if not clean.get(COL_LOC):
            continue
        rows.append(clean)
    return rows


def is_cove_point_row(row: dict[str, str]) -> bool:
    """Return True when *row* is the Cove Point feedgas meter (Loc 40704, Flow Ind R).

    What:
        Filter rule: ``Interconnect Party Name == "COVE POINT LNG LP"`` AND
        ``Flow Ind == "R"`` (case-insensitive on both).
    """
    return (
        row.get(COL_INTERCONNECT, "").strip().upper() == COVE_POINT_INTERCONNECT
        and row.get(COL_FLOW_IND, "").strip().upper() == COVE_POINT_FLOW_IND
    )


def cycle_from_subject(subject: str) -> Cycle | None:
    """Map a posting subject like ``MAY Capacity Available 05/16/2026 Intraday 3`` to a cycle.

    What:
        Scans for the cycle name as the final token(s) of the subject; the
        CSV's own ``CycleDesc`` column remains the authoritative cycle at
        parse time — this only feeds raw-file naming and dedupe bookkeeping.

    Failure modes:
        Returns ``None`` when no known cycle token is present.
    """
    s = subject.upper()
    if s.endswith("TIMELY"):
        return "TIMELY"
    if s.endswith("EVENING"):
        return "EVENING"
    if s.endswith("INTRADAY 1"):
        return "ID1"
    if s.endswith("INTRADAY 2"):
        return "ID2"
    if s.endswith("INTRADAY 3"):
        return "ID3"
    return None


async def fetch_postings(
    client: HttpClient,
    begin_date: date | None,
    end_date: date | None,
    tsp: str = TSP_SLUG,
) -> list[dict[str, Any]]:
    """POST the search payload and return filtered OAC CSV postings.

    Failure modes:
        ``HttpClientError`` propagates after retry exhaustion (403/429 are
        retryable with backoff per the client configuration in ``run``).
    """
    url = SEARCH_HISTORICAL_PATH.format(tsp=tsp)
    payload = await client.post_json(url, build_search_payload(begin_date, end_date))
    postings = extract_csv_postings(payload)
    if not postings:
        log.warning("searchHistoricalData returned no OAC CSV postings for the window.")
    return postings


async def download_posting_csv(client: HttpClient, csv_url: str) -> str:
    """Download and decode one posting CSV blob (UTF-8 with BOM tolerated).

    Failure modes:
        ``HttpClientError`` propagates; decode errors (a WAF challenge served
        as 200) raise ``UnicodeDecodeError``/``ValueError`` to the caller.
    """
    raw = await client.get_bytes(csv_url)
    return raw.decode("utf-8-sig")


async def run(
    begin_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    """Scrape EGTS OAC postings into raw payloads for the Cove Point meter.

    Why:
        Orchestrator entry point: lists postings in the window (default:
        null dates = full available history per the API), skips postings
        whose raw file already exists, downloads and parses each CSV, and
        writes transformer-compatible raw JSON per (gas_day, cycle).

    What:
        Raw payload shape mirrors the Gulf South scraper (``fetched_at``,
        ``cycle``, ``gas_day``, ``posted_at``, ``row_count``, ``data``) so
        the transformer pattern carries over unchanged. Files are named
        ``{gas_day}_{cycle}_{noticeId}.json``; the noticeId suffix keeps
        same-cycle re-postings from clobbering one another.

    Failure modes:
        Per-posting HTTP faults are contained (logged, counted, walk
        continues); only a listing-endpoint failure propagates. Health is
        recorded either way.
    """
    from datetime import UTC, datetime
    from pathlib import Path

    from scrapers.base.health_writer import HealthWriter
    from scrapers.base.safe_writer import safe_write_json

    health = HealthWriter(source_name=SOURCE_NAME)
    raw_dir = Path(RAW_DIR)

    try:
        async with HttpClient(
            base_url=INFOPOST_BASE_URL,
            timeout_seconds=30.0,
            max_retries=3,
            backoff_base_seconds=1.0,
            rate_limit_per_second=1.0,
            retryable_status_codes=frozenset({403}),
        ) as client:
            processed_count = 0
            skipped_count = 0
            failed_count = 0

            # Scrape BOTH pipelines: EGTS (interconnect view) and CPL (the
            # terminal's own postings, which enumerate receipts from ALL
            # feeders — Transco Pleasant Valley, Columbia/TCO Loudoun, CPL
            # storage). Raw filenames carry the tsp prefix so the two feeds
            # never collide on (gas_day, cycle, notice_id).
            for tsp in (TSP_SLUG, CPL_TSP_SLUG):
                postings = await fetch_postings(client, begin_date, end_date, tsp=tsp)
                if not postings and tsp == TSP_SLUG:
                    raise RuntimeError("No OAC postings returned from BHE GT&S infopost.")

                for item in postings:
                    cycle = cycle_from_subject(item["subject"])
                    gas_day_str = item["gas_day"]
                    if not cycle or not gas_day_str:
                        log.warning(
                            "Skipping notice %s: unmapped cycle/gas day (%r / %r)",
                            item["notice_id"],
                            item["subject"],
                            gas_day_str,
                        )
                        failed_count += 1
                        continue

                    out_path = (
                        raw_dir / f"{tsp}_{gas_day_str}_{cycle}_{item['notice_id']}.json"
                    )
                    if out_path.exists():
                        skipped_count += 1
                        continue

                    try:
                        csv_text = await download_posting_csv(client, item["csv_url"])
                    except HttpClientError as exc:
                        log.warning("Skipping notice %s after HTTP failure: %s", item["notice_id"], exc)
                        failed_count += 1
                        continue
                    except (UnicodeDecodeError, ValueError) as exc:
                        log.warning(
                            "Skipping notice %s: CSV failed to decode (%s: %s)",
                            item["notice_id"],
                            type(exc).__name__,
                            exc,
                        )
                        failed_count += 1
                        continue

                    rows = parse_oac_csv(csv_text, tsp=tsp)
                    # Rate-limit courtesy pause between blob downloads.
                    await asyncio.sleep(1.0)

                    payload = {
                        "fetched_at": datetime.now(UTC)
                        .replace(microsecond=0)
                        .isoformat()
                        .replace("+00:00", "Z"),
                        "source": SOURCE_NAME,
                        "tsp": tsp,
                        "notice_id": item["notice_id"],
                        "cycle": cycle,
                        "gas_day": gas_day_str,
                        "posted_at": item["posted_at"],
                        "row_count": len(rows),
                        "data": rows,
                    }
                    safe_write_json(out_path, payload)
                    log.info(
                        "Fetched %d rows for %s %s (notice %s) → %s",
                        len(rows),
                        gas_day_str,
                        cycle,
                        item["notice_id"],
                        out_path,
                    )
                    processed_count += 1

        status = "ok" if processed_count else "skipped"
        run_metadata = {
            "processed_count": processed_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        }
        if processed_count:
            health.record_success(metadata=run_metadata)
        else:
            health.record_no_op(
                reason="no postings matched the requested window",
                metadata=run_metadata,
            )
        return {
            "status": status,
            "processed_count": processed_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        }

    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.error("BHE GT&S scraper failed: %s", err)
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

    _begin: _date | None = None
    _end: _date | None = None
    if len(_sys.argv) >= 2:
        _begin = _date.fromisoformat(_sys.argv[1])
    if len(_sys.argv) >= 3:
        _end = _date.fromisoformat(_sys.argv[2])

    _result = _asyncio.run(run(_begin, _end))
    print(_json.dumps(_result, indent=2, default=str))
