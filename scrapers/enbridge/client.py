"""Enbridge rtba informational-postings client — Texas Eastern (TETCO) OAC.

Why:
    Freeport LNG is fed by Gulf South AND TETCO. Blue Tide captures Gulf
    South only, understating Freeport feedgas. TETCO's meter-level OAC
    (sub-type MLC = Meter Level Capacity) embeds Total Scheduled Quantity
    per NAESB cycle for every meter, including Loc 79999 "STRATTON RIDGE"
    (Zone STX, Delivery) — the Freeport lateral interconnect.

What:
    Talks to the public, tokenless ASP.NET AJAX page-method API on
    ``rtba.enbridge.com`` (verified live 2026-08-23; no auth, no ViewState,
    no cookies — cold calls work):

    * ``call_page_method`` — POSTs JSON to ``Download.aspx/{Method}``
      through the shared ``HttpClient`` (retry + rate limit apply) and
      unwraps the ASP.NET ``{"d": ...}`` envelope.
    * ``fetch_oac_zip_bytes`` — runs the download chain the site's own
      ``Scripts/download.js`` runs: StartFile → AddToFile loop (one call
      per gas day, each returning the next gas day) → ZipFile, then
      form-POSTs ``HttpHandlers/FileHandler.ashx`` for the ZIP of
      per-cycle CSVs.
    * ``parse_oac_zip`` — unpacks that ZIP into transformer-compatible raw
      payloads (one CSV per cycle-posting inside).

Cycle vocabulary:
    Each CSV in the archive is one posting: ``TIMELY``, ``EVENING``, or an
    unnumbered intraday slot stamped with its post time. To keep series ids
    stable AND lossless, intraday slots become ``id{HHMM}`` of their post
    time (e.g. ``id0901``) — same shape as Gulf South's ``id1/id2/id3`` but
    derived from the posting clock instead of a counter.

Failure modes:
    * Page-method application faults arrive as HTTP 500 or an
      ``"Error-..."`` string inside the ``d`` envelope — surfaced as
      ``HttpClientError``/``EnbridgeApiError``.
    * The API is scoped to ``businessUnitAbbreviation="TE"``; other Enbridge
      business units answer HTTP 500 from this host (ANR/ETNG are separate
      infopost hosts in the same vendor family — pass a different
      ``business_unit``/base URL pair when pointing this client at them).
    * Dates are ``MM-DD-YYYY`` on the wire, ISO-8601 in CSV filenames —
      normalize at the boundary via ``to_api_date``/``from_api_date``.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import urllib.parse
import zipfile
from datetime import UTC, date, datetime
from io import StringIO
from typing import Any

from scrapers.base.errors import HttpClientError
from scrapers.base.http_client import HttpClient

log = logging.getLogger(__name__)

SOURCE_NAME = "enbridge"
BUSINESS_UNIT = "TE"  # Texas Eastern Transmission, LP — the only BU this host serves
RAW_DIR = "data/raw/enbridge"

BASE_URL = "https://rtba.enbridge.com/InformationalPosting/"
DOWNLOAD_PAGE = "Download.aspx"
FILE_HANDLER_PATH = "HttpHandlers/FileHandler.ashx"

POSTING_TYPE = "OA"
POSTING_SUBTYPE = "MLC"  # Meter Level Capacity — TSQ embedded per meter/cycle
FILE_TYPE = "csv"

#: ``cycle:"All"`` = every posted cycle per gas day (the site UI restricts
#: fileType to csv in this mode, which matches our use).
CYCLE_ALL = "All"

# CSV columns (exact headers of TE_OA_MLC_* blobs).
COL_CYCLE = "Cycle_Desc"
COL_POST_DATE = "Post_Date"
COL_POST_TIME = "Post_Time"
COL_GAS_DAY = "Eff_Gas_Day"
COL_LOC = "Loc"
COL_LOC_NAME = "Loc_Name"
COL_LOC_ZN = "Loc_Zn"
COL_FLOW_IND = "Flow_Ind_Desc"
COL_TSQ = "Total_Scheduled_Quantity"
COL_OP_CAP = "Operating_Capacity"
COL_DESIGN_CAP = "Total_Design_Capacity"
COL_OAC = "Operationally_Available_Capacity"


class EnbridgeApiError(HttpClientError):
    """Raised when the rtba page-method envelope carries an application error."""


def to_api_date(d: date) -> str:
    """Format a date as the API's ``MM-DD-YYYY`` wire format."""
    return f"{d.month:02d}-{d.day:02d}-{d.year:04d}"


def from_api_date(raw: str) -> date:
    """Parse an API date — ``MM-DD-YYYY`` on requests, ``MM/DD/YYYY`` in
    AddToFile responses (both observed live).

    Failure modes:
        ``ValueError`` on malformed input.
    """
    cleaned = raw.strip().replace("/", "-")
    mm, dd, yyyy = cleaned.split("-")
    return datetime.strptime(f"{yyyy}-{mm}-{dd}", "%Y-%m-%d").date()


def resolve_cycle_token(cycle_desc: str, post_time_hhmm: str | None = None) -> str | None:
    """Map a CSV cycle/posting descriptor to the repo's cycle vocabulary.

    What:
        ``TIMELY*``→timely, ``EVENING*``→evening, ``LATE*``→late (a legacy
        2023-era overnight cycle), and every remaining intraday flavor
        (``INTRADAY*``/``INTRDY*``/``INTRDYC*`` — TETCO posts OAC roughly
        hourly, with ``C``-suffixed correction re-posts) → ``id{HH}00``,
        bucketed to the posting hour so ids are stable across days despite
        minute-level post-time jitter (a 04:01 re-post revises the 04:00
        snapshot; dedup-on-posted-at keeps the newer).

    Failure modes:
        Unknown shapes return ``None`` so callers skip the row/file loudly.
    """
    c = cycle_desc.strip().upper()
    if not c:
        return None
    head = c.split("_", 1)[0].split(" ", 1)[0]
    if head == "TIMELY":
        return "timely"
    if head == "EVENING":
        return "evening"
    if head == "LATE":
        return "late"
    if head.startswith(("INTRADAY", "INTRDY")):
        digits = "".join(ch for ch in (post_time_hhmm or "") if ch.isdigit())[:4]
        if len(digits) < 4:
            # Descriptor sometimes embeds its own slot stamp: INTRDY_<date>_<hhmm>
            tail = c.rsplit("_", 1)[-1]
            if tail.isdigit() and len(tail) == 4:
                digits = tail
        if len(digits) == 4 and digits.isdigit():
            return f"id{digits[:2]}00"
        return "intraday"
    return None


def parse_csv_filename(name: str) -> tuple[str, str] | None:
    """Extract ``(cycle_token, gas_day_iso)`` from a CSV member of the ZIP.

    What:
        Filenames look like ``TE_OA_MLC_2026-08-21_TIMELY_2026-08-20_1511``
        or ``TE_OA_MLC_2026-08-21_INTRDY_2026-08-22_0901`` — seven underscore
        separated tokens ``{BU}_{posting}_{subtype}_{eff_gas_day}_{CYCLE}_
        {post_date}_{hhmm}``. Intraday slots resolve to ``id{HHMM}``.

    Failure modes:
        Returns ``None`` for names that don't match so unexpected archive
        members never crash a walk.
    """
    stem = name.rsplit(".", 1)[0]
    if not name.lower().endswith(".csv"):
        return None
    parts = stem.split("_")
    if len(parts) < 7:
        return None
    try:
        gas_day = date.fromisoformat(parts[3])
    except ValueError:
        return None
    cycle_raw = "_".join(parts[4:-2])
    hhmm = parts[-1]
    token = resolve_cycle_token(cycle_raw, hhmm)
    if token is None:
        log.warning("Unmapped cycle %r in archive member %s", cycle_raw, name)
        return None
    return token, gas_day.isoformat()


async def call_page_method(
    client: HttpClient,
    method: str,
    params: dict[str, object],
) -> Any:
    """POST one ASP.NET AJAX page-method and return its unwrapped result.

    What:
        Uses ``HttpClient.post_json`` (which issues a real POST with a JSON
        body through the retry/rate-limit engine — see ``_request``) against
        ``Download.aspx/{method}``, the exact contract in the site's own
        ``Scripts/service.js``, then unwraps ``{"d": ...}``.

    Failure modes:
        ``HttpClientError`` on transport/retry exhaustion;
        ``EnbridgeApiError`` on envelope/application faults.
    """
    result = await client.post_json(DOWNLOAD_PAGE + "/" + method, params)
    if isinstance(result, dict):
        if "d" not in result:
            raise EnbridgeApiError(
                url=f"{BASE_URL}{DOWNLOAD_PAGE}/{method}",
                status=200,
                attempts=1,
                elapsed_s=0.0,
                reason=f"Missing ASP.NET 'd' envelope: {str(result)[:200]}",
            )
        inner = result["d"]
    else:
        inner = result  # tolerate servers that unwrap already
    if isinstance(inner, str) and inner.startswith("Error-"):
        raise EnbridgeApiError(
            url=f"{BASE_URL}{DOWNLOAD_PAGE}/{method}",
            status=200,
            attempts=1,
            elapsed_s=0.0,
            reason=inner,
        )
    return inner


def build_download_params(
    *,
    start_gas_date: str,
    end_gas_date: str,
    cycle: str = CYCLE_ALL,
    business_unit: str = BUSINESS_UNIT,
    posting_type: str = POSTING_TYPE,
    posting_subtype: str = POSTING_SUBTYPE,
    file_type: str = FILE_TYPE,
) -> dict[str, object]:
    """Return the parameter block shared by StartFile/AddToFile."""
    return {
        "businessUnitAbbreviation": business_unit,
        "postingType": posting_type,
        "postingSubType": posting_subtype,
        "fileType": file_type,
        "cycle": cycle,
        "startGasDate": start_gas_date,
        "endGasDate": end_gas_date,
    }


async def fetch_oac_zip_bytes(
    client: HttpClient,
    begin: date,
    end: date,
    *,
    cycle: str = CYCLE_ALL,
    business_unit: str = BUSINESS_UNIT,
    inter_call_sleep_seconds: float = 1.0,
) -> bytes:
    """Run the full download chain and return the OAC ZIP bytes.

    Why:
        Mirrors exactly what the site's Download button does, so behaviour
        stays stable even if the frontend changes cosmetics.

    What:
        StartFile (returns a GUID filename) → AddToFile once per gas day
        (each call returns the next gas day string; stops past *end*) →
        ZipFile → form-POST ``FileHandler.ashx`` for the bytes.

    Failure modes:
        ``HttpClientError``/``EnbridgeApiError`` propagate. The caller owns
        pacing between chains (the backfill throttles per window).
    """
    start_s, end_s = to_api_date(begin), to_api_date(end)
    base = build_download_params(
        start_gas_date=start_s, end_gas_date=end_s, cycle=cycle, business_unit=business_unit
    )

    filename = await call_page_method(client, "StartFile", base)
    if not isinstance(filename, str) or not filename.strip():
        raise EnbridgeApiError(
            url=f"{BASE_URL}{DOWNLOAD_PAGE}/StartFile",
            status=200,
            attempts=1,
            elapsed_s=0.0,
            reason=f"StartFile returned {filename!r}",
        )
    log.info("StartFile(%s..%s) -> %s", start_s, end_s, filename)

    current = begin
    while current <= end:
        nxt = await call_page_method(
            client,
            "AddToFile",
            {**base, "currentDate": to_api_date(current), "fileName": filename},
        )
        if not isinstance(nxt, str) or not nxt.strip():
            break
        current = from_api_date(nxt)
        if inter_call_sleep_seconds > 0:
            await asyncio.sleep(inter_call_sleep_seconds)

    await call_page_method(client, "ZipFile", {"fileName": filename})

    form = urllib.parse.urlencode(
        {
            "fileName": filename,
            "postingAbbreviation": POSTING_TYPE,
            "postingSubType": POSTING_SUBTYPE,
            "startGasDate": start_s,
            "endGasDate": end_s,
        }
    ).encode()
    # Form POST (not JSON) — outside HttpClient's helpers, which are typed
    # around GET/JSON; single low-risk call so direct httpx use is contained.
    response = await client._client.post(  # noqa: SLF001
        FILE_HANDLER_PATH,
        content=form,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    if not response.is_success:
        raise HttpClientError(
            url=BASE_URL + FILE_HANDLER_PATH,
            status=response.status_code,
            attempts=1,
            elapsed_s=0.0,
            reason=f"FileHandler HTTP {response.status_code}",
        )
    return response.content


def parse_oac_zip(
    zip_bytes: bytes,
    fetched_at: str,
    *,
    business_unit: str = BUSINESS_UNIT,
    max_files: int | None = None,
) -> list[dict[str, Any]]:
    """Unpack an OAC ZIP into transformer-compatible raw payloads.

    What:
        One payload per CSV inside the archive (each CSV is one cycle-post
        of the full meter list). Payload shape mirrors the Gulf South/BHE
        scrapers: ``fetched_at, source, business_unit, archive_member,
        cycle, gas_day, row_count, data`` where ``data`` rows carry cleaned
        CSV columns.

    Failure modes:
        Members whose names don't parse are skipped with a warning; empty
        CSVs produce no payloads; a corrupt archive raises ``BadZipFile``.
    """
    out: list[dict[str, Any]] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
        if max_files is not None:
            names = names[:max_files]
        for name in names:
            parsed = parse_csv_filename(name)
            if parsed is None:
                continue
            cycle_tok, gas_day_iso = parsed
            text = zf.read(name).decode("utf-8-sig", errors="replace")
            rows = [
                {(k or "").strip(): (v or "").strip() for k, v in row.items() if k is not None}
                for row in csv.DictReader(StringIO(text))
            ]
            rows = [r for r in rows if r.get(COL_LOC)]
            if not rows:
                continue
            out.append(
                {
                    "source": SOURCE_NAME,
                    "business_unit": business_unit,
                    "archive_member": name,
                    "cycle": cycle_tok,
                    "gas_day": gas_day_iso,
                    "fetched_at": fetched_at,
                    "row_count": len(rows),
                    "data": rows,
                }
            )
    return out


async def run(begin: date | None = None, end: date | None = None) -> dict[str, object]:
    """Scrape TETCO OAC into raw payloads for the configured window.

    Why:
        Orchestrator entry point used by the scheduled workflow: fetches the
        window's ZIP, unpacks it, writes one raw JSON per cycle-post. Existing
        raw files act as a staleness gate — repeats are free.

    Failure modes:
        Transport faults propagate as ``HttpClientError`` (health records a
        failure either way); an empty archive raises ``RuntimeError``.
    """
    from pathlib import Path

    from scrapers.base.health_writer import HealthWriter
    from scrapers.base.safe_writer import safe_write_json

    health = HealthWriter(source_name=SOURCE_NAME)
    raw_dir = Path(RAW_DIR)
    today = datetime.now(UTC).date()
    effective_end = end or today
    effective_begin = begin or effective_end

    try:
        async with HttpClient(
            base_url=BASE_URL,
            timeout_seconds=120.0,
            max_retries=3,
            backoff_base_seconds=2.0,
            rate_limit_per_second=0.5,
        ) as client:
            zip_bytes = await fetch_oac_zip_bytes(client, effective_begin, effective_end)
            payloads = parse_oac_zip(
                zip_bytes,
                datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            )
        if not payloads:
            raise RuntimeError("OAC archive contained no parseable CSV postings.")

        written = 0
        skipped = 0
        for p in payloads:
            stem = str(p["archive_member"]).rsplit(".", 1)[0]
            out_path = raw_dir / f"{p['gas_day']}_{p['cycle']}_{stem}.json"
            if out_path.exists():
                skipped += 1
                continue
            safe_write_json(out_path, p)
            written += 1

        total_rows = sum(int(p["row_count"]) for p in payloads)
        health.record_success(metadata={"written": written, "skipped_existing": skipped, "rows": total_rows})
        return {
            "status": "ok",
            "window": [effective_begin.isoformat(), effective_end.isoformat()],
            "payloads": len(payloads),
            "written": written,
            "skipped_existing": skipped,
        }
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.error("Enbridge rtba scraper failed: %s", err)
        health.record_failure(error=err)
        return {"status": "failed", "error": err}


if __name__ == "__main__":
    import argparse as _argparse
    import json as _json
    import logging as _logging
    import sys as _sys

    _logging.basicConfig(
        level=_logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    _parser = _argparse.ArgumentParser(prog="python -m scrapers.enbridge.client")
    _parser.add_argument("begin", nargs="?", default=None, help="Begin gas day YYYY-MM-DD")
    _parser.add_argument("end", nargs="?", default=None, help="End gas day YYYY-MM-DD (default today)")
    _args = _parser.parse_args(_sys.argv[1:])

    _begin = date.fromisoformat(_args.begin) if _args.begin else None
    _end = date.fromisoformat(_args.end) if _args.end else None

    _result = asyncio.run(run(_begin, _end))
    print(_json.dumps(_result, indent=2, default=str))
