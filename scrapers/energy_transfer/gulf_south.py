"""Gulf South Pipeline Company, LLC — Scheduled Quantities (SQ) scraper.

──────────────────────────────────────────────────────────────────────────────
ENDPOINT DISCOVERY  (conducted 2026-05-16)
──────────────────────────────────────────────────────────────────────────────

OPERATOR CLARIFICATION
    Gulf South Pipeline Company, LLC is a subsidiary of Boardwalk Pipeline
    Partners, LP (Loews Corp), NOT Energy Transfer. The pipeline uses the
    Boardwalk GasQuest system built by Slalom on Quorum iPost Enterprise.

    The old FERC EBB (https://infopost.gulfsouthpl.com) now redirects to
    https://www.gasquest.com/informational-posting.

    The direct BWP portal URL is https://infopost.bwpipelines.com/?tspid=1
    (tspid=1 = Gulf South). Both domains are Angular SPAs sharing the same
    JS bundles and backend.

BACKEND API DISCOVERY
    Entry: https://www.gasquest.com/assets/app-config.json  (public JSON)
    That file lists all microservice base URLs:
        nomination  → https://nomination.prod.bwpmlp.org
        scheduling  → https://scheduling.prod.bwpmlp.org
        reporting   → https://reporting.prod.bwpmlp.org
        tsp         → https://tsp.prod.bwpmlp.org

    SQ ENDPOINT (reverse-engineered from chunk-RUUN7BQC.js):
        Method : GET
        URL    : https://nomination.prod.bwpmlp.org/scheduled-quantity
        Params :
            tsp_id              integer    Gulf South = 1
            gas_flow_date       YYYY-MM-DD
            cycle_code          string     TIMELY | EVENING | ID1 | ID2
            page_size           integer    default 100
            page_number         integer    default 1
            sort_by             string     optional
            location_id         integer    optional (filter to one meter)
            service_requestor_id integer   optional (shipper filter)
            exclude_zero_reduction_qty boolean optional

    EXPORT ENDPOINT (CSV download, also auth-gated):
        Method : GET
        URL    : https://nomination.prod.bwpmlp.org/scheduled-quantity/export
        Params : same as above minus pagination

    SUMMARY ENDPOINT:
        Method : GET
        URL    : https://nomination.prod.bwpmlp.org/scheduled-quantity/summary

    AUTHENTICATION
        Provider : AWS Cognito
        Domain   : https://bwpgmsprodadfs.auth.us-east-1.amazoncognito.com
        Flow     : Authorization Code (ADFS SSO) or client credentials
        Header   : Authorization: Bearer <id_token>
        Env var  : GASQUEST_API_TOKEN  (paste the id_token from your browser
                   session, or obtain via the ADFS OAuth2 token endpoint)

    TLS FINGERPRINTING
        The API at *.bwpmlp.org accepts standard Python/httpx TLS without
        Cloudflare-style JA3 fingerprint blocking.  curl_cffi is NOT needed
        here (unlike Baker Hughes / infopost.energytransfer.com).

RESPONSE SHAPE  (inferred from JS + NAESB GISB standards)
    {
      "data": [
        {
          "id": 12345,
          "locationNumber": "24329",
          "locationName": "STRATTON RIDGE",
          "locationId": 987,
          "cycleCode": "TIMELY",
          "gasFlowDate": "2026-05-16",
          "scheduledQuantity": 952000,
          "unit": "DTH",
          "flowIndicator": "D",
          "transactionTypeCode": "FTS"
        }
      ],
      "total": 150,
      "pageSize": 100,
      "pageNumber": 1
    }

FREEPORT LNG INTERCONNECTION
    Meter name: STRATTON RIDGE  (Brazoria County, TX)
    Location:   24329  (SLN 24329)
    Direction:  Delivery (D) — gas flows from Gulf South to Freeport LNG
    Also alias: QUINTANA, FREEPORT, COASTAL BEND HEADER TAILGATE
    Noted in JS: FetchBISAStrattonRidgeSuccess action exists in the system

CYCLE TIMING  (NAESB GISB Order 809, all times Central)
    TIMELY:  nominations due by 11:30 AM, posted ~1:00 PM
    EVENING: nominations due by 6:00 PM,  posted ~8:00 PM
    ID1:     nominations due by 10:00 AM (following gas day), posted ~11:30 AM
    ID2:     nominations due by  2:30 PM (following gas day), posted ~4:00 PM

UNIT CONVERSION
    GasQuest reports SQ in Dth/d (dekatherms per day = MMBtu/d).
    Blue Tide canonical unit is MMcf/d.
    Conversion: 1 Mcf ≈ HHV_DTH_PER_MCF Dth at typical pipeline quality gas.
    HHV_DTH_PER_MCF = 1.025  (industry median for Gulf Coast dry gas)
    Formula: value_mmcfd = value_dthd / (HHV_DTH_PER_MCF * 1_000)
──────────────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

from dotenv import load_dotenv

from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json

load_dotenv()

log = logging.getLogger(__name__)

SOURCE_NAME = "gulf_south"
TSP_ID = 1  # Gulf South = tspid=1 in BWP GasQuest
RAW_DIR = Path("data/raw/gulf_south")

BASE_URL = "https://nomination.prod.bwpmlp.org"
SQ_PATH = "/scheduled-quantity"

# GasQuest reports Dth/d (dekatherms); we store MMcf/d.
# 1 Mcf of pipeline-quality gas ≈ 1.025 Dth at typical Gulf Coast HHV.
HHV_DTH_PER_MCF: float = 1.025
DTH_PER_MMCF: float = HHV_DTH_PER_MCF * 1_000  # 1025 Dth = 1 MMcf

CycleCode = Literal["TIMELY", "EVENING", "ID1", "ID2"]
_CYCLES: list[CycleCode] = ["TIMELY", "EVENING", "ID1", "ID2"]

PAGE_SIZE = 500  # generous upper bound; Gulf South has ~300-500 active meters


def _load_api_token() -> str:
    """Read GASQUEST_API_TOKEN from the environment.

    Failure modes:
        RuntimeError if the variable is absent — callers must handle.
    """
    token = os.environ.get("GASQUEST_API_TOKEN", "").strip()
    if not token:
        raise RuntimeError(
            "GASQUEST_API_TOKEN is not set. Obtain a Bearer token from your "
            "BWP GasQuest session (bwpgmsprodadfs.auth.us-east-1.amazoncognito.com) "
            "and export it as GASQUEST_API_TOKEN=<id_token>."
        )
    return token


def _raw_path(cycle: CycleCode, gas_day: date) -> Path:
    """Return the target raw JSON path for a given cycle + gas day."""
    return RAW_DIR / str(gas_day.year) / f"{gas_day.month:02d}" / f"{cycle}_{gas_day}.json"


def _latest_local_date(cycle: CycleCode) -> str | None:
    """Return the most recent gas_day (YYYY-MM-DD) stored locally for *cycle*."""
    if not RAW_DIR.exists():
        return None
    files = list(RAW_DIR.rglob(f"{cycle}_*.json"))
    if not files:
        return None
    # Stem format: CYCLE_YYYY-MM-DD → strip cycle prefix
    prefix = f"{cycle}_"
    dates = [p.stem[len(prefix) :] for p in files if p.stem.startswith(prefix)]
    return max(dates) if dates else None


async def _fetch_all_pages(
    client: HttpClient,
    token: str,
    cycle: CycleCode,
    gas_day: date,
) -> list[dict[str, Any]]:
    """Fetch all paginated SQ records for one cycle + gas day.

    Why:
        The API caps responses at PAGE_SIZE rows. Gulf South has ~300-500 active
        meter points. We loop until the page exhausts.

    What:
        Issues GET /scheduled-quantity with pagination, collects all data[].

    Failure modes:
        HttpClientError propagates to caller on network or auth errors.
    """
    all_rows: list[dict[str, Any]] = []
    page = 1

    while True:
        params: dict[str, str] = {
            "tsp_id": str(TSP_ID),
            "gas_flow_date": gas_day.isoformat(),
            "cycle_code": cycle,
            "page_size": str(PAGE_SIZE),
            "page_number": str(page),
        }

        raw = await client.get_json(SQ_PATH, params=params)

        if not isinstance(raw, dict):
            log.warning("Unexpected SQ response type: %s", type(raw))
            break

        rows: list[dict[str, Any]] = raw.get("data", [])
        all_rows.extend(rows)

        log.debug(
            "cycle=%s gas_day=%s page=%d rows=%d cumulative=%d",
            cycle,
            gas_day,
            page,
            len(rows),
            len(all_rows),
        )

        total: int = int(raw.get("total", 0))
        if len(all_rows) >= total or not rows:
            break
        page += 1

    return all_rows


async def fetch_sq(
    cycle: CycleCode,
    gas_day: date,
    *,
    token: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch Scheduled Quantities for one cycle + gas day, write raw JSON.

    Why:
        Public entry point for ad-hoc pulls and the GHA workflow.

    What:
        1. Loads bearer token from environment (or *token* arg).
        2. Fetches all pages from the nomination API.
        3. Wraps in an envelope and atomically writes to raw JSON.
        4. Returns the data rows.

    Failure modes:
        RuntimeError if no token is available.
        HttpClientError on network/auth failure.
    """
    tok = token or _load_api_token()

    async with HttpClient(
        base_url=BASE_URL,
        default_headers={"Authorization": f"Bearer {tok}"},
        timeout_seconds=30.0,
        max_retries=3,
        backoff_base_seconds=1.0,
        rate_limit_per_second=2.0,
    ) as client:
        rows = await _fetch_all_pages(client, tok, cycle, gas_day)

    out_path = _raw_path(cycle, gas_day)
    payload: dict[str, Any] = {
        "fetched_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "tsp_id": TSP_ID,
        "cycle": cycle,
        "gas_day": gas_day.isoformat(),
        "row_count": len(rows),
        "data": rows,
    }
    safe_write_json(out_path, payload)
    log.info("Written %d rows → %s", len(rows), out_path)
    return rows


async def run(
    cycle: CycleCode | None = None,
    gas_day: date | None = None,
) -> dict[str, Any]:
    """Scrape SQ for one cycle + gas day, write raw + health files.

    Why:
        Public entrypoint invoked by the GHA workflow and ``__main__``.

    What:
        1. Resolves *cycle* (default: TIMELY) and *gas_day* (default: today).
        2. Checks staleness gate: skip if local file already exists for this
           cycle + gas_day.
        3. Fetches all pages and writes raw JSON atomically.
        4. Writes health record.

    Returns:
        dict with status ("ok" | "skipped" | "failed"), and on success:
        cycle, gas_day, row_count, path.
    """
    resolved_cycle: CycleCode = cycle or "TIMELY"
    resolved_day: date = gas_day or date.today()

    health = HealthWriter(source_name=SOURCE_NAME)

    try:
        token = _load_api_token()
    except RuntimeError as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}

    # Staleness gate: skip if the raw file already exists for this cycle+day.
    out_path = _raw_path(resolved_cycle, resolved_day)
    if out_path.exists():
        reason = f"already fetched {resolved_cycle} {resolved_day}"
        log.info("Skipping: %s", reason)
        health.record_skipped(reason=reason)
        return {"status": "skipped", "cycle": resolved_cycle, "gas_day": resolved_day.isoformat()}

    try:
        rows = await fetch_sq(resolved_cycle, resolved_day, token=token)
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.error("Fetch failed for %s %s: %s", resolved_cycle, resolved_day, err)
        health.record_failure(error=err)
        return {"status": "failed", "error": err}

    health.record_success(
        metadata={
            "cycle": resolved_cycle,
            "gas_day": resolved_day.isoformat(),
            "row_count": len(rows),
            "path": str(out_path),
        }
    )
    return {
        "status": "ok",
        "cycle": resolved_cycle,
        "gas_day": resolved_day.isoformat(),
        "row_count": len(rows),
        "path": str(out_path),
    }


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    # Optional CLI: python -m scrapers.energy_transfer.gulf_south TIMELY 2026-05-16
    _cycle: CycleCode = "TIMELY"
    _gas_day: date = date.today()

    if len(sys.argv) >= 2:
        arg = sys.argv[1].upper()
        if arg in ("TIMELY", "EVENING", "ID1", "ID2"):
            _cycle = arg  # type: ignore[assignment]
        else:
            print(f"Unknown cycle '{sys.argv[1]}'. Use TIMELY|EVENING|ID1|ID2.", file=sys.stderr)
            sys.exit(1)

    if len(sys.argv) >= 3:
        try:
            _gas_day = date.fromisoformat(sys.argv[2])
        except ValueError:
            print(f"Invalid gas_day '{sys.argv[2]}'. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    result = asyncio.run(run(_cycle, _gas_day))
    print(json.dumps(result, indent=2, default=str))
