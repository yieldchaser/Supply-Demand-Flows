"""Gulf South Pipeline Company, LLC — Scheduled Quantities (SQ) scraper.

Uses the public, tokenless Operational Capacity (OAC) reporting endpoint
to fetch scheduled quantities and capacity details.

Retry semantics: the BWP reporting WAF intermittently serves transient
403s to datacenter IPs (CI runners), so this scraper opts 403 into the
retryable status set with literal backoff delays of 5s/15s/45s.
"""

from __future__ import annotations

import asyncio
import base64
import csv
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json

log = logging.getLogger(__name__)

SOURCE_NAME = "gulf_south"
TSP_ID = 1  # Gulf South = tspid=1 in BWP GasQuest
RAW_DIR = Path("data/raw/gulf_south")

REPORTING_BASE_URL = "https://reporting.prod.bwpmlp.org"
INFOPOST_PATH = "/infopost/infopostdetails"
POSTINGS_PATH = "/infopost/postings"

CycleCode = Literal["TIMELY", "EVENING", "ID1", "ID2", "ID3"]
_CYCLES: list[CycleCode] = ["TIMELY", "EVENING", "ID1", "ID2", "ID3"]


def _raw_path(cycle: CycleCode, gas_day: date) -> Path:
    """Return the target raw JSON path for a given cycle + gas day."""
    return RAW_DIR / f"{gas_day.isoformat()}_{cycle}.json"


async def fetch_postings_list(client: HttpClient, page_size: int = 20) -> list[dict[str, Any]]:
    """Fetch the list of postings from the public OAC endpoint.

    Why:
        Operational Capacity list details are fetched via POST.
    """
    payload = {
        "infoPostID": 1,
        "tspId": TSP_ID,
        "pageNumber": 1,
        "pageSize": page_size,
        "sortBy": "datetimePostingEffective",
        "groupCode": "INFOPOST",
        "sortDescending": True,
    }
    res = await client.post_json(INFOPOST_PATH, payload)
    if isinstance(res, dict):
        postings = res.get("postings")
        if isinstance(postings, list):
            return postings
    return []


def extract_csv_tracker_ids(postings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Extract CSV tracker document IDs, cycle, and gas day from the postings list."""
    extracted = []
    for p in postings:
        cycle_str = p.get("cycleCode") or ""
        if not cycle_str and p.get("description"):
            desc = p.get("description", "").upper()
            if "TIMELY" in desc:
                cycle_str = "TIMELY"
            elif "EVENING" in desc:
                cycle_str = "EVENING"
            elif "INTRADAY 1" in desc or "ID1" in desc:
                cycle_str = "ID1"
            elif "INTRADAY 2" in desc or "ID2" in desc:
                cycle_str = "ID2"
            elif "INTRADAY 3" in desc or "ID3" in desc:
                cycle_str = "ID3"

        if not cycle_str:
            continue
        cycle = cycle_str.upper()
        if cycle not in _CYCLES:
            continue

        dt_effective = p.get("datetimePostingEffective", "")
        if not dt_effective:
            continue
        # Split datetime (e.g. 2026-05-22T21:54:00+00:00) to get YYYY-MM-DD
        gas_day_str = dt_effective.split("T")[0]

        report_files = p.get("reportFiles", [])
        tracker_id = None
        for rf in report_files:
            if rf.get("infoPostDocumentTypeTitle") == "CSV Documents":
                tracker_id = rf.get("infoPostTrackerID")
                break

        if tracker_id:
            extracted.append(
                {
                    "tracker_id": tracker_id,
                    "cycle": cycle,
                    "gas_day": gas_day_str,
                    "posted_at": dt_effective,
                }
            )
    return extracted


async def fetch_oac_csv(client: HttpClient, tracker_id: int) -> str:
    """Download the OAC report CSV and decode it (base64 UTF-8-sig)."""
    params = {"postingsDocumentId": str(tracker_id)}
    raw_bytes = await client.get_bytes(POSTINGS_PATH, params=params)
    csv_bytes = base64.b64decode(raw_bytes)
    return csv_bytes.decode("utf-8-sig")


def parse_oac_csv(csv_text: str) -> list[dict[str, str]]:
    """Parse the OAC CSV into clean dictionaries, stripping whitespace."""
    reader = csv.DictReader(csv_text.splitlines())
    rows = []
    for row in reader:
        # Strip whitespace from keys and values
        clean_row = {(k.strip() if k else ""): (v.strip() if v else "") for k, v in row.items()}
        # Ignore empty/header placeholder rows
        if not clean_row.get("Loc"):
            continue
        rows.append(clean_row)
    return rows


async def run(
    cycle: CycleCode | None = None,
    gas_day: date | None = None,
) -> dict[str, Any]:
    """Scrape Operational Capacity (OAC) to extract SQ and available capacity.

    Why:
        Orchestrator entry point. Syncs missing cycle files for target gas day.
    """
    health = HealthWriter(source_name=SOURCE_NAME)

    try:
        async with HttpClient(
            base_url=REPORTING_BASE_URL,
            timeout_seconds=30.0,
            max_retries=3,
            backoff_base_seconds=1.0,
            rate_limit_per_second=2.0,
            retryable_status_codes=frozenset({403}),
            backoff_delays=(5.0, 15.0, 45.0),
        ) as client:
            postings = await fetch_postings_list(client, page_size=20)
            if not postings:
                raise RuntimeError("No postings returned from Gulf South OAC endpoint.")

            extracted = extract_csv_tracker_ids(postings)

            # GHA compatibility filters
            filter_day_str = gas_day.isoformat() if gas_day else None

            processed_count = 0
            skipped_count = 0

            for item in extracted:
                posting_cycle = item["cycle"]
                posting_day_str = item["gas_day"]
                posting_day = date.fromisoformat(posting_day_str)

                # Filter by gas day if specified
                if filter_day_str and posting_day_str != filter_day_str:
                    continue
                # Filter by cycle if specified
                if cycle and posting_cycle != cycle:
                    continue

                # Staleness gate
                out_path = _raw_path(posting_cycle, posting_day)
                if out_path.exists():
                    log.info("Skipping: already fetched %s %s", posting_cycle, posting_day_str)
                    skipped_count += 1
                    continue

                log.info(
                    "Fetching OAC CSV for %s %s (tracker: %s)",
                    posting_cycle,
                    posting_day_str,
                    item["tracker_id"],
                )
                csv_text = await fetch_oac_csv(client, item["tracker_id"])
                rows = parse_oac_csv(csv_text)

                payload = {
                    "fetched_at": datetime.now(UTC)
                    .replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "tsp_id": TSP_ID,
                    "cycle": posting_cycle,
                    "gas_day": posting_day_str,
                    "posted_at": item["posted_at"],
                    "row_count": len(rows),
                    "data": rows,
                }
                safe_write_json(out_path, payload)
                log.info("Written %d rows to %s", len(rows), out_path)
                processed_count += 1

            status = "ok"
            if processed_count == 0 and skipped_count > 0 or processed_count == 0:
                status = "skipped"

            health.record_success(
                metadata={
                    "processed_count": processed_count,
                    "skipped_count": skipped_count,
                    "filter_gas_day": filter_day_str,
                    "filter_cycle": cycle,
                }
            )
            return {
                "status": status,
                "processed_count": processed_count,
                "skipped_count": skipped_count,
            }

    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        log.error("Gulf South scraper failed: %s", err)
        health.record_failure(error=err)
        return {"status": "failed", "error": err}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    # Optional CLI: python -m scrapers.energy_transfer.gulf_south TIMELY 2026-05-16
    _cycle: CycleCode | None = None
    _gas_day: date | None = None

    if len(sys.argv) >= 2:
        arg = sys.argv[1].upper()
        if arg in ("TIMELY", "EVENING", "ID1", "ID2", "ID3"):
            _cycle = arg  # type: ignore[assignment]
        else:
            print(
                f"Unknown cycle '{sys.argv[1]}'. Use TIMELY|EVENING|ID1|ID2|ID3.", file=sys.stderr
            )
            sys.exit(1)

    if len(sys.argv) >= 3:
        try:
            _gas_day = date.fromisoformat(sys.argv[2])
        except ValueError:
            print(f"Invalid gas_day '{sys.argv[2]}'. Use YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    result = asyncio.run(run(_cycle, _gas_day))
    import json as _json

    print(_json.dumps(result, indent=2, default=str))
