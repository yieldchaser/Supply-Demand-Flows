"""GIE AGSI+ European Natural Gas Storage Scraper.

Why:
    GIE (Gas Infrastructure Europe) publishes daily storage levels for EU
    member-state facilities via the AGSI+ API. Adding this source gives Blue
    Tide a peer-of-EIA-storage perspective — EU/US storage divergence is the
    primary driver of transatlantic LNG flow economics.

What:
    Fetches 5 years of daily storage data for 11 countries (EU aggregate +
    top-10 consumers: DE, FR, IT, NL, ES, AT, BE, CZ, HU, PL) and writes a
    single flat JSON file to data/raw/gie_agsi/european_storage.json.

Failure modes:
    - Missing data for a date → data: [] (HTTP 200, not 404). Empty array is
      not an error — some countries report sparsely on gas-day boundaries.
    - Invalid country code → HTTP 400. We use ISO 3166-1 Alpha-2 so this
      shouldn't happen; logged at ERROR and the country is skipped.
    - Bad API key → HTTP 401. Propagated as RuntimeError, run() returns failed.
    - Rate limit (60 req/min) → HTTP 429 with Retry-After header. We sleep
      Retry-After seconds and retry once. Between countries we sleep 1 s.
    - Partial success (some countries failed) is valid. Health records which
      countries succeeded / failed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import UTC, datetime, date
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json

log = logging.getLogger(__name__)

SOURCE_NAME = "gie_agsi"
BASE_URL = "https://agsi.gie.eu"
RAW_PATH = Path("data/raw/gie_agsi/european_storage.json")

# 5 years of history — AGSI has data back to 2011 but we mirror the EIA
# storage envelope window.
START_DATE = "2021-01-01"

# Top-10 EU gas consumers + EU aggregate. ISO 3166-1 Alpha-2.
COUNTRIES = ["EU", "DE", "FR", "IT", "NL", "ES", "AT", "BE", "CZ", "HU", "PL"]

# Maximum rows per API response page. AGSI caps at 300.
PAGE_SIZE = 300


def _load_api_key() -> str:
    """Read GIE_API_KEY from environment.

    Failure modes:
        RuntimeError if the variable is absent or empty — caller must handle.
    """
    key = os.environ.get("GIE_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "GIE_API_KEY environment variable is not set. "
            "Set it in .env or as a GitHub Actions secret."
        )
    return key


def _read_prior_state() -> tuple[str | None, int]:
    """Return (latest_gas_day, row_count) from the previously written file.

    Returns (None, 0) when no prior file exists or it can't be parsed.
    """
    if not RAW_PATH.exists():
        return None, 0
    try:
        payload: dict[str, Any] = json.loads(RAW_PATH.read_text(encoding="utf-8"))
        return payload.get("latest_gas_day"), len(payload.get("data", []))
    except Exception:
        return None, 0


async def _fetch_country_pages(
    client: HttpClient,
    country: str,
    start: str,
    end: str,
) -> list[dict[str, Any]]:
    """Fetch all pages for one country between *start* and *end* (inclusive).

    Why:
        AGSI returns at most PAGE_SIZE rows per request. 5 years × 365 days
        per country = ~1,825 rows, requiring at least 7 pages per country.

    What:
        Loops page=1, 2, … until current_page >= last_page.
        Extracts and flattens the "data" arrays.

    Failure modes:
        - HTTP 429 is handled inside HttpClient's retry logic (it treats 429
          as retryable). We additionally enforce a 1-second inter-country gap
          at the call-site.
        - Any non-retryable error raises HttpClientError — callers catch it and
          record the country as failed.
    """
    all_rows: list[dict[str, Any]] = []
    page = 1

    while True:
        params: dict[str, str] = {
            "country": country,
            "from": start,
            "to": end,
            "size": str(PAGE_SIZE),
            "page": str(page),
        }
        raw = await client.get_json("/api", params=params)
        if not isinstance(raw, dict):
            log.warning("Unexpected response type for %s page %d: %s", country, page, type(raw))
            break

        rows: list[dict[str, Any]] = raw.get("data", [])
        all_rows.extend(rows)

        current = int(raw.get("current_page", page))
        last = int(raw.get("last_page", page))
        log.debug("%s: page %d/%d — %d rows this page", country, current, last, len(rows))

        if current >= last:
            break
        page += 1

    return all_rows


def _normalise_row(raw_row: dict[str, Any]) -> dict[str, Any]:
    """Convert one raw API row into the canonical output shape.

    Fields that may be None (e.g. consumption, consumptionFull, info) are
    excluded — only the four fields we use are emitted.
    """

    def _float(v: object) -> float | None:
        try:
            return float(v) if v is not None else None  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    return {
        "country_code": raw_row.get("code") or raw_row.get("name"),
        "gas_day": raw_row.get("gasDayStart"),
        "gas_in_storage_twh": _float(raw_row.get("gasInStorage")),
        "injection_gwh": _float(raw_row.get("injection")),
        "withdrawal_gwh": _float(raw_row.get("withdrawal")),
        "working_gas_volume_twh": _float(raw_row.get("workingGasVolume")),
        "full_pct": _float(raw_row.get("full")),
        "trend_twh": _float(raw_row.get("trend")),
        "status": raw_row.get("status"),
        "injection_capacity_gwh": _float(raw_row.get("injectionCapacity")),
        "withdrawal_capacity_gwh": _float(raw_row.get("withdrawalCapacity")),
    }


async def run() -> dict[str, Any]:
    """Fetch GIE AGSI+ European storage data and write to raw JSON.

    Why:
        Public entrypoint invoked by the GitHub Actions workflow and by
        ``__main__``. Mirrors the ``run()`` signature used by EIA scrapers.

    What:
        1. Reads the API key from the environment.
        2. Loads prior state (latest_gas_day, row_count) for the staleness gate.
        3. For each country, fetches all pages and normalises rows.
        4. Applies staleness gate: skip if latest_gas_day unchanged AND row
           count within 10 of prior.
        5. Writes the merged output atomically via safe_write_json.
        6. Records health status.

    Returns:
        dict with status ("ok" | "skipped" | "failed"), and on success:
        latest_gas_day, row_count, countries_ok, countries_failed.
    """
    health = HealthWriter(source_name=SOURCE_NAME)

    try:
        api_key = _load_api_key()
    except RuntimeError as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}

    prior_gas_day, prior_row_count = _read_prior_state()

    today = date.today().isoformat()
    fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    all_data: list[dict[str, Any]] = []
    countries_ok: list[str] = []
    countries_failed: list[str] = []

    async with HttpClient(
        base_url=BASE_URL,
        default_headers={"x-key": api_key},
        timeout_seconds=45.0,
        max_retries=3,
        backoff_base_seconds=2.0,
        # AGSI rate limit is 60 req/min. With pagination we can hit several
        # requests per country. We enforce a 1-second inter-country sleep
        # at the call-site; the client's internal rate limit is kept loose.
    ) as client:
        for i, country in enumerate(COUNTRIES):
            if i > 0:
                # 1-second polite gap between countries.
                await asyncio.sleep(1.0)

            log.info("Fetching %s (%d/%d) …", country, i + 1, len(COUNTRIES))
            try:
                raw_rows = await _fetch_country_pages(
                    client=client,
                    country=country,
                    start=START_DATE,
                    end=today,
                )
            except Exception as exc:
                log.error("Failed to fetch %s: %s", country, exc)
                countries_failed.append(country)
                continue

            normalised = [_normalise_row(r) for r in raw_rows]
            all_data.extend(normalised)
            countries_ok.append(country)
            latest_for_country = max(
                (r["gas_day"] for r in normalised if r.get("gas_day")),
                default="N/A",
            )
            log.info(
                "%s: %d rows, latest gas_day=%s",
                country,
                len(normalised),
                latest_for_country,
            )

    if not all_data:
        err = "No data fetched for any country"
        log.error(err)
        health.record_failure(
            error=err,
            metadata={"countries_failed": countries_failed},
        )
        return {"status": "failed", "error": err}

    latest_gas_day = max(
        (r["gas_day"] for r in all_data if r.get("gas_day")),
        default=None,
    )
    new_row_count = len(all_data)

    # Staleness gate: skip if latest_gas_day is unchanged AND row count is
    # within 10 of the prior run (allows for minor retroactive revisions).
    if (
        prior_gas_day is not None
        and latest_gas_day == prior_gas_day
        and abs(new_row_count - prior_row_count) <= 10
    ):
        reason = f"no new data since {latest_gas_day} (rows: {new_row_count})"
        log.info("Skipping: %s", reason)
        health.record_skipped(reason=reason)
        return {"status": "skipped", "latest_gas_day": latest_gas_day}

    payload: dict[str, Any] = {
        "fetched_at": fetched_at,
        "start_date": START_DATE,
        "end_date": today,
        "latest_gas_day": latest_gas_day,
        "countries": COUNTRIES,
        "row_count": new_row_count,
        "data": all_data,
    }

    safe_write_json(RAW_PATH, payload)
    log.info(
        "Written %d rows to %s (latest_gas_day=%s)",
        new_row_count,
        RAW_PATH,
        latest_gas_day,
    )

    health.record_success(
        metadata={
            "latest_gas_day": latest_gas_day,
            "row_count": new_row_count,
            "countries_ok": countries_ok,
            "countries_failed": countries_failed,
        }
    )

    return {
        "status": "ok",
        "latest_gas_day": latest_gas_day,
        "row_count": new_row_count,
        "countries_ok": countries_ok,
        "countries_failed": countries_failed,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    result = asyncio.run(run())
    print(json.dumps(result, indent=2))
