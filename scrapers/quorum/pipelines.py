"""Quorum myQuorumCloud IPWS — Operational Availability posting scraper.

Covers Venture Global's two LNG lateral pipelines on the shared
``web-prd.myquorumcloud.com`` tenant (VGPPB1IPWS):

    * Gator Express   (Plaquemines LNG feedgas)  — TspNo=2
    * TransCameron    (Calcasieu Pass LNG feedgas) — TspNo=10

The endpoint is a stateless, tokenless GET per (gas day, TSP):

    GET {tenant}/OpAvailPosting/ExportToCSV
        ?CycleId=&GasDay=YYYY-MM-DD&LocId=&TspNo={N}

Empty ``CycleId``/``LocId`` return every cycle x every location for that gas
day (five NAESB cycles per day: Timely, Evening, Intraday 1/2/3).

Tenant-generic design:
    The same ExportToCSV pattern serves other Quorum IPWS tenants (BBTPA1IPWS,
    HPEPA1IPWS, PNGPA1IPWS families), so the scraper is parameterised by
    ``tenant`` + ``tsp_no`` rather than hard-coding Venture Global.

Failure modes:
    * The dead tenant VGPPA1IPWS serves HTTP 503 — it must never be used.
    * Gas days before a TSP's first posting return HTTP 200 with a header-only
      CSV; the scraper treats those as empty days, not errors.
    * Transient WAF 403s and 429s are retried with literal 5s/15s/45s backoff
      via the shared HttpClient, matching the gulf_south scraper's policy.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.http_client import HttpClient
from scrapers.base.identity import assert_response_identity
from scrapers.base.safe_writer import safe_write_json

log = logging.getLogger(__name__)

SOURCE_NAME = "quorum"

#: Shared Quorum myQuorumCloud host (no scheme-specific quirks observed).
BASE_URL = "https://web-prd.myquorumcloud.com"

#: Live Venture Global tenant. VGPPA1IPWS is DEAD (HTTP 503) — never use it.
TENANT_VGPP = "VGPPB1IPWS"

EXPORT_PATH = "/OpAvailPosting/ExportToCSV"

RAW_DIR = Path("data/raw/quorum")

#: Venture Global TSP numbers within the VGPPB1IPWS tenant.
TSP_GATOR_EXPRESS = 2  # Plaquemines LNG feedgas lateral
TSP_TRANSCAMERON = 10  # Calcasieu Pass LNG feedgas lateral

#: Literal backoff for transient WAF 403s / rate-limit 429s (matches gulf_south).
BACKOFF_DELAYS_SECONDS: tuple[float, ...] = (5.0, 15.0, 45.0)

#: CSV column that is present but unnamed in the live export (sits between
#: "Quantity Not Available Reason" and "Design Capacity"). csv.DictReader
#: maps it to ``None``; we re-key it to a stable placeholder.
_UNNAMED_COLUMN_KEY = "_unnamed_blank"

#: Cycle description → canonical cycle code (lowercase, series-id safe).
_CYCLE_CODES: dict[str, str] = {
    "TIMELY": "timely",
    "EVENING": "evening",
    "INTRADAY 1": "id1",
    "INTRADAY 2": "id2",
    "INTRADAY 3": "id3",
}


@dataclass(frozen=True)
class QuorumTenant:
    """One pipeline posting on a Quorum IPWS tenant."""

    tenant: str
    tsp_no: int
    prefix: str  # series-id prefix, e.g. "gator_express"


GATOR_EXPRESS = QuorumTenant(TENANT_VGPP, TSP_GATOR_EXPRESS, "gator_express")
TRANSCAMERON = QuorumTenant(TENANT_VGPP, TSP_TRANSCAMERON, "trans_cameron")

#: All Venture Global pipelines on the shared tenant (backfill/CI order).
QUORUM_TENANTS: tuple[QuorumTenant, ...] = (GATOR_EXPRESS, TRANSCAMERON)


def normalize_cycle(cycle_desc: str) -> str:
    """Map a Cycle Desc value to a canonical lowercase cycle code.

    What:
        "Intraday 3" → "id3", "Evening" → "evening", etc. Unknown values are
        slugified (lowercase, spaces → underscores) so novel cycle names still
        produce stable series ids instead of crashing the transformer.

    Failure modes:
        None — unknown descriptions degrade to a slugified passthrough.
    """
    code = _CYCLE_CODES.get(cycle_desc.strip().upper())
    if code:
        return code
    return cycle_desc.strip().lower().replace(" ", "_")


def build_export_url(tenant: str, tsp_no: int, gas_day: date) -> str:
    """Build the stateless ExportToCSV URL for one gas day + TSP.

    Why:
        The endpoint takes empty CycleId/LocId to mean "all cycles, all
        locations"; only GasDay and TspNo vary.
    """
    return (
        f"{BASE_URL}/{tenant}{EXPORT_PATH}"
        f"?CycleId=&GasDay={gas_day.isoformat()}&LocId=&TspNo={tsp_no}"
    )


def parse_export_csv(csv_text: str) -> list[dict[str, str]]:
    """Parse one ExportToCSV payload into clean row dictionaries.

    Why:
        The live export contains a header-present but unnamed blank column
        between "Quantity Not Available Reason" and "Design Capacity".
        ``csv.DictReader`` surfaces that as a ``None`` key; leaving it as
        ``None`` breaks downstream dict handling, so it is re-keyed to a
        stable placeholder.

    What:
        Decodes via ``io.StringIO`` (caller handles BOM/encoding) and strips
        whitespace from keys and values. Two quirks are normalised:

        * The live export's unnamed blank column arrives as an empty-string
          header key; it is re-keyed to ``_unnamed_blank`` so downstream
          dict handling sees a stable name.
        * Fully-empty separator rows (no Loc) are dropped.

    Failure modes:
        ``ValueError`` if any row carries more fields than the header — that
        means Quorum changed the schema and silent mis-alignment would follow,
        so it fails loudly instead.
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    if reader.fieldnames is None:
        return []

    field_count = len(reader.fieldnames)
    rows: list[dict[str, str]] = []
    for raw_row in reader:
        extras = raw_row.get(None)
        if extras:
            raise ValueError(
                f"ExportToCSV row has {field_count + len(extras)} fields "
                f"vs {field_count} header columns — Quorum schema changed?"
            )
        clean: dict[str, str] = {}
        for key, value in raw_row.items():
            if key is None:
                continue  # already handled via the extras guard above
            col = key.strip()
            if not col:
                col = _UNNAMED_COLUMN_KEY
            clean[col] = (value or "").strip()
        if not clean.get("Loc"):
            continue
        rows.append(clean)
    return rows


class QuorumIPWSScraper:
    """Scrape Operational Availability ExportToCSV postings for one TSP.

    Why:
        Venture Global's two LNG laterals publish nominations on a Quorum
        myQuorumCloud IPWS tenant that no free aggregator covers.

    What:
        ``run()`` fetches the ExportToCSV for the target gas day (all cycles,
        all locations), parses it, and writes one raw JSON payload per cycle
        under ``data/raw/quorum/`` using the same payload shape as the
        gulf_south scraper, so transformers stay symmetric.

    Failure modes:
        HTTP failures raise ``HttpClientError`` after retries; the run is
        recorded as failed in health and ``status: failed`` is returned.
        Header-only CSVs (pre-first-posting gas days) yield ``status: skipped``.
    """

    def __init__(
        self,
        tenant: str,
        tsp_no: int,
        *,
        prefix: str | None = None,
        raw_dir: Path = RAW_DIR,
    ) -> None:
        self.tenant = tenant
        self.tsp_no = tsp_no
        self.prefix = prefix or f"tsp{tsp_no}"
        self.raw_dir = raw_dir

    def _raw_path(self, gas_day: date, cycle_code: str) -> Path:
        """Return the raw payload path for one gas day + cycle.

        What:
            Files land under ``data/raw/quorum/{prefix}/`` so multiple
            pipelines on the shared tenant never collide on
            ``{gas_day}_{cycle}.json`` filenames.
        """
        return self.raw_dir / self.prefix / f"{gas_day.isoformat()}_{cycle_code}.json"

    async def fetch_day(self, client: HttpClient, gas_day: date) -> str:
        """Fetch the ExportToCSV text for one gas day (all cycles/locations)."""
        url = build_export_url(self.tenant, self.tsp_no, gas_day)
        raw_bytes = await client.get_bytes(url)
        # Live endpoint serves plain UTF-8 (no BOM observed); utf-8-sig strips
        # a BOM if Quorum ever adds one.
        text = raw_bytes.decode("utf-8-sig")
        # Tenant-fallback guard: the IPWS portal serves the REQUESTED tenant's
        # export for known TspNo values but can silently fall back for others.
        # The CSV carries a 'TSP Name' column — verify it matches the tenant's
        # pipeline family (Gator Express / TransCameron, both Venture Global).
        assert_response_identity(
            expected="Venture",
            response_text=text,
            context=f"quorum/{self.prefix} (TspNo={self.tsp_no})",
        )
        return text

    async def run(
        self,
        gas_day: date | None = None,
        *,
        client: HttpClient | None = None,
    ) -> dict[str, Any]:
        """Scrape all cycles for *gas_day* (default: today UTC) and write raw files.

        Why:
            Orchestrator entry point, mirroring gulf_south.run().

        What:
            One GET per gas day; rows are grouped by canonical cycle code and
            written as ``{gas_day}_{cycle}.json`` raw payloads. Existing files
            are skipped (staleness gate). Returns per-cycle counts.

        Failure modes:
            ``status: failed`` with the error string on HTTP/parse faults;
            ``status: skipped`` when the day carries no postings (or all
            cycle files already exist).
        """
        target_day = gas_day or datetime.now(UTC).date()
        health = HealthWriter(source_name=SOURCE_NAME)

        try:
            if client is not None:
                return await self._run_with_client(client, target_day, health)
            async with HttpClient(
                base_url=BASE_URL,
                timeout_seconds=30.0,
                max_retries=3,
                backoff_base_seconds=1.0,
                rate_limit_per_second=1.0,
                retryable_status_codes=frozenset({403, 429}),
                backoff_delays=BACKOFF_DELAYS_SECONDS,
            ) as owned_client:
                return await self._run_with_client(owned_client, target_day, health)
        except Exception as exc:
            err = f"{type(exc).__name__}: {exc}"
            log.error("Quorum IPWS scraper failed (%s tsp=%d): %s", self.tenant, self.tsp_no, err)
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

    async def _run_with_client(
        self,
        client: HttpClient,
        target_day: date,
        health: HealthWriter,
    ) -> dict[str, Any]:
        """Execute the fetch-parse-write cycle with an existing client."""

        csv_text = await self.fetch_day(client, target_day)
        rows = parse_export_csv(csv_text)

        if not rows:
            log.info(
                "No postings for %s tsp=%d on %s (header-only CSV).",
                self.tenant,
                self.tsp_no,
                target_day,
            )
            health.record_no_op(
                reason=f"no postings for {target_day} (header-only CSV)",
                metadata={"gas_day": target_day.isoformat(), "rows": 0},
            )
            return {"status": "skipped", "processed_count": 0, "cycles": {}}

        by_cycle: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            cycle_code = normalize_cycle(row.get("Cycle Desc", ""))
            by_cycle.setdefault(cycle_code, []).append(row)

        fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        processed = 0
        skipped = 0
        per_cycle_counts: dict[str, int] = {}

        for cycle_code, cycle_rows in sorted(by_cycle.items()):
            out_path = self._raw_path(target_day, cycle_code)
            if out_path.exists():
                log.info("Skipping: already fetched %s %s", cycle_code, target_day)
                skipped += 1
                per_cycle_counts[cycle_code] = 0
                continue

            payload = {
                "fetched_at": fetched_at,
                "source": SOURCE_NAME,
                "tenant": self.tenant,
                "tsp_no": self.tsp_no,
                "prefix": self.prefix,
                "cycle": cycle_code,
                "gas_day": target_day.isoformat(),
                "row_count": len(cycle_rows),
                "data": cycle_rows,
            }
            safe_write_json(out_path, payload)
            log.info("Written %d rows to %s", len(cycle_rows), out_path)
            per_cycle_counts[cycle_code] = len(cycle_rows)
            processed += 1

        health.record_success(
            metadata={
                "gas_day": target_day.isoformat(),
                "processed_count": processed,
                "skipped_count": skipped,
                "rows": len(rows),
                "cycles": sorted(per_cycle_counts),
            }
        )
        return {
            "status": "ok",
            "processed_count": processed,
            "skipped_count": skipped,
            "rows": len(rows),
            "cycles": per_cycle_counts,
        }


async def run(
    tenant: str = TENANT_VGPP,
    tsp_no: int = TSP_GATOR_EXPRESS,
    gas_day: date | None = None,
    *,
    prefix: str | None = None,
) -> dict[str, Any]:
    """Module-level convenience entry (mirrors gulf_south.run signature style)."""
    scraper = QuorumIPWSScraper(tenant, tsp_no, prefix=prefix)
    return await scraper.run(gas_day)


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    # CLI: python -m scrapers.quorum.pipelines [gator|transcameron] [YYYY-MM-DD]
    _which = sys.argv[1].lower() if len(sys.argv) >= 2 else "gator"
    _day: date | None = None
    if len(sys.argv) >= 3:
        _day = date.fromisoformat(sys.argv[2])

    if _which in ("gator", "gator_express", "2"):
        _t = GATOR_EXPRESS
    elif _which in ("transcameron", "trans_cameron", "10"):
        _t = TRANSCAMERON
    else:
        print(f"Unknown pipeline '{sys.argv[1]}'. Use gator|transcameron.", file=sys.stderr)
        sys.exit(1)

    _result = asyncio.run(run(_t.tenant, _t.tsp_no, _day, prefix=_t.prefix))
    import json as _json

    print(_json.dumps(_result, indent=2, default=str))
