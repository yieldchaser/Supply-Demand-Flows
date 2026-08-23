"""GASNom / ESG-Latitude ColdFusion scraper package.

Pipeline coverage (all public, zero-auth, Imperva passthrough):
    Golden Pass Pipeline LLC        [goldenpass]           → terminal golden_pass_lng
    Cameron Interstate Pipeline LLC [cameron]              → terminal cameron_lng
    Sabine Pipe Line, LLC           [SABINE] (uppercase)   → terminal sabine_pass_lng
    Port Arthur Pipeline, LLC       [portarthurpipeline]   → terminal port_arthur_lng

Modules:
    pipelines.py — frozen per-pipeline config + slug registry
    client.py    — session priming, HTML OAC fetch, WAF-aware curl_cffi retry,
                   HTML/TSV parsers
    backfill.py  — 90-day bulk TSV walk (recovers ALL cycles per gas day)
    __init__.py  — ``run()`` daily-cycle orchestrator + CLI

Endpoints (confirmed 2026-08-23):
    GET  https://www.gasnom.com/ip/{SLUG}/oauc.cfm?dt=MM/DD/YYYY&type=1
         → HTML table of the latest posted cycle for that gas day.
    POST https://www.gasnom.com/ip/{SLUG}/OAC.cfm  (form: qry=1,
         frmEffectiveDt=MM/DD/YYYY, FRMENDDT=MM/DD/YYYY, B1=Download)
         → tab-delimited TSV covering a date range (90-day window max) with
         ALL cycles — CycleDesc + Posting_Date/Time included per row.
    GET  https://www.gasnom.com/ip/{SLUG}/frameindex.cfm primes the
         CFID/CFTOKEN/visid_incap_* session cookies; requests carry its Referer.

Raw payload contract (shared by live scrape and backfill so
``transformers.gasnom`` ingests either unchanged):
    {
      "fetched_at":  ISO-8601 UTC,
      "source_slug": "goldenpass",
      "series_prefix": "golden_pass",
      "tsp_name":    "Golden Pass Pipeline LLC",
      "gas_day":     "2026-08-22",
      "cycle":       "id3",                  # series cycle code
      "cycle_desc":  "Intraday 3",           # verbatim site description
      "posted_at":   "August 22, 2026 09:31 PM CT",
      "row_count":   N,
      "data": [ {canonical snake_case row}, ... ]
    }
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.safe_writer import safe_write_json
from scrapers.gasnom.client import GasnomClient, cycle_code_from_description
from scrapers.gasnom.pipelines import GASNOM_PIPELINES, GasnomPipeline

log = logging.getLogger(__name__)

SOURCE_NAME = "gasnom"
RAW_DIR = Path("data/raw/gasnom")

#: Canonical snake_case row keys emitted into raw payloads.
_ROW_KEYS: tuple[str, ...] = (
    "loc_name",
    "loc",
    "loc_zone",
    "loc_purp",
    "loc_qti",
    "flow_ind",
    "all_qty_avail",
    "design_cap",
    "operating_cap",
    "tsq",
    "oac",
    "it_indicator",
    "qty_reason",
)

#: HTML-view column names (see client.parse_html_oac) → canonical keys.
_HTML_KEY_MAP: dict[str, str] = {
    "Loc Name": "loc_name",
    "Loc": "loc",
    "Loc Zn": "loc_zone",
    "Loc Purp": "loc_purp",
    "Loc/QTI": "loc_qti",
    "Flow Ind": "flow_ind",
    "All Qty": "all_qty_avail",
    "Design Capacity": "design_cap",
    "Operating Capacity": "operating_cap",
    "TSQ": "tsq",
    "OAC": "oac",
    "IT Indicator": "it_indicator",
    "Qty Reason": "qty_reason",
}

#: Bulk-TSV column names (see client.parse_bulk_tsv) → canonical keys.
_TSV_KEY_MAP: dict[str, str] = {
    "Location_Name": "loc_name",
    "Location": "loc",
    "Location_Zone": "loc_zone",
    "Loc_Purp": "loc_purp",
    "Loc/QTI": "loc_qti",
    "Flow_Ind": "flow_ind",
    "All_Qty_Avail": "all_qty_avail",
    "Design_Capacity": "design_cap",
    "Operational_Capacity": "operating_cap",
    "TSQ": "tsq",
    "OAC": "oac",
    "IT": "it_indicator",
    "Measurement_Basis": "measurement_basis",
    "Pressure_Base": "pressure_base",
}


def _raw_path(raw_dir: Path, slug: str, gas_day: date, cycle_code: str) -> Path:
    """Target raw JSON path for one slug + gas day + cycle capture."""
    return raw_dir / f"{slug}_{gas_day.isoformat()}_{cycle_code}.json"


def _normalize_html_rows(
    rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Map parsed HTML-table rows onto the canonical snake_case keys."""
    out: list[dict[str, str]] = []
    for row in rows:
        normalized = {
            canonical: (row.get(html_key, "") or "").strip()
            for html_key, canonical in _HTML_KEY_MAP.items()
        }
        if normalized["loc"]:
            out.append(normalized)
    return out


def _normalize_tsv_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Map parsed bulk-TSV rows onto the canonical snake_case keys."""
    out: list[dict[str, str]] = []
    for row in rows:
        normalized = {
            canonical: (row.get(tsv_key, "") or "").strip()
            for tsv_key, canonical in _TSV_KEY_MAP.items()
        }
        if normalized["loc"]:
            out.append(normalized)
    return out


def build_payload(
    *,
    pipeline: GasnomPipeline,
    gas_day: date,
    cycle_code: str,
    cycle_desc: str,
    posted_at: str,
    tsp_name: str,
    rows: list[dict[str, str]],
) -> dict[str, Any]:
    """Assemble the transformer-compatible raw payload."""
    return {
        "fetched_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_slug": pipeline.slug,
        "series_prefix": pipeline.series_prefix,
        "terminal": pipeline.terminal,
        "tsp_name": tsp_name or pipeline.name,
        "gas_day": gas_day.isoformat(),
        "cycle": cycle_code,
        "cycle_desc": cycle_desc,
        "posted_at": posted_at,
        "row_count": len(rows),
        "data": rows,
    }


def run(  # noqa: PLR0912 — linear control flow mirrors gulf_south.run
    slug: str,
    cycle: str | None = None,
    gas_day: date | None = None,
    *,
    raw_dir: Path = RAW_DIR,
    client_cls: type[GasnomClient] = GasnomClient,
) -> dict[str, Any]:
    """Scrape the latest posted OAC cycle for *slug* on *gas_day*.

    Why:
        Cron entry point (one invocation per slug per NAESB cycle).  The
        ColdFusion template exposes only the LATEST posted cycle per gas day,
        so a requested *cycle* that doesn't match the returned one is recorded
        as a skip rather than mislabelled data — missed windows are recovered
        by the bulk TSV backfill instead.

    What:
        Primes the session, fetches the HTML OAC table, normalizes rows to the
        canonical payload schema, applies the staleness gate (existing raw
        file → skip), writes ``data/raw/gasnom/{slug}_{day}_{cycle}.json``, and
        records per-slug health.  An unposted gas day (e.g. Port Arthur before
        its postings began) is a legitimate ``empty`` outcome — never a crash.

    Failure modes:
        WAF challenges persisting through the curl_cffi fallback raise
        ``GasnomWafError`` internally, are recorded as a health failure, and
        surface as ``status: "failed"`` — the process exits non-zero only via
        the CLI wrapper.
    """
    pipeline = GASNOM_PIPELINES[slug]
    target_day = gas_day or datetime.now(UTC).date()
    health = HealthWriter(source_name=f"{SOURCE_NAME}_{slug}")

    try:
        with client_cls(pipeline) as client:
            header, html_rows = client.fetch_oac_html(target_day)
        rows = _normalize_html_rows(html_rows)

        latest_cycle_code = cycle_code_from_description(header.cycle_desc)
        latest_cycle_desc = header.cycle_desc

        # Unposted gas day: header cells come back blank.
        if not latest_cycle_desc and not rows:
            health.record_success(metadata={
                "slug": slug,
                "gas_day": target_day.isoformat(),
                "outcome": "no_posting_for_gas_day",
                "row_count": 0,
            })
            return {"status": "empty", "slug": slug, "gas_day": target_day.isoformat(), "rows": 0}

        if cycle and latest_cycle_code != cycle.strip().lower():
            log.info(
                "%s %s: latest posted cycle is %s (%s), requested %s — skipping.",
                slug, target_day, latest_cycle_code, latest_cycle_desc, cycle,
            )
            health.record_skipped(
                f"latest posted cycle {latest_cycle_code} != requested {cycle}"
            )
            return {
                "status": "skipped",
                "slug": slug,
                "gas_day": target_day.isoformat(),
                "latest_cycle": latest_cycle_code,
                "requested_cycle": cycle,
            }

        out_path = _raw_path(raw_dir, slug, target_day, latest_cycle_code)
        if out_path.exists():
            health.record_skipped(f"raw file exists: {out_path.name}")
            return {"status": "skipped", "slug": slug, "path": str(out_path)}

        payload = build_payload(
            pipeline=pipeline,
            gas_day=target_day,
            cycle_code=latest_cycle_code,
            cycle_desc=latest_cycle_desc,
            posted_at=header.posting_time,
            tsp_name=header.tsp_name,
            rows=rows,
        )
        safe_write_json(out_path, payload)
        log.info("%s: wrote %d rows → %s", slug, len(rows), out_path)

        health.record_success(metadata={
            "slug": slug,
            "gas_day": target_day.isoformat(),
            "cycle": latest_cycle_code,
            "cycle_desc": latest_cycle_desc,
            "posted_at": header.posting_time,
            "row_count": len(rows),
        })
        return {
            "status": "ok",
            "slug": slug,
            "gas_day": target_day.isoformat(),
            "cycle": latest_cycle_code,
            "rows": len(rows),
            "path": str(out_path),
        }

    except Exception as exc:  # noqa: BLE001 — mirrored from gulf_south.run
        err = f"{type(exc).__name__}: {exc}"
        log.error("GASNom scraper failed for %s: %s", slug, err)
        health.record_failure(error=err, metadata={"slug": slug, "gas_day": target_day.isoformat()})
        return {"status": "failed", "slug": slug, "error": err}


def main(argv: list[str] | None = None) -> int:
    """CLI: ``python -m scrapers.gasnom [SLUG ...] [--cycle C] [--date D]``.

    With no SLUG arguments, every registered pipeline is scraped in turn.
    Returns 0 unless every requested slug failed (partial failure exits 0 so
    the per-slug matrix workflow can judge each slug independently).
    """
    parser = argparse.ArgumentParser(prog="python -m scrapers.gasnom")
    parser.add_argument("slugs", nargs="*", help="Pipeline slugs (default: all)")
    parser.add_argument("--cycle", default=None, help="Expected cycle (timely|evening|id1|id2|id3)")
    parser.add_argument("--date", default=None, help="Gas day YYYY-MM-DD (default: today)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    gas_day = date.fromisoformat(args.date) if args.date else None
    targets = args.slugs or list(GASNOM_PIPELINES)
    results = [
        run(slug, args.cycle, gas_day)
        for slug in targets
    ]

    import json as _json

    for result in results:
        print(_json.dumps(result, default=str))
    failed = sum(1 for r in results if r["status"] == "failed")
    return 1 if failed == len(results) else 0


if __name__ == "__main__":
    sys.exit(main())
