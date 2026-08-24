"""Williams/Transco 1Line scraper package.

Pipeline coverage (public informational postings, zero-auth):
    TRANSCONTINENTAL GAS PIPE LINE COMPANY, LLC — TSP 007933021, BUID 80
    Largest US pipe (~10 Bcf/d design); feeds Sabine Pass (Zone 3),
    Golden Pass (Texas/Southern Gas lateral corridor), and Cove Point
    (Zone 6) LNG terminals plus the entire Northeast demand corridor.

Modules:
    client.py    — legacy JSP OAC form flow (session POST -> report HTML),
                   browser-TLS fallback via curl_cffi, TSQ-table parser
    backfill.py  — checkpointed multi-cycle history walk (3-year server cap)
    __init__.py  — ``run()`` daily-cycle orchestrator + CLI

Platform recon findings (confirmed 2026-08-24):
    * Platform is 1Line (www.1line.williams.com). A UI migration with B2C
      login pre-registration was announced July 2026 for CUSTOMER activities;
      Informational Postings remain public and unauthenticated.
    * The Operational Capacity page
      (``Transco/info-postings/capacity/Operationally-Available_NEW.html``)
      iframes ``ebbCode/OACQueryRequest.jsp?BUID=80&type=OAC`` — a plain JSP
      form (server-rendered, not XHR) that POSTs back to itself with
      ``tbGasFlowBeginDate``/``tbGasFlowEndDate`` (MM/DD/YYYY), ``cycle``
      radio values {1: Timely, 2: Evening, 3: ID1, 4: ID2, 8: ID3,
      5: Post, 7: Retro}, comma-separated ``locationIDs``, ``submitflag=true``,
      ``reportType=OAC``, ``MapID=0``, then opens ``ebbCode/OACreport.jsp``
      which renders the report from session state.
    * The report table embeds a Total Scheduled Quantity column — confirmed
      against the live-era Wayback captures of OACreport.jsp (header row:
      Loc | Loc Prop | Loc Purp Desc | Flow Ind | Loc Name | Design Capacity |
      Operating Capacity | Total Scheduled Quantity | Operationally Available
      Capacity | IT Indicator; label cells carry TSP:, Cycle Desc:, Effective
      Gas Day:, Posting Date:, Posting Time:, Meas Basis Desc:).
      Full-system reports show ~541 locations per cycle.
    * ``oacquery.js`` enforces a THREE-YEAR lookback floor and warns when an
      unrestricted all-location range exceeds the server record limit —
      bounded windows or explicit location lists are required.
    * Batch file exports (Master Location List, Unsubscribed) publish through
      ``portal-service/api/portal-service/v1/public/pdf-proxy`` pointing into
      the internal ``ECI`` share; OAC itself is generated per-query, so the
      JSP HTML table is the only stable ingestion surface.
    * www.1line.williams.com sits behind Microsoft Azure Application Gateway
      v2 which serves bare ``403 Forbidden`` (581-byte body) to disallowed
      egress — including python http stacks AND headless browsers. Datacenter
      IPs vary: GitHub Actions runners are expected to pass; this workstation
      is currently blocked end-to-end (verified 2026-08-24). The client
      treats 403-or-session-timeout as a transient WAF response, retries ONCE
      through curl_cffi ``chrome124`` sharing the cookie jar, then fails
      health (mirrors the GASNom pattern).

Raw payload contract (mirrors scrapers.gasnom so transformers stay uniform):
    {
      "fetched_at":  ISO-8601 UTC,
      "source_slug": "transco",
      "series_prefix": "transco",
      "tsp_name":    "TRANSCONTINENTAL GAS PIPE LINE COMPANY, LLC",
      "gas_day":     "2026-08-22",
      "cycle":       "id3",                  # series cycle code
      "cycle_desc":  "Intraday 3",           # verbatim Cycle Desc cell
      "posted_at":   "08/22/2026 09:31:03 PM",  # verbatim Posting Time cell
      "meas_basis":  "Vol" or "",
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
from scrapers.williams.client import TranscoClient, cycle_code_from_value
from scrapers.williams.config import (
    SOURCE_NAME,
    WATCHLIST,
)

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/williams")

#: Canonical snake_case row keys emitted into raw payloads (positional map of
#: the OACreport.jsp table; see client.parse_oac_table).
_ROW_KEYS: tuple[str, ...] = (
    "loc",
    "loc_prop",
    "loc_purp",
    "flow_ind",
    "loc_name",
    "design_cap",
    "operating_cap",
    "tsq",
    "oac",
    "it_indicator",
)


def _raw_path(raw_dir: Path, gas_day: date, cycle_code: str) -> Path:
    """Target raw JSON path for one gas day + cycle capture."""
    return raw_dir / f"transco_{gas_day.isoformat()}_{cycle_code}.json"


def _resolve_watchlist(
    rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[str]]:
    """Filter parsed rows down to the configured meter watchlist.

    Why:
        Transco publishes ~541 locations per cycle; emitting everything would
        balloon the curated parquet (TETCO lesson: wholesale emission across
        hourly cycles reached nine-figure row counts). Scope to LNG-terminal
        feedgas interconnects, storage fields, and basin-egress anchors.

    What:
        A row survives when its Loc ID appears in ``WATCHLIST`` ids OR its
        Loc Name matches one of the compiled case-insensitive patterns.
        Returns (kept_rows, matched_watchlist_labels) so callers can log which
        named candidates fired.

    Failure modes:
        None — unmatched rows are simply dropped; an entirely-empty result is
        a legitimate outcome (callers decide whether that is a no-op).
    """
    kept: list[dict[str, str]] = []
    matched: list[str] = []
    for row in rows:
        entry = WATCHLIST.match(row.get("loc", ""), row.get("loc_name", ""))
        if entry is None:
            continue
        enriched = dict(row)
        enriched["watchlist_label"] = entry.label
        enriched["watchlist_conf"] = entry.confidence
        kept.append(enriched)
        if entry.label not in matched:
            matched.append(entry.label)
    return kept, matched


def build_payload(
    *,
    gas_day: date,
    cycle_code: str,
    cycle_desc: str,
    posted_at: str,
    tsp_name: str,
    meas_basis: str,
    rows: list[dict[str, str]],
) -> dict[str, Any]:
    """Assemble the transformer-compatible raw payload."""
    return {
        "fetched_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_slug": SOURCE_NAME,
        "series_prefix": SOURCE_NAME,
        "tsp_name": tsp_name,
        "gas_day": gas_day.isoformat(),
        "cycle": cycle_code,
        "cycle_desc": cycle_desc,
        "posted_at": posted_at,
        "meas_basis": meas_basis,
        "row_count": len(rows),
        "data": rows,
    }


def run(  # noqa: PLR0912 — linear control flow mirrors gasnom.run
    cycle: str,
    gas_day: date | None = None,
    *,
    raw_dir: Path = RAW_DIR,
    client_cls: type[TranscoClient] = TranscoClient,
) -> dict[str, Any]:
    """Scrape one posted OAC cycle for Transco on *gas_day*.

    Why:
        Cron entry point (one invocation per NAESB cycle). Transco's JSP form
        accepts any requested cycle; a cycle that has not posted yet returns a
        report whose header carries a different cycle description, which is
        recorded as a skip rather than mislabelled data.

    What:
        Fetches the OAC report for the (gas day, cycle) pair, filters rows to
        the meter watchlist, applies the staleness gate (existing raw file →
        skip), writes ``data/raw/williams/transco_{day}_{cycle}.json``, and
        records source health. An unposted gas day is a legitimate
        ``empty`` outcome — never a crash (Port Arthur lesson).

    Failure modes:
        Gateway blocks persisting through the curl_cffi fallback raise
        internally, are recorded as health failures, and surface as
        ``status: "failed"``; the process exits non-zero only via the CLI.
    """
    target_day = gas_day or datetime.now(UTC).date()
    health = HealthWriter(source_name=SOURCE_NAME)

    try:
        cycle_code = cycle.strip().lower()
        with client_cls() as client:
            header, html_rows = client.fetch_oac(cycle_code, target_day)

        # Unposted gas day FIRST: a blank header parses as cycle 'unknown',
        # which would otherwise masquerade as a cycle mismatch.
        if not html_rows and not header.cycle_desc and not header.gas_day:
            health.record_no_op(
                reason=f"no posting for {target_day} cycle {cycle_code}",
                metadata={
                    "gas_day": target_day.isoformat(),
                    "cycle": cycle_code,
                    "outcome": "no_posting_for_gas_day",
                    "row_count": 0,
                },
            )
            return {"status": "empty", "gas_day": target_day.isoformat(), "rows": 0}

        latest_cycle_code = cycle_code_from_value(header.cycle_value)
        if latest_cycle_code != cycle_code:
            log.info(
                "transco %s: requested %s but report carries %s (%s) — skipping.",
                target_day, cycle_code, latest_cycle_code, header.cycle_desc,
            )
            health.record_skipped(
                f"report cycle {latest_cycle_code} != requested {cycle_code}"
            )
            return {
                "status": "skipped",
                "gas_day": target_day.isoformat(),
                "requested_cycle": cycle_code,
                "latest_cycle": latest_cycle_code,
            }

        kept, matched = _resolve_watchlist(html_rows)

        # Unposted gas day: header cells come back blank and no rows survive.
        if not html_rows and not kept:
            health.record_no_op(
                reason=f"no posting for {target_day} cycle {cycle_code}",
                metadata={
                    "gas_day": target_day.isoformat(),
                    "cycle": cycle_code,
                    "outcome": "no_posting_for_gas_day",
                    "row_count": 0,
                },
            )
            return {"status": "empty", "gas_day": target_day.isoformat(), "rows": 0}

        out_path = _raw_path(raw_dir, target_day, latest_cycle_code)
        if out_path.exists():
            health.record_skipped(f"raw file exists: {out_path.name}")
            return {"status": "skipped", "path": str(out_path)}

        payload = build_payload(
            gas_day=target_day,
            cycle_code=latest_cycle_code,
            cycle_desc=header.cycle_desc,
            posted_at=header.posting_time,
            tsp_name=header.tsp_name,
            meas_basis=header.meas_basis,
            rows=kept,
        )
        safe_write_json(out_path, payload)
        log.info(
            "transco %s %s: wrote %d/%d rows (%d watchlist labels) → %s",
            target_day, latest_cycle_code, len(kept), len(html_rows),
            len(matched), out_path,
        )

        health.record_success(metadata={
            "gas_day": target_day.isoformat(),
            "cycle": latest_cycle_code,
            "cycle_desc": header.cycle_desc,
            "posted_at": header.posting_time,
            "row_count": len(kept),
            "system_rows": len(html_rows),
            "watchlist_matched": matched,
        })
        return {
            "status": "ok",
            "gas_day": target_day.isoformat(),
            "cycle": latest_cycle_code,
            "rows": len(kept),
            "system_rows": len(html_rows),
            "path": str(out_path),
        }

    except Exception as exc:  # noqa: BLE001 — mirrored from gasnom.run
        err = f"{type(exc).__name__}: {exc}"
        log.error("Williams scraper failed for %s %s: %s", target_day, cycle, err)
        health.record_failure(
            error=err,
            metadata={"gas_day": target_day.isoformat(), "cycle": cycle},
        )
        return {"status": "failed", "error": err}


def main(argv: list[str] | None = None) -> int:
    """CLI: ``python -m scrapers.williams [--cycle C]... [--date D]``.

    With no --cycle arguments every schedulable cycle is attempted in turn.
    Returns 0 unless EVERY requested cycle failed (partial failure exits 0 so
    per-cycle scheduling can judge independently).
    """
    parser = argparse.ArgumentParser(prog="python -m scrapers.williams")
    parser.add_argument(
        "--cycle",
        action="append",
        default=None,
        help="Cycle to fetch (timely|evening|id1|id2|id3); repeatable",
    )
    parser.add_argument("--date", default=None, help="Gas day YYYY-MM-DD (default: today)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    gas_day = date.fromisoformat(args.date) if args.date else None
    cycles = args.cycle or ["timely", "evening", "id1", "id2", "id3"]
    results = [run(c, gas_day) for c in cycles]

    import json as _json

    for result in results:
        print(_json.dumps(result, default=str))
    failed = sum(1 for r in results if r["status"] == "failed")
    return 1 if failed == len(results) else 0


if __name__ == "__main__":
    sys.exit(main())
