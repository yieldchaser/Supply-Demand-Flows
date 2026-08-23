"""Historical backfill for GASNom pipelines via the bulk OAC.cfm TSV endpoint.

Why:
    The HTML ``oauc.cfm`` view exposes only the LATEST posted cycle per gas
    day, and raw captures are gitignored while CI runners are ephemeral —
    without the bulk export, every cycle except the last one of each day is
    permanently unrecoverable (the site's retention is a rolling 90 days).

What:
    For each slug, POSTs the ``transposting.cfm?id=1`` form to ``OAC.cfm``
    with a date window and parses the tab-delimited response.  Unlike the
    HTML view, the TSV carries ``CycleDesc`` + ``Posting_Date/Time`` per row,
    so one download recovers EVERY cycle for EVERY gas day in the window.
    Rows are grouped by (gas day, cycle) into payloads identical in shape to
    the live scraper's and written under ``data/raw/gasnom/`` so
    ``python -m transformers.gasnom`` ingests backfilled files unchanged.
    Existing raw files are skipped before any network call; runs are
    checkpointed per slug for crash-safe resume.

Failure modes:
    * A WAF challenge persisting through the curl_cffi fallback aborts that
      slug's walk (checkpoint keeps prior progress) but not the others'.
    * An empty TSV (e.g. Port Arthur before 2026-06-08) yields no files and
      is reported as zero rows — never an error.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.safe_writer import safe_write_json
from scrapers.gasnom.client import (
    GasnomClient,
    GasnomWafError,
    cycle_code_from_description,
    parse_bulk_tsv,
)
from scrapers.gasnom.pipelines import GASNOM_PIPELINES, GasnomPipeline

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/gasnom")
STATE_FILENAME = "_backfill_state.json"
MAX_WINDOW_DAYS = 89  # inclusive span; site enforces a rolling 90-day cap
DEFAULT_SINCE_DAYS = 90
DOWNLOAD_GAP_SECONDS = 2.0


def _default_since() -> date:
    return date.today() - timedelta(days=DEFAULT_SINCE_DAYS - 1)


def _chunked_window(start: date, end: date) -> list[tuple[date, date]]:
    """Split [start, end] into <=90-day inclusive chunks (site limit)."""
    chunks: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=MAX_WINDOW_DAYS), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def load_checkpoint(state_path: Path) -> dict[str, Any]:
    """Load the checkpoint file if readable; otherwise a fresh-state dict."""
    if state_path.exists():
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(state, dict):
                state.setdefault("processed_files", [])
                return state
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Ignoring unreadable checkpoint %s: %s", state_path, exc)
    return {"processed_files": []}


class GasnomBackfill:
    """Bulk-TSV walker recovering multi-cycle history for GASNom slugs.

    Why:
        One class over the frozen pipeline registry; per-slug state is small
        (a processed-file set) but must survive crashes via checkpoints.

    What:
        ``run_slug`` downloads the bulk TSV in ≤90-day chunks, groups rows by
        (gas day, cycle), skips already-captured files, writes raw payloads,
        and returns accounting used by the final report.

    Failure modes:
        Per-chunk WAF/network failures are logged and contained; only the
        affected slug loses the remaining chunks of its window.
    """

    def __init__(
        self,
        *,
        raw_dir: Path = RAW_DIR,
        download_gap_seconds: float = DOWNLOAD_GAP_SECONDS,
        client_cls: type[GasnomClient] = GasnomClient,
    ) -> None:
        self.raw_dir = raw_dir
        self.download_gap_seconds = download_gap_seconds
        self.client_cls = client_cls

    # ------------------------------------------------------------------

    def run_slug(
        self,
        pipeline: GasnomPipeline,
        since: date,
        until: date,
    ) -> dict[str, Any]:
        """Backfill one slug from *since* through *until* (inclusive)."""
        health = HealthWriter(source_name=f"gasnom_{pipeline.slug}")
        state_path = self.raw_dir / STATE_FILENAME
        state = load_checkpoint(state_path)
        processed: set[str] = set(state["processed_files"])

        fetched = 0
        skipped_existing = 0
        errors: list[str] = []
        oldest: str | None = None
        newest: str | None = None
        cycles_seen: set[str] = set()
        rows_total = 0

        try:
            with self.client_cls(pipeline) as client:
                for chunk_start, chunk_end in _chunked_window(since, until):
                    log.info(
                        "%s: bulk TSV %s → %s",
                        pipeline.slug, chunk_start, chunk_end,
                    )
                    tsv_text = client.fetch_bulk_tsv(chunk_start, chunk_end)
                    rows = parse_bulk_tsv(tsv_text)

                    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
                    for row in rows:
                        gas_day_str = self._tsv_date_to_iso(
                            str(row.get("Eff_Gas_Day/Time", ""))[:10]
                        )
                        cycle_code = cycle_code_from_description(str(row.get("CycleDesc", "")))
                        if gas_day_str:
                            grouped[(gas_day_str, cycle_code)].append(row)

                    for (gas_day_str, cycle_code), group_rows in sorted(grouped.items()):
                        out_path = (
                            self.raw_dir
                            / f"{pipeline.slug}_{gas_day_str}_{cycle_code}.json"
                        )
                        key = out_path.name
                        if key in processed or out_path.exists():
                            skipped_existing += 1
                            continue

                        posting_times = sorted({
                            str(r.get("Posting_Date/Time", ""))
                            for r in group_rows
                            if r.get("Posting_Date/Time")
                        })
                        payload = {
                            "fetched_at": datetime.now(UTC)
                            .replace(microsecond=0)
                            .isoformat()
                            .replace("+00:00", "Z"),
                            "source_slug": pipeline.slug,
                            "series_prefix": pipeline.series_prefix,
                            "terminal": pipeline.terminal,
                            "tsp_name": str(group_rows[0].get("TSP Name", "")) or pipeline.name,
                            "gas_day": gas_day_str,
                            "cycle": cycle_code,
                            "cycle_desc": str(group_rows[0].get("CycleDesc", "")),
                            "posted_at": posting_times[-1] if posting_times else "",
                            "row_count": len(group_rows),
                            "data": [
                                {
                                    "loc_name": str(r.get("Location_Name", "")).strip(),
                                    "loc": str(r.get("Location", "")).strip(),
                                    "loc_zone": str(r.get("Location_Zone", "")).strip(),
                                    "loc_purp": str(r.get("Loc_Purp", "")).strip(),
                                    "loc_qti": str(r.get("Loc/QTI", "")).strip(),
                                    "flow_ind": str(r.get("Flow_Ind", "")).strip(),
                                    "all_qty_avail": str(r.get("All_Qty_Avail", "")).strip(),
                                    "design_cap": str(r.get("Design_Capacity", "")).strip(),
                                    "operating_cap": str(r.get("Operational_Capacity", "")).strip(),
                                    "tsq": str(r.get("TSQ", "")).strip(),
                                    "oac": str(r.get("OAC", "")).strip(),
                                    "it_indicator": str(r.get("IT", "")).strip(),
                                    "measurement_basis": str(r.get("Measurement_Basis", "")).strip(),
                                    "pressure_base": str(r.get("Pressure_Base", "")).strip(),
                                }
                                for r in group_rows
                            ],
                        }
                        safe_write_json(out_path, payload)
                        processed.add(key)
                        state["processed_files"] = sorted(processed)
                        safe_write_json(state_path, state)

                        fetched += 1
                        rows_total += len(group_rows)
                        cycles_seen.add(cycle_code)
                        oldest = gas_day_str if oldest is None else min(oldest, gas_day_str)
                        newest = gas_day_str if newest is None else max(newest, gas_day_str)

                    if chunk_end < until:
                        time.sleep(self.download_gap_seconds)

        except GasnomWafError as exc:
            errors.append(f"{pipeline.slug}: WAF challenge persisted: {exc}")
            health.record_failure(error=str(exc), metadata={"slug": pipeline.slug})
        except Exception as exc:  # noqa: BLE001 — containment mirrors gulf_south backfill
            errors.append(f"{pipeline.slug}: {type(exc).__name__}: {exc}")
            health.record_failure(error=str(exc), metadata={"slug": pipeline.slug})
        else:
            health.record_success(metadata={
                "slug": pipeline.slug,
                "fetched_files": fetched,
                "skipped_existing": skipped_existing,
                "range": [oldest, newest],
            })

        return {
            "slug": pipeline.slug,
            "fetched_files": fetched,
            "skipped_existing": skipped_existing,
            "rows_total": rows_total,
            "oldest_gas_day": oldest,
            "newest_gas_day": newest,
            "cycles": sorted(cycles_seen),
            "errors": errors,
        }

    @staticmethod
    def _tsv_date_to_iso(raw: str) -> str:
        """Convert TSV 'MM/DD/YYYY' to ISO 'YYYY-MM-DD' ('' when absent).

        Failure modes:
            Unparseable input returns '' and the group is dropped rather than
            written with a corrupt gas day.
        """
        parts = raw.strip().split("/")
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            month, day, year = parts
            return f"{year}-{month}-{day}"
        return ""

    # ------------------------------------------------------------------

    def run_all(self, since: date, until: date) -> list[dict[str, Any]]:
        """Backfill every registered pipeline sequentially."""
        results: list[dict[str, Any]] = []
        for pipeline in GASNOM_PIPELINES.values():
            results.append(self.run_slug(pipeline, since, until))
        return results


def main(argv: list[str] | None = None) -> int:
    """CLI: ``python -m scrapers.gasnom.backfill [--since D] [--until D] [--slug S]``."""
    parser = argparse.ArgumentParser(prog="python -m scrapers.gasnom.backfill")
    parser.add_argument("--since", default=None, help="Start gas day YYYY-MM-DD (default: today-89d)")
    parser.add_argument("--until", default=None, help="End gas day YYYY-MM-DD (default: today)")
    parser.add_argument("--slug", action="append", default=[], help="Restrict to slug(s); repeatable")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    until = date.fromisoformat(args.until) if args.until else date.today()
    since = date.fromisoformat(args.since) if args.since else _default_since()

    pipelines = [
        GASNOM_PIPELINES[s]
        for s in (args.slug or list(GASNOM_PIPELINES))
    ]

    backfill = GasnomBackfill()
    results = []
    for pipeline in pipelines:
        result = backfill.run_slug(pipeline, since, until)
        results.append(result)
        log.info(
            "%s: fetched=%d skipped=%d rows=%d range=%s..%s cycles=%s errors=%s",
            result["slug"], result["fetched_files"], result["skipped_existing"],
            result["rows_total"], result["oldest_gas_day"], result["newest_gas_day"],
            result["cycles"], result["errors"] or "none",
        )

    print(json.dumps(results, indent=2, default=str))
    return 1 if any(r["errors"] for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
