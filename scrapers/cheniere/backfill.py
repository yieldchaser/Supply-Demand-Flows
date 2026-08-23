"""Historical backfill for Cheniere OAC (Creole Trail + Corpus Christi).

Why:
    ``GetCapacity`` accepts any historical ``beginDate`` but the server
    keeps a bounded window (observed floor: 2026-05-25 — older dates return
    an empty report). This module walks day by day from ``--since`` so every
    available gas day is captured once and checkpointed.

What:
    Iterates [since, today] oldest → newest, skipping days already present
    as raw files or in the curated parquet, rate-limited at one request per
    pipeline-pair tick. Progress is checkpointed daily for crash-safe
    resume; the run summary reports the effective data floor.

Failure modes:
    * Per-day HTTP faults are contained: logged, counted, walk continues.
    * A missing/malformed checkpoint or a ``--since`` mismatch starts fresh;
      ``--fresh`` deletes it outright.
    * Empty reports (pre-floor dates) are normal: recorded as empty days.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json
from scrapers.cheniere.client import (
    API_BASE_URL,
    TSP_CORPUS_CHRISTI,
    TSP_CREOLE_TRAIL,
    default_headers,
    fetch_capacity,
    parse_capacity_rows,
)

log = logging.getLogger(__name__)

CURATED_PATH = Path("data/curated/cheniere.parquet")
STATE_FILENAME = "_backfill_state.json"
DEFAULT_SINCE_DAYS = 120
REQUEST_GAP_SECONDS = 1.0


@dataclass
class BackfillConfig:
    """Knobs for one Cheniere backfill run.

    Why:
        Paths and pauses are injectable so tests can shrink sleeps and
        redirect I/O while production defaults stay polite to the API.

    What:
        ``since`` bounds the walk (the server's own floor cuts it off);
        ``request_gap_seconds`` spaces GetCapacity calls.
    """

    since: date
    raw_dir: Path = Path("data/raw/cheniere")
    curated_path: Path = CURATED_PATH
    request_gap_seconds: float = REQUEST_GAP_SECONDS

    @property
    def state_path(self) -> Path:
        """Checkpoint file location (inside the raw directory)."""
        return self.raw_dir / STATE_FILENAME


def load_curated_periods(curated_path: Path) -> set[str]:
    """Return gas days already present in the curated parquet.

    Failure modes:
        Missing parquet → empty set; unreadable parquet propagates.
    """
    if not curated_path.exists():
        log.warning("Curated parquet not found at %s — curated skip disabled.", curated_path)
        return set()
    frame = pd.read_parquet(curated_path, columns=["period"])
    return {str(p)[:10] for p in frame["period"].astype(str)}


def load_checkpoint(state_path: Path, since_iso: str) -> dict[str, Any] | None:
    """Load the checkpoint file if it exists and matches *since_iso*.

    Failure modes:
        Missing file, unreadable JSON, wrong type, or a ``since`` mismatch
        all return ``None`` so the caller starts a fresh walk.
    """
    if not state_path.exists():
        return None
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Ignoring unreadable checkpoint %s: %s", state_path, exc)
        return None
    if not isinstance(state, dict) or state.get("since") != since_iso:
        log.info(
            "Checkpoint at %s does not match --since=%s — starting fresh.",
            state_path,
            since_iso,
        )
        return None
    return state


class CheniereBackfill:
    """Stateful walker that backfills both pipelines day by day.

    Why:
        Carries cross-day state (checkpoint cursor, counters, skip sets).

    What:
        ``run()`` iterates [since, today] ascending, fetches each pipeline's
        report for each missing day, writes raw payloads, and checkpoints
        the next-day cursor after every day.

    Failure modes:
        Per-day HTTP faults contained; only client-construction faults
        propagate.
    """

    def __init__(self, *, config: BackfillConfig, client: HttpClient) -> None:
        self.config = config
        self.client = client

        self.days_seen = 0
        self.fetched = 0
        self.skipped_curated = 0
        self.skipped_raw_file = 0
        self.empty_days = 0
        self.errors_http = 0

        self._curated_periods: set[str] = set()

    # ------------------------------------------------------------------
    # Checkpoint handling
    # ------------------------------------------------------------------

    def _restore_checkpoint(self) -> date | None:
        """Seed counters from a matching checkpoint; returns resume cursor."""
        state = load_checkpoint(self.config.state_path, self.config.since.isoformat())
        if state is None:
            return None
        next_day_raw = str(state.get("next_day", ""))
        try:
            next_day = date.fromisoformat(next_day_raw)
        except ValueError:
            log.warning("Checkpoint has invalid next_day %r — starting fresh.", next_day_raw)
            return None
        self.fetched = int(state.get("fetched", self.fetched))
        self.errors_http = int(state.get("errors_http", self.errors_http))
        log.info("Resuming from checkpoint at %s.", next_day)
        return next_day

    def _write_checkpoint(self, next_day: date) -> None:
        """Persist run state atomically."""
        safe_write_json(
            self.config.state_path,
            {
                "since": self.config.since.isoformat(),
                "next_day": next_day.isoformat(),
                "fetched": self.fetched,
                "errors_http": self.errors_http,
                "updated_at": datetime.now(UTC)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
            },
        )

    # ------------------------------------------------------------------
    # Main walk
    # ------------------------------------------------------------------

    async def _fetch_day(self, target_day: date) -> int:
        """Fetch + write one day for both pipelines; returns rows written.

        What:
            Writes one raw file per pipeline unless it exists. Empty reports
            are written too (as ``row_count: 0`` payloads) so pre-floor dates
            are never re-fetched on resume.

        Failure modes:
            HTTP errors raise to the caller for containment.
        """
        written = 0
        for tsp_no in (TSP_CREOLE_TRAIL, TSP_CORPUS_CHRISTI):
            out_path = self.config.raw_dir / f"{target_day.isoformat()}_tsp{tsp_no}.json"
            if out_path.exists():
                continue
            payload = await fetch_capacity(self.client, tsp_no, target_day)
            rows = parse_capacity_rows(payload)
            body = {
                "fetched_at": datetime.now(UTC)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
                "source": "cheniere",
                "tsp_no": tsp_no,
                "gas_day": target_day.isoformat(),
                "cycle_id": None,
                "row_count": len(rows),
                "rows": rows,
            }
            safe_write_json(out_path, body)
            written += len(rows)
            await asyncio.sleep(self.config.request_gap_seconds)
        return written

    async def run(self, *, fresh: bool = False) -> dict[str, Any]:
        """Walk [since, today] and return the run summary.

        Why:
            Single entry point orchestrating the daily walk, skipping,
            fetching, checkpointing, and accounting.

        What:
            Starts at the restored checkpoint cursor when present; stops
            after today.

        Failure modes:
            Per-day faults are contained (logged, counted); nothing here
            aborts the whole walk short of task cancellation.
        """
        self._curated_periods = load_curated_periods(self.config.curated_path)
        if fresh and self.config.state_path.exists():
            self.config.state_path.unlink()
            log.info("--fresh set — deleted checkpoint %s", self.config.state_path)

        start = self._restore_checkpoint() or self.config.since
        today = date.today()
        current = start

        while current <= today:
            period = current.isoformat()
            self.days_seen += 1

            if period in self._curated_periods:
                self.skipped_curated += 1
            else:
                day_files_exist = all(
                    (self.config.raw_dir / f"{period}_tsp{tsp}.json").exists()
                    for tsp in (TSP_CREOLE_TRAIL, TSP_CORPUS_CHRISTI)
                )
                if day_files_exist:
                    self.skipped_raw_file += 1
                else:
                    try:
                        rows_written = await self._fetch_day(current)
                    except Exception as exc:
                        self.errors_http += 1
                        log.warning("Skipping %s after failure: %s: %s", period, type(exc).__name__, exc)
                        current += timedelta(days=1)
                        continue
                    if rows_written == 0:
                        self.empty_days += 1
                    else:
                        self.fetched += 1

            self._write_checkpoint(next_day=current + timedelta(days=1))
            current += timedelta(days=1)

        summary = self._summary()
        log.info("Backfill finished: %s", json.dumps(summary))
        return summary

    def _summary(self) -> dict[str, Any]:
        return {
            "days_seen": self.days_seen,
            "fetched": self.fetched,
            "skipped_curated": self.skipped_curated,
            "skipped_raw_file": self.skipped_raw_file,
            "empty_days": self.empty_days,
            "errors_http": self.errors_http,
        }


async def run_backfill(*, since: date, fresh: bool = False) -> dict[str, Any]:
    """Production entry point: build the HTTP client and execute the backfill.

    Failure modes:
        Propagates infrastructure faults to ``main``; per-day faults are
        contained upstream.
    """
    config = BackfillConfig(since=since)
    async with HttpClient(
        base_url=API_BASE_URL,
        default_headers=default_headers(),
        timeout_seconds=30.0,
        max_retries=3,
        backoff_base_seconds=1.0,
        retryable_status_codes=frozenset({403}),
    ) as client:
        runner = CheniereBackfill(config=config, client=client)
        return await runner.run(fresh=fresh)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scrapers.cheniere.backfill",
        description=(
            "Backfill historical Cheniere OAC raw files from the lngconnection "
            "GetCapacity endpoint."
        ),
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Oldest gas day to try, YYYY-MM-DD (default: 90 days ago today).",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore and delete any existing checkpoint before starting.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; returns the process exit code (0 unless catastrophic)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    args = _build_parser().parse_args(argv)
    if args.since is not None:
        try:
            since = date.fromisoformat(str(args.since))
        except ValueError:
            print(f"Invalid --since '{args.since}'. Use YYYY-MM-DD.", file=sys.stderr)
            return 1
    else:
        since = date.today() - timedelta(days=DEFAULT_SINCE_DAYS)
    try:
        summary = asyncio.run(run_backfill(since=since, fresh=bool(args.fresh)))
    except Exception as exc:
        log.error("Backfill aborted: %s: %s", type(exc).__name__, exc)
        return 1
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
