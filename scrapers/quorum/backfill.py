"""Historical backfill for Quorum IPWS Operational Availability postings.

Why:
    Retention on the ExportToCSV endpoint is unbounded today but the tenant
    is production infrastructure — capture history now while it exists. The
    walk goes backwards one gas day at a time; each GET returns all cycles x
    locations for that day (header-only CSV = day not yet posted = floor).

What:
    For each configured pipeline (Gator Express TspNo=2, TransCameron
    TspNo=10), fetches the ExportToCSV, parses rows, groups them by canonical
    cycle code, and writes the same raw payloads as the live scraper so
    ``transformers.quorum`` ingests them unchanged. Progress is checkpointed
    to ``data/raw/quorum/_backfill_state.json`` every N days per pipeline for
    crash-safe resume; days already represented on disk or in the curated
    parquet are skipped before any download.

Failure modes:
    * HTTP 403/429 are retried with literal 5s/15s/45s backoff (WAF quirks);
      a persistent failure logs a warning and continues with the next day.
    * Header-only CSVs mark the probed floor and end that pipeline's walk.
    * A malformed checkpoint is ignored (fresh walk); ``--fresh`` deletes it.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.base.errors import HttpClientError
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json
from scrapers.quorum.pipelines import (
    BACKOFF_DELAYS_SECONDS,
    BASE_URL,
    GATOR_EXPRESS,
    QUORUM_TENANTS,
    TRANSCAMERON,
    QuorumIPWSScraper,
    QuorumTenant,
    normalize_cycle,
    parse_export_csv,
)

log = logging.getLogger(__name__)

CURATED_PATH = Path("data/curated/quorum.parquet")
STATE_FILENAME = "_backfill_state.json"
CHECKPOINT_EVERY_DAYS = 20
REQUEST_GAP_SECONDS = 1.0

#: Conservative default window when --since is not given.
DEFAULT_SINCE_DAYS = 400


@dataclass
class BackfillConfig:
    """Knobs for one backfill run (injectable for tests)."""

    since: date
    raw_dir: Path = Path("data/raw/quorum")
    curated_path: Path = CURATED_PATH
    checkpoint_every: int = CHECKPOINT_EVERY_DAYS
    request_gap_seconds: float = REQUEST_GAP_SECONDS
    retry_delays: tuple[float, ...] = BACKOFF_DELAYS_SECONDS
    pipelines: tuple[QuorumTenant, ...] = (GATOR_EXPRESS, TRANSCAMERON)
    state_path: Path | None = None

    def __post_init__(self) -> None:
        if self.state_path is None:
            self.state_path = self.raw_dir / STATE_FILENAME


def load_curated_keys(curated_path: Path) -> set[tuple[str, str]]:
    """Derive (prefix, gas_day) pairs already present in the curated parquet.

    Why:
        Skipping curated days avoids re-downloading hundreds of CSVs whose
        data the transformer has already merged into history.
    """
    if not curated_path.exists():
        log.warning("Curated parquet not found at %s — curated skip disabled.", curated_path)
        return set()
    frame = pd.read_parquet(curated_path, columns=["series_id", "period"])
    # Split on the literal "_sq_" / "_oac_" markers rather than the first
    # underscore — prefixes like "trans_cameron" contain underscores too.
    prefixes = frame["series_id"].astype(str).str.replace(r"_(sq|oac)_.*", "", regex=True)
    periods = frame["period"].astype(str).str.slice(0, 10)
    return {(str(prefix), str(period)) for prefix, period in zip(prefixes, periods, strict=True)}


def load_checkpoint(state_path: Path) -> dict[str, Any]:
    """Load the checkpoint file, tolerating missing/corrupt state."""
    if not state_path.exists():
        return {}
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Ignoring unreadable checkpoint %s: %s", state_path, exc)
        return {}
    return state if isinstance(state, dict) else {}


def raw_files_for_day(raw_dir: Path, day: date) -> int:
    """Count existing cycle files for one gas day (staleness gate helper)."""
    return sum(
        1
        for c in ("timely", "evening", "id1", "id2", "id3")
        if (raw_dir / f"{day.isoformat()}_{c}.json").exists()
    )


class QuorumBackfill:
    """Stateful backwards walker over ExportToCSV gas days.

    Failure modes:
        Per-day faults are contained (logged + counted); only client-level
        infrastructure faults propagate to the caller.
    """

    def __init__(self, *, config: BackfillConfig, client: HttpClient) -> None:
        self.config = config
        self.client = client

        # Per-pipeline counters keyed by series prefix.
        self.fetched: dict[str, int] = {}
        self.errors: dict[str, int] = {}
        self.floors: dict[str, str] = {}
        self.stop_reason: dict[str, str] = {}
        #: Resume cursors: next day each pipeline should process (inclusive).
        self.next_days: dict[str, str] = {}

        self.days_since_checkpoint: dict[str, int] = {}
        self._curated_keys: set[tuple[str, str]] = set()
        self._last_request_monotonic: float | None = None

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def persist_state(self, *, completed: bool) -> None:
        """Write the checkpoint atomically."""
        assert self.config.state_path is not None
        payload = {
            "updated_at": datetime.now(UTC)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "completed": completed,
            "floors": self.floors,
            "next_days": self.next_days,
            "fetched": self.fetched,
            "errors": self.errors,
        }
        safe_write_json(self.config.state_path, payload)

    def _mark(self, prefix: str, *, fetched_delta: int = 0, error: bool = False) -> None:
        """Advance counters and checkpoint on cadence."""
        if error:
            self.errors[prefix] = self.errors.get(prefix, 0) + 1
        else:
            self.fetched[prefix] = self.fetched.get(prefix, 0) + fetched_delta
        self.days_since_checkpoint[prefix] = self.days_since_checkpoint.get(prefix, 0) + 1
        if self.days_since_checkpoint[prefix] >= self.config.checkpoint_every:
            self.persist_state(completed=False)
            self.days_since_checkpoint[prefix] = 0

    # ------------------------------------------------------------------
    # Download plumbing
    # ------------------------------------------------------------------

    async def _throttle(self) -> None:
        """Enforce the minimum gap between consecutive requests."""
        if self._last_request_monotonic is not None:
            elapsed = time.monotonic() - self._last_request_monotonic
            remaining = self.config.request_gap_seconds - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
        self._last_request_monotonic = time.monotonic()

    async def _fetch_day_rows(self, pipeline: QuorumTenant, day: date) -> list[dict[str, str]]:
        """Fetch+parse one gas day, retrying transient 403/429 with literal backoff.

        Failure modes:
            Raises the final ``HttpClientError`` once retries are exhausted;
            callers decide containment. Header-only days return [].
        """
        scraper = QuorumIPWSScraper(pipeline.tenant, pipeline.tsp_no, prefix=pipeline.prefix)
        delays = (0.0, *self.config.retry_delays)
        last_error: HttpClientError | None = None
        for attempt, delay in enumerate(delays, start=1):
            if delay:
                await asyncio.sleep(delay)
            await self._throttle()
            try:
                csv_text = await scraper.fetch_day(self.client, day)
                return parse_export_csv(csv_text)
            except HttpClientError as exc:
                if exc.status not in (403, 429):
                    raise
                last_error = exc
                log.warning(
                    "HTTP %d for %s %s (attempt %d/%d)",
                    exc.status,
                    pipeline.prefix,
                    day,
                    attempt,
                    len(delays),
                )
        assert last_error is not None
        raise last_error

    async def _write_cycle_files(
        self, pipeline: QuorumTenant, day: date, rows: list[dict[str, str]]
    ) -> int:
        """Group rows by cycle and write raw payloads; returns files written."""
        by_cycle: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            cycle_code = normalize_cycle(row.get("Cycle Desc", ""))
            by_cycle.setdefault(cycle_code, []).append(row)

        fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        written = 0
        raw_dir = self.config.raw_dir / pipeline.prefix
        for cycle_code, cycle_rows in sorted(by_cycle.items()):
            out_path = raw_dir / f"{day.isoformat()}_{cycle_code}.json"
            if out_path.exists():
                continue
            payload = {
                "fetched_at": fetched_at,
                "source": "quorum",
                "tenant": pipeline.tenant,
                "tsp_no": pipeline.tsp_no,
                "prefix": pipeline.prefix,
                "cycle": cycle_code,
                "gas_day": day.isoformat(),
                "row_count": len(cycle_rows),
                "data": cycle_rows,
            }
            safe_write_json(out_path, payload)
            written += 1
        return written

    # ------------------------------------------------------------------
    # Walk
    # ------------------------------------------------------------------

    async def _walk_pipeline(self, pipeline: QuorumTenant, start_day: date) -> None:
        """Walk one pipeline backwards from *start_day* to its floor/--since."""
        prefix = pipeline.prefix
        raw_dir = self.config.raw_dir / prefix
        stop_day = self.config.since
        day = start_day

        while day >= stop_day:
            # Staleness gate: decide BEFORE any download.
            have = raw_files_for_day(raw_dir, day)
            fully_captured = have == 5 or (prefix, day.isoformat()) in self._curated_keys
            if fully_captured:
                log.info("[%s] %s already captured — skipping.", prefix, day)
            else:
                try:
                    rows = await self._fetch_day_rows(pipeline, day)
                except HttpClientError as exc:
                    log.warning(
                        "[%s] %s failed after retries (%s) — continuing.",
                        prefix,
                        day,
                        exc.reason,
                    )
                    self._mark(prefix, error=True)
                else:
                    if not rows:
                        log.info("[%s] %s header-only — data floor reached.", prefix, day)
                        self.floors[prefix] = (
                            min(self.floors.get(prefix, day.isoformat()), day.isoformat())
                            if prefix in self.floors
                            else day.isoformat()
                        )
                        self.stop_reason[prefix] = f"floor {day.isoformat()} (empty day)"
                        self.next_days[prefix] = day.isoformat()
                        return
                    written = await self._write_cycle_files(pipeline, day, rows)
                    log.info("[%s] %s → %d file(s), %d rows.", prefix, day, written, len(rows))
                    self._mark(prefix, fetched_delta=written)

            # Advance the cursor and checkpoint before moving on.
            day -= timedelta(days=1)
            self.next_days[prefix] = day.isoformat()

        self.stop_reason[prefix] = f"reached --since {stop_day.isoformat()}"

    async def run(self, *, fresh: bool = False) -> dict[str, Any]:
        """Run the full backfill across all pipelines; returns the summary.

        What:
            Restores resume cursors from the checkpoint (unless ``fresh``),
            walks each pipeline backwards, and writes a final completed
            checkpoint.
        """
        assert self.config.state_path is not None
        if fresh and self.config.state_path.exists():
            self.config.state_path.unlink()
            log.info("--fresh set — deleted checkpoint %s", self.config.state_path)

        state = load_checkpoint(self.config.state_path)
        saved_floors: dict[str, str] = state.get("floors", {})
        self.floors.update(saved_floors)
        saved_next: dict[str, str] = state.get("next_days", {})
        self._curated_keys = load_curated_keys(self.config.curated_path)

        summary: dict[str, Any] = {"pipelines": {}}
        for pipeline in self.config.pipelines:
            prefix = pipeline.prefix
            resume_raw = saved_next.get(prefix)
            start_day: date | None = date.fromisoformat(resume_raw) if resume_raw else None
            if start_day is None or start_day > datetime.now(UTC).date():
                start_day = datetime.now(UTC).date()
            elif start_day < self.config.since:
                log.info("[%s] checkpoint already beyond --since — nothing to do.", prefix)
                summary["pipelines"][prefix] = {"status": "already-complete"}
                continue

            await self._walk_pipeline(pipeline, start_day)
            summary["pipelines"][prefix] = {
                "status": "done",
                "stop_reason": self.stop_reason.get(prefix, ""),
                "floor": self.floors.get(prefix),
                "files_fetched_this_run": self.fetched.get(prefix, 0),
                "errors": self.errors.get(prefix, 0),
            }

        self.persist_state(completed=True)
        return summary


async def main_async(days: int, fresh: bool, pipelines: tuple[QuorumTenant, ...]) -> dict[str, Any]:
    """CLI entry: build config/client and run the walk."""
    config = BackfillConfig(
        since=datetime.now(UTC).date() - timedelta(days=days),
        pipelines=pipelines,
    )
    async with HttpClient(
        base_url=BASE_URL,
        timeout_seconds=30.0,
        max_retries=3,
        backoff_base_seconds=1.0,
        rate_limit_per_second=1.0 / config.request_gap_seconds,
        retryable_status_codes=frozenset({403, 429}),
        backoff_delays=config.retry_delays,
    ) as client:
        backfill = QuorumBackfill(config=config, client=client)
        return await backfill.run(fresh=fresh)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    parser = argparse.ArgumentParser(description="Backfill Quorum IPWS postings.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_SINCE_DAYS,
        help="Walk back this many days from today (default: %(default)s).",
    )
    parser.add_argument("--fresh", action="store_true", help="Delete the checkpoint first.")
    parser.add_argument(
        "--pipeline",
        choices=["gator", "transcameron"],
        action="append",
        help="Restrict to one pipeline (repeatable; default: both).",
    )
    args = parser.parse_args()

    by_name = {"gator": GATOR_EXPRESS, "transcameron": TRANSCAMERON}
    if args.pipeline:
        selected = tuple(dict.fromkeys(by_name[name] for name in args.pipeline))
    else:
        selected = QUORUM_TENANTS

    result = asyncio.run(main_async(args.days, args.fresh, selected))
    print(json.dumps(result, indent=2))
