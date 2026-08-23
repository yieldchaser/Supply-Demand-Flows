"""Historical backfill for Gulf South Pipeline SQ postings (BWP GasQuest).

Why:
    An overwrite bug in the live scraper clobbered historical raw captures
    under ``data/raw/gulf_south/``, destroying curated history that cannot be
    recovered from the current snapshot alone.  This module walks the
    reporting API backwards in time to rebuild what was lost.

What:
    Pages the ``/infopost/infopostdetails`` listing (sorted descending by
    ``datetimePostingEffective``, so paging forward goes back in time) until a
    posting older than ``--since`` appears.  Each posting carrying a
    "CSV Documents" attachment is downloaded (base64 OAC CSV), parsed, and
    written as a raw payload identical in shape to the live scraper's, so
    ``python -m transformers.gulf_south`` ingests backfilled files unchanged.
    Postings already represented in ``data/curated/gulf_south.parquet`` or
    with an existing raw file are skipped *before* any download; progress is
    checkpointed every N postings for crash-safe resume.

Failure modes:
    * Transient WAF 403s are retried (5s/15s/45s); a persistent 403 is
      logged, followed by a 120s cooldown, and the walk continues with the
      next posting rather than aborting.
    * Other HTTP failures (404 etc.) log a warning and skip the posting.
    * A missing, malformed, or since-mismatched checkpoint is ignored and a
      fresh walk starts at page 1; ``--fresh`` deletes it outright.
    * A server that keeps repeating already-seen documents trips a
      defensive no-progress guard and ends the walk.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.base.errors import HttpClientError
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json
from scrapers.energy_transfer.gulf_south import (
    INFOPOST_PATH,
    POSTINGS_PATH,
    RAW_DIR,
    REPORTING_BASE_URL,
    TSP_ID,
    extract_csv_tracker_ids,
    fetch_oac_csv,
    parse_oac_csv,
)

log = logging.getLogger(__name__)

CURATED_PATH = Path("data/curated/gulf_south.parquet")
STATE_FILENAME = "_backfill_state.json"
PAGE_SIZE = 100
DEFAULT_SINCE_DAYS = 180
CHECKPOINT_EVERY = 20
DOWNLOAD_GAP_SECONDS = 1.5
CSV_RETRY_DELAYS_SECONDS: tuple[float, ...] = (5.0, 15.0, 45.0)
PERSISTENT_403_COOLDOWN_SECONDS = 120.0


@dataclass
class BackfillConfig:
    """Knobs for one backfill run.

    Why:
        Paths and pause durations are injectable so tests can shrink sleeps
        and redirect I/O to temp directories while production defaults stay
        polite to the WAF-fronted endpoint.

    What:
        ``since`` bounds the backwards walk; ``csv_retry_delays`` are the
        sleeps between successive 403 retries on a single CSV download;
        ``persistent_403_cooldown_seconds`` is the long pause applied once a
        posting exhausts those retries; ``download_gap_seconds`` enforces the
        minimum spacing between any two CSV downloads.
    """

    since: date
    raw_dir: Path = RAW_DIR
    curated_path: Path = CURATED_PATH
    page_size: int = PAGE_SIZE
    checkpoint_every: int = CHECKPOINT_EVERY
    download_gap_seconds: float = DOWNLOAD_GAP_SECONDS
    csv_retry_delays: tuple[float, ...] = CSV_RETRY_DELAYS_SECONDS
    persistent_403_cooldown_seconds: float = PERSISTENT_403_COOLDOWN_SECONDS

    @property
    def state_path(self) -> Path:
        """Checkpoint file location (inside the raw directory)."""
        return self.raw_dir / STATE_FILENAME


def load_curated_keys(curated_path: Path) -> set[tuple[str, str]]:
    """Derive (gas_day, cycle) pairs already present in the curated parquet.

    Why:
        Skipping curated pairs avoids re-downloading hundreds of CSVs whose
        data the transformer has already merged into history.

    What:
        Reads only the series_id/period columns; the cycle is the final
        underscore token of each series id, uppercased (e.g.
        ``gulf_south_sq_24329_id1`` → ``(period, "ID1")``).

    Failure modes:
        Missing parquet → empty set (the raw-file skip still applies);
        unreadable parquet → exception propagates to the caller.
    """
    if not curated_path.exists():
        log.warning("Curated parquet not found at %s — curated skip disabled.", curated_path)
        return set()
    frame = pd.read_parquet(curated_path, columns=["series_id", "period"])
    cycles = frame["series_id"].astype(str).str.rsplit("_", n=1).str[-1].str.upper()
    periods = frame["period"].astype(str).str.slice(0, 10)
    return {(str(period), str(cycle)) for period, cycle in zip(periods, cycles, strict=True)}


def _read_int(source: dict[str, Any], key: str, default: int) -> int:
    value = source.get(key, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


class GulfSouthBackfill:
    """Stateful walker that backfills Gulf South raw files page by page.

    Why:
        The walk carries cross-posting state (checkpoint, counters, skip
        sets) that pure functions would force through awkward signatures.

    What:
        ``run()`` pages the posting list backwards, resolves each posting via
        skip checks or a guarded CSV download, writes transformer-compatible
        raw payloads, and maintains ``_backfill_state.json`` for resume.

    Failure modes:
        Individual posting failures (persistent 403, other HTTP errors) are
        contained: logged, counted, and the walk continues. Only infrastructure
        faults on the listing endpoint itself propagate to the caller.
    """

    def __init__(self, *, config: BackfillConfig, client: HttpClient) -> None:
        self.config = config
        self.client = client

        self.pages_paged = 0
        self.postings_seen = 0
        self.fetched = 0
        self.skipped_curated = 0
        self.skipped_raw_file = 0
        self.skipped_no_csv = 0
        self.errors_403 = 0
        self.retries_403 = 0
        self.errors_decode = 0
        self.oldest_gas_day: str | None = None
        self.newest_gas_day: str | None = None

        self.processed_trackers: set[int] = set()
        self._curated_keys: set[tuple[str, str]] = set()
        self._seen_doc_ids: set[int] = set()
        self._next_page = 1
        self._since_iso = ""
        self._since_checkpoint = 0
        self._carried_skipped_existing = 0
        self._last_download_monotonic: float | None = None

    # ------------------------------------------------------------------
    # Checkpoint handling
    # ------------------------------------------------------------------

    def _restore_checkpoint(self) -> None:
        """Seed counters and cursors from a matching checkpoint, if any."""
        state = load_checkpoint(self.config.state_path, self._since_iso)
        if state is None:
            return
        self._next_page = max(1, _read_int(state, "next_page", 1))
        raw_trackers = state.get("processed_trackers", [])
        if isinstance(raw_trackers, list):
            for value in raw_trackers:
                try:
                    self.processed_trackers.add(int(value))
                except (TypeError, ValueError):
                    continue
        self._seen_doc_ids = set(self.processed_trackers)
        self.fetched = _read_int(state, "fetched", self.fetched)
        self.errors_403 = _read_int(state, "errors_403", self.errors_403)
        self.errors_decode = _read_int(state, "errors_decode", self.errors_decode)
        self._carried_skipped_existing = _read_int(state, "skipped_existing", 0)
        log.info(
            "Resuming from checkpoint: next_page=%d, %d tracker(s) already processed.",
            self._next_page,
            len(self.processed_trackers),
        )

    def _write_checkpoint(self) -> None:
        """Persist run state atomically and reset the cadence counter."""
        state = {
            "since": self._since_iso,
            "next_page": self._next_page,
            "processed_trackers": sorted(self.processed_trackers),
            "skipped_existing": (
                self.skipped_curated + self.skipped_raw_file + self._carried_skipped_existing
            ),
            "fetched": self.fetched,
            "errors_403": self.errors_403,
            "errors_decode": self.errors_decode,
            "updated_at": datetime.now(UTC)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        safe_write_json(self.config.state_path, state)
        self._since_checkpoint = 0
        log.debug(
            "Checkpoint written: next_page=%d, trackers=%d",
            self._next_page,
            len(self.processed_trackers),
        )

    def _mark_processed(self, tracker_id: int) -> None:
        self.processed_trackers.add(tracker_id)
        self._since_checkpoint += 1

    # ------------------------------------------------------------------
    # Download plumbing
    # ------------------------------------------------------------------

    async def _throttle(self) -> None:
        """Enforce the minimum gap between consecutive CSV downloads."""
        if self._last_download_monotonic is not None:
            elapsed = time.monotonic() - self._last_download_monotonic
            remaining = self.config.download_gap_seconds - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
        self._last_download_monotonic = time.monotonic()

    async def _download_csv(self, tracker_id: int) -> str:
        """Download and decode one OAC CSV with WAF-aware 403 retries.

        Why:
            The endpoint's WAF intermittently serves transient 403s that the
            shared HttpClient deliberately does not retry (non-retryable 4xx).

        What:
            Attempts the download, sleeping ``csv_retry_delays`` between
            successive 403 responses. Every 403 is logged with tracker id and
            URL. Non-403 HttpClientErrors propagate untouched.

        Failure modes:
            Raises the final ``HttpClientError`` (status preserved) once the
            delay schedule is exhausted; callers decide containment.
        """
        url = f"{REPORTING_BASE_URL}{POSTINGS_PATH}?postingsDocumentId={tracker_id}"
        delays = (0.0, *self.config.csv_retry_delays)
        last_error: HttpClientError | None = None
        for attempt, delay in enumerate(delays, start=1):
            if delay:
                await asyncio.sleep(delay)
            try:
                return await fetch_oac_csv(self.client, tracker_id)
            except HttpClientError as exc:
                if exc.status != 403:
                    raise
                last_error = exc
                log.warning(
                    "HTTP 403 for tracker %d at %s (attempt %d/%d)",
                    tracker_id,
                    url,
                    attempt,
                    len(delays),
                )
                if attempt < len(delays):
                    self.retries_403 += 1
        assert last_error is not None
        raise last_error

    async def _fetch_postings_page(self, page_number: int) -> list[dict[str, Any]]:
        """Query one page of the posting list (descending effective datetime)."""
        body: dict[str, object] = {
            "infoPostID": 1,
            "tspId": TSP_ID,
            "pageNumber": page_number,
            "pageSize": self.config.page_size,
            "sortBy": "datetimePostingEffective",
            "groupCode": "INFOPOST",
            "sortDescending": True,
        }
        res = await self.client.post_json(INFOPOST_PATH, body)
        if isinstance(res, dict):
            postings = res.get("postings")
            if isinstance(postings, list):
                return postings
        return []

    # ------------------------------------------------------------------
    # Per-posting resolution
    # ------------------------------------------------------------------

    async def _resolve_item(self, item: dict[str, Any]) -> None:
        """Skip or download-and-write one posting; never raises on HTTP faults."""
        tracker_id = int(item["tracker_id"])
        cycle = str(item["cycle"])
        gas_day_str = str(item["gas_day"])
        out_path = self.config.raw_dir / f"{gas_day_str}_{cycle}.json"

        if (gas_day_str, cycle) in self._curated_keys:
            log.info(
                "Skipping tracker %d (%s %s): already in curated parquet.",
                tracker_id,
                gas_day_str,
                cycle,
            )
            self.skipped_curated += 1
            self._mark_processed(tracker_id)
            return

        if out_path.exists():
            log.info(
                "Skipping tracker %d (%s %s): raw file exists.", tracker_id, gas_day_str, cycle
            )
            self.skipped_raw_file += 1
            self._mark_processed(tracker_id)
            return

        await self._throttle()
        try:
            csv_text = await self._download_csv(tracker_id)
        except HttpClientError as exc:
            if exc.status == 403:
                self.errors_403 += 1
                cooldown = self.config.persistent_403_cooldown_seconds
                log.error(
                    "Persistent 403 for tracker %d at %s — cooling down %.0fs, continuing.",
                    tracker_id,
                    exc.url,
                    cooldown,
                )
                await asyncio.sleep(cooldown)
            else:
                log.warning("Skipping tracker %d after HTTP failure: %s", tracker_id, exc)
            return
        except (ValueError, OSError) as exc:
            # A WAF challenge served as HTTP 200 explodes inside fetch_oac_csv's
            # base64/utf-8-sig decode instead of raising HttpClientError.
            # binascii.Error and UnicodeDecodeError both subclass ValueError,
            # so (ValueError, OSError) covers them without tripping ruff B014.
            self.errors_decode += 1
            log.warning(
                "Skipping tracker %d: CSV response failed to decode (%s: %s)",
                tracker_id,
                type(exc).__name__,
                exc,
            )
            return

        rows = parse_oac_csv(csv_text)
        payload = {
            "fetched_at": datetime.now(UTC)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "tsp_id": TSP_ID,
            "cycle": cycle,
            "gas_day": gas_day_str,
            "posted_at": str(item["posted_at"]),
            "row_count": len(rows),
            "data": rows,
        }
        safe_write_json(out_path, payload)
        self.fetched += 1
        if self.oldest_gas_day is None or gas_day_str < self.oldest_gas_day:
            self.oldest_gas_day = gas_day_str
        if self.newest_gas_day is None or gas_day_str > self.newest_gas_day:
            self.newest_gas_day = gas_day_str
        log.info(
            "Fetched %d rows for %s %s (tracker %d) → %s",
            len(rows),
            gas_day_str,
            cycle,
            tracker_id,
            out_path,
        )
        self._mark_processed(tracker_id)

    # ------------------------------------------------------------------
    # Main walk
    # ------------------------------------------------------------------

    async def run(self, *, fresh: bool = False) -> dict[str, Any]:
        """Walk the posting history backwards and return the run summary.

        Why:
            Single entry point orchestrating pagination, skipping, download
            containment, checkpointing, and summary accounting.

        What:
            Stops on the first of: an empty page, a posting older than
            ``--since``, or a defensively detected no-progress page. Writes a
            final checkpoint regardless of exit path.

        Failure modes:
            Only listing-endpoint infrastructure faults (network death,
            non-HTTP exceptions) propagate; per-posting faults are contained.
        """
        self._since_iso = self.config.since.isoformat()
        self._curated_keys = load_curated_keys(self.config.curated_path)
        if fresh and self.config.state_path.exists():
            self.config.state_path.unlink()
            log.info("--fresh set — deleted checkpoint %s", self.config.state_path)
        self._restore_checkpoint()

        page = self._next_page
        while True:
            postings = await self._fetch_postings_page(page)
            self.pages_paged += 1
            self.postings_seen += len(postings)
            if not postings:
                log.info("Page %d returned no postings — pagination exhausted.", page)
                break

            boundary_reached = False
            page_doc_ids: set[int] = set()
            for posting in postings:
                extracted = extract_csv_tracker_ids([posting])
                if not extracted:
                    self.skipped_no_csv += 1
                    continue
                item = extracted[0]
                tracker_id = int(item["tracker_id"])
                page_doc_ids.add(tracker_id)
                gas_day_str = str(item["gas_day"])
                try:
                    posting_gas_day = date.fromisoformat(gas_day_str)
                except ValueError:
                    log.warning(
                        "Tracker %d has unparseable gas day %r — skipping.",
                        tracker_id,
                        gas_day_str,
                    )
                    self.skipped_no_csv += 1
                    continue
                if posting_gas_day < self.config.since:
                    log.info(
                        "Reached posting older than --since (%s < %s) — stopping.",
                        gas_day_str,
                        self._since_iso,
                    )
                    boundary_reached = True
                    break
                if tracker_id in self.processed_trackers:
                    continue
                await self._resolve_item(item)
                if self._since_checkpoint >= self.config.checkpoint_every:
                    self._write_checkpoint()

            if page_doc_ids and page_doc_ids <= self._seen_doc_ids:
                log.warning("Page %d repeated only known documents — stopping defensively.", page)
                # A fully-seen page can never yield new work, so advancing past
                # it is always safe — including when resuming from a checkpoint
                # whose next_page points at this exact page (avoids a livelock
                # where every future resume re-walks and re-stops here).
                self._next_page = page + 1
                break
            self._seen_doc_ids.update(page_doc_ids)
            if boundary_reached:
                break
            self._next_page = page + 1
            page += 1

        self._write_checkpoint()
        summary = self._summary()
        log.info("Backfill finished: %s", json.dumps(summary))
        return summary

    def _summary(self) -> dict[str, Any]:
        return {
            "pages_paged": self.pages_paged,
            "postings_seen": self.postings_seen,
            "fetched": self.fetched,
            "skipped_curated": self.skipped_curated,
            "skipped_raw_file": self.skipped_raw_file,
            "skipped_no_csv": self.skipped_no_csv,
            "errors_403": self.errors_403,
            "errors_decode": self.errors_decode,
            "oldest_gas_day_fetched": self.oldest_gas_day,
            "newest_gas_day_fetched": self.newest_gas_day,
        }


async def run_backfill(*, since: date, fresh: bool = False) -> dict[str, Any]:
    """Production entry point: build the HTTP client and execute the backfill.

    Failure modes:
        Propagates listing-endpoint faults to ``main`` (which converts them
        into a nonzero exit); per-posting faults are contained upstream.
    """
    config = BackfillConfig(since=since)
    async with HttpClient(
        base_url=REPORTING_BASE_URL,
        timeout_seconds=30.0,
        max_retries=3,
        backoff_base_seconds=1.0,
    ) as client:
        runner = GulfSouthBackfill(config=config, client=client)
        return await runner.run(fresh=fresh)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scrapers.energy_transfer.backfill",
        description=(
            "Backfill historical Gulf South SQ raw files from the "
            "Boardwalk/BWP GasQuest reporting endpoint."
        ),
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Oldest gas day to keep, YYYY-MM-DD (default: 180 days ago today).",
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
