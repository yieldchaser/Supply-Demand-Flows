"""Historical backfill for BHE GT&S (EGTS) OAC postings.

Why:
    ``searchHistoricalData`` accepts a POSTed date window, so the whole
    available history (UI caps at 39 months) is one parameterised call per
    window plus one CSV download per posting. This module walks the window
    month by month so a long backfill stays checkpointed and resumable.

What:
    Splits [since, today] into month-sized windows, lists postings per
    window via the shared client, skips postings whose raw file already
    exists or whose (gas_day, cycle) is already in the curated parquet,
    downloads and writes transformer-compatible raw payloads, and persists
    progress to ``_backfill_state.json`` after every window for crash-safe
    resume.

Failure modes:
    * Per-posting HTTP/decode faults are contained: logged, counted, walk
      continues.
    * A missing/malformed checkpoint or a ``--since`` mismatch starts a
      fresh walk; ``--fresh`` deletes it outright.
    * Only listing-endpoint infrastructure faults propagate to the caller.
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

from scrapers.base.errors import HttpClientError
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json
from scrapers.bhe.client import (
    INFOPOST_BASE_URL,
    RAW_DIR,
    cycle_from_subject,
    download_posting_csv,
    fetch_postings,
    parse_oac_csv,
)

log = logging.getLogger(__name__)

CURATED_PATH = Path("data/curated/bhe.parquet")
STATE_FILENAME = "_backfill_state.json"
DEFAULT_SINCE_DAYS = 395  # just past the UI's 39-month cap is pointless; ~13 months default
CHECKPOINT_EVERY_WINDOWS = 1
DOWNLOAD_GAP_SECONDS = 1.0
WINDOW_DAYS = 31


@dataclass
class BackfillConfig:
    """Knobs for one BHE backfill run.

    Why:
        Paths and pauses are injectable so tests can shrink sleeps and
        redirect I/O while production defaults stay polite to infopost.

    What:
        ``since`` bounds the backwards walk; ``download_gap_seconds`` is the
        minimum spacing between CSV downloads; windows are walked newest →
        oldest with a checkpoint after each.
    """

    since: date
    raw_dir: Path = Path(RAW_DIR)
    curated_path: Path = CURATED_PATH
    checkpoint_every_windows: int = CHECKPOINT_EVERY_WINDOWS
    download_gap_seconds: float = DOWNLOAD_GAP_SECONDS

    @property
    def state_path(self) -> Path:
        """Checkpoint file location (inside the raw directory)."""
        return self.raw_dir / STATE_FILENAME


def load_curated_gas_day_cycles(curated_path: Path) -> set[tuple[str, str]]:
    """Derive (gas_day, cycle) pairs already present in the curated parquet.

    What:
        The cycle is the final underscore token of each series id (e.g.
        ``egts_sq_40704_id3`` → ``(period, "id3")``, matched case-insensitively).

    Failure modes:
        Missing parquet → empty set; unreadable parquet propagates.
    """
    if not curated_path.exists():
        log.warning("Curated parquet not found at %s — curated skip disabled.", curated_path)
        return set()
    frame = pd.read_parquet(curated_path, columns=["series_id", "period"])
    cycles = frame["series_id"].astype(str).str.rsplit("_", n=1).str[-1].str.upper()
    periods = frame["period"].astype(str).str.slice(0, 10)
    return {(str(period), str(cycle)) for period, cycle in zip(periods, cycles, strict=True)}


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


def month_windows(since: date, until: date) -> list[tuple[date, date]]:
    """Split [since, until] into ascending month-sized (begin, end) windows."""
    windows: list[tuple[date, date]] = []
    start = since
    while start <= until:
        end = min(start + timedelta(days=WINDOW_DAYS - 1), until)
        windows.append((start, end))
        start = end + timedelta(days=1)
    return windows


class BheBackfill:
    """Stateful walker that backfills EGTS OAC postings window by window.

    Why:
        Carries cross-posting state (checkpoint, counters, skip sets).

    What:
        ``run()`` walks windows oldest → newest, resolves each posting via
        skip checks or a rate-limited CSV download, writes raw payloads, and
        checkpoints per window for resume.

    Failure modes:
        Per-posting faults contained; only listing-endpoint faults propagate.
    """

    def __init__(self, *, config: BackfillConfig, client: HttpClient) -> None:
        self.config = config
        self.client = client

        self.windows_walked = 0
        self.postings_seen = 0
        self.fetched = 0
        self.skipped_curated = 0
        self.skipped_raw_file = 0
        self.skipped_unmapped = 0
        self.errors_http = 0
        self.errors_decode = 0

        self.processed_notices: set[int] = set()
        self._curated_keys: set[tuple[str, str]] = set()
        self._last_download_monotonic: float | None = None

    # ------------------------------------------------------------------
    # Checkpoint handling
    # ------------------------------------------------------------------

    def _restore_checkpoint(self) -> int:
        """Seed counters from a matching checkpoint; returns next window index."""
        state = load_checkpoint(self.config.state_path, self.config.since.isoformat())
        if state is None:
            return 0
        next_window = max(0, int(state.get("next_window", 0)))
        for value in state.get("processed_notices", []):
            try:
                self.processed_notices.add(int(value))
            except (TypeError, ValueError):
                continue
        self.fetched = int(state.get("fetched", self.fetched))
        self.errors_http = int(state.get("errors_http", self.errors_http))
        self.errors_decode = int(state.get("errors_decode", self.errors_decode))
        log.info(
            "Resuming from checkpoint: next_window=%d, %d notice(s) already processed.",
            next_window,
            len(self.processed_notices),
        )
        return next_window

    def _write_checkpoint(self, next_window: int) -> None:
        """Persist run state atomically."""
        state = {
            "since": self.config.since.isoformat(),
            "next_window": next_window,
            "processed_notices": sorted(self.processed_notices),
            "fetched": self.fetched,
            "errors_http": self.errors_http,
            "errors_decode": self.errors_decode,
            "updated_at": datetime.now(UTC)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        safe_write_json(self.config.state_path, state)

    # ------------------------------------------------------------------
    # Download plumbing
    # ------------------------------------------------------------------

    async def _throttle(self) -> None:
        """Enforce the minimum gap between consecutive CSV downloads."""
        loop = asyncio.get_running_loop()
        now = loop.time()
        if self._last_download_monotonic is not None:
            remaining = self.config.download_gap_seconds - (now - self._last_download_monotonic)
            if remaining > 0:
                await asyncio.sleep(remaining)
        self._last_download_monotonic = loop.time()

    async def _resolve_item(self, item: dict[str, Any]) -> bool:
        """Skip or download-and-write one posting.

        Returns:
            True when a new raw payload was written.

        Failure modes:
            Never raises on HTTP/decode faults; logs and counts them.
        """
        notice_id = item["notice_id"]
        subject = item["subject"]
        gas_day_str = str(item["gas_day"])
        cycle = cycle_from_subject(subject)

        if cycle is None or not gas_day_str:
            log.warning("Notice %s has unmapped cycle/gas day (%r) — skipping.", notice_id, subject)
            self.skipped_unmapped += 1
            return False

        out_path = self.config.raw_dir / f"{gas_day_str}_{cycle}_{notice_id}.json"

        if (gas_day_str, cycle.upper()) in self._curated_keys:
            log.debug(
                "Skipping notice %s (%s %s): already in curated parquet.",
                notice_id,
                gas_day_str,
                cycle,
            )
            self.skipped_curated += 1
            self.processed_notices.add(int(notice_id))
            return False

        if out_path.exists():
            log.debug("Skipping notice %s: raw file exists.", notice_id)
            self.skipped_raw_file += 1
            self.processed_notices.add(int(notice_id))
            return False

        await self._throttle()
        try:
            csv_text = await download_posting_csv(self.client, str(item["csv_url"]))
        except HttpClientError as exc:
            self.errors_http += 1
            log.warning("Skipping notice %s after HTTP failure: %s", notice_id, exc)
            return False
        except (UnicodeDecodeError, ValueError) as exc:
            # A WAF challenge served as HTTP 200 explodes inside decode.
            self.errors_decode += 1
            log.warning(
                "Skipping notice %s: CSV failed to decode (%s: %s)",
                notice_id,
                type(exc).__name__,
                exc,
            )
            return False

        rows = parse_oac_csv(csv_text)
        payload = {
            "fetched_at": datetime.now(UTC)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "source": "bhe",
            "tsp": "egts",
            "notice_id": notice_id,
            "cycle": cycle,
            "gas_day": gas_day_str,
            "posted_at": str(item["posted_at"]),
            "row_count": len(rows),
            "data": rows,
        }
        safe_write_json(out_path, payload)
        self.fetched += 1
        self.processed_notices.add(int(notice_id))
        log.info(
            "Fetched %d rows for %s %s (notice %s) → %s",
            len(rows),
            gas_day_str,
            cycle,
            notice_id,
            out_path,
        )
        return True

    # ------------------------------------------------------------------
    # Main walk
    # ------------------------------------------------------------------

    async def run(self, *, fresh: bool = False) -> dict[str, Any]:
        """Walk every monthly window and return the run summary.

        Why:
            Single entry point orchestrating windows, skipping, downloads,
            checkpointing, and accounting.

        What:
            Walks windows oldest → newest starting at the restored cursor;
            checkpoints after each window. Stops after the final window.

        Failure modes:
            Only listing-endpoint faults propagate; per-posting faults are
            contained upstream.
        """
        self._curated_keys = load_curated_gas_day_cycles(self.config.curated_path)
        if fresh and self.config.state_path.exists():
            self.config.state_path.unlink()
            log.info("--fresh set — deleted checkpoint %s", self.config.state_path)

        today = date.today()
        windows = month_windows(self.config.since, today)
        start_index = self._restore_checkpoint()

        for index in range(start_index, len(windows)):
            begin, end = windows[index]
            postings = await fetch_postings(self.client, begin, end)
            self.windows_walked += 1
            self.postings_seen += len(postings)

            for item in postings:
                if item["notice_id"] in self.processed_notices:
                    continue
                await self._resolve_item(item)

            self._write_checkpoint(next_window=index + 1)
            log.debug(
                "Window %s..%s done (%d postings seen cumulatively).",
                begin,
                end,
                self.postings_seen,
            )

        summary = self._summary()
        log.info("Backfill finished: %s", json.dumps(summary))
        return summary

    def _summary(self) -> dict[str, Any]:
        return {
            "windows_walked": self.windows_walked,
            "postings_seen": self.postings_seen,
            "fetched": self.fetched,
            "skipped_curated": self.skipped_curated,
            "skipped_raw_file": self.skipped_raw_file,
            "skipped_unmapped": self.skipped_unmapped,
            "errors_http": self.errors_http,
            "errors_decode": self.errors_decode,
            "processed_notices": len(self.processed_notices),
        }


async def run_backfill(*, since: date, fresh: bool = False) -> dict[str, Any]:
    """Production entry point: build the HTTP client and execute the backfill.

    Failure modes:
        Propagates listing-endpoint faults to ``main``; per-posting faults
        are contained upstream.
    """
    config = BackfillConfig(since=since)
    async with HttpClient(
        base_url=INFOPOST_BASE_URL,
        timeout_seconds=30.0,
        max_retries=3,
        backoff_base_seconds=1.0,
        retryable_status_codes=frozenset({403}),
    ) as client:
        runner = BheBackfill(config=config, client=client)
        return await runner.run(fresh=fresh)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scrapers.bhe.backfill",
        description=(
            "Backfill historical BHE GT&S (EGTS) OAC raw files from the "
            "infopost searchHistoricalData endpoint."
        ),
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Oldest gas day to keep, YYYY-MM-DD (default: 365 days ago today).",
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
        since = date.today() - timedelta(days=DEFAULT_SINCE_DAYS - 30)
    try:
        summary = asyncio.run(run_backfill(since=since, fresh=bool(args.fresh)))
    except Exception as exc:
        log.error("Backfill aborted: %s: %s", type(exc).__name__, exc)
        return 1
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
