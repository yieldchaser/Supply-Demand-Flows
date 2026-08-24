"""Historical backfill for Enbridge rtba OAC (Texas Eastern, MLC sub-type).

Why:
    The rtba download API accepts an arbitrary (start, end) gas-day window
    and serves the full 3-year history, but the OA report is capped at six
    months per request server-side. Walking six-month chunks keeps each
    StartFile/AddToFile/ZipFile chain inside that cap and gives natural
    checkpoint boundaries.

What:
    Splits [since, today] into ascending six-month windows, runs one
    download chain per window via the shared client, unpacks the ZIP into
    transformer-compatible raw payloads (skipping files already on disk or
    already represented in the curated parquet), and persists progress to
    ``_backfill_state.json`` after every window for crash-safe resume.

Failure modes:
    * Per-window HTTP faults are contained: logged, counted, walk continues.
    * A missing/malformed checkpoint or a ``--since`` mismatch starts a
      fresh walk; ``--fresh`` deletes it outright.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import logging
import sys
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from scrapers.base.errors import HttpClientError
from scrapers.base.http_client import HttpClient
from scrapers.base.safe_writer import safe_write_json
from scrapers.enbridge.client import (
    BASE_URL,
    BUSINESS_UNIT,
    COL_LOC,
    RAW_DIR,
    SOURCE_NAME,
    fetch_oac_zip_bytes,
    parse_csv_filename,
)

log = logging.getLogger(__name__)

CURATED_PATH = Path("data/curated/enbridge.parquet")
STATE_FILENAME = "_backfill_state.json"
DEFAULT_SINCE_DAYS = 365 * 3  # rtba serves three years of OA history
WINDOW_DAYS = 182  # ~6 months: the server-side OA chunk cap
CHECKPOINT_EVERY_WINDOWS = 1
CHAIN_GAP_SECONDS = 2.0  # courtesy pause between download chains


@dataclass
class BackfillConfig:
    """Knobs for one Enbridge backfill run.

    Why:
        Paths and pauses are injectable so tests can shrink sleeps and
        redirect I/O while production defaults stay polite to rtba.

    What:
        ``since`` bounds the backwards walk; ``chain_gap_seconds`` spaces out
        StartFile→…→FileHandler chains; windows are walked oldest → newest
        with a checkpoint after each.
    """

    since: date
    business_unit: str = BUSINESS_UNIT
    raw_dir: Path = Path(RAW_DIR)
    curated_path: Path = CURATED_PATH
    chain_gap_seconds: float = CHAIN_GAP_SECONDS


def six_month_windows(since: date, until: date) -> list[tuple[date, date]]:
    """Split [since, until] into ascending six-month (begin, end) windows."""
    windows: list[tuple[date, date]] = []
    start = since
    while start <= until:
        end = min(start + timedelta(days=WINDOW_DAYS - 1), until)
        windows.append((start, end))
        start = end + timedelta(days=1)
    return windows


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
        log.info("Checkpoint at %s does not match --since=%s — starting fresh.", state_path, since_iso)
        return None
    return state


def curated_gas_days(curated_path: Path) -> set[str]:
    """Return gas days already present in the curated parquet.

    Failure modes:
        Missing parquet → empty set; unreadable parquet propagates.
    """
    if not curated_path.exists():
        return set()
    import pandas as pd

    frame = pd.read_parquet(curated_path, columns=["period"])
    return {str(p)[:10] for p in frame["period"].tolist()}


class EnbridgeBackfill:
    """Stateful walker that backfills TETCO OAC window by window.

    Why:
        Carries cross-window state (checkpoint, counters).

    What:
        ``run()`` walks six-month windows oldest → newest, fetches one OAC
        archive per window, writes raw payloads for unseen cycle-posts, and
        checkpoints after each window.

    Failure modes:
        Per-window faults contained; nothing propagates except catastrophic
        local I/O errors.
    """

    def __init__(self, *, config: BackfillConfig, client: HttpClient) -> None:
        self.config = config
        self.client = client

        self.windows_walked = 0
        self.chains_run = 0
        self.payloads_seen = 0
        self.fetched = 0
        self.skipped_curated_day = 0
        self.skipped_raw_file = 0
        self.errors_http = 0

        self._done_members: set[str] = set()

    # ------------------------------------------------------------------
    # Checkpoint handling
    # ------------------------------------------------------------------

    def _state_path(self) -> Path:
        return self.config.raw_dir / STATE_FILENAME

    def _restore_checkpoint(self) -> int:
        """Seed counters from a matching checkpoint; returns next window index."""
        state = load_checkpoint(self._state_path(), self.config.since.isoformat())
        if state is None:
            return 0
        next_window = max(0, int(state.get("next_window", 0)))
        members = state.get("done_members", [])
        if isinstance(members, list):
            self._done_members = {str(m) for m in members}
        self.fetched = int(state.get("fetched", self.fetched))
        self.errors_http = int(state.get("errors_http", self.errors_http))
        log.info(
            "Resuming from checkpoint: next_window=%d, %d archive member(s) already done.",
            next_window,
            len(self._done_members),
        )
        return next_window

    def _write_checkpoint(self, next_window: int) -> None:
        """Persist run state atomically."""
        state = {
            "since": self.config.since.isoformat(),
            "next_window": next_window,
            "done_members": sorted(self._done_members),
            "fetched": self.fetched,
            "errors_http": self.errors_http,
            "updated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        }
        safe_write_json(self._state_path(), state)

    # ------------------------------------------------------------------
    # Main walk
    # ------------------------------------------------------------------

    async def run(self, *, fresh: bool = False) -> dict[str, Any]:
        """Walk every six-month window and return the run summary.

        What:
            Walks windows oldest → newest starting at the restored cursor;
            fetches one archive per window; skips payloads whose raw file
            exists, whose member hash is in the checkpoint, or whose gas day
            is fully present in curated; checkpoints after each window.

        Failure modes:
            Per-window HTTP faults are contained and counted; only local
            I/O failures propagate.
        """

        if fresh and self._state_path().exists():
            self._state_path().unlink()
            log.info("--fresh set — deleted checkpoint %s", self._state_path())

        have_curated_days = curated_gas_days(self.config.curated_path)
        today = datetime.now(UTC).date()
        windows = six_month_windows(self.config.since, today)
        start_index = self._restore_checkpoint()

        for index in range(start_index, len(windows)):
            begin, end = windows[index]
            try:
                zip_bytes = await fetch_oac_zip_bytes(
                    self.client,
                    begin,
                    end,
                    business_unit=self.config.business_unit,
                )
                self.chains_run += 1
                fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                # Stream the archive member-by-member: a six-month window is
                # ~6,000 CSVs and holding every parsed payload in memory at
                # once OOM'd a 16GB machine during the first backfill run.
                seen = 0
                with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                    members = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
                    for member in members:
                        parsed = parse_csv_filename(member)
                        if parsed is None:
                            continue
                        cycle_tok, gas_day_iso = parsed
                        stem = member.rsplit(".", 1)[0]
                        out_path = self.config.raw_dir / f"{gas_day_iso}_{cycle_tok}_{stem}.json"

                        if member in self._done_members or out_path.exists():
                            self.skipped_raw_file += 1
                            self._done_members.add(member)
                            continue

                        text = zf.read(member).decode("utf-8-sig", errors="replace")
                        rows = [
                            {(k or "").strip(): (v or "").strip() for k, v in row.items() if k is not None}
                            for row in csv.DictReader(io.StringIO(text))
                        ]
                        rows = [r for r in rows if r.get(COL_LOC)]
                        if not rows:
                            continue
                        payload = {
                            "source": SOURCE_NAME,
                            "business_unit": self.config.business_unit,
                            "archive_member": member,
                            "cycle": cycle_tok,
                            "gas_day": gas_day_iso,
                            "fetched_at": fetched_at,
                            "row_count": len(rows),
                            "data": rows,
                        }
                        safe_write_json(out_path, payload)
                        self._done_members.add(member)
                        self.fetched += 1
                        seen += 1
                        del payload, rows, text
            except HttpClientError as exc:
                self.errors_http += 1
                log.warning("Window %s..%s failed after retries: %s", begin, end, exc)
                self.windows_walked += 1
                self._write_checkpoint(next_window=index + 1)
                continue
            except (httpx.HTTPError, OSError) as exc:
                # Transport faults inside the FileHandler form-POST bypass
                # HttpClient's retry engine (it is a direct httpx call); an
                # ISP drop must cost one skipped window, not the whole walk.
                self.errors_http += 1
                log.warning("Window %s..%s failed on transport: %s: %s", begin, end, type(exc).__name__, exc)
                self.windows_walked += 1
                self._write_checkpoint(next_window=index + 1)
                continue

            self.windows_walked += 1
            self.payloads_seen += seen
            del zip_bytes
            log.info(
                "Window %s..%s: %d posting(s) written, %d cumulative.",
                begin,
                end,
                seen,
                self.fetched,
            )
            self._write_checkpoint(next_window=index + 1)
            if self.config.chain_gap_seconds > 0:
                await asyncio.sleep(self.config.chain_gap_seconds)

        summary = self._summary(have_curated_days)
        log.info("Backfill finished: %s", json.dumps(summary))
        return summary

    def _summary(self, have_curated_days: set[str]) -> dict[str, Any]:
        return {
            "windows_walked": self.windows_walked,
            "chains_run": self.chains_run,
            "payloads_seen": self.payloads_seen,
            "fetched": self.fetched,
            "skipped_curated_day": self.skipped_curated_day,
            "skipped_raw_file": self.skipped_raw_file,
            "errors_http": self.errors_http,
            "archive_members_done": len(self._done_members),
            "curated_days_before": len(have_curated_days),
            "raw_files_total": len(list(self.config.raw_dir.glob("*.json"))) - 1
            if self.config.raw_dir.exists()
            else 0,
        }


async def run_backfill(*, since: date, fresh: bool = False) -> dict[str, Any]:
    """Production entry point: build the HTTP client and execute the backfill.

    Failure modes:
        Propagates catastrophic faults to ``main``; per-window faults are
        contained upstream.
    """
    config = BackfillConfig(since=since)
    async with HttpClient(
        base_url=BASE_URL,
        timeout_seconds=120.0,
        max_retries=3,
        backoff_base_seconds=2.0,
        rate_limit_per_second=0.5,
    ) as client:
        runner = EnbridgeBackfill(config=config, client=client)
        return await runner.run(fresh=fresh)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scrapers.enbridge.backfill",
        description=(
            "Backfill historical Texas Eastern OAC (meter-level, TSQ embedded) "
            "raw files from the Enbridge rtba page-method API."
        ),
    )
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Oldest gas day to keep, YYYY-MM-DD (default: 3 years ago today).",
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
