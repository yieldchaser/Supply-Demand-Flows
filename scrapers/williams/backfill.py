"""Checkpointed multi-cycle backfill for the Williams/Transco scraper.

Why:
    Transco's OAC query accepts arbitrary (date, cycle) pairs within a
    THREE-YEAR lookback (enforced client-side by oacquery.js; server-side
    retention may be shorter for intraday cycles — the walk records the true
    recoverable floor). Every day of delay can lose a day permanently when
    upstream prunes, so backfill runs immediately after a working scrape and
    is checkpointed to survive CI restarts.

What:
    ``WilliamsBackfill`` walks month windows from *since* to *until*. For each
    window it queries each cycle with explicit location IDs (the watchlist
    ids plus any discovered terminal ids), parses the report table, filters
    to the watchlist, and writes one raw JSON per (gas_day, cycle) using the
    SAME payload contract as the live scraper — so
    ``transformers.williams`` ingests both unchanged.

Failure modes:
    Per-window HTTP faults are contained (logged, counted, walk continues);
    only an unrecoverable block before the first success propagates. The
    checkpoint file records the next unprocessed window index.
"""

from __future__ import annotations

import calendar
import json
import logging
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from scrapers.base.safe_writer import safe_write_json
from scrapers.williams.client import TranscoClient
from scrapers.williams.config import SOURCE_NAME

log = logging.getLogger(__name__)

RAW_DIR = Path("data/raw/williams")
CHECKPOINT_PATH = Path("data/raw/williams/_backfill_state.json")

#: Cycles walked by default — Post/Retro are excluded (revisions, not primary).
DEFAULT_CYCLES: tuple[str, ...] = ("timely", "evening", "id1", "id2", "id3")


def _month_windows(since: date, until: date) -> list[tuple[date, date]]:
    """Return consecutive month-bounded windows covering [since, until].

    What:
        Windows are capped at ~31 days because oacquery.js warns that
        unrestricted all-location ranges exceed the server record limit;
        scoped location lists tolerate more, but monthly windows keep every
        request well inside it.

    Failure modes:
        None — pure function.
    """
    windows: list[tuple[date, date]] = []
    cursor = since
    while cursor <= until:
        last_day = calendar.monthrange(cursor.year, cursor.month)[1]
        window_end = min(date(cursor.year, cursor.month, last_day), until)
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


class WilliamsBackfill:
    """Walk historical OAC reports into raw payloads."""

    def __init__(
        self,
        *,
        client: TranscoClient,
        raw_dir: Path = RAW_DIR,
        checkpoint_path: Path = CHECKPOINT_PATH,
        cycles: tuple[str, ...] = DEFAULT_CYCLES,
    ) -> None:
        self.client = client
        self.raw_dir = raw_dir
        self.checkpoint_path = checkpoint_path
        self.cycles = tuple(cycles)

    # -- checkpointing ----------------------------------------------------

    def _restore_checkpoint(self) -> int:
        try:
            state = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            return int(state.get("next_window", 0))
        except (OSError, ValueError, TypeError):
            return 0

    def _write_checkpoint(self, next_window: int) -> None:
        safe_write_json(
            self.checkpoint_path,
            {"source": SOURCE_NAME, "next_window": next_window,
             "updated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")},
        )

    # -- walk ---------------------------------------------------------------

    def run(self, since: date, until: date) -> dict[str, Any]:
        """Backfill [since, until] across all cycles; returns walk stats.

        Failure modes:
            Contained per window/cycle; raises only if EVERY window failed.
        """
        windows = _month_windows(since, until)
        start_index = min(self._restore_checkpoint(), len(windows))
        log.info("Backfill %s → %s: %d windows, resuming at #%d",
                 since, until, len(windows), start_index)

        fetched = 0
        skipped = 0
        empty = 0
        failed = 0
        floors: dict[str, str] = {}

        for idx in range(start_index, len(windows)):
            win_start, win_end = windows[idx]
            for cycle in self.cycles:
                day = win_start
                while day <= win_end:
                    out_path = self.raw_dir / f"transco_{day.isoformat()}_{cycle}.json"
                    if out_path.exists():
                        skipped += 1
                        day += timedelta(days=1)
                        continue
                    try:
                        header, rows = self.client.fetch_oac(cycle, day)
                    except Exception as exc:  # noqa: BLE001 — contained per-day
                        log.warning("%s %s: %s: %s", day, cycle,
                                    type(exc).__name__, exc)
                        failed += 1
                        day += timedelta(days=1)
                        continue

                    reported_cycle = header.cycle_desc.strip()
                    if rows and reported_cycle:
                        floors.setdefault(cycle, day.isoformat())
                    if not rows or not header.gas_day:
                        empty += 1
                        day += timedelta(days=1)
                        continue

                    from scrapers.williams import _resolve_watchlist, build_payload

                    kept, matched = _resolve_watchlist(rows)
                    if not kept:
                        empty += 1
                        day += timedelta(days=1)
                        continue

                    payload = build_payload(
                        gas_day=day,
                        cycle_code=cycle,
                        cycle_desc=reported_cycle,
                        posted_at=header.posting_time,
                        tsp_name=header.tsp_name,
                        meas_basis=header.meas_basis,
                        rows=kept,
                    )
                    safe_write_json(out_path, payload)
                    fetched += 1
                    day += timedelta(days=1)

            self._write_checkpoint(idx + 1)

        return {
            "windows": len(windows),
            "fetched": fetched,
            "skipped_existing": skipped,
            "empty": empty,
            "failed": failed,
            "cycle_floors": floors,
            "resumed_from": start_index,
        }


async def run_backfill(
    since: date | str | None = None,
    until: date | str | None = None,
    *,
    cycles: tuple[str, ...] = DEFAULT_CYCLES,
) -> dict[str, Any]:
    """CLI entry point: backfill the full requested history.

    What:
        Defaults to the maximum documented lookback (three years minus one
        day, the oacquery.js floor) ending yesterday. Records source health
        via HealthWriter exactly like the live scraper.

    Failure modes:
        Health records failure and re-raises after the walk if every window
        errored; partial walks record success with their stats.
    """
    from scrapers.base.health_writer import HealthWriter

    health = HealthWriter(source_name=SOURCE_NAME)
    today = datetime.now(UTC).date()
    end = date.fromisoformat(until) if isinstance(until, str) else (
        until or today - timedelta(days=1)
    )
    begin = date.fromisoformat(since) if isinstance(since, str) else (
        since or today - timedelta(days=3 * 365 + 1)
    )

    walker = WilliamsBackfill(client=TranscoClient(), cycles=cycles)
    try:
        stats = walker.run(begin, end)
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {exc}"
        health.record_failure(error=err, metadata={"since": str(begin), "until": str(end)})
        raise
    health.record_success(metadata=stats)
    return stats


def main(argv: list[str] | None = None) -> int:
    """CLI: ``python -m scrapers.williams.backfill [--since D] [--until D]``."""
    import argparse

    parser = argparse.ArgumentParser(prog="python -m scrapers.williams.backfill")
    parser.add_argument("--since", default=None, help="YYYY-MM-DD (default: 3 years ago)")
    parser.add_argument("--until", default=None, help="YYYY-MM-DD (default: yesterday)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )
    stats = run_backfill(args.since, args.until)
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
