"""Centralised health-status writer for every Blue Tide data source.

Why:
    Every external HTTP call or scrape must leave an auditable trace of
    success, failure, or intentional skip.  Without this, silent failures
    accumulate and curated data rots.

What:
    ``HealthWriter`` writes a small JSON file per source into
    ``data/health/{source}.json``.  Before overwriting it preserves the
    previous state as ``{source}.prev.json`` for debugging.

Failure modes:
    * Disk-full or permission errors during write propagate to the caller
      (but the previous health file is never lost — the atomic write
      guarantees this).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scrapers.base.safe_writer import safe_write_json

log = logging.getLogger(__name__)


class HealthWriter:
    """Record per-source health status to ``data/health/``.

    Why:
        Operators need a single glance to know whether each data pipeline is
        healthy.

    What:
        Produces deterministic JSON files that downstream monitors can poll.

    Failure modes:
        * Write failures are logged and re-raised — callers decide whether to
          swallow or propagate.
    """

    def __init__(
        self,
        source_name: str,
        health_dir: Path = Path("data/health"),
    ) -> None:
        self._source_name = source_name
        self._health_dir = health_dir
        self._health_file = health_dir / f"{source_name}.json"
        self._prev_file = health_dir / f"{source_name}.prev.json"

    def _rotate_previous(self) -> None:
        """Copy the current health file to ``.prev.json`` before overwrite."""
        if self._health_file.exists():
            # Read then write rather than shutil.copy to go through atomic path
            content = self._health_file.read_bytes()
            self._prev_file.write_bytes(content)

    def _write(self, record: dict[str, Any]) -> None:
        """Write *record* atomically, preserving the prior state first."""
        self._rotate_previous()
        safe_write_json(self._health_file, record)
        log.info(
            "Health [%s]: %s",
            self._source_name,
            record.get("status", "unknown"),
        )

    @staticmethod
    def _now_utc() -> str:
        """ISO-8601 timestamp with ``Z`` suffix, no microseconds."""
        return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def record_success(self, metadata: dict[str, Any] | None = None) -> None:
        """Write a success record.

        Why:
            Confirms the most recent scrape ingested data without errors.

        What:
            Writes ``status: "ok"`` with a UTC timestamp and optional
            metadata (e.g. ``rows_ingested``, ``latest_date``).

        Failure modes:
            Disk errors propagate.
        """
        self._write(
            {
                "source": self._source_name,
                "status": "ok",
                "timestamp_utc": self._now_utc(),
                "error": None,
                "metadata": metadata,
            }
        )

    def record_failure(
        self,
        error: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Write a failure record.

        Why:
            Makes failures visible immediately for operators and monitors.

        What:
            Writes ``status: "failed"`` with the error message and metadata.

        Failure modes:
            Disk errors propagate.
        """
        self._write(
            {
                "source": self._source_name,
                "status": "failed",
                "timestamp_utc": self._now_utc(),
                "error": error,
                "metadata": metadata,
            }
        )

    def record_skipped(self, reason: str) -> None:
        """Write a skipped record.

        Why:
            Staleness gates intentionally skip scrapes when no new data is
            available.  This must be recorded so monitors don't alarm on
            stale health files.

        What:
            Writes ``status: "skipped"`` with the reason string.

        Failure modes:
            Disk errors propagate.
        """
        self._write(
            {
                "source": self._source_name,
                "status": "skipped",
                "timestamp_utc": self._now_utc(),
                "error": reason,
                "metadata": None,
            }
        )
