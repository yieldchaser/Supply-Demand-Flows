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

import json
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
            Also clears the consecutive-no-op streak: a run that actually
            ingested records proves the pipeline is alive again.

        What:
            Writes ``status: "ok"`` with a UTC timestamp and optional
            metadata (e.g. ``rows_ingested``, ``latest_date``).

        Failure modes:
            Disk errors propagate.
        """
        self._clear_no_op_state()
        self._write(
            {
                "source": self._source_name,
                "status": "ok",
                "timestamp_utc": self._now_utc(),
                "error": None,
                "metadata": metadata,
            }
        )

    def record_no_op(
        self,
        reason: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a zero-records run with escalating severity.

        Why:
            A run that processed nothing and wrote nothing is NOT a healthy
            run — stamping it ``ok`` hides a dead feed behind green stamps
            (the Gulf South 2026-08 silent-skip incident: two days of
            "skipped"-but-"ok" health while the feed was effectively down).
            This is the systemic fix: no-op runs are WARN immediately and
            escalate to FAIL after three consecutive occurrences.

        What:
            Maintains a per-source ``{name}.state.json`` streak counter next
            to the health file. Streak < 3 -> ``status: "warn"``; streak
            >= 3 -> ``status: "fail"``. The streak clears on the next
            :meth:`record_success` or :meth:`record_failure`.

        Failure modes:
            A corrupt/missing state file resets the streak to 1 (this run).
            Disk errors propagate.
        """
        streak = self._read_no_op_state() + 1
        self._write_no_op_state(streak)

        merged_meta: dict[str, Any] = dict(metadata or {})
        merged_meta["reason"] = reason
        merged_meta["consecutive_no_ops"] = streak

        if streak >= 3:
            status = "fail"
            error = f"{streak} consecutive no-op runs — feed appears stalled ({reason})"
        else:
            status = "warn"
            error = f"no-op run ({reason})"

        self._write(
            {
                "source": self._source_name,
                "status": status,
                "timestamp_utc": self._now_utc(),
                "error": error,
                "metadata": merged_meta,
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
            Also clears the no-op streak (a hard failure is louder than any
            streak).

        Failure modes:
            Disk errors propagate.
        """
        self._clear_no_op_state()
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
            stale health files.  Informational only — deliberate skips do
            NOT feed the no-op escalation ladder; zero-RECORD runs should
            call :meth:`record_no_op` instead.

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

    # ------------------------------------------------------------------
    # Consecutive no-op streak state
    # ------------------------------------------------------------------

    def _state_path(self) -> Path:
        """Path of the per-source streak counter file."""
        return self._health_dir / f"{self._source_name}.state.json"

    def _read_no_op_state(self) -> int:
        """Read the current consecutive-no-op count (0 if none/corrupt)."""
        path = self._state_path()
        if not path.exists():
            return 0
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            log.warning(
                "Corrupt no-op state for %s — resetting streak.",
                self._source_name,
            )
            return 0
        if isinstance(data, dict):
            try:
                return max(0, int(data.get("consecutive_no_ops", 0)))
            except (TypeError, ValueError):
                return 0
        return 0

    def _write_no_op_state(self, streak: int) -> None:
        """Persist the streak counter atomically."""
        safe_write_json(
            self._state_path(),
            {
                "source": self._source_name,
                "consecutive_no_ops": streak,
                "updated_at": self._now_utc(),
            },
        )

    def _clear_no_op_state(self) -> None:
        """Remove the streak file after a success/failure resets it."""
        path = self._state_path()
        if path.exists():
            try:
                path.unlink()
            except OSError as exc:
                log.warning(
                    "Could not clear no-op state for %s: %s",
                    self._source_name,
                    exc,
                )
