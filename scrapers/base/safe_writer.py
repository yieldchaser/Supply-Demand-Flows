"""Atomic file-writing utilities and state-preserving guard.

Why:
    Half-written files are unacceptable in a production data pipeline.  Every
    write in Blue Tide goes through this module so that a crash or exception
    mid-write never corrupts an existing artefact.

What:
    * ``safe_write_*`` family — write to a temp file in the *same* directory as
      the target, then ``os.replace`` atomically.
    * ``StatePreservingWriter`` — a higher-level guard that combines
      ``safe_write_*`` with ``HealthWriter`` so that a failed compute step
      never overwrites good data.

Failure modes:
    * If ``os.replace`` fails (e.g. cross-device rename), the original file is
      untouched and the temp file is cleaned up.
    * If the caller's ``compute_fn`` raises, ``StatePreservingWriter`` logs the
      error and records a health failure — the target path is never modified.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tmp_path(target: Path) -> Path:
    """Return a sibling temp-file path that is unique per process + instant."""
    pid = os.getpid()
    ns = time.time_ns()
    return target.with_name(f"{target.name}.tmp.{pid}.{ns}")


def _ensure_parent(path: Path) -> None:
    """Create all parent directories if they don't already exist."""
    path.parent.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Public write functions
# ---------------------------------------------------------------------------


def safe_write_bytes(path: Path, data: bytes) -> None:
    """Write *data* to *path* atomically.

    Why:
        Guarantees the file is either fully written or not modified at all.

    What:
        Writes to a temp file, then ``os.replace`` to the target.

    Failure modes:
        On any exception the temp file is removed and the original is
        untouched.  The exception is re-raised.
    """
    _ensure_parent(path)
    tmp = _tmp_path(path)
    try:
        tmp.write_bytes(data)
        os.replace(tmp, path)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def safe_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    """Write *text* to *path* atomically.

    Why:
        Same atomicity guarantee as ``safe_write_bytes`` for text content.

    What:
        Encodes to bytes and delegates to ``safe_write_bytes``.

    Failure modes:
        Identical to ``safe_write_bytes``.
    """
    safe_write_bytes(path, text.encode(encoding))


def safe_write_json(path: Path, data: dict[str, Any] | list[Any], indent: int = 2) -> None:
    """Write *data* as formatted JSON to *path* atomically.

    Why:
        JSON is the standard interchange format for health files and
        intermediate artefacts.

    What:
        Serialises via ``json.dumps`` then delegates to ``safe_write_text``.

    Failure modes:
        ``TypeError`` / ``ValueError`` from ``json.dumps`` if *data* contains
        non-serialisable objects.  The target file is untouched.
    """
    text = json.dumps(data, indent=indent, ensure_ascii=False) + "\n"
    safe_write_text(path, text)


def safe_write_parquet(path: Path, df: pd.DataFrame) -> None:
    """Write a pandas DataFrame as Parquet to *path* atomically.

    Why:
        Parquet is the columnar storage format for curated data.

    What:
        Late-imports pandas/pyarrow to keep this module lightweight, writes to
        a temp file, then ``os.replace``.

    Failure modes:
        ``ImportError`` if pandas/pyarrow are not installed.  Any pyarrow
        serialisation error leaves the target untouched.
    """
    import pandas as _pd  # noqa: F811 — late import to keep module light

    if not isinstance(df, _pd.DataFrame):
        msg = f"Expected pd.DataFrame, got {type(df).__name__}"
        raise TypeError(msg)

    _ensure_parent(path)
    tmp = _tmp_path(path)
    try:
        df.to_parquet(tmp, engine="pyarrow")
        os.replace(tmp, path)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


# ---------------------------------------------------------------------------
# StatePreservingWriter
# ---------------------------------------------------------------------------


class StatePreservingWriter:
    """Guard that never overwrites good data with a failed computation.

    Why:
        500+ deployment lessons from Weather Desk taught us that scrapers
        *will* fail, and when they do the previously-good output must survive.

    What:
        Wraps an async *compute_fn* — if it raises or returns empty data, the
        existing file at *path* is untouched and a health failure is recorded.
        On success the result is written atomically and health success is
        recorded.

    Failure modes:
        * ``compute_fn`` raises → health failure, ``False`` returned.
        * ``compute_fn`` returns ``None`` / empty → treated as failure.
        * Writer itself raises (disk full, permissions) → health failure,
          ``False`` returned.
    """

    def __init__(
        self,
        source_name: str,
        writer: Callable[..., None] = safe_write_json,
    ) -> None:
        # Deferred import to avoid circular dependency at module level.
        from scrapers.base.health_writer import HealthWriter

        self._source_name = source_name
        self._writer = writer
        self._health = HealthWriter(source_name=source_name)

    async def guarded_write(
        self,
        path: Path,
        compute_fn: Callable[[], Awaitable[Any]],
    ) -> bool:
        """Run *compute_fn*, validate the result, and write atomically.

        Why:
            Centralises the "compute → validate → write → record health"
            pattern so individual scrapers can't accidentally skip a step.

        What:
            Returns ``True`` on success, ``False`` on any failure.

        Failure modes:
            See class docstring.
        """
        try:
            result = await compute_fn()
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            log.error("Compute failed for %s: %s", self._source_name, error_msg)
            self._health.record_failure(error=error_msg)
            return False

        # Treat None or empty containers as failure.
        if result is None or (isinstance(result, dict | list) and len(result) == 0):
            reason = "compute_fn returned empty result"
            log.error("Guard tripped for %s: %s", self._source_name, reason)
            self._health.record_failure(error=reason)
            return False

        try:
            self._writer(path, result)
        except Exception as exc:
            error_msg = f"Write failed: {type(exc).__name__}: {exc}"
            log.error("Write failed for %s: %s", self._source_name, error_msg)
            self._health.record_failure(error=error_msg)
            return False

        log.info("Guarded write succeeded for %s → %s", self._source_name, path)
        self._health.record_success()
        return True
