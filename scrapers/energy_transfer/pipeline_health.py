"""End-of-pipeline health reconciliation for Gulf South.

Why:
    Health was previously recorded *by the scraper alone*, which made the
    stamp mean "the fetch worked" while every downstream guard reads it as
    "the data landed". On 2026-09-02 a run fetched five files, transformed
    7,380 rows, then died in the meter-inventory step before committing; the
    curated parquet was discarded on the ephemeral runner and
    ``data/health/gulf_south.json`` still read ``"status": "ok"``. The health
    file lied about the only thing consumers care about.

    The alternative fix — having the scraper validate its own output against
    the gas day it will later be transformed for — would re-couple the
    scraper to a downstream target day. That coupling was deliberately
    removed: the scraper takes no gas-day argument and syncs whatever the
    postings listing serves, because a wall-clock gas-day filter silently
    matched nothing and stalled the feed for two days while health stayed
    green. Re-adding it to satisfy health would reintroduce that bug.

    So health is recorded at the END of the pipeline instead. The scraper
    stays honest about fetching (and keeps recording hard failures, which is
    the only way a network error is ever caught), and this module makes the
    final stamp mean "the data landed".

What:
    Compares what the pipeline actually added to curated against what the
    scraper already stamped, and corrects the stamp only when the two
    disagree. Corrections are deliberately conservative: a hard failure is
    never softened, and an already-escalated no-op streak is never
    incremented twice for one run.

Failure modes:
    Uses only the existing ``HealthWriter`` vocabulary. No new statuses.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from scrapers.base.health_writer import HealthWriter, default_health_dir

log = logging.getLogger(__name__)

SOURCE_NAME = "gulf_south"

#: Statuses that already represent a loud, correct outcome. A reconciliation
#: must never soften one of these into something greener.
_TERMINAL_BAD = frozenset({"failed", "fail", "guard_failure"})


def _read_status(health_file: Path) -> str:
    """Return the ``status`` written in *health_file*, or ``""`` if unknown.

    Failure modes:
        A missing, unreadable, or malformed file yields ``""`` — callers
        treat that as "no stamp recorded yet" rather than as a real status.
    """
    if not health_file.exists():
        return ""
    try:
        data = json.loads(health_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    if isinstance(data, dict):
        status = data.get("status")
        if isinstance(status, str):
            return status
    return ""


def reconcile_pipeline_health(
    rows_added: int,
    files_fetched: int = 0,
    source_name: str = SOURCE_NAME,
    health_dir: Path | None = None,
    transform_failed: bool = False,
) -> str:
    """Reconcile the health stamp for *source_name* with what actually landed.

    What:
        * ``transform_failed``        → ``failed`` (an exception is
          infrastructure, not a no-op).
        * ``rows_added > 0``          → ``ok`` (recorded unless already ``ok``).
        * ``rows_added == 0`` and the scraper stamped ``ok`` → demoted to a
          ``record_no_op``, so the existing streak ladder escalates a feed
          that fetches but lands nothing.
        * A pre-existing ``failed``/``fail``/``guard_failure`` → left alone.
        * A pre-existing ``warn``/``fail`` from the scraper's own no-op →
          left alone, so one bad run cannot double-increment the streak.

    Returns:
        The status now recorded in the health file.
    """
    target_dir = Path(health_dir) if health_dir is not None else default_health_dir()
    health_file = target_dir / f"{source_name}.json"
    writer = HealthWriter(source_name=source_name, health_dir=target_dir)

    current = _read_status(health_file)

    if current in _TERMINAL_BAD:
        log.info(
            "Health [%s] left as '%s' — a hard failure is already loud; "
            "end-of-pipeline reconciliation will not soften it.",
            source_name,
            current,
        )
        return current

    metadata = {
        "rows_added": rows_added,
        "files_fetched": files_fetched,
        "stage": "end_of_pipeline",
    }

    if transform_failed:
        # An exception in the transform is infrastructure, not "nothing new
        # arrived" — record_failure so it is not folded into the no-op ladder.
        writer.record_failure(
            error="transform stage failed — curated was not updated",
            metadata=metadata,
        )
        return "failed"

    if rows_added > 0:
        if current == "ok":
            return "ok"
        writer.record_success(metadata=metadata)
        log.info(
            "Health [%s]: pipeline landed %d row(s) — stamp set to ok.",
            source_name,
            rows_added,
        )
        return "ok"

    if current == "ok":
        # The scraper fetched files and stamped ok, but nothing reached
        # curated. That is a no-op run, not a success.
        writer.record_no_op(
            reason=(
                f"scraper fetched {files_fetched} file(s) but 0 row(s) landed "
                f"in curated — nothing new was published"
            ),
            metadata=metadata,
        )
        log.warning(
            "Health [%s]: fetched %d file(s) but added 0 curated row(s) — "
            "demoting ok → no-op.",
            source_name,
            files_fetched,
        )
        return _read_status(health_file) or "warn"

    return current or "unknown"


def _int_or_zero(value: str) -> int:
    """Coerce a CLI value to int, treating empty/missing as 0.

    Why:
        The ``skipped`` pipeline path leaves ``steps.transform.outputs.rows_added``
        unset, so GitHub expands ``--rows-added ""`` to an empty string. A bare
        ``type=int`` raises ``invalid int value: ''`` and exits 2, failing the
        whole job on a routine, healthy staleness-gate hold. An empty value means
        "no transform ran", which is unambiguously 0 rows added — never an error.
    """
    value = (value or "").strip()
    return int(value) if value else 0


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: reconcile health and print the resulting status."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--rows-added",
        type=_int_or_zero,
        required=False,
        default=0,
        help="Rows the transform stage added to curated (after - before). "
        "Empty/unset is treated as 0 (no transform ran).",
    )
    parser.add_argument(
        "--files-fetched",
        type=int,
        default=0,
        help="Raw files the scraper wrote this run.",
    )
    parser.add_argument(
        "--transform-failed",
        action="store_true",
        help="The transform stage raised; record a hard failure, not a no-op.",
    )
    parser.add_argument("--source", default=SOURCE_NAME)
    parser.add_argument("--health-dir", default=None)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    status = reconcile_pipeline_health(
        rows_added=args.rows_added,
        files_fetched=args.files_fetched,
        source_name=args.source,
        health_dir=Path(args.health_dir) if args.health_dir else None,
        transform_failed=args.transform_failed,
    )
    print(status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
