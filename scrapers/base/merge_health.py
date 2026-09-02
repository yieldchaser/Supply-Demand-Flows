"""Shared multi-leg health record merger.

Why:
    Scrapers that run across parallel matrix legs (such as Quorum across
    tenants, or GasNom across pipelines) produce independent health records
    per leg. If each leg writes to or overwrites a single file, or if the
    monitor watches only a single leg, failures on the other legs are
    invisible. This module combines per-leg health stamps into a single
    canonical health file that represents the entire source.

Escalation discipline:
    * Worst status wins across all legs.
    * Status rank: failed/fail (4) > guard_failure (3) > warn (2) > ok/skipped (1).
    * Any UNKNOWN status automatically escalates to rank 4 (failed).
    * Guard failure (guard_failure) is a distinct failure class and is never
      demoted to warning.
    * Errors across legs are joined with semicolons preserving leg identity.
    * Timestamp is the newest UTC timestamp among the legs.
    * Rows, processed counts, and skipped counts are summed.
    * Cycles are unioned.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import default_health_dir
from scrapers.base.safe_writer import safe_write_json

log = logging.getLogger(__name__)

# Authoritative ranking based on scrapers.base.health_writer.HealthWriter
STATUS_RANK: dict[str, int] = {
    "ok": 1,
    "skipped": 1,
    "empty": 1,
    "warn": 2,
    "guard_failure": 3,
    "fail": 4,
    "failed": 4,
}

RANK_TO_STATUS: dict[int, str] = {
    1: "ok",
    2: "warn",
    3: "guard_failure",
    4: "failed",
}


def merge_multi_health(
    source_name: str,
    file_pattern: str,
    target_filename: str,
    *,
    health_dir: Path | None = None,
    cleanup_inputs: bool = False,
    legs_key: str = "legs",
) -> Path | None:
    """Merge per-leg health files into a canonical health file.

    Args:
        source_name: Identifier for the dataset (e.g. 'quorum', 'gasnom').
        file_pattern: Glob pattern for leg files (e.g. 'quorum_*.json').
        target_filename: Output filename in health_dir (e.g. 'quorum.json').
        health_dir: Directory containing health files. Defaults to BLUETIDE_HEALTH_DIR.
        cleanup_inputs: If True, delete the input leg files after merging.
        legs_key: Metadata key name for per-leg breakdown (e.g. 'tenants', 'pipelines').

    Returns:
        Path to the written merged file, or None if no input files were found.
    """
    hdir = health_dir if health_dir is not None else default_health_dir()
    all_matching = sorted(hdir.glob(file_pattern))
    leg_files = [p for p in all_matching if p.name != target_filename]

    if not leg_files:
        log.info("No per-leg health files found matching '%s' in %s", file_pattern, hdir)
        return None

    prefix = file_pattern.split("*")[0]
    leg_records: dict[str, dict[str, Any]] = {}
    for path in leg_files:
        leg_name = path.stem
        if leg_name.startswith(prefix):
            leg_name = leg_name[len(prefix) :]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                leg_records[leg_name] = data
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Failed to parse leg health file %s: %s", path, exc)
            # A corrupted leg health file is an infrastructure failure
            leg_records[leg_name] = {
                "source": source_name,
                "status": "failed",
                "error": f"corrupt health file ({exc})",
            }

    if not leg_records:
        return None

    worst_rank = 0
    errors: list[str] = []
    latest_ts = ""
    total_processed = 0
    total_skipped = 0
    total_rows = 0
    all_cycles: set[str] = set()
    gas_day: str | None = None
    all_statuses: list[str] = []

    for leg_name, record in sorted(leg_records.items()):
        raw_status = str(record.get("status", "failed")).lower()
        all_statuses.append(raw_status)
        # Unknown statuses escalate to rank 4 (failed)
        rank = STATUS_RANK.get(raw_status, 4)
        if rank > worst_rank:
            worst_rank = rank

        err = record.get("error")
        if err:
            errors.append(f"{leg_name}: {err}")
        elif rank == 4 and raw_status not in STATUS_RANK:
            errors.append(f"{leg_name}: unrecognized health status '{raw_status}' (escalated to failed)")

        ts = str(record.get("timestamp_utc", ""))
        if ts > latest_ts:
            latest_ts = ts

        meta = record.get("metadata") or {}
        if not gas_day and meta.get("gas_day"):
            gas_day = str(meta.get("gas_day"))
        total_processed += int(meta.get("processed_count", 0))
        total_skipped += int(meta.get("skipped_count", 0))
        total_rows += int(meta.get("rows", 0))
        for c in meta.get("cycles") or []:
            all_cycles.add(str(c))

    if not latest_ts:
        latest_ts = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if worst_rank == 1 and all(s == "skipped" for s in all_statuses):
        overall_status = "skipped"
    else:
        overall_status = RANK_TO_STATUS.get(worst_rank, "failed")

    merged_payload: dict[str, Any] = {
        "source": source_name,
        "status": overall_status,
        "timestamp_utc": latest_ts,
        "error": "; ".join(errors) if errors else None,
        "metadata": {
            "gas_day": gas_day,
            "processed_count": total_processed,
            "skipped_count": total_skipped,
            "rows": total_rows,
            "cycles": sorted(all_cycles),
            legs_key: {k: rec.get("status") for k, rec in sorted(leg_records.items())},
        },
    }

    target_path = hdir / target_filename
    safe_write_json(target_path, merged_payload)
    log.info("Merged %d %s health records into %s", len(leg_records), source_name, target_path)

    if cleanup_inputs:
        for path in leg_files:
            try:
                path.unlink()
            except OSError as exc:
                log.warning("Could not delete temporary leg health file %s: %s", path, exc)

    return target_path
