"""Merge individual Quorum tenant health files into the unified data/health/quorum.json.

Why:
    Quorum scrapes across a matrix of tenants (Gator Express, TransCameron)
    in parallel GitHub Actions runner jobs. Each runner writes its own health
    record. To prevent one tenant arbitrarily clobbering the other on artifact
    download/commit, each matrix job uploads ``data/health/quorum_{tenant}.json``.
    The publish job downloads all tenant health files and runs this module
    to combine them into a single canonical ``data/health/quorum.json`` that reflects
    the whole run.

What:
    Combines status (fail > warn > ok > skipped), errors, timestamps (latest UTC),
    and aggregates metadata (total rows, processed_count, skipped_count, union of cycles,
    and a per-tenant breakdown). Cleans up the temporary per-tenant files.
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

_STATUS_RANK = {
    "ok": 1,
    "skipped": 1,
    "warn": 2,
    "fail": 3,
    "failed": 3,
}


def merge_quorum_health(health_dir: Path | None = None) -> Path | None:
    """Merge all ``quorum_*.json`` files in *health_dir* into ``quorum.json``."""
    hdir = health_dir if health_dir is not None else default_health_dir()
    tenant_files = sorted(hdir.glob("quorum_*.json"))
    if not tenant_files:
        log.info("No per-tenant quorum health files found in %s to merge.", hdir)
        return None

    tenant_records: dict[str, dict[str, Any]] = {}
    for path in tenant_files:
        # e.g. quorum_gator.json -> gator
        tenant_name = path.stem.replace("quorum_", "")
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                tenant_records[tenant_name] = data
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("Failed to parse tenant health file %s: %s", path, exc)

    if not tenant_records:
        return None

    # Determine overall status and error
    worst_rank = 0
    overall_status = "ok"
    errors: list[str] = []
    latest_ts = ""
    total_processed = 0
    total_skipped = 0
    total_rows = 0
    all_cycles: set[str] = set()
    gas_day = None

    for tenant_name, record in sorted(tenant_records.items()):
        status = str(record.get("status", "ok")).lower()
        rank = _STATUS_RANK.get(status, 2)
        if rank > worst_rank:
            worst_rank = rank
            overall_status = "failed" if rank == 3 else ("warn" if rank == 2 else status)

        err = record.get("error")
        if err:
            errors.append(f"{tenant_name}: {err}")

        ts = str(record.get("timestamp_utc", ""))
        if ts > latest_ts:
            latest_ts = ts

        meta = record.get("metadata") or {}
        if not gas_day and meta.get("gas_day"):
            gas_day = meta.get("gas_day")
        total_processed += int(meta.get("processed_count", 0))
        total_skipped += int(meta.get("skipped_count", 0))
        total_rows += int(meta.get("rows", 0))
        for c in meta.get("cycles") or []:
            all_cycles.add(str(c))

    if not latest_ts:
        latest_ts = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    merged_payload: dict[str, Any] = {
        "source": "quorum",
        "status": overall_status,
        "timestamp_utc": latest_ts,
        "error": "; ".join(errors) if errors else None,
        "metadata": {
            "gas_day": gas_day,
            "processed_count": total_processed,
            "skipped_count": total_skipped,
            "rows": total_rows,
            "cycles": sorted(all_cycles),
            "tenants": {t: rec.get("status") for t, rec in sorted(tenant_records.items())},
        },
    }

    target_path = hdir / "quorum.json"
    safe_write_json(target_path, merged_payload)
    log.info("Merged %d tenant health records into %s", len(tenant_records), target_path)

    # Clean up temporary per-tenant files
    for path in tenant_files:
        try:
            path.unlink()
        except OSError as exc:
            log.warning("Could not delete temporary tenant health file %s: %s", path, exc)

    return target_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    merge_quorum_health()
