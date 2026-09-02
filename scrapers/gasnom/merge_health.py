"""Merge individual GasNom pipeline health files into unified data/health/gasnom.json.

Why:
    GasNom scrapes across a matrix of 4 pipelines (goldenpass, cameron, SABINE,
    portarthurpipeline) in parallel GitHub Actions runner jobs. Each pipeline writes
    its own health record (``data/health/gasnom_{slug}.json``). If the monitor watches
    only one slug (e.g. goldenpass), failures on the other three are completely
    invisible. This module combines all four pipeline stamps into a single canonical
    ``data/health/gasnom.json`` that the integrity monitor reads.

What:
    Delegates to ``scrapers.base.merge_health.merge_multi_health``, combining
    status, errors, timestamps, rows, processed counts, and cycles with strict
    escalation discipline (unknown status -> failed, guard_failure -> guard_failure).
    Preserves per-pipeline JSON files alongside the merged file.
"""

from __future__ import annotations

import logging
from pathlib import Path

from scrapers.base.merge_health import merge_multi_health

log = logging.getLogger(__name__)


def merge_gasnom_health(health_dir: Path | None = None) -> Path | None:
    """Merge all ``gasnom_*.json`` pipeline health files in *health_dir* into ``gasnom.json``."""
    return merge_multi_health(
        source_name="gasnom",
        file_pattern="gasnom_*.json",
        target_filename="gasnom.json",
        health_dir=health_dir,
        cleanup_inputs=False,
        legs_key="pipelines",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    merge_gasnom_health()
