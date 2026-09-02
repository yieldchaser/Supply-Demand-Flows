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
    Delegates to ``scrapers.base.merge_health.merge_multi_health``, combining
    status, errors, timestamps, rows, processed counts, and cycles with strict
    escalation discipline (unknown status -> failed, guard_failure -> guard_failure).
    Deletes temporary per-tenant files.
"""

from __future__ import annotations

import logging
from pathlib import Path

from scrapers.base.merge_health import merge_multi_health

log = logging.getLogger(__name__)


def merge_quorum_health(health_dir: Path | None = None) -> Path | None:
    """Merge all ``quorum_*.json`` files in *health_dir* into ``quorum.json``."""
    return merge_multi_health(
        source_name="quorum",
        file_pattern="quorum_*.json",
        target_filename="quorum.json",
        health_dir=health_dir,
        cleanup_inputs=True,
        legs_key="tenants",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    merge_quorum_health()
