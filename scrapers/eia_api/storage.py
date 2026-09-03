"""EIA Natural Gas Weekly Storage Scraper."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from scrapers.base.health_writer import HealthWriter
from scrapers.base.safe_writer import StatePreservingWriter, safe_write_json
from scrapers.eia_api.client import EIAClient, load_api_key_from_env

log = logging.getLogger(__name__)

ROUTE = "natural-gas/stor/wkly"
SOURCE_NAME = "eia_storage"
RAW_DIR = Path("data/raw/eia_storage")
CURATED_PATH = Path("data/curated/eia_storage.parquet")

# 8 years of history gives the dashboard 5+ years for the seasonal envelope.
# EIA returns up to 5000 rows per call; 8y × 52w × 5 regions ≈ 2,080 rows.
START_DATE = "2018-01-01"


def _get_latest_curated_date(curated_path: Path | None = None) -> str | None:
    """Return the newest period in curated parquet, or None if missing."""
    p = curated_path or CURATED_PATH
    if not p.exists():
        return None
    try:
        import pandas as pd

        df = pd.read_parquet(p, columns=["period"])
        if df.empty:
            return None
        return str(df["period"].max())
    except Exception:
        return None


def _get_latest_local_date() -> str | None:
    """Find the most recent period actually present in the newest downloaded payload.

    Why:
        Filenames can claim a newer date than their contents (e.g. the 2026-08-25
        fetch named eia_storage_2026-08-21.json whose payload only ran through
        2026-08-14). Deriving freshness from payload content prevents silent
        indefinite skip-locks (Prompt T §02).
    """
    path = _get_latest_local_path()
    if not path or not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data.get("response", {}).get("data", [])
        if not rows:
            return None
        periods = [r["period"] for r in rows if isinstance(r, dict) and "period" in r]
        return max(periods) if periods else None
    except Exception:
        return None


def _get_latest_local_path() -> Path | None:
    """Return the Path of the most recent stored JSON file, or None."""
    if not RAW_DIR.exists():
        return None
    files = list(RAW_DIR.rglob("eia_storage_*.json"))
    if not files:
        return None
    return max(files, key=lambda p: p.stem)


def _count_existing_rows(path: Path | None) -> int:
    """Return row count from the latest stored JSON, or 0 if missing."""
    if not path or not path.exists():
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return len(data.get("response", {}).get("data", []))
    except Exception:
        return 0


async def run() -> dict[str, Any]:
    """Fetch latest weekly natural gas storage data from EIA."""
    health = HealthWriter(source_name=SOURCE_NAME)
    try:
        api_key = load_api_key_from_env()
    except RuntimeError as exc:
        health.record_failure(error=str(exc))
        return {"status": "failed", "error": str(exc)}

    async with EIAClient(api_key=api_key) as client:
        try:
            latest_api_date = await client.get_latest_date(route=ROUTE, frequency="weekly")
        except Exception as exc:
            err = f"API error: {exc}"
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

        if not latest_api_date:
            err = "No data returned from EIA API latest-date check"
            health.record_failure(error=err)
            return {"status": "failed", "error": err}

        # CI CAVEAT (Prompt U §05):
        # data/raw/ is gitignored (.gitignore:24) and zero raw files are tracked in git.
        # Every CI run starts with an empty data/raw/ directory on the ephemeral runner.
        # As a result, _get_latest_local_date() always returns None in CI, so this
        # staleness skip gate cannot fire in CI and record_skipped() is unreachable there.
        # Every scheduled CI run proceeds to fetch. This gate only protects local
        # developer environments with retained raw files.
        latest_local = _get_latest_local_date()
        latest_existing_path = _get_latest_local_path()
        existing_rows = _count_existing_rows(latest_existing_path)

        # Staleness gate: skip only when the latest date is unchanged AND the
        # existing row count indicates a full backfill has already been done.
        # The 500-row threshold distinguishes the old 52-week fetch (~416 rows)
        # from the new 8-year backfill (~2,080 rows).
        # - First run post-commit: 416 rows < 500 → bypasses skip → fetches 2,080 rows.
        # - Subsequent same-date runs: rows >= 500 and date matches → skips cleanly.
        if latest_local == latest_api_date and existing_rows >= 500:
            health.record_skipped(reason=f"no new data since {latest_api_date}")
            return {"status": "skipped", "latest_date": latest_api_date}

        if latest_local == latest_api_date and existing_rows < 500:
            # Date unchanged but history is sparse — force a backfill re-fetch.
            log.info(
                "Sparse history detected (%d rows < 500). Re-fetching full backfill.",
                existing_rows,
            )

        data_to_write: dict[str, Any] | None = None

        async def compute_data() -> dict[str, Any]:
            nonlocal data_to_write
            data_to_write = await client.get_series(
                route=ROUTE,
                frequency="weekly",
                start=START_DATE,
                data_columns=["value"],
                length=5000,  # 8y × 52w × 5 regions ≈ 2,080 rows; well within EIA max
            )
            return data_to_write

        try:
            dt = datetime.strptime(latest_api_date, "%Y-%m-%d")
        except ValueError:
            dt = datetime.now()

        out_path = (
            RAW_DIR / str(dt.year) / f"{dt.month:02d}" / f"eia_storage_{latest_api_date}.json"
        )

        writer = StatePreservingWriter(source_name=SOURCE_NAME, writer=safe_write_json)
        success = await writer.guarded_write(out_path, compute_data)

        if success and data_to_write:
            # Overwrite the default health record with metadata
            resp_obj = data_to_write.get("response", {})
            rows_count = len(resp_obj.get("data", [])) if isinstance(resp_obj, dict) else 0

            # Determine whether this run advanced the dataset or was a no-op (Prompt U §05)
            newest_period = None
            if isinstance(resp_obj, dict):
                raw_rows = resp_obj.get("data", [])
                periods = [r["period"] for r in raw_rows if isinstance(r, dict) and "period" in r]
                if periods:
                    newest_period = max(periods)

            effective_latest = newest_period or latest_api_date
            latest_curated = _get_latest_curated_date()

            if latest_curated and effective_latest and effective_latest <= latest_curated:
                # Fetch completed, but latest period is unchanged from what curated already holds.
                reason = (
                    f"fetch completed ({rows_count} rows) but newest period "
                    f"{effective_latest} <= curated {latest_curated}"
                )
                health.record_no_op(
                    reason=reason,
                    metadata={
                        "latest_date": effective_latest,
                        "curated_latest": latest_curated,
                        "rows": rows_count,
                    },
                )
                return {
                    "status": "no_op",
                    "latest_date": effective_latest,
                    "curated_latest": latest_curated,
                    "rows": rows_count,
                    "path": str(out_path),
                }

            health.record_success(metadata={"latest_date": effective_latest, "rows": rows_count})
            return {
                "status": "ok",
                "latest_date": effective_latest,
                "rows": rows_count,
                "path": str(out_path),
            }

        return {"status": "failed"}


if __name__ == "__main__":
    result = asyncio.run(run())
    print(json.dumps(result, indent=2))
