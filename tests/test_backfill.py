"""Tests for scrapers.energy_transfer.backfill — Gulf South SQ historical backfill.

Test strategy:
    All HTTP traffic is served by httpx.MockTransport (zero live calls).
    Filesystem I/O goes to pytest tmp_path.  Retry/cooldown sleeps are
    injected as zeros via BackfillConfig so tests run instantly.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from scrapers.base.http_client import HttpClient
from scrapers.energy_transfer.backfill import BackfillConfig, GulfSouthBackfill
from scrapers.energy_transfer.gulf_south import REPORTING_BASE_URL

_SINCE = date(2026, 1, 1)

_CSV_TEXT = (
    "\ufeffTSP Name,TSP,Post Date/Time,Effective Gas Day,Effective Time,LineCode,"
    "Loc,Loc Name,Loc Zn,Loc Purp Desc,Loc/QTI,Flow Ind,Design Capacity,"
    "Operating Capacity,Total Scheduled Quantity,Operationally Available Capacity,"
    "IT,All Qty Avail,Quantity Not Available Reason,Meas Basis Desc\n"
    '"Gulf South Pipeline Company, LLC",078444247,20260110 21:53:00,20260110,22:00:00,'
    "625,24329,Stratton Ridge,SYS,Delivery Location,DPQ,D,1779129,1779129,920149,"
    "858980,,Y,,MMBtu\n"
)
_CSV_B64 = base64.b64encode(_CSV_TEXT.encode("utf-8")).decode("ascii")


def _posting(tracker_id: int, gas_day: str, cycle: str) -> dict[str, Any]:
    """Build a minimal posting dict shaped like the live API's entries."""
    return {
        "infoPostName": "Operational Capacity",
        "tspId": 1,
        "datetimePostingEffective": f"{gas_day}T21:54:00+00:00",
        "cycleCode": cycle,
        "description": f"{gas_day} {cycle}",
        "reportFiles": [
            {
                "infoPostDocumentTypeTitle": "CSV Documents",
                "infoPostTrackerID": tracker_id,
                "fileName": f"OACY-1-{tracker_id}.csv",
                "infoPostDocumentOrder": 2,
            }
        ],
    }


class _RecordingHandler:
    """MockTransport handler that routes listing pages and CSV downloads.

    ``csv_scripts`` maps tracker id → status sequence; statuses are consumed
    in order and the final status repeats once the list is exhausted.  The
    pseudo-status ``"bad"`` serves HTTP 200 with an undecodable body.
    """

    def __init__(
        self,
        pages: dict[int, list[dict[str, Any]]],
        csv_scripts: dict[int, list[int | str]] | None = None,
    ) -> None:
        self.pages = pages
        self.csv_scripts = csv_scripts or {}
        self.page_numbers: list[int] = []
        self.csv_calls: list[int] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/infopostdetails"):
            body = json.loads(request.content)
            page_number = int(body.get("pageNumber", 1))
            self.page_numbers.append(page_number)
            return httpx.Response(200, json={"postings": self.pages.get(page_number, [])})
        if path.endswith("/infopost/postings"):
            tracker_id = int(str(request.url.params["postingsDocumentId"]))
            self.csv_calls.append(tracker_id)
            script = self.csv_scripts.get(tracker_id, [])
            index = min(self.csv_calls.count(tracker_id) - 1, len(script) - 1)
            status = script[index] if script else 200
            if status == "bad":
                return httpx.Response(200, content=b"not-base64!!!")
            if status == 200:
                return httpx.Response(200, content=_CSV_B64.encode("ascii"))
            return httpx.Response(status)
        raise AssertionError(f"Unexpected request path: {path}")


def _make_client(handler: Callable[[httpx.Request], httpx.Response]) -> HttpClient:
    """Build an HttpClient whose transport is the mock, keeping base_url."""
    client = HttpClient(base_url=REPORTING_BASE_URL, backoff_base_seconds=0.0)
    client._client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url=REPORTING_BASE_URL,
    )
    return client


def _make_runner(
    tmp_path: Path,
    handler: _RecordingHandler,
    **overrides: Any,
) -> GulfSouthBackfill:
    config = _make_config(tmp_path, **overrides)
    return GulfSouthBackfill(config=config, client=_make_client(handler))


def _make_config(tmp_path: Path, **overrides: Any) -> BackfillConfig:
    defaults: dict[str, Any] = {
        "since": _SINCE,
        "raw_dir": tmp_path / "raw",
        "curated_path": tmp_path / "curated.parquet",
        "csv_retry_delays": (0.0, 0.0, 0.0),
        "persistent_403_cooldown_seconds": 0.0,
        "download_gap_seconds": 0.0,
    }
    defaults.update(overrides)
    return BackfillConfig(**defaults)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pagination_stops_on_empty_page(tmp_path: Path) -> None:
    """A page with zero postings ends pagination; payloads match transformer schema."""
    handler = _RecordingHandler(
        {
            1: [_posting(101, "2026-01-10", "TIMELY"), _posting(102, "2026-01-09", "EVENING")],
            2: [],
        }
    )
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert handler.page_numbers == [1, 2]
    assert summary["pages_paged"] == 2
    assert summary["postings_seen"] == 2
    assert summary["fetched"] == 2
    assert sorted(handler.csv_calls) == [101, 102]
    assert summary["oldest_gas_day_fetched"] == "2026-01-09"
    assert summary["newest_gas_day_fetched"] == "2026-01-10"

    payload = json.loads((tmp_path / "raw" / "2026-01-10_TIMELY.json").read_text(encoding="utf-8"))
    assert set(payload) == {
        "fetched_at",
        "tsp_id",
        "cycle",
        "gas_day",
        "posted_at",
        "row_count",
        "data",
    }
    assert payload["tsp_id"] == 1
    assert payload["cycle"] == "TIMELY"
    assert payload["gas_day"] == "2026-01-10"
    assert payload["posted_at"] == "2026-01-10T21:54:00+00:00"
    assert payload["fetched_at"].endswith("Z")
    assert payload["row_count"] == len(payload["data"]) == 1
    assert payload["data"][0]["Loc"] == "24329"


@pytest.mark.asyncio
async def test_stops_when_posting_older_than_since(tmp_path: Path) -> None:
    """First posting older than --since terminates the walk before any download."""
    handler = _RecordingHandler(
        {1: [_posting(201, "2025-12-31", "TIMELY"), _posting(202, "2025-12-30", "ID1")]}
    )
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert handler.page_numbers == [1]
    assert handler.csv_calls == []
    assert summary["fetched"] == 0
    assert summary["pages_paged"] == 1


@pytest.mark.asyncio
async def test_curated_pair_skips_download(tmp_path: Path) -> None:
    """(gas_day, cycle) present in curated parquet → no HTTP call for its tracker."""
    curated = tmp_path / "curated.parquet"
    pd.DataFrame(
        {"series_id": ["gulf_south_sq_24329_timely"], "period": ["2026-01-05"]}
    ).to_parquet(curated)

    handler = _RecordingHandler(
        {1: [_posting(301, "2026-01-05", "TIMELY"), _posting(302, "2026-01-04", "ID1")]}
    )
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert 301 not in handler.csv_calls
    assert handler.csv_calls == [302]
    assert summary["skipped_curated"] == 1
    assert summary["fetched"] == 1
    assert not (tmp_path / "raw" / "2026-01-05_TIMELY.json").exists()
    assert (tmp_path / "raw" / "2026-01-04_ID1.json").exists()


@pytest.mark.asyncio
async def test_existing_raw_file_skips_download(tmp_path: Path) -> None:
    """Raw file already on disk → tracker never requested; counted as skipped."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    (raw_dir / "2026-01-06_TIMELY.json").write_text("{}", encoding="utf-8")

    handler = _RecordingHandler({1: [_posting(311, "2026-01-06", "TIMELY")]})
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert handler.csv_calls == []
    assert summary["skipped_raw_file"] == 1
    assert summary["fetched"] == 0


@pytest.mark.asyncio
async def test_transient_403_retried_then_success(tmp_path: Path) -> None:
    """403 twice then 200 → download succeeds and exactly 2 retries recorded."""
    handler = _RecordingHandler(
        {1: [_posting(401, "2026-01-03", "TIMELY")]},
        csv_scripts={401: [403, 403, 200]},
    )
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert handler.csv_calls.count(401) == 3
    assert runner.retries_403 == 2
    assert summary["errors_403"] == 0
    assert summary["fetched"] == 1
    assert (tmp_path / "raw" / "2026-01-03_TIMELY.json").exists()


@pytest.mark.asyncio
async def test_persistent_403_skips_and_continues(tmp_path: Path) -> None:
    """Exhausted 403 retries skip the posting but the walk continues."""
    handler = _RecordingHandler(
        {1: [_posting(501, "2026-01-02", "TIMELY"), _posting(502, "2026-01-01", "EVENING")]},
        csv_scripts={501: [403]},
    )
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert handler.csv_calls.count(501) == 4
    assert summary["errors_403"] == 1
    assert summary["fetched"] == 1
    assert not (tmp_path / "raw" / "2026-01-02_TIMELY.json").exists()
    assert (tmp_path / "raw" / "2026-01-01_EVENING.json").exists()


@pytest.mark.asyncio
async def test_resume_from_checkpoint(tmp_path: Path) -> None:
    """Matching checkpoint skips processed trackers and resumes at next_page."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    state = {
        "since": _SINCE.isoformat(),
        "next_page": 2,
        "processed_trackers": [601],
        "skipped_existing": 0,
        "fetched": 0,
        "errors_403": 0,
        "updated_at": "2026-08-01T00:00:00Z",
    }
    (raw_dir / "_backfill_state.json").write_text(json.dumps(state), encoding="utf-8")

    handler = _RecordingHandler(
        {
            1: [_posting(601, "2026-01-06", "TIMELY")],
            2: [_posting(602, "2026-01-05", "ID1")],
        }
    )
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert handler.page_numbers == [2, 3]
    assert handler.csv_calls == [602]
    assert summary["fetched"] == 1

    final_state = json.loads((raw_dir / "_backfill_state.json").read_text(encoding="utf-8"))
    assert final_state["next_page"] == 3
    assert set(final_state["processed_trackers"]) == {601, 602}


@pytest.mark.asyncio
async def test_resume_past_fully_seen_page_advances_checkpoint(tmp_path: Path) -> None:
    """Repeat-guard over a checkpointed fully-seen page must persist next_page+1.

    Regression: a checkpoint with next_page=2 whose page-2 documents are all in
    processed_trackers used to stop defensively and re-persist next_page=2,
    silently truncating every future resume.
    """
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    state = {
        "since": _SINCE.isoformat(),
        "next_page": 2,
        "processed_trackers": [901],
        "skipped_existing": 0,
        "fetched": 0,
        "errors_403": 0,
        "updated_at": "2026-08-01T00:00:00Z",
    }
    (raw_dir / "_backfill_state.json").write_text(json.dumps(state), encoding="utf-8")

    handler = _RecordingHandler({2: [_posting(901, "2026-01-04", "TIMELY")]})
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert handler.page_numbers == [2]
    assert handler.csv_calls == []
    assert summary["fetched"] == 0

    final_state = json.loads((raw_dir / "_backfill_state.json").read_text(encoding="utf-8"))
    assert final_state["next_page"] == 3


@pytest.mark.asyncio
async def test_poisoned_200_body_contained(tmp_path: Path) -> None:
    """HTTP 200 with an undecodable body skips that posting; the walk continues."""
    handler = _RecordingHandler(
        {
            1: [
                _posting(1001, "2026-01-08", "TIMELY"),
                _posting(1002, "2026-01-07", "EVENING"),
            ]
        },
        csv_scripts={1001: ["bad"]},
    )
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run()

    assert sorted(handler.csv_calls) == [1001, 1002]
    assert summary["errors_decode"] == 1
    assert summary["errors_403"] == 0
    assert summary["fetched"] == 1
    assert not (tmp_path / "raw" / "2026-01-08_TIMELY.json").exists()
    assert (tmp_path / "raw" / "2026-01-07_EVENING.json").exists()


@pytest.mark.asyncio
async def test_mismatched_since_starts_fresh(tmp_path: Path) -> None:
    """Checkpoint whose --since differs is ignored; walk starts at page 1."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    state = {
        "since": "2025-06-01",
        "next_page": 5,
        "processed_trackers": [601],
        "skipped_existing": 0,
        "fetched": 0,
        "errors_403": 0,
        "updated_at": "2026-08-01T00:00:00Z",
    }
    (raw_dir / "_backfill_state.json").write_text(json.dumps(state), encoding="utf-8")

    handler = _RecordingHandler({1: [_posting(701, "2026-01-06", "TIMELY")]})
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        await runner.run()

    assert handler.page_numbers == [1, 2]
    assert handler.csv_calls == [701]


@pytest.mark.asyncio
async def test_fresh_flag_deletes_checkpoint(tmp_path: Path) -> None:
    """--fresh removes a matching checkpoint so previously-seen trackers refetch."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True)
    state = {
        "since": _SINCE.isoformat(),
        "next_page": 2,
        "processed_trackers": [801],
        "skipped_existing": 0,
        "fetched": 0,
        "errors_403": 0,
        "updated_at": "2026-08-01T00:00:00Z",
    }
    (raw_dir / "_backfill_state.json").write_text(json.dumps(state), encoding="utf-8")

    handler = _RecordingHandler(
        {
            1: [_posting(801, "2026-01-06", "TIMELY")],
            2: [_posting(802, "2026-01-05", "ID1")],
        }
    )
    runner = _make_runner(tmp_path, handler)

    async with runner.client:
        summary = await runner.run(fresh=True)

    assert handler.page_numbers == [1, 2, 3]
    assert handler.csv_calls == [801, 802]
    assert summary["fetched"] == 2
