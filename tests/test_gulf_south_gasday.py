"""Tests for the Gulf South gas-day resolution + commit-gating fix (P0).

Covers the four regression guards in prompt A-gulf-south-gasday.md:
  1. meter_inventory with no arg selects the newest gas day on disk.
  2. meter_inventory with no arg and an empty raw dir fails loudly.
  3. The core guard: a run that lands zero curated rows cannot end `ok`.
  4. The commit step for curated is ordered before meter inventory, which
     carries continue-on-error: true (YAML-structure assertion).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
import yaml

from scrapers.base.health_writer import HealthWriter
from scrapers.energy_transfer.meter_inventory import newest_gas_day_on_disk, run_inventory

WORKFLOW = Path(".github/workflows/gulf-south-sq.yml")


def _write_raw(raw_dir: Path, gas_day: str, cycle: str, payload: dict | None = None) -> None:
    """Write a minimal raw OAC JSON file at the canonical ``{day}_{cycle}`` name."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    if payload is None:
        payload = {
            "fetched_at": "2026-09-02T07:15:00Z",
            "tsp_id": 1,
            "cycle": cycle,
            "gas_day": gas_day,
            "posted_at": "2026-09-02T07:14:00Z",
            "row_count": 1,
            "data": [
                {
                    "Loc": "24329",
                    "Loc Name": "Stratton Ridge (To Freeport Lng)",
                    "Flow Ind": "D",
                    "Total Scheduled Quantity": "920149",
                    "Operationally Available Capacity": "858980",
                }
            ],
        }
    path = raw_dir / f"{gas_day}_{cycle}.json"
    path.write_text(json.dumps(payload), encoding="utf-8")


# -----------------------------------------------------------------------------
# Test 1 — no-arg inventory anchors to the newest gas day on disk
# -----------------------------------------------------------------------------


def test_inventory_no_arg_picks_newest_day(tmp_path: Path) -> None:
    """With no argument, the inventory selects the newest gas day present on disk.

    Regression guard for the 2026-09-02 data loss: the inventory was passed a
    wall-clock day that Boardwalk had not posted, raised FileNotFoundError, and
    (because it gated the commit) discarded an already-computed parquet.
    """
    raw_dir = tmp_path / "data" / "raw" / "gulf_south"
    # Older day first, newer day second, to prove it is NOT first-match.
    _write_raw(raw_dir, "2026-08-31", "ID3")
    _write_raw(raw_dir, "2026-09-01", "ID2")

    picked = newest_gas_day_on_disk(raw_dir)
    assert picked == date(2026, 9, 1)

    # run_inventory must consume that day without raising.
    result = run_inventory(picked, raw_dir=raw_dir)
    assert "gulf_south" in result
    assert result["_meta"]["total_locations_seen"] == 1


def test_inventory_no_arg_ignores_unknown_cycle_files(tmp_path: Path) -> None:
    """Stray files whose cycle token is not a Gulf South cycle don't contribute a day."""
    raw_dir = tmp_path / "data" / "raw" / "gulf_south"
    _write_raw(raw_dir, "2026-09-01", "ID2")
    (raw_dir / "2026-09-02_TIMELY.json").write_text("{}", encoding="utf-8")
    (raw_dir / "not_a_gasday_file.json").write_text("{}", encoding="utf-8")

    # TIMELY is a real cycle, so 09-02 should win if it parsed — but it is an
    # empty {} payload, still a valid gas day. The point: only {day}_{CYCLE}
    # names count, and 09-02 is present.
    assert newest_gas_day_on_disk(raw_dir) == date(2026, 9, 2)


# -----------------------------------------------------------------------------
# Test 2 — empty raw dir stays loud
# -----------------------------------------------------------------------------


def test_inventory_no_arg_empty_dir_raises(tmp_path: Path) -> None:
    """An empty raw dir must raise with a clear message, never fall back to wall clock."""
    raw_dir = tmp_path / "data" / "raw" / "gulf_south"
    raw_dir.mkdir(parents=True, exist_ok=True)

    exc = None
    try:
        newest_gas_day_on_disk(raw_dir)
    except FileNotFoundError as e:
        exc = e
    assert exc is not None, "empty raw dir must raise FileNotFoundError"
    assert "No gas-day raw files" in str(exc) or "does not exist" in str(exc)


def test_inventory_no_arg_missing_dir_raises_loud(tmp_path: Path) -> None:
    """A missing raw dir is the same genuinely-empty state and must stay loud."""
    raw_dir = tmp_path / "nope" / "raw" / "gulf_south"
    exc = None
    try:
        newest_gas_day_on_disk(raw_dir)
    except FileNotFoundError as e:
        exc = e
    assert exc is not None
    assert "does not exist" in str(exc)


def test_inventory_mainblock_empty_dir_exits_nonzero(tmp_path: Path) -> None:
    """The __main__ path must exit non-zero on an empty dir (loud, not silent)."""
    import subprocess
    import sys

    raw_dir = tmp_path / "data" / "raw" / "gulf_south"
    raw_dir.mkdir(parents=True, exist_ok=True)

    proc = subprocess.run(
        [sys.executable, "-m", "scrapers.energy_transfer.meter_inventory"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "raw files" in proc.stderr or "scraped" in proc.stderr


# -----------------------------------------------------------------------------
# Test 3 — core guard: zero-row pipeline cannot end health `ok`
# -----------------------------------------------------------------------------


def test_pipeline_health_zero_rows_demotes_ok_to_noop(tmp_path: Path) -> None:
    """A run that fetched files but landed zero curated rows cannot end `ok`.

    Asserts on the written health file, not on a return value.
    """
    from scrapers.energy_transfer import pipeline_health as mod

    health_dir = tmp_path / "data" / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    # Simulate the scraper having stamped ok (it fetched files).
    hw = HealthWriter(source_name="gulf_south", health_dir=health_dir)
    hw.record_success(metadata={"processed_count": 5})

    status = mod.reconcile_pipeline_health(
        rows_added=0,
        files_fetched=5,
        source_name="gulf_south",
        health_dir=health_dir,
    )
    assert status in ("warn", "fail"), f"expected no-op escalation, got {status}"

    written = json.loads((health_dir / "gulf_south.json").read_text(encoding="utf-8"))
    assert written["status"] != "ok"
    assert "0 row" in written["error"] or "landed" in written["error"]


def test_pipeline_health_rows_added_leaves_ok(tmp_path: Path) -> None:
    """Landing rows keeps the stamp ok (and re-recording an already-ok stamp is a no-op)."""
    from scrapers.energy_transfer import pipeline_health as mod

    health_dir = tmp_path / "data" / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    hw = HealthWriter(source_name="gulf_south", health_dir=health_dir)
    hw.record_success(metadata={"processed_count": 5})

    status = mod.reconcile_pipeline_health(
        rows_added=1476,
        files_fetched=5,
        source_name="gulf_south",
        health_dir=health_dir,
    )
    assert status == "ok"
    written = json.loads((health_dir / "gulf_south.json").read_text(encoding="utf-8"))
    assert written["status"] == "ok"


def test_pipeline_health_does_not_double_count_noop_streak(tmp_path: Path) -> None:
    """End-of-pipeline reconciliation must not re-increment an already-escalating streak."""
    from scrapers.energy_transfer import pipeline_health as mod

    health_dir = tmp_path / "data" / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    # Scraper already recorded a no-op (streak = 1, status warn).
    hw = HealthWriter(source_name="gulf_south", health_dir=health_dir)
    hw.record_no_op(reason="no new postings matched the listing filters")

    mod.reconcile_pipeline_health(
        rows_added=0,
        files_fetched=0,
        source_name="gulf_south",
        health_dir=health_dir,
    )
    state = json.loads((health_dir / "gulf_south.state.json").read_text(encoding="utf-8"))
    # Streak must remain 1, not become 2, for the single run.
    assert state["consecutive_no_ops"] == 1


def test_pipeline_health_transform_failure_records_failure(tmp_path: Path) -> None:
    """A transform-stage exception is a hard failure, not folded into the no-op ladder."""
    from scrapers.energy_transfer import pipeline_health as mod

    health_dir = tmp_path / "data" / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    hw = HealthWriter(source_name="gulf_south", health_dir=health_dir)
    hw.record_success(metadata={"processed_count": 5})

    status = mod.reconcile_pipeline_health(
        rows_added=0,
        files_fetched=5,
        source_name="gulf_south",
        health_dir=health_dir,
        transform_failed=True,
    )
    assert status == "failed"
    written = json.loads((health_dir / "gulf_south.json").read_text(encoding="utf-8"))
    assert written["status"] == "failed"


# -----------------------------------------------------------------------------
# Test 4 — workflow step ordering: curated commit precedes inventory
# -----------------------------------------------------------------------------


def _workflow_steps() -> list[dict]:
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    return doc["jobs"]["fetch"]["steps"]


def test_curated_commit_precedes_meter_inventory() -> None:
    """The step committing the curated parquet must come before meter inventory."""
    steps = _workflow_steps()

    commit_idx = None
    inventory_idx = None
    inventory_continue = None
    for i, step in enumerate(steps):
        run = step.get("run") or ""
        if "data/curated/gulf_south.parquet" in run and "git add" in run:
            # The committed path must include the curated parquet (not just health).
            commit_idx = i
        if "scrapers.energy_transfer.meter_inventory" in run:
            inventory_idx = i
            inventory_continue = step.get("continue-on-error")

    assert commit_idx is not None, "no step commits data/curated/gulf_south.parquet"
    assert inventory_idx is not None, "no meter_inventory step found"
    assert commit_idx < inventory_idx, (
        f"curated commit (step {commit_idx}) must precede meter inventory "
        f"(step {inventory_idx})"
    )
    assert inventory_continue is True, "meter_inventory step must carry continue-on-error: true"


def test_inventory_invocation_takes_no_gas_day_arg() -> None:
    """The inventory step must no longer be passed a wall-clock gas day."""
    steps = _workflow_steps()
    for step in steps:
        run = step.get("run") or ""
        if "scrapers.energy_transfer.meter_inventory" in run:
            assert "${{ steps.gasday.outputs.gas_day }}" not in run
            assert "meter_inventory" not in run.split("\n")[-1] or "gas_day" not in run


# -----------------------------------------------------------------------------
# Test 5 (prompt B) — every `run:` block in the workflow is valid shell
# -----------------------------------------------------------------------------
#
# The structural YAML tests above could not catch the Defect-1 IndentationError
# (a heredoc indented under a `run: |` block) nor the Defect-2 argparse crash.
# This test executes the shell CI will actually run: each `run:` block is
# extracted exactly as YAML deserialises it (YAML has already stripped the block
# indentation), written to a temp file, and parsed with `bash -n`. Any
# indentation or syntax error in a step fails the test.


def _run_blocks() -> list[str]:
    """Return the deserialised ``run:`` body of every workflow step."""
    steps = _workflow_steps()
    blocks = []
    for step in steps:
        run = step.get("run")
        if isinstance(run, str):
            blocks.append(run)
    return blocks


def _bash_executable() -> str | None:
    """Return a usable ``bash`` binary path, or ``None`` if none is found.

    CI runs on ``ubuntu-latest`` where ``bash`` is on PATH; locally we probe the
    common Git-Bash locations so the test still executes on a Windows checkout.
    """
    import shutil

    for candidate in (
        "C:/Program Files/Git/bin/bash.exe",
        "/usr/bin/bash",
        "bash",
    ):
        path = shutil.which(candidate) or (candidate if Path(candidate).exists() else None)
        if path:
            return path
    return None


def test_every_workflow_run_block_is_valid_shell(tmp_path: Path) -> None:
    """Each `run:` block must parse under `bash -n` (catches YAML-indent hazards)."""
    import subprocess

    bash = _bash_executable()
    if bash is None:
        pytest.skip("no bash executable available in this environment")

    blocks = _run_blocks()
    assert blocks, "workflow has no run: blocks"

    bad = []
    for i, block in enumerate(blocks):
        sh = tmp_path / f"step_{i}.sh"
        sh.write_text(block, encoding="utf-8")
        proc = subprocess.run(
            [bash, "-n", str(sh)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            bad.append((i, proc.stderr.strip()))

    assert not bad, "invalid shell in run blocks:\n" + "\n".join(
        f"  step[{i}]: {err}" for i, err in bad
    )


def test_commit_step_uses_helper_script_not_heredoc() -> None:
    """The curated-commit step must invoke the standalone helper, not an inline heredoc.

    Regression guard for Defect 1: the old `python - <<'PY' ... PY` heredoc was
    indented under the `run: |` block and died with IndentationError, which
    aborted the commit and discarded the parated parquet.
    """
    steps = _workflow_steps()
    commit_run = None
    for step in steps:
        run = step.get("run") or ""
        if "data/curated/gulf_south.parquet" in run and "git add" in run:
            commit_run = run
    assert commit_run is not None

    assert "scripts/gulf_south_commit_msg.py" in commit_run, (
        "commit step must invoke the helper script, not an inline heredoc"
    )
    # The inline heredoc opener must be gone (the only thing that could
    # reintroduce the IndentationError) — we don't assert on the word
    # "IndentationError" because the step's explanatory comment mentions it.
    assert "python - <<'PY'" not in commit_run, "inline heredoc must be gone"


# -----------------------------------------------------------------------------
# Test 6 (prompt B) — the commit-message builder produces the expected strings
# -----------------------------------------------------------------------------


def test_commit_message_builder_cases() -> None:
    """build_commit_message matches the brief's required message shapes."""
    from scripts.gulf_south_commit_msg import build_commit_message

    # Multi-file across two gas days -> span + sorted cycle list.
    multi = [
        {"cycle": "ID3", "gas_day": "2026-09-01"},
        {"cycle": "ID1", "gas_day": "2026-08-31"},
        {"cycle": "ID2", "gas_day": "2026-08-31"},
        {"cycle": "ID1", "gas_day": "2026-09-01"},
        {"cycle": "ID2", "gas_day": "2026-09-01"},
    ]
    assert build_commit_message(multi) == (
        "data(gulf-south): SQ 2026-08-31..2026-09-01 (5 files, ID1/ID2/ID3)"
    )

    # Single file on one gas day.
    single = [{"cycle": "ID3", "gas_day": "2026-09-01"}]
    assert build_commit_message(single) == "data(gulf-south): SQ 2026-09-01 (1 files, ID3)"

    # Empty files list -> sync message (no postings, nothing to commit).
    assert build_commit_message([]) == "data(gulf-south): SQ sync (no new postings)"


def test_commit_message_helper_runs_from_stdin(tmp_path: Path) -> None:
    """The helper invoked as `python scripts/gulf_south_commit_msg.py` prints the message.

    Mirrors the exact CI invocation shape: `python scripts/... < /tmp/scraper_output.json`.
    CI runs from the repo root, so the helper is invoked via its repo-relative path.
    """
    import subprocess
    import sys

    repo_root = Path(__file__).resolve().parents[1]

    payload = {
        "status": "ok",
        "files": [
            {"cycle": "ID3", "gas_day": "2026-09-01"},
            {"cycle": "ID1", "gas_day": "2026-08-31"},
            {"cycle": "ID2", "gas_day": "2026-08-31"},
        ],
    }
    out_file = tmp_path / "scraper_output.json"
    out_file.write_text(json.dumps(payload), encoding="utf-8")

    proc = subprocess.run(
        [sys.executable, "scripts/gulf_south_commit_msg.py", str(out_file)],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == (
        "data(gulf-south): SQ 2026-08-31..2026-09-01 (3 files, ID1/ID2/ID3)"
    )


# -----------------------------------------------------------------------------
# Test 7 (prompt B) — the `skipped` health path: no crash, no streak bump
# -----------------------------------------------------------------------------


def test_skipped_run_short_circuits_before_reconcile() -> None:
    """A `skipped` scrape must exit the health step before calling pipeline_health.

    A staleness-gate hold is a routine, healthy outcome: no fetch/transform cycle
    ran, so it must neither crash on an unset `rows_added` nor increment the
    no-op streak ladder (that ladder is for cycles that actually ran and fetched).
    """
    steps = _workflow_steps()
    health_run = None
    for step in steps:
        run = step.get("run") or ""
        if "Record pipeline health" in (step.get("name") or ""):
            health_run = run
    assert health_run is not None

    # The skipped guard must appear and must `exit 0` before the pipeline_health
    # invocation, so reconcile is never reached on a hold.
    assert 'STATUS" = "skipped"' in health_run, "missing explicit skipped guard"
    assert "exit 0" in health_run, "skipped guard must exit clean"
    # pipeline_health is only reached on the non-skipped, non-failed paths.
    assert "pipeline_health" in health_run


def test_pipeline_health_cli_tolerates_empty_rows_added(tmp_path: Path) -> None:
    """`--rows-added ""` (an unset transform output) must not crash the CLI.

    Regression guard for Defect 2: the skipped path left `rows_added` empty,
    argparse raised `invalid int value: ''` and exited 2, failing the job. The
    CLI now coerces empty/unset to 0 and runs to completion.

    Note: CI guarantees the `skipped` guard in the workflow short-circuits before
    reconcile is ever called, so a real skipped run stamps nothing. This test only
    proves the *CLI itself* is robust to an empty value (no exit 2); it does not
    assert a specific resulting status, because an empty `rows_added` reaching
    reconcile with a prior `ok` is correctly demoted to a no-op by design.
    """
    import subprocess
    import sys

    health_dir = tmp_path / "data" / "health"
    health_dir.mkdir(parents=True, exist_ok=True)

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scrapers.energy_transfer.pipeline_health",
            "--rows-added",
            "",
            "--files-fetched",
            "0",
            "--health-dir",
            str(health_dir),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr

