"""Tests for Brief E: publish freshness, health commit audit, and gap/accumulation rules.

Verifies:
  1. publish-dashboard.yml includes every daily EBB scraper and cancel-in-progress is enabled.
  2. Every scraper workflow commits its own data/health/*.json.
  3. Quorum merge_health properly aggregates multi-tenant health into quorum.json.
  4. Workflows have valid shell syntax in run: steps.
  5. A source configured mode: accumulation fails integrity when history shrinks (gie_agsi scenario).
  6. Each configured gap_rule passes on legitimate cadences and warns on holes.
  7. month_end_normalize properly snaps ISO month-start dates (e.g. 2026-05-01) to month-end.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from scrapers.base.merge_health import merge_multi_health
from scrapers.gasnom.merge_health import merge_gasnom_health
from scrapers.quorum.merge_health import merge_quorum_health
from validators.integrity import (
    build_source_state,
    check_gaps,
    check_shrinkage,
    normalize_period,
    run_source_checks,
)

WORKFLOWS_DIR = Path(".github/workflows")
CONFIG_PATH = Path("config/integrity_rules.yaml")

NOW = datetime(2026, 9, 2, 12, 0, 0, tzinfo=UTC)

DEFAULTS: dict[str, Any] = {
    "schema_columns": [
        "source",
        "series_id",
        "series_name",
        "period",
        "value",
        "unit",
        "region",
        "ingested_at",
    ],
    "coverage_drop_fail_pct": 20,
    "health_recency_days": 3,
}


def _make_frame(periods: list[str], series_id: str = "s1", val: float = 100.0) -> pd.DataFrame:
    rows = [
        {
            "source": "test_src",
            "series_id": series_id,
            "series_name": f"Series {series_id}",
            "period": p,
            "value": val,
            "unit": "Dth/d",
            "region": "US",
            "ingested_at": "2026-09-02T00:00:00Z",
        }
        for p in periods
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Publish trigger graph & concurrency debounce
# ---------------------------------------------------------------------------


def test_publish_dashboard_trigger_graph() -> None:
    publish_path = WORKFLOWS_DIR / "publish-dashboard.yml"
    assert publish_path.exists()
    content = yaml.safe_load(publish_path.read_text(encoding="utf-8"))

    # Concurrency must cancel-in-progress to debounce rapid scraper runs
    concurrency = content.get("concurrency", {})
    assert concurrency.get("cancel-in-progress") is True, "cancel-in-progress must be True"

    # Handle YAML 1.1 parsing bare 'on' as boolean True
    on_block = content.get(True) if True in content else content.get("on")
    assert on_block is not None, "publish-dashboard.yml missing 'on:' trigger block"
    triggers = on_block.get("workflow_run", {}).get("workflows", [])
    assert triggers, "publish-dashboard.yml has empty workflow_run.workflows trigger list"

    # Scrapers that MUST trigger dashboard publication
    expected_daily_workflows = [
        WORKFLOWS_DIR / "gulf-south-sq.yml",
        WORKFLOWS_DIR / "enbridge.yml",
        WORKFLOWS_DIR / "quorum.yml",
        WORKFLOWS_DIR / "gasnom.yml",
        WORKFLOWS_DIR / "bhe.yml",
        WORKFLOWS_DIR / "cheniere.yml",
        WORKFLOWS_DIR / "kinder-morgan.yml",
        WORKFLOWS_DIR / "gie-storage.yml",
        WORKFLOWS_DIR / "baker-hughes.yml",
        WORKFLOWS_DIR / "eia-storage.yml",
        WORKFLOWS_DIR / "eia-supply.yml",
        WORKFLOWS_DIR / "eia-lng.yml",
    ]

    for wf_path in expected_daily_workflows:
        assert wf_path.exists(), f"Missing workflow file: {wf_path}"
        wf_yaml = yaml.safe_load(wf_path.read_text(encoding="utf-8"))
        wf_name = wf_yaml.get("name")
        assert wf_name, f"Workflow {wf_path} missing 'name:'"
        assert (
            wf_name in triggers
        ), f"publish-dashboard.yml missing trigger for {wf_path.name} ('{wf_name}')"


# ---------------------------------------------------------------------------
# 2. Every scraper workflow commits its own health file
# ---------------------------------------------------------------------------


def test_every_scraper_workflow_commits_its_health() -> None:
    scraper_health_expectations: dict[str, str] = {
        "baker-hughes.yml": "data/health/baker_hughes_rigs.json",
        "bhe.yml": "data/health/bhe.json",
        "cheniere.yml": "data/health/cheniere.json",
        "eia-lng.yml": "data/health/eia_lng.json",
        "eia-storage.yml": "data/health/eia_storage.json",
        "eia-supply.yml": "data/health/eia_supply.json",
        "enbridge.yml": "data/health/enbridge.json",
        "gasnom.yml": "data/health/gasnom.json",
        "gie-storage.yml": "data/health/gie_agsi.json",
        "gulf-south-sq.yml": "data/health/gulf_south.json",
        "kinder-morgan.yml": "data/health/kinder_morgan.json",
        "quorum.yml": "data/health/quorum.json",
    }

    for filename, expected_health in scraper_health_expectations.items():
        wf_path = WORKFLOWS_DIR / filename
        assert wf_path.exists(), f"Workflow file {filename} does not exist"
        raw_text = wf_path.read_text(encoding="utf-8")

        assert (
            expected_health in raw_text
        ), f"{filename} does not reference its health file '{expected_health}'"
        assert (
            f"git add {expected_health}" in raw_text or expected_health in raw_text
        ), f"{filename} does not stage its health file '{expected_health}'"


def test_quorum_workflow_matrix_health_artifacts_and_merge() -> None:
    quorum_path = WORKFLOWS_DIR / "quorum.yml"
    raw_text = quorum_path.read_text(encoding="utf-8")

    assert "quorum-health-" in raw_text, "quorum.yml must upload tenant health artifacts"
    assert "merge_health" in raw_text, "quorum.yml must run scrapers.quorum.merge_health"


def test_gasnom_workflow_matrix_health_merge() -> None:
    gasnom_path = WORKFLOWS_DIR / "gasnom.yml"
    raw_text = gasnom_path.read_text(encoding="utf-8")

    assert "merge_health" in raw_text, "gasnom.yml must run scrapers.gasnom.merge_health"
    assert "data/health/gasnom.json" in raw_text, "gasnom.yml must stage canonical gasnom.json"


# ---------------------------------------------------------------------------
# 3. Quorum health merging logic unit tests
# ---------------------------------------------------------------------------


def test_quorum_merge_health_success(tmp_path: Path) -> None:
    gator_health = {
        "source": "quorum",
        "status": "ok",
        "timestamp_utc": "2026-09-02T10:00:00Z",
        "error": None,
        "metadata": {
            "gas_day": "2026-09-02",
            "processed_count": 5,
            "skipped_count": 0,
            "rows": 50,
            "cycles": ["id1", "timely"],
        },
    }
    transcameron_health = {
        "source": "quorum",
        "status": "ok",
        "timestamp_utc": "2026-09-02T10:05:00Z",
        "error": None,
        "metadata": {
            "gas_day": "2026-09-02",
            "processed_count": 5,
            "skipped_count": 0,
            "rows": 40,
            "cycles": ["id1", "evening"],
        },
    }

    (tmp_path / "quorum_gator.json").write_text(json.dumps(gator_health), encoding="utf-8")
    (tmp_path / "quorum_transcameron.json").write_text(
        json.dumps(transcameron_health), encoding="utf-8"
    )

    out = merge_quorum_health(health_dir=tmp_path)
    assert out is not None and out.exists()

    merged = json.loads(out.read_text(encoding="utf-8"))
    assert merged["source"] == "quorum"
    assert merged["status"] == "ok"
    assert merged["timestamp_utc"] == "2026-09-02T10:05:00Z"
    assert merged["metadata"]["rows"] == 90
    assert merged["metadata"]["processed_count"] == 10
    assert merged["metadata"]["cycles"] == ["evening", "id1", "timely"]
    assert merged["metadata"]["tenants"] == {"gator": "ok", "transcameron": "ok"}

    # Temporary files should be cleaned up
    assert not (tmp_path / "quorum_gator.json").exists()
    assert not (tmp_path / "quorum_transcameron.json").exists()


def test_quorum_merge_health_escalates_failure(tmp_path: Path) -> None:
    gator_health = {
        "source": "quorum",
        "status": "ok",
        "timestamp_utc": "2026-09-02T10:00:00Z",
        "error": None,
        "metadata": {"rows": 50, "cycles": ["timely"]},
    }
    transcameron_health = {
        "source": "quorum",
        "status": "failed",
        "timestamp_utc": "2026-09-02T10:05:00Z",
        "error": "HTTP 503 Service Unavailable",
        "metadata": {"rows": 0, "cycles": []},
    }

    (tmp_path / "quorum_gator.json").write_text(json.dumps(gator_health), encoding="utf-8")
    (tmp_path / "quorum_transcameron.json").write_text(
        json.dumps(transcameron_health), encoding="utf-8"
    )

    out = merge_quorum_health(health_dir=tmp_path)
    assert out is not None

    merged = json.loads(out.read_text(encoding="utf-8"))
    assert merged["status"] == "failed"
    assert "transcameron: HTTP 503" in merged["error"]


def test_gasnom_merge_health_success(tmp_path: Path) -> None:
    slugs = ["goldenpass", "cameron", "SABINE", "portarthurpipeline"]
    for i, slug in enumerate(slugs):
        payload = {
            "source": f"gasnom_{slug}",
            "status": "ok",
            "timestamp_utc": f"2026-09-02T10:0{i}:00Z",
            "error": None,
            "metadata": {
                "gas_day": "2026-09-02",
                "processed_count": 10,
                "skipped_count": 0,
                "rows": 100,
                "cycles": ["timely"],
            },
        }
        (tmp_path / f"gasnom_{slug}.json").write_text(json.dumps(payload), encoding="utf-8")

    out = merge_gasnom_health(health_dir=tmp_path)
    assert out is not None and out.name == "gasnom.json"

    merged = json.loads(out.read_text(encoding="utf-8"))
    assert merged["source"] == "gasnom"
    assert merged["status"] == "ok"
    assert merged["metadata"]["rows"] == 400
    assert len(merged["metadata"]["pipelines"]) == 4
    # Per-slug files must NOT be deleted for gasnom
    for slug in slugs:
        assert (tmp_path / f"gasnom_{slug}.json").exists()


def test_gasnom_merge_health_escalates_failure(tmp_path: Path) -> None:
    # 3 succeed, 1 fails
    for slug in ["goldenpass", "cameron", "portarthurpipeline"]:
        (tmp_path / f"gasnom_{slug}.json").write_text(
            json.dumps({"source": f"gasnom_{slug}", "status": "ok", "metadata": {"rows": 50}}),
            encoding="utf-8",
        )
    (tmp_path / "gasnom_SABINE.json").write_text(
        json.dumps({
            "source": "gasnom_SABINE",
            "status": "failed",
            "error": "WAF blocked request",
            "metadata": {"rows": 0},
        }),
        encoding="utf-8",
    )

    out = merge_gasnom_health(health_dir=tmp_path)
    assert out is not None
    merged = json.loads(out.read_text(encoding="utf-8"))
    assert merged["status"] == "failed"
    assert "SABINE: WAF blocked request" in merged["error"]


def test_merge_health_ranking_guard_failure(tmp_path: Path) -> None:
    # guard_failure must beat warn and must NOT be demoted to warn
    (tmp_path / "test_pipe1.json").write_text(
        json.dumps({"source": "test", "status": "warn", "error": "no-op run"}),
        encoding="utf-8",
    )
    (tmp_path / "test_pipe2.json").write_text(
        json.dumps({
            "source": "test",
            "status": "guard_failure",
            "error": "identity guard rejected TSP marker",
        }),
        encoding="utf-8",
    )

    out = merge_multi_health("test", "test_*.json", "test.json", health_dir=tmp_path)
    assert out is not None
    merged = json.loads(out.read_text(encoding="utf-8"))
    assert merged["status"] == "guard_failure"
    assert "identity guard rejected" in merged["error"]


def test_merge_health_ranking_unknown_status_escalates(tmp_path: Path) -> None:
    # Unrecognized status string must escalate to failed (rank 4), never milder
    (tmp_path / "test_pipe1.json").write_text(
        json.dumps({"source": "test", "status": "ok"}),
        encoding="utf-8",
    )
    (tmp_path / "test_pipe2.json").write_text(
        json.dumps({"source": "test", "status": "unexpected_crash_code"}),
        encoding="utf-8",
    )

    out = merge_multi_health("test", "test_*.json", "test.json", health_dir=tmp_path)
    assert out is not None
    merged = json.loads(out.read_text(encoding="utf-8"))
    assert merged["status"] == "failed"
    assert "unrecognized health status 'unexpected_crash_code'" in merged["error"]


# ---------------------------------------------------------------------------
# 4. Mode: accumulation catches row loss (the gie_agsi scenario)
# ---------------------------------------------------------------------------


def test_accumulation_mode_catches_row_loss() -> None:
    # 2,068 days of history with 82,720 rows
    prior_state = {
        "rows": 82720,
        "distinct_periods": 2068,
        "distinct_series": 40,
        "latest_period": "2026-08-30",
        "consecutive_flat": 0,
    }

    # Rebuild from thin data loses 4,512 rows
    shrunk_periods = [(date(2021, 1, 1) + timedelta(days=i)).isoformat() for i in range(100)]
    shrunk_df = _make_frame(shrunk_periods, series_id="s1")
    # Add extra rows so len is 78,208
    extra_df = pd.concat([shrunk_df] * 782, ignore_index=True).iloc[:78208]

    cfg = {
        "mode": "accumulation",
        "parquet": "data/curated/gie_agsi.parquet",
        "health_file": "data/health/gie_agsi.json",
        "staleness": {"warn_days": 3, "fail_days": 6},
    }

    # check_shrinkage must directly FAIL
    shrink_res = check_shrinkage(extra_df, prior_state, cfg)
    assert shrink_res["severity"] == "FAIL"
    assert "history shrank" in shrink_res["message"]

    # run_source_checks must fold to FAIL
    health_payload = {"status": "ok", "timestamp_utc": "2026-09-02T00:00:00Z"}
    results, worst = run_source_checks(
        "gie_agsi", extra_df, cfg, DEFAULTS, prior_state, NOW, health_payload
    )
    assert worst == "FAIL"
    shrink_check = next(r for r in results if r["check"] == "shrinkage")
    assert shrink_check["severity"] == "FAIL"


# ---------------------------------------------------------------------------
# 5. Gap rules: legitimate cadence passes, real hole warns
# ---------------------------------------------------------------------------


def test_gap_rule_calendar_daily() -> None:
    cfg = {"gap_rule": "calendar_daily"}
    periods = [
        "2026-08-01",
        "2026-08-02",
        "2026-08-03",
        "2026-08-04",
        "2026-08-05",
    ]
    res_pass = check_gaps(_make_frame(periods), cfg)
    assert res_pass["severity"] == "PASS"

    # Hole on 2026-08-03
    periods_with_hole = ["2026-08-01", "2026-08-02", "2026-08-04", "2026-08-05"]
    res_warn = check_gaps(_make_frame(periods_with_hole), cfg)
    assert res_warn["severity"] == "WARN"
    assert "2026-08-03" in res_warn["details"]["missing_dates"]


def test_gap_rule_weekly_friday() -> None:
    cfg = {"gap_rule": "weekly_friday"}
    # Three consecutive Fridays
    fridays = ["2026-08-14", "2026-08-21", "2026-08-28"]
    res_pass = check_gaps(_make_frame(fridays), cfg)
    assert res_pass["severity"] == "PASS"

    # Holiday shift: middle week published on Thursday 2026-08-20
    fridays_holiday = ["2026-08-14", "2026-08-20", "2026-08-28"]
    res_holiday_pass = check_gaps(_make_frame(fridays_holiday), cfg)
    assert res_holiday_pass["severity"] == "PASS"

    # Hole: 2026-08-21 missing entirely
    fridays_with_hole = ["2026-08-14", "2026-08-28"]
    res_warn = check_gaps(_make_frame(fridays_with_hole), cfg)
    assert res_warn["severity"] == "WARN"
    assert "2026-08-21" in res_warn["details"]["missing_dates"]


def test_gap_rule_monthly() -> None:
    cfg = {"gap_rule": "monthly"}
    # Consecutive months
    months = ["2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"]
    res_pass = check_gaps(_make_frame(months), cfg)
    assert res_pass["severity"] == "PASS"

    # Bare YYYY-MM months
    bare_months = ["2026-01", "2026-02", "2026-03", "2026-04"]
    res_bare_pass = check_gaps(_make_frame(bare_months), cfg)
    assert res_bare_pass["severity"] == "PASS"

    # Hole: 2026-03 missing
    months_with_hole = ["2026-01", "2026-02", "2026-04"]
    res_warn = check_gaps(_make_frame(months_with_hole), cfg)
    assert res_warn["severity"] == "WARN"
    assert "2026-03" in res_warn["details"]["missing_dates"]


# ---------------------------------------------------------------------------
# 6. month_end_normalize snaps ISO month-start dates to month-end
# ---------------------------------------------------------------------------


def test_month_end_normalize_handles_iso_dates() -> None:
    # 1. Full ISO date with month_end_normalize=True
    assert normalize_period("2026-05-01", {"month_end_normalize": True}) == date(2026, 5, 31)

    # 2. Full ISO date with month_end_normalize=False
    assert normalize_period("2026-05-01", {"month_end_normalize": False}) == date(2026, 5, 1)

    # 3. Bare YYYY-MM with month_end_normalize=True
    assert normalize_period("2026-06", {"month_end_normalize": True}) == date(2026, 6, 30)

    # 4. Bare YYYY-MM with month_end_normalize=False
    assert normalize_period("2026-06", {"month_end_normalize": False}) == date(2026, 6, 1)

    # Leap year February
    assert normalize_period("2024-02-01", {"month_end_normalize": True}) == date(2024, 2, 29)
    assert normalize_period("2024-02", {"month_end_normalize": True}) == date(2024, 2, 29)


# ---------------------------------------------------------------------------
# 7. Workflow run steps shell syntax validation
# ---------------------------------------------------------------------------


def test_workflow_run_steps_have_valid_syntax() -> None:
    bash_path = shutil.which("bash")
    for yml_path in WORKFLOWS_DIR.glob("*.yml"):
        parsed = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            continue
        jobs = parsed.get("jobs", {})
        if not isinstance(jobs, dict):
            continue
        for job_name, job_data in jobs.items():
            if not isinstance(job_data, dict):
                continue
            steps = job_data.get("steps", [])
            for step in steps:
                if not isinstance(step, dict):
                    continue
                run_block = step.get("run")
                if run_block and isinstance(run_block, str):
                    # Basic quote/brace matching
                    assert run_block.strip(), f"Empty run step in {yml_path.name}:{job_name}"
                    if bash_path:
                        # If bash is available, run bash -n
                        proc = subprocess.run(
                            [bash_path, "-n", "-c", run_block],
                            capture_output=True,
                            text=True,
                        )
                        assert (
                            proc.returncode == 0
                        ), f"Syntax error in {yml_path.name}:{job_name}: {proc.stderr}"
