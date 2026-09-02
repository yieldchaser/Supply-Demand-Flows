"""Tests for validators.integrity — dataset-integrity checks.

Zero network, zero disk (except tmp_path for state JSON); ``now`` is always
injected explicitly so no freezegun is needed.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from validators.integrity import (
    build_source_state,
    check_coverage,
    check_divergence,
    check_gaps,
    check_schema,
    check_shrinkage,
    check_stagnation,
    check_value_sanity,
    load_state,
    normalize_period,
    run_source_checks,
)
from validators.run_integrity import main as run_integrity_main
from validators.run_integrity import render_table

NOW = datetime(2026, 8, 23, 12, 0, 0, tzinfo=UTC)
RULES_PATH = Path(__file__).resolve().parents[1] / "config" / "integrity_rules.yaml"

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


def day(offset: int) -> str:
    """ISO date ``offset`` days before NOW."""
    return (NOW.date() - timedelta(days=offset)).isoformat()


def daily_periods(days: int, skip: set[int] | None = None) -> list[str]:
    """Last ``days`` calendar days ending today, skipping the given offsets."""
    skip = skip or set()
    return [day(offset) for offset in range(days) if offset not in skip]


def make_cfg(**over: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "parquet": "data/curated/test_src.parquet",
        "health_file": "data/health/test_src.json",
        "staleness": {"warn_days": 2, "fail_days": 4},
        "unit_expected": "Dth/d",
        "negative_values_fail": True,
        "gap_rule": "calendar_daily",
    }
    cfg.update(over)
    return cfg


BAND_CFG = make_cfg(
    bands=[
        {
            "label": "freeport_lng_feedgas",
            "series_glob": "gulf_south_sq_24329_*",
            "min": 0.0,
            "max": 2860000.0,
        }
    ]
)


def make_frame(
    periods: list[str],
    *,
    series: list[str] | None = None,
    value: float = 100.0,
    unit: str = "Dth/d",
) -> pd.DataFrame:
    series = series or ["s1"]
    rows = [
        {
            "source": "test_src",
            "series_id": s,
            "series_name": f"name-{s}",
            "period": p,
            "value": value,
            "unit": unit,
            "region": "US",
            "ingested_at": NOW.isoformat(),
        }
        for p in periods
        for s in series
    ]
    return pd.DataFrame(rows)


# --------------------------------------------------------------------- schema


class TestSchema:
    def test_missing_column_fails(self) -> None:
        df = make_frame([day(1)]).drop(columns=["unit"])
        res = check_schema(df, make_cfg(), DEFAULTS)
        assert res["severity"] == "FAIL"
        assert res["check"] == "schema"
        assert "unit" in res["message"]

    def test_non_numeric_value_fails(self) -> None:
        df = make_frame([day(1)])
        df["value"] = df["value"].astype(str)
        res = check_schema(df, make_cfg(), DEFAULTS)
        assert res["severity"] == "FAIL"
        assert "non-numeric" in res["message"]

    def test_unparseable_period_fails(self) -> None:
        df = make_frame(["not-a-date"])
        res = check_schema(df, make_cfg(), DEFAULTS)
        assert res["severity"] == "FAIL"
        assert "not-a-date" in res["message"]

    def test_wrong_unit_fails(self) -> None:
        df = make_frame([day(1)], unit="Mcf/d")
        res = check_schema(df, make_cfg(), DEFAULTS)
        assert res["severity"] == "FAIL"
        assert "Mcf/d" in res["message"]

    def test_happy_passes(self) -> None:
        df = make_frame(daily_periods(3), series=["s1", "s2"])
        res = check_schema(df, make_cfg(), DEFAULTS)
        assert res["severity"] == "PASS"
        assert res["details"]["rows"] == 6


# --------------------------------------------------------------------- sanity


class TestValueSanity:
    def test_over_band_fails_with_offender_text(self) -> None:
        df = make_frame(daily_periods(3), series=["gulf_south_sq_24329_ID1"])
        df.loc[df.index[0], "value"] = 3_000_000.0
        res = check_value_sanity(df, BAND_CFG)
        assert res["severity"] == "FAIL"
        assert "gulf_south_sq_24329_ID1@" in res["message"]
        assert "3000000.0" in res["message"]

    def test_zero_never_flags_against_min_zero(self) -> None:
        df = make_frame(daily_periods(3), series=["gulf_south_sq_24329_ID1"], value=0.0)
        assert check_value_sanity(df, BAND_CFG)["severity"] == "PASS"

    def test_exact_band_max_is_not_a_breach(self) -> None:
        df = make_frame([day(1)], series=["gulf_south_sq_24329_ID1"], value=2_860_000.0)
        assert check_value_sanity(df, BAND_CFG)["severity"] == "PASS"

    def test_negative_fails_when_configured(self) -> None:
        df = make_frame(daily_periods(3))
        df.loc[df.index[1], "value"] = -5.0
        res = check_value_sanity(df, make_cfg())
        assert res["severity"] == "FAIL"
        assert "-5.0" in res["message"]

    def test_negative_allowed_without_flag(self) -> None:
        df = make_frame([day(1)], value=-5.0)
        res = check_value_sanity(df, make_cfg(negative_values_fail=False))
        assert res["severity"] == "PASS"

    def test_bandless_source_passes(self) -> None:
        df = make_frame(daily_periods(3), value=123.0)
        res = check_value_sanity(df, make_cfg())
        assert res["severity"] == "PASS"
        assert "3" in res["message"]

    def test_gie_mixed_units_do_not_trip_unit_check(self) -> None:
        frames = [
            make_frame([day(1)], series=["de_storage"], unit="TWh"),
            make_frame([day(1)], series=["nl_storage"], unit="GWh"),
            make_frame([day(1)], series=["eu_full"], unit="%"),
        ]
        mixed = pd.concat(frames, ignore_index=True)
        res = check_schema(mixed, make_cfg(unit_expected=None), DEFAULTS)
        assert res["severity"] == "PASS"


# ---------------------------------------------------------------- stagnation


class TestStagnation:
    def test_exactly_warn_days_is_pass(self) -> None:
        res = check_stagnation(make_frame([day(2)]), make_cfg(), NOW)
        assert res["severity"] == "PASS"
        assert f"{day(2)}" in res["message"] and "2d old" in res["message"]

    def test_warn_plus_one_is_warn(self) -> None:
        res = check_stagnation(make_frame([day(3)]), make_cfg(), NOW)
        assert res["severity"] == "WARN"

    def test_fail_plus_one_is_fail(self) -> None:
        res = check_stagnation(make_frame([day(5)]), make_cfg(), NOW)
        assert res["severity"] == "FAIL"
        assert "stagnant" in res["message"]

    def test_future_period_is_pass(self) -> None:
        future = (NOW.date() + timedelta(days=1)).isoformat()
        res = check_stagnation(make_frame([future]), make_cfg(), NOW)
        assert res["severity"] == "PASS"

    def test_bare_month_normalizes_to_month_end(self) -> None:
        assert (
            normalize_period("2026-05", {"month_end_normalize": True})
            == datetime(2026, 5, 31).date()
        )

    def test_monthly_lag_cfg_uses_month_end(self) -> None:
        monthly = make_cfg(
            period_format="%Y-%m",
            month_end_normalize=True,
            staleness={"warn_days": 45, "fail_days": 135},
        )
        # 2026-05-31 → 84 days stale: WARN under the 45/135 monthly thresholds.
        res = check_stagnation(make_frame(["2026-05"]), monthly, NOW)
        assert res["severity"] == "WARN"
        assert "2026-05-31" in res["message"] and "84d" in res["message"]


# ---------------------------------------------------------------------- gaps


class TestGaps:
    def test_two_holes_warn_listing_exact_dates(self) -> None:
        expected_missing = sorted([day(7), day(3)])
        df = make_frame(daily_periods(10, skip={3, 7}))
        res = check_gaps(df, make_cfg())
        assert res["severity"] == "WARN"
        assert res["details"]["missing_dates"] == expected_missing
        for hole in expected_missing:
            assert hole in res["message"]

    def test_complete_calendar_passes(self) -> None:
        res = check_gaps(make_frame(daily_periods(10)), make_cfg())
        assert res["severity"] == "PASS"

    def test_no_gap_rule_skips(self) -> None:
        res = check_gaps(make_frame(daily_periods(10)), make_cfg(gap_rule=None))
        assert res["severity"] == "SKIPPED"

    def test_weekly_friday_rule(self) -> None:
        fridays = ["2026-08-07", "2026-08-14", "2026-08-21", "2026-08-28"]
        res = check_gaps(make_frame(fridays), make_cfg(gap_rule="weekly_friday"))
        assert res["severity"] == "PASS"

        # Missing 2026-08-14
        hole = ["2026-08-07", "2026-08-21", "2026-08-28"]
        res_hole = check_gaps(make_frame(hole), make_cfg(gap_rule="weekly_friday"))
        assert res_hole["severity"] == "WARN"
        assert "2026-08-14" in res_hole["details"]["missing_dates"]

    def test_monthly_rule(self) -> None:
        months = ["2026-01", "2026-02", "2026-03", "2026-04"]
        res = check_gaps(make_frame(months), make_cfg(gap_rule="monthly"))
        assert res["severity"] == "PASS"

        hole = ["2026-01", "2026-03", "2026-04"]
        res_hole = check_gaps(make_frame(hole), make_cfg(gap_rule="monthly"))
        assert res_hole["severity"] == "WARN"
        assert "2026-02" in res_hole["details"]["missing_dates"]


# ------------------------------------------------------------------ coverage


class TestCoverage:
    def test_quarter_series_drop_fails(self) -> None:
        prior = {"distinct_series": 4}
        df = make_frame(daily_periods(2), series=["a", "b", "c"])
        res = check_coverage(df, prior, make_cfg(), DEFAULTS)
        assert res["severity"] == "FAIL"
        assert "4\u21923" in res["message"] and "25.0%" in res["message"]

    def test_growth_passes(self) -> None:
        prior = {"distinct_series": 2}
        df = make_frame(daily_periods(2), series=["a", "b", "c"])
        assert check_coverage(df, prior, make_cfg(), DEFAULTS)["severity"] == "PASS"

    def test_no_prior_skips(self) -> None:
        res = check_coverage(make_frame(daily_periods(2)), {}, make_cfg(), DEFAULTS)
        assert res["severity"] == "SKIPPED"


# ---------------------------------------------------------------- shrinkage


class TestShrinkage:
    def test_row_drop_fails(self) -> None:
        prior = build_source_state(make_frame(daily_periods(4)), NOW)
        res = check_shrinkage(make_frame(daily_periods(3)), prior, make_cfg())
        assert res["severity"] == "FAIL"
        assert "rows 4\u21923 / days 4\u21923" in res["message"]

    def test_period_drop_alone_fails(self) -> None:
        prior = build_source_state(make_frame(daily_periods(10)), NOW)
        squeezed = make_frame(daily_periods(9) + [day(0)])  # 10 rows, 9 distinct days
        res = check_shrinkage(squeezed, prior, make_cfg())
        assert res["severity"] == "FAIL"
        assert "days 10\u21929" in res["message"]

    def test_identical_passes(self) -> None:
        prior = build_source_state(make_frame(daily_periods(4)), NOW)
        res = check_shrinkage(make_frame(daily_periods(4)), prior, make_cfg())
        assert res["severity"] == "PASS"

    def test_growth_passes(self) -> None:
        prior = build_source_state(make_frame(daily_periods(4)), NOW)
        res = check_shrinkage(make_frame(daily_periods(5)), prior, make_cfg())
        assert res["severity"] == "PASS"

    def test_no_prior_skips(self) -> None:
        res = check_shrinkage(make_frame(daily_periods(4)), {}, make_cfg())
        assert res["severity"] == "SKIPPED"


# --------------------------------------------------------------- divergence


RECENT_OK_HEALTH = {"status": "ok", "timestamp_utc": "2026-08-23T09:00:00Z"}
MONTHLY_LAG = make_cfg(staleness={"warn_days": 45, "fail_days": 135})


class TestDivergence:
    def test_ok_recent_but_stale_dataset_fails(self) -> None:
        # Newest period ~175d old while health said ok 3h ago — beyond the
        # source's *fail* threshold (135d), the frozen-parquet-while-green
        # incident this module exists for.
        df = make_frame(["2026-03-01"])
        res = check_divergence(df, RECENT_OK_HEALTH, {}, MONTHLY_LAG, DEFAULTS, NOW)
        assert res["severity"] == "FAIL"
        assert "DIVERGENCE" in res["message"]
        assert "2026-03-01" in res["message"] and "175d stale" in res["message"]

    def test_warn_level_staleness_with_ok_health_does_not_fire(self) -> None:
        # Regression: 83d old beats warn 45d but sits under fail 135d — the
        # genuine eia_lng_exports publication lag must stay PASS, not FAIL.
        df = make_frame(["2026-06-01"])
        res = check_divergence(df, RECENT_OK_HEALTH, {}, MONTHLY_LAG, DEFAULTS, NOW)
        assert res["severity"] == "PASS"
        assert "no divergence" in res["message"]
        assert "83d" in res["message"]

    def test_health_failed_yields_pass_with_note(self) -> None:
        df = make_frame(["2026-06-01"])
        health = {"status": "failed", "timestamp_utc": "2026-08-23T09:00:00Z"}
        res = check_divergence(df, health, {}, MONTHLY_LAG, DEFAULTS, NOW)
        assert res["severity"] == "PASS"
        assert "failed" in res["message"]

    def test_missing_health_skips(self) -> None:
        res = check_divergence(make_frame([day(1)]), None, {}, make_cfg(), DEFAULTS, NOW)
        assert res["severity"] == "SKIPPED"

    def test_stale_health_stamp_skips(self) -> None:
        health = {"status": "ok", "timestamp_utc": "2026-08-10T09:00:00Z"}
        res = check_divergence(make_frame([day(1)]), health, {}, make_cfg(), DEFAULTS, NOW)
        assert res["severity"] == "SKIPPED"
        assert "recency" in res["message"]

    def test_accumulation_flat_arm_suppressed_when_fresh(self) -> None:
        """Flat accumulation count is benign when the latest period is fresh.

        A daily EBB source legitimately has a flat row count between
        postings (weekends, gaps, or a run before the new gas day lands).
        The divergence flat-run arm must NOT fail purely on flatness when
        the data is still within warn_days — staleness owns that signal.
        """
        cfg = make_cfg(mode="accumulation")
        prior = {"rows": 4, "consecutive_flat": 2}
        df = make_frame(daily_periods(2), series=["s1", "s2"])  # 4 rows, fresh
        res = check_divergence(df, RECENT_OK_HEALTH, prior, cfg, DEFAULTS, NOW)
        assert res["severity"] == "PASS"

    def test_accumulation_flat_arm_fires_when_stale(self) -> None:
        """Flat count + stale latest period = genuine stop-growing signal.

        This is the real divergence case: the scraper reports ok but the
        dataset is both flat AND aging, i.e. it stopped accumulating while
        it should be growing.
        """
        cfg = make_cfg(mode="accumulation", staleness={"warn_days": 2, "fail_days": 4})
        prior = {"rows": 4, "consecutive_flat": 2}
        # 30 days stale, still 4 rows -> flat + stale
        df = make_frame([day(30), day(31)], series=["s1", "s2"])
        res = check_divergence(df, RECENT_OK_HEALTH, prior, cfg, DEFAULTS, NOW)
        assert res["severity"] == "FAIL"
        assert "flat 3 consecutive runs" in res["message"]

    def test_accumulation_second_flat_run_does_not_fire(self) -> None:
        cfg = make_cfg(mode="accumulation")
        prior = {"rows": 4, "consecutive_flat": 1}
        df = make_frame(daily_periods(2), series=["s1", "s2"])
        res = check_divergence(df, RECENT_OK_HEALTH, prior, cfg, DEFAULTS, NOW)
        assert res["severity"] == "PASS"

    def test_healthy_scraper_and_fresh_dataset_passes(self) -> None:
        res = check_divergence(
            make_frame([day(1)]), RECENT_OK_HEALTH, {}, make_cfg(), DEFAULTS, NOW
        )
        assert res["severity"] == "PASS"


# ------------------------------------------------------- run_source_checks


class TestRunSourceChecks:
    def test_any_fail_wins_over_everything(self) -> None:
        df = make_frame(daily_periods(3)).drop(columns=["unit"])  # schema FAIL
        results, worst = run_source_checks("t", df, make_cfg(), DEFAULTS, None, NOW, None)
        assert worst == "FAIL"
        assert {r["check"]: r["severity"] for r in results}["schema"] == "FAIL"

    def test_warn_beats_pass_and_skipped(self) -> None:
        df = make_frame(daily_periods(10, skip={5}))  # gaps WARN, rest PASS/SKIPPED
        results, worst = run_source_checks("t", df, make_cfg(), DEFAULTS, None, NOW, None)
        assert worst == "WARN"
        severities = {r["severity"] for r in results}
        assert "WARN" in severities and "SKIPPED" in severities

    def test_skipped_transparent_unless_all_skip(self) -> None:
        empty = pd.DataFrame(columns=DEFAULTS["schema_columns"])
        _, worst = run_source_checks("t", empty, make_cfg(), DEFAULTS, None, NOW, None)
        # Empty frame only trips schema's WARN; every SKIPPED stays transparent.
        assert worst == "WARN"

    def test_full_green_run_is_pass(self) -> None:
        df = make_frame(daily_periods(5))
        _, worst = run_source_checks("t", df, make_cfg(), DEFAULTS, None, NOW, None)
        assert worst == "PASS"


# ------------------------------------------------------------ state helpers


class TestStateHelpers:
    def test_load_state_missing_returns_empty(self, tmp_path: Path) -> None:
        assert load_state(tmp_path / "absent.json") == {}

    def test_load_state_corrupt_returns_empty(self, tmp_path: Path) -> None:
        bad = tmp_path / "state.json"
        bad.write_text("{not json", encoding="utf-8")
        assert load_state(bad) == {}

    def test_load_state_roundtrip(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text(json.dumps({"gulf_south": {"rows": 7}}), encoding="utf-8")
        assert load_state(path) == {"gulf_south": {"rows": 7}}

    def test_build_source_state_shape(self) -> None:
        state = build_source_state(make_frame(daily_periods(3), series=["a", "b"]), NOW)
        assert state["rows"] == 6
        assert state["distinct_periods"] == 3
        assert state["distinct_series"] == 2
        assert state["max_period"] == day(0)
        assert state["checked_at"] == NOW.isoformat()


# ----------------------------------------------------------- shipped config


class TestShippedRules:
    @pytest.fixture()
    def rules(self) -> dict[str, Any]:
        return yaml.safe_load(RULES_PATH.read_text(encoding="utf-8"))

    def test_defaults_block(self, rules: dict[str, Any]) -> None:
        defaults = rules["defaults"]
        assert set(DEFAULTS) <= set(defaults)
        assert len(defaults["schema_columns"]) == 8

    def test_active_sources_present(self, rules: dict[str, Any]) -> None:
        sources = rules["sources"]
        assert set(sources) == {
            "gulf_south",
            "gie_agsi",
            "baker_hughes",
            "eia_storage",
            "eia_lng_exports",
            "eia_supply",
            "gasnom",
            "quorum",
            "bhe",
            "cheniere",
            # TETCO via Enbridge rtba enabled in the TETCO integration
            "enbridge",
            # Kinder Morgan pipeline2 OpAvail (Sabine/Corpus measured feeds)
            "kinder_morgan",
            # SHELVED 2026-08-25: transco removed — legacy 1Line JSP endpoints
            # are gone (portal-app migration); stanza commented out in
            # config/integrity_rules.yaml to stop permanent WARN noise.
        }

    def test_gulf_south_rules(self, rules: dict[str, Any]) -> None:
        gulf = rules["sources"]["gulf_south"]
        assert gulf["staleness"] == {"warn_days": 2, "fail_days": 4}
        assert gulf["gap_rule"] == "calendar_daily"
        assert gulf["unit_expected"] == "Dth/d"
        assert gulf["negative_values_fail"] is True
        band = gulf["bands"][0]
        assert band["series_glob"] == "gulf_south_sq_24329_*"
        assert band["min"] == 0.0 and band["max"] == 2860000.0

    def test_monthly_sources_use_publication_lag_thresholds(self, rules: dict[str, Any]) -> None:
        for key in ("eia_lng_exports", "eia_supply"):
            source = rules["sources"][key]
            assert source["month_end_normalize"] is True
            assert source["staleness"]["fail_days"] == 135
        assert rules["sources"]["eia_lng_exports"]["staleness"]["warn_days"] == 45
        assert rules["sources"]["eia_supply"]["staleness"]["warn_days"] == 75
        assert rules["sources"]["eia_supply"]["period_format"] == "%Y-%m"

    def test_ebb_sources_use_daily_thresholds(self, rules: dict[str, Any]) -> None:
        """All four EBB daily sources activated 2026-08-23 share EBB thresholds."""
        for key in ("gasnom", "quorum", "bhe", "cheniere"):
            source = rules["sources"][key]
            assert source["staleness"] == {"warn_days": 2, "fail_days": 4}
            assert source["unit_expected"] == "Dth/d"
            assert source["mode"] == "accumulation"
            # All four daily EBB sources enforce calendar_daily gap detection.
            # For quorum, known upstream 3-day retention holes emit WARN (never FAIL),
            # ensuring active observability rather than hiding future gaps behind SKIPPED.
            assert source["gap_rule"] == "calendar_daily"


# ------------------------------------------------------------ render_table


class TestRenderTable:
    @staticmethod
    def _line_for(table: str, source: str) -> str:
        return next(line for line in table.splitlines() if line.startswith(f"{source} "))

    def test_findings_list_only_warn_fail(self) -> None:
        report = {
            "sources": {
                "broken": {
                    "overall": "FAIL",
                    "results": [
                        {"check": "divergence", "severity": "FAIL", "message": "m"},
                        {"check": "gaps", "severity": "SKIPPED", "message": "m"},
                        {"check": "coverage", "severity": "SKIPPED", "message": "m"},
                    ],
                    "stats": {},
                },
                "ageing": {
                    "overall": "WARN",
                    "results": [{"check": "stagnation", "severity": "WARN", "message": "m"}],
                    "stats": {},
                },
            }
        }
        out = render_table(report)
        broken = self._line_for(out, "broken")
        assert "divergence" in broken
        assert "gaps" not in broken and "coverage" not in broken
        ageing = self._line_for(out, "ageing")
        assert "stagnation" in ageing

    def test_all_clear_row_shows_dash(self) -> None:
        report = {
            "sources": {
                "calm": {
                    "overall": "PASS",
                    "results": [
                        {"check": "schema", "severity": "PASS", "message": "m"},
                        {"check": "divergence", "severity": "SKIPPED", "message": "m"},
                    ],
                    "stats": {},
                }
            }
        }
        calm = self._line_for(render_table(report), "calm")
        assert calm.rstrip().endswith("-")
        assert "schema" not in calm and "divergence" not in calm


# ------------------------------------------------------------- CLI end-to-end


def _recent_days(count: int) -> list[str]:
    """``count`` consecutive calendar days ending at the real wall-clock today."""
    today = datetime.now(UTC).date()
    return [(today - timedelta(days=offset)).isoformat() for offset in range(count - 1, -1, -1)]


def _source_block(tmp_path: Path, name: str, **over: Any) -> dict[str, Any]:
    block: dict[str, Any] = {
        "parquet": str(tmp_path / "curated" / f"{name}.parquet"),
        "health_file": str(tmp_path / "health" / f"{name}.json"),
        "staleness": {"warn_days": 2, "fail_days": 4},
        "unit_expected": "Dth/d",
        "negative_values_fail": False,
        "gap_rule": "calendar_daily",
    }
    block.update(over)
    return block


def _write_rules_yaml(tmp_path: Path, sources: dict[str, dict[str, Any]]) -> Path:
    rules = {
        "defaults": {
            "schema_columns": DEFAULTS["schema_columns"],
            "coverage_drop_fail_pct": 20,
            "health_recency_days": 3,
        },
        "sources": sources,
    }
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump(rules), encoding="utf-8")
    return path


def _write_parquet(tmp_path: Path, name: str, periods: list[str]) -> Path:
    path = tmp_path / "curated" / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    make_frame(periods).to_parquet(path, engine="pyarrow")
    return path


class TestCliMain:
    def _run(
        self, tmp_path: Path, sources: dict[str, dict[str, Any]], state_seed: dict[str, Any]
    ) -> tuple[int, dict[str, Any], dict[str, Any]]:
        rules = _write_rules_yaml(tmp_path, sources)
        state_path = tmp_path / "_integrity_state.json"
        if state_seed:
            state_path.write_text(json.dumps(state_seed), encoding="utf-8")
        report_path = tmp_path / "_integrity_report.json"
        code = run_integrity_main(
            ["--config", str(rules), "--state", str(state_path), "--report", str(report_path)]
        )
        report = json.loads(report_path.read_text(encoding="utf-8"))
        state = json.loads(state_path.read_text(encoding="utf-8"))
        return code, report, state

    def test_shrunken_source_fails_and_keeps_old_baseline(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path, "healthy", _recent_days(5))
        _write_parquet(tmp_path, "shrunk", _recent_days(2))
        seed = {
            # healthy ran flat once already; shrunk holds the pre-corruption baseline
            "healthy": {
                "rows": 5,
                "distinct_periods": 5,
                "distinct_series": 1,
                "max_period": _recent_days(1)[0],
                "consecutive_flat": 1,
                "checked_at": NOW.isoformat(),
            },
            "shrunk": {
                "rows": 10,
                "distinct_periods": 5,
                "distinct_series": 1,
                "max_period": _recent_days(5)[0],
                "checked_at": NOW.isoformat(),
            },
        }
        sources = {
            "healthy": _source_block(tmp_path, "healthy"),
            "shrunk": _source_block(tmp_path, "shrunk"),
        }

        code, report, state = self._run(tmp_path, sources, seed)

        assert code == 1
        assert report["overall"] == "FAIL"
        assert report["sources"]["healthy"]["overall"] == "PASS"
        assert report["sources"]["shrunk"]["overall"] == "FAIL"
        assert report["sources"]["healthy"]["stats"]["rows"] == 5
        assert "generated_at" in report
        # CRITICAL contract: the shrunken source's stored baseline is untouched.
        assert state["shrunk"] == seed["shrunk"]
        assert state["shrunk"]["rows"] == 10
        # Healthy source gets fresh numbers; flat-arm counter advanced 1 → 2.
        assert state["healthy"]["rows"] == 5
        assert state["healthy"]["consecutive_flat"] == 2

    def test_all_healthy_exits_zero(self, tmp_path: Path) -> None:
        _write_parquet(tmp_path, "only", _recent_days(4))
        sources = {"only": _source_block(tmp_path, "only")}

        code, report, state = self._run(tmp_path, sources, {})

        assert code == 0
        assert report["overall"] == "PASS"
        assert report["sources"]["only"]["stats"]["rows"] == 4
        assert state["only"]["rows"] == 4

    def test_placeholder_source_skipped_without_crash(self, tmp_path: Path) -> None:
        sources = {
            "future_src": _source_block(tmp_path, "future_src"),  # no parquet written
        }

        code, report, state = self._run(tmp_path, sources, {})

        assert code == 0
        entry = report["sources"]["future_src"]
        assert entry["overall"] == "SKIPPED"
        assert entry["results"][0]["check"] == "availability"
        assert entry["results"][0]["message"] == "parquet absent (source not live yet)"
        assert entry["stats"]["rows"] is None
        assert "future_src" not in state  # nothing measured, nothing persisted
        # All-SKIPPED run folds to a SKIPPED overall without any FAIL.
        assert report["overall"] == "SKIPPED"

    def test_alert_flag_stays_silent_on_all_clear(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import httpx

        def mock_post(*args, **kwargs):
            pytest.fail("httpx.post must not fire on an all-clear CLI run")

        monkeypatch.setattr(httpx, "post", mock_post)

        _write_parquet(tmp_path, "calm", _recent_days(3))
        rules = _write_rules_yaml(tmp_path, {"calm": _source_block(tmp_path, "calm")})
        report_path = tmp_path / "_integrity_report.json"

        code = run_integrity_main(
            [
                "--config",
                str(rules),
                "--state",
                str(tmp_path / "_integrity_state.json"),
                "--report",
                str(report_path),
                "--alert",
            ]
        )

        assert code == 0
        assert json.loads(report_path.read_text(encoding="utf-8"))["overall"] == "PASS"

    def test_alert_failure_raises_out_of_main(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """--alert opts in to loud failure: a broken sender surfaces AlertError."""
        import publishers.alerts as alerts
        from publishers.alerts import AlertError

        def failing_sender(report_path: Path) -> bool:
            raise AlertError("Telegram API returned 502: bad gateway")

        monkeypatch.setattr(alerts, "send_integrity_alert_if_needed", failing_sender)

        _write_parquet(tmp_path, "calm", _recent_days(3))
        rules = _write_rules_yaml(tmp_path, {"calm": _source_block(tmp_path, "calm")})
        report_path = tmp_path / "_integrity_report.json"

        with pytest.raises(AlertError):
            run_integrity_main(
                [
                    "--config",
                    str(rules),
                    "--state",
                    str(tmp_path / "_integrity_state.json"),
                    "--report",
                    str(report_path),
                    "--alert",
                ]
            )

        # Report/state were persisted before the raise, but the exit path was
        # never reached: no green exit code and no swallowed NOT-SENT line.
        assert json.loads(report_path.read_text(encoding="utf-8"))["overall"] == "PASS"
        assert "NOT-SENT" not in capsys.readouterr().out
