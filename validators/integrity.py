"""Dataset-integrity checks catching silent degradation of curated parquets.

Why:
    A curated parquet once sat frozen at a single gas day for 74 consecutive
    runs while ``data/health/`` cheerily reported ``status: ok`` — the scraper
    was healthy, the *dataset* was dead.  Scraper health alone cannot see rot
    that lives in the accumulated artefact, so these checks audit the parquet
    itself: schema drift, impossible values, stagnation, calendar holes,
    shrinking history, and finally a meta-check that fires when a freshly
    "ok" scraper contradicts a degraded dataset.

What:
    Pure, dependency-injected predicates: every ``check_*`` receives the
    frame, its config slice, prior-run state, ``now``, and the health payload
    explicitly — no globals, no I/O inside checks.  Each returns one
    :class:`CheckResult` (``check``/``severity``/``message``/``details``)
    whose human-readable message carries the deciding concrete numbers.
    ``run_source_checks`` fans one source through the suite and folds
    severities into a worst-of verdict (FAIL > WARN > PASS; SKIPPED counts
    only when nothing else ran).  Per-source knobs live in
    ``config/integrity_rules.yaml``.  ``load_state`` / ``build_source_state``
    are the only helpers that touch disk or persist anything, kept separate
    so the check logic stays trivially testable.

Failure modes:
    * Checks prefer SKIPPED over guessing: missing priors, empty frames, or
      unparseable periods degrade to SKIPPED rather than false alarms
      (schema owns period parseability and reports it as FAIL).
    * Violations FAIL with inline examples (up to 5 offenders) so on-call
      sees *what* broke without opening the parquet.
    * ``load_state`` returns ``{}`` on missing/corrupt state after logging a
      warning — a corrupt ledger must not brick the monitor.
    * CRITICAL caller contract: after a shrinkage FAIL for a source, the
      caller must NOT overwrite that source's stored baseline with fresh
      state.  Keeping the pre-corruption baseline is what makes recovery
      visible on later runs (rows climbing back above it re-PASSes).
"""

from __future__ import annotations

import calendar
import fnmatch
import json
import logging
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TypedDict

import pandas as pd

log = logging.getLogger(__name__)

Severity = Literal["PASS", "WARN", "SKIPPED", "FAIL"]

_SEVERITY_RANK: dict[str, int] = {"SKIPPED": 0, "PASS": 1, "WARN": 2, "FAIL": 3}


class CheckResult(TypedDict):
    """One check verdict; ``message`` always embeds the deciding numbers."""

    check: str
    severity: Severity
    message: str
    details: dict[str, Any]


def _result(
    check: str,
    severity: Severity,
    message: str,
    details: dict[str, Any] | None = None,
) -> CheckResult:
    return {"check": check, "severity": severity, "message": message, "details": details or {}}


def _collision_check(
    df: pd.DataFrame,
    src_cfg: Mapping[str, Any],
    defaults: Mapping[str, Any],
) -> CheckResult:
    """Thin adapter over :mod:`validators.collision` (kept import-lazy to avoid a cycle)."""
    from validators.collision import check_collision

    return check_collision(df, src_cfg, defaults)


def normalize_period(p: str, cfg: Mapping[str, Any]) -> date:
    """Resolve a raw period string to a concrete calendar date.

    Full ISO dates pass through untouched.  Bare ``YYYY-MM`` (or whatever
    ``period_format`` specifies) resolves to that month, snapped to the last
    day of the month when ``month_end_normalize`` is set.  Raises ValueError
    on anything unparseable — schema turns that into FAIL.
    """
    text = str(p).strip()
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass

    fmt = cfg.get("period_format")
    if fmt is not None:
        try:
            parsed = datetime.strptime(text, str(fmt))
        except ValueError as exc:
            raise ValueError(f"period {p!r} does not match period_format {fmt!r}") from exc
    elif len(text) == 7 and text[4] == "-":
        try:
            parsed = datetime.strptime(text, "%Y-%m")
        except ValueError as exc:
            raise ValueError(f"unparseable period {p!r}") from exc
    else:
        raise ValueError(f"unparseable period {p!r}")

    if cfg.get("month_end_normalize"):
        last_day = calendar.monthrange(parsed.year, parsed.month)[1]
        return date(parsed.year, parsed.month, last_day)
    return parsed.date()


def _stale_days(
    df: pd.DataFrame,
    src_cfg: Mapping[str, Any],
    now: datetime,
) -> tuple[int, str, str] | None:
    """Return ``(days since newest period, raw max, resolved date iso)`` or ``None``."""
    if df.empty or "period" not in df.columns:
        return None
    max_raw = str(df["period"].max())
    try:
        newest = normalize_period(max_raw, src_cfg)
    except ValueError:
        return None
    return (now.date() - newest).days, max_raw, newest.isoformat()


def _staleness_thresholds(src_cfg: Mapping[str, Any]) -> tuple[int, int]:
    stale_cfg = src_cfg.get("staleness") or {}
    warn_days = int(stale_cfg.get("warn_days", 7))
    fail_days = int(stale_cfg.get("fail_days", 14))
    return warn_days, fail_days


def check_schema(
    df: pd.DataFrame,
    src_cfg: Mapping[str, Any],
    defaults: Mapping[str, Any],
) -> CheckResult:
    """Enforce canonical columns, numeric values, parsable periods, unit vocabulary.

    The unit arm is skipped entirely when ``unit_expected`` is null/falsy
    (e.g. gie_agsi mixes TWh/GWh/% by row).
    """
    required = [str(col) for col in defaults.get("schema_columns", [])]
    missing = [col for col in required if col not in df.columns]
    if missing:
        return _result(
            "schema",
            "FAIL",
            f"missing required column(s): {', '.join(missing)}",
            {"missing": missing},
        )

    n = len(df)
    if n == 0:
        return _result("schema", "WARN", "frame is empty — nothing to validate", {"rows": 0})

    if not pd.api.types.is_numeric_dtype(df["value"]):
        return _result(
            "schema",
            "FAIL",
            f"value column is non-numeric (dtype={df['value'].dtype}) across {n} rows",
            {"rows": n},
        )

    sampled = df["period"].head(50)
    bad: list[str] = []
    for raw in sampled:
        try:
            normalize_period(str(raw), src_cfg)
        except ValueError:
            bad.append(str(raw))
    if bad:
        return _result(
            "schema",
            "FAIL",
            f"{len(bad)}/{len(sampled)} sampled period values unparseable (e.g. {bad[0]!r})",
            {"rows": n, "unparseable_sample": bad[:5]},
        )

    unit_note = "unit check skipped"
    expected = src_cfg.get("unit_expected")
    if expected:
        unexpected = sorted(set(df["unit"].dropna().astype(str)) - {str(expected)})
        if unexpected:
            return _result(
                "schema",
                "FAIL",
                f"unexpected unit value(s) {unexpected} — expected only {expected!r}",
                {"rows": n, "unexpected_units": unexpected},
            )
        unit_note = f"units all {expected!r}"

    return _result(
        "schema",
        "PASS",
        f"schema ok: {n} rows, all {len(required)} canonical columns, value numeric, "
        f"periods parseable, {unit_note}",
        {"rows": n},
    )


def check_value_sanity(df: pd.DataFrame, src_cfg: Mapping[str, Any]) -> CheckResult:
    """Flag physically impossible values: band breaches and (optionally) negatives.

    Band bounds compare strictly outside the edges — a ``min`` of ``0.0``
    never flags zero flows, which are legitimate (idle meters happen).
    """
    if "value" not in df.columns:
        return _result("value_sanity", "SKIPPED", "no value column — schema owns this failure")

    bands = list(src_cfg.get("bands") or [])
    offenders: list[str] = []
    breach_count = 0
    scanned = 0
    has_keys = "series_id" in df.columns and "period" in df.columns
    for band in bands:
        pattern = str(band.get("series_glob", "*"))
        low = float(band.get("min", float("-inf")))
        high = float(band.get("max", float("inf")))
        if not has_keys:
            break
        mask = df["series_id"].astype(str).map(lambda s, pat=pattern: fnmatch.fnmatch(s, pat))
        sub = df.loc[mask]
        scanned += len(sub)
        breaches = sub.loc[(sub["value"] > high) | (sub["value"] < low)]
        breach_count += len(breaches)
        for series_id, period, value in breaches[["series_id", "period", "value"]].itertuples(
            index=False
        ):
            if len(offenders) < 5:
                offenders.append(f"{series_id}@{period}={value}")

    if breach_count:
        return _result(
            "value_sanity",
            "FAIL",
            f"{breach_count} out-of-band value(s) across {len(bands)} band(s), "
            f"e.g. {'; '.join(offenders)}",
            {"breaches": int(breach_count), "examples": offenders},
        )

    if src_cfg.get("negative_values_fail") and has_keys:
        negatives = df.loc[df["value"] < 0]
        neg_count = len(negatives)
        if neg_count:
            examples = [
                f"{series_id}@{period}={value}"
                for series_id, period, value in negatives[
                    ["series_id", "period", "value"]
                ].itertuples(index=False)
            ][:5]
            return _result(
                "value_sanity",
                "FAIL",
                f"{neg_count} negative value(s) with negative_values_fail set, "
                f"e.g. {'; '.join(examples)}",
                {"negatives": int(neg_count), "examples": examples},
            )

    return _result(
        "value_sanity",
        "PASS",
        f"value sanity ok: {len(df)} row(s) scanned, {scanned} within {len(bands)} band(s), "
        "0 breaches",
        {"rows": int(len(df)), "banded_scanned": int(scanned)},
    )


def check_stagnation(df: pd.DataFrame, src_cfg: Mapping[str, Any], now: datetime) -> CheckResult:
    """Newest period must track the wall clock within configured thresholds.

    Strict > semantics: exactly ``warn_days`` old is still PASS.
    """
    stale_info = _stale_days(df, src_cfg, now)
    if stale_info is None:
        return _result("stagnation", "SKIPPED", "no parsable periods to age")

    stale_days, max_period, resolved = stale_info
    warn_days, fail_days = _staleness_thresholds(src_cfg)

    if stale_days < 0:
        return _result(
            "stagnation",
            "PASS",
            f"newest period {max_period} sits {-stale_days}d ahead of now",
            {"stale_days": stale_days},
        )
    period_label = max_period if max_period == resolved else f"{max_period} (\u2192{resolved})"
    base = (
        f"newest period {period_label} is {stale_days}d old "
        f"(warn {warn_days}d / fail {fail_days}d)"
    )
    if stale_days > fail_days:
        return _result(
            "stagnation", "FAIL", f"dataset stagnant: {base}", {"stale_days": stale_days}
        )
    if stale_days > warn_days:
        return _result("stagnation", "WARN", f"dataset ageing: {base}", {"stale_days": stale_days})
    return _result("stagnation", "PASS", f"dataset fresh: {base}", {"stale_days": stale_days})


def check_gaps(df: pd.DataFrame, src_cfg: Mapping[str, Any]) -> CheckResult:
    """Calendar continuity for daily sources — holes WARN, never FAIL.

    A hole usually means an upstream posting gap, not local corruption;
    recoverable, so WARN keeps the signal without paging anyone.
    """
    rule = src_cfg.get("gap_rule")
    if not rule:
        return _result("gaps", "SKIPPED", "no gap_rule configured")
    if rule != "calendar_daily":
        return _result("gaps", "SKIPPED", f"unknown gap_rule {rule!r}")
    if df.empty or "period" not in df.columns:
        return _result("gaps", "SKIPPED", "no periods to scan")

    unique: list[date] = []
    for raw in df["period"].astype(str).unique():
        try:
            unique.append(normalize_period(raw, src_cfg))
        except ValueError:
            return _result("gaps", "SKIPPED", "unparseable periods — schema owns this failure")
    unique = sorted(set(unique))

    present = set(unique)
    span_days = (unique[-1] - unique[0]).days + 1
    missing: list[str] = []
    for offset in range(span_days):
        day = unique[0] + timedelta(days=offset)
        if day not in present:
            missing.append(day.isoformat())

    if missing:
        preview = ", ".join(missing[:15])
        return _result(
            "gaps",
            "WARN",
            f"{len(missing)} missing calendar day(s) between {unique[0]} and {unique[-1]}: "
            f"{preview}",
            {"missing_dates": missing},
        )
    return _result(
        "gaps",
        "PASS",
        f"calendar complete: {span_days} consecutive day(s) {unique[0]}..{unique[-1]}",
        {"days": span_days},
    )


def check_coverage(
    df: pd.DataFrame,
    prior: Mapping[str, Any],
    src_cfg: Mapping[str, Any],
    defaults: Mapping[str, Any],
) -> CheckResult:
    """Distinct-series population must not silently collapse vs the prior run."""
    if not prior:
        return _result("coverage", "SKIPPED", "no prior state — first run for this source")
    before = int(prior.get("distinct_series", 0))
    if before <= 0:
        return _result(
            "coverage", "SKIPPED", f"prior baseline holds {before} series — nothing to compare"
        )

    after = int(df["series_id"].nunique()) if "series_id" in df.columns else 0
    limit = float(defaults.get("coverage_drop_fail_pct", 20))
    drop_pct = (before - after) / before * 100.0

    if after < before and drop_pct > limit:
        return _result(
            "coverage",
            "FAIL",
            f"series coverage collapsed: {before}\u2192{after} distinct series_id "
            f"({drop_pct:.1f}% drop > {limit:.0f}% limit)",
            {"before": before, "after": after, "drop_pct": round(drop_pct, 2)},
        )
    return _result(
        "coverage",
        "PASS",
        f"series coverage holds: {before}\u2192{after} distinct series_id (limit {limit:.0f}%)",
        {"before": before, "after": after},
    )


def check_shrinkage(
    df: pd.DataFrame, prior: Mapping[str, Any], src_cfg: Mapping[str, Any]
) -> CheckResult:
    """Row count and distinct-period count may never decrease vs the prior run.

    This is the direct guard against the frozen-at-one-gas-day incident.
    """
    if not prior:
        return _result("shrinkage", "SKIPPED", "no prior state — first run for this source")

    rows_before = int(prior.get("rows", 0))
    days_before = int(prior.get("distinct_periods", 0))
    rows_after = int(len(df))
    days_after = int(df["period"].nunique()) if "period" in df.columns else 0

    summary = f"rows {rows_before}\u2192{rows_after} / days {days_before}\u2192{days_after}"
    metrics = {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "days_before": days_before,
        "days_after": days_after,
    }
    if rows_after < rows_before or days_after < days_before:
        return _result("shrinkage", "FAIL", f"history shrank: {summary}", metrics)
    return _result("shrinkage", "PASS", f"history intact: {summary}", metrics)


def _parse_health_ts(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        log.warning("unreadable health timestamp %r", raw)
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def check_divergence(
    df: pd.DataFrame,
    health: Mapping[str, Any] | None,
    prior: Mapping[str, Any],
    src_cfg: Mapping[str, Any],
    defaults: Mapping[str, Any],
    now: datetime,
) -> CheckResult:
    """THE meta-check: catch a healthy scraper feeding a degraded dataset.

    Fires FAIL only when health says ``ok`` *recently* (within
    ``health_recency_days`` of ``now``) yet the dataset is demonstrably
    degraded, via either arm:

    * staleness beyond the source's own ``fail_days`` (deliberately NOT
      ``warn_days``: fail-level staleness under an ok stamp means degradation;
      warn-level staleness is ordinary publication lag — e.g. eia_lng_exports
      legitimately sits ~114d old against warn 45/fail 135 — which stagnation
      already reports as WARN), or
    * consecutive-flat: for ``mode: accumulation`` sources, current rows ==
      prior rows while the caller's counter already shows 2+ flat runs
      (i.e. this is flat run #3).

    Health status != ok → PASS-with-note: plain stagnation already flags the
    outage, so divergence would be redundant noise.
    """
    if not health:
        return _result("divergence", "SKIPPED", "no health payload available")

    status = str(health.get("status", ""))
    if status != "ok":
        return _result(
            "divergence",
            "PASS",
            f"no divergence signal: health already reports {status!r} — plain stagnation "
            "covers the outage",
            {"status": status},
        )

    stamp = _parse_health_ts(health.get("timestamp_utc"))
    if stamp is None:
        return _result("divergence", "SKIPPED", "health payload lacks a readable timestamp_utc")
    now_aware = now if now.tzinfo is not None else now.replace(tzinfo=UTC)

    age_days = (now_aware - stamp).total_seconds() / 86400.0
    recency = float(defaults.get("health_recency_days", 3))
    if age_days > recency:
        return _result(
            "divergence",
            "SKIPPED",
            f"health stamp {age_days:.1f}d old exceeds recency window {recency:.0f}d — "
            "it cannot vouch for today's dataset",
            {"age_days": round(age_days, 2)},
        )

    reasons: list[str] = []
    warn_days, fail_days = _staleness_thresholds(src_cfg)
    stale_info = _stale_days(df, src_cfg, now)
    if stale_info is not None:
        stale_days, max_period, _resolved = stale_info
        # Fail-level staleness + ok health = degradation (scraper alive,
        # dataset rotting). Warn-level = normal publication lag; stagnation
        # owns that WARN, so divergence must not double-page on it.
        if stale_days > fail_days:
            reasons.append(f"newest period {max_period} is {stale_days}d stale (fail {fail_days}d)")

    if str(src_cfg.get("mode", "")) == "accumulation" and prior:
        rows_prior = int(prior.get("rows", 0))
        if rows_prior and int(len(df)) == rows_prior:
            streak = int(prior.get("consecutive_flat", 0)) + 1
            # A flat accumulation count is ONLY suspicious when the newest
            # period is ALSO aging — that means "stopped growing while it
            # should be growing". A flat count with a still-fresh latest
            # period is NORMAL for a daily EBB source between postings
            # (weekends, gaps, or a run that landed before the source
            # published the new gas day). Never FAIL purely on flatness when
            # the data is within warn_days; staleness owns that signal.
            flat_is_suspicious = (
                stale_info is not None and stale_days > warn_days
            )
            if streak >= 3 and flat_is_suspicious:
                reasons.append(
                    f"accumulation row count flat {streak} consecutive runs at "
                    f"{rows_prior} rows while {stale_days}d stale (warn {warn_days}d)"
                )

    age_hours = age_days * 24.0
    if reasons:
        return _result(
            "divergence",
            "FAIL",
            f"DIVERGENCE: scraper 'ok' {age_hours:.0f}h ago but dataset degraded — "
            f"{'; '.join(reasons)}",
            {"reasons": reasons, "health_age_hours": round(age_hours, 1)},
        )

    stale_txt = f"{stale_info[0]}d" if stale_info is not None else "n/a"
    resolved_txt = f", resolved {stale_info[2]}" if stale_info is not None else ""
    return _result(
        "divergence",
        "PASS",
        f"no divergence: scraper 'ok' {age_hours:.0f}h ago and dataset consistent "
        f"(stale {stale_txt}{resolved_txt}, warn {warn_days}d / fail {fail_days}d)",
        {"stale_days": stale_info[0] if stale_info is not None else None},
    )


def check_flow_leg_uniqueness(df: pd.DataFrame) -> CheckResult:
    """FAIL when any (meter, kind, cycle, flow, period) holds >1 row.

    Why:
        The 2026-08 dual-leg collision: series keys without a flow-direction
        token let a meter's R and D rows overwrite each other within one
        cycle — which leg survived varied day to day. This check pins the
        post-fix invariant: with the flow token embedded, each
        (series_id, period) pair must still be UNIQUE. Two rows sharing a
        series_id and period mean legs collapsed back into one key (or a
        re-merge duplicated rows).

    What:
        Exact duplicate detection on (series_id, period). Sources whose ids
        predate the flow token are skipped via ``skip_checks`` config.
    """
    dup_mask = df.duplicated(subset=["series_id", "period"], keep=False)
    n_dup = int(dup_mask.sum())
    if n_dup == 0:
        return _result(
            "flow_legs",
            "PASS",
            "one row per (series_id, period) — no leg collisions",
            {},
        )
    examples = df.loc[dup_mask, "series_id"].drop_duplicates().head(5).tolist()
    return _result(
        "flow_legs",
        "FAIL",
        f"{n_dup} rows collide on (series_id, period) — flow legs collapsed or rows duplicated",
        {"rows": n_dup, "examples": examples},
    )


def run_source_checks(
    source_key: str,
    df: pd.DataFrame,
    src_cfg: Mapping[str, Any],
    defaults: Mapping[str, Any],
    prior: Mapping[str, Any] | None,
    now: datetime,
    health: Mapping[str, Any] | None,
) -> tuple[list[CheckResult], Severity]:
    """Fan one source through every check; fold to the worst severity.

    SKIPPED results are transparent to the verdict unless *every* check
    skipped, in which case the verdict itself is SKIPPED.
    """
    prior_map: Mapping[str, Any] = prior or {}
    skipped_names = set(src_cfg.get("skip_checks") or [])
    results: list[CheckResult] = [
        check_schema(df, src_cfg, defaults),
        check_value_sanity(df, src_cfg),
        check_flow_leg_uniqueness(df),
        _collision_check(df, src_cfg, defaults),
        check_stagnation(df, src_cfg, now),
        check_gaps(df, src_cfg),
        check_coverage(df, prior_map, src_cfg, defaults),
        check_shrinkage(df, prior_map, src_cfg),
        check_divergence(df, health, prior_map, src_cfg, defaults, now),
    ]

    # Per-source opt-outs (config ``skip_checks``): mark as SKIPPED so the
    # report shows them transparently without failing the source.
    results = [
        (
            res
            if res["check"] not in skipped_names
            else _result(res["check"], "SKIPPED", "skipped by skip_checks config", {})
        )
        for res in results
    ]

    worst: Severity = "SKIPPED"
    for res in results:
        if res["severity"] != "SKIPPED" and _SEVERITY_RANK[res["severity"]] > _SEVERITY_RANK[worst]:
            worst = res["severity"]

    log.info("integrity[%s]: verdict=%s across %d check(s)", source_key, worst, len(results))
    return results, worst


# --------------------------------------------------------------------------
# State helpers — the only code here that persists anything.
# --------------------------------------------------------------------------


def load_state(path: Path) -> dict[str, Any]:
    """Read persisted prior-run baselines; ``{}`` on missing/corrupt file.

    Why tolerate corruption: the monitor must stay runnable so it can report
    the corruption instead of crashing the pipeline that feeds it.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        log.warning("integrity state %s not found — starting with empty baselines", path)
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("integrity state %s corrupt (%s) — starting with empty baselines", path, exc)
        return {}
    if not isinstance(payload, dict):
        log.warning("integrity state %s is not a JSON object — ignoring", path)
        return {}
    return payload


def build_source_state(df: pd.DataFrame, now: datetime) -> dict[str, Any]:
    """Snapshot the metrics shrinkage/coverage/divergence compare next run.

    CRITICAL caller contract: do **not** persist this snapshot for a source
    whose shrinkage check just FAILED.  Overwriting the baseline hides the
    loss; keeping the pre-corruption baseline lets recovery stay visible when
    row counts climb back above the old watermark.
    """
    has_periods = "period" in df.columns and not df.empty
    return {
        "rows": int(len(df)),
        "distinct_periods": int(df["period"].nunique()) if has_periods else 0,
        "distinct_series": int(df["series_id"].nunique()) if "series_id" in df.columns else 0,
        "max_period": str(df["period"].max()) if has_periods else "",
        "checked_at": now.isoformat(),
    }
