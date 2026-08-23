"""TASK 3 — collision guard: raw rows mapping to the same (series_id, period).

Why:
    ``merge_into_curated`` dedups on (series_id, period) keeping max
    ingested_at. If a transformer maps TWO source rows with DIFFERENT values
    onto the same key, one value is silently destroyed — exactly how Gulf
    South lost one leg of every dual-leg storage meter (R vs D rows sharing a
    series_id). This check makes that bug class impossible to ship again:
    any key fed >1 row whose values differ is a FAIL naming the keys.

What:
    Runs on any frame carrying series_id/period/value. Curated parquets hold
    post-dedup data, so on them the check detects residual duplicates (a
    transformer that stopped deduping, or a new dimension collapse shipped
    without updating series_id). For full pre-dedup power, feed it the
    transformer's batch BEFORE its drop_duplicates step (see
    scripts/collision_guard_report.py for the gulf_south reconstruction).

Verdict semantics:
    * FAIL  — ≥1 key with multiple DIFFERING values: guaranteed silent
      overwrite at accumulation time. Message names up to 5 keys with their
      distinct values; details carry 25 more.
    * WARN  — duplicate keys exist but all identical, above the configured
      benign-repeat rate.
    * PASS  — no collisions (or only benign identical repeats below the bar).

Config (per source in integrity_rules.yaml)::

    collision:
      ignore_series_globs: ["gulf_south_oac_*"]   # opt-out list
      identical_dup_warn_pct: 5.0                 # benign-repeat WARN bar (%)

Failure modes:
    Missing columns or empty frame -> SKIPPED (schema owns missing columns).
"""

from __future__ import annotations

import fnmatch
from collections.abc import Mapping
from typing import Any

import pandas as pd

from validators.integrity import CheckResult, _result


def check_collision(
    df: pd.DataFrame,
    src_cfg: Mapping[str, Any],
    defaults: Mapping[str, Any],
) -> CheckResult:
    """FAIL when multiple rows share (series_id, period) with differing values."""
    if df.empty or "series_id" not in df.columns or "period" not in df.columns:
        return _result("collision", "SKIPPED", "no series_id/period columns to audit")
    if "value" not in df.columns:
        return _result("collision", "SKIPPED", "no value column — schema owns this failure")

    cfg: Mapping[str, Any] = src_cfg.get("collision") or {}
    ignore_globs = [str(g) for g in cfg.get("ignore_series_globs", [])]
    warn_pct = float(cfg.get("identical_dup_warn_pct", 100.0))

    work = df[["series_id", "period", "value"]].copy()
    work["series_id"] = work["series_id"].astype(str)
    for pat in ignore_globs:
        mask = work["series_id"].map(lambda s, p=pat: fnmatch.fnmatch(s, p))
        work = work.loc[~mask]

    grouped = work.groupby(["series_id", "period"])["value"]
    sizes = grouped.size()
    n_keys = int(sizes.shape[0])

    dup_keys = sizes[sizes > 1]
    n_dup = int(dup_keys.shape[0])
    if n_dup == 0:
        return _result(
            "collision",
            "PASS",
            f"no collisions: {n_keys:,} distinct (series_id, period) keys, each from 1 row",
            {"keys": n_keys},
        )

    nunique = grouped.nunique()
    colliding = nunique[nunique > 1]
    n_colliding = int(colliding.shape[0])
    identical_only = n_dup - n_colliding

    examples: list[str] = []
    for sid, per in colliding.index[:5]:
        vals = sorted(
            work.loc[(work["series_id"] == sid) & (work["period"] == per), "value"]
            .unique()
            .tolist()
        )
        examples.append(f"{sid}@{per}: {vals[:4]}")

    if n_colliding:
        pct = n_colliding / max(n_keys, 1) * 100
        return _result(
            "collision",
            "FAIL",
            f"SILENT-OVERWRITE risk: {n_colliding:,} of {n_keys:,} ({pct:.2f}%) "
            f"(series_id, period) keys map from MULTIPLE raw rows with DIFFERING "
            f"values; {identical_only:,} more keys have benign identical repeats. "
            f"The accumulator keeps only the last row per key, so one value is "
            f"destroyed per colliding key. Worst: {'; '.join(examples)}",
            {
                "colliding_keys": n_colliding,
                "keys_total": n_keys,
                "benign_identical_keys": identical_only,
                "examples": examples,
                "colliding_sample": [
                    {"series_id": sid, "period": str(per), "n_values": int(nv)}
                    for (sid, per), nv in colliding.head(25).items()
                ],
            },
        )

    # duplicates exist but all identical — informational unless above the WARN bar
    dup_pct = n_dup / max(n_keys, 1) * 100
    if dup_pct > warn_pct:
        return _result(
            "collision",
            "WARN",
            f"{n_dup:,} of {n_keys:,} keys ({dup_pct:.1f}%) carry identical-value "
            f"duplicate rows (above warn bar {warn_pct:.0f}%) — re-scrape amplification?",
            {"duplicate_keys": n_dup, "keys_total": n_keys},
        )
    return _result(
        "collision",
        "PASS",
        f"{n_dup:,} duplicate keys all carry identical values (no overwrite risk)",
        {"duplicate_keys": n_dup, "keys_total": n_keys},
    )
