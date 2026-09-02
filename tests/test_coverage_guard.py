"""Coverage Anti-Rot Guard for US LNG Export Terminals.

Recomputes empirical headline feedgas utilization from data/curated/*.parquet
under the settled cycle rule and feed parity rule, and asserts that the
registry's documented claims in docs/js/util/lng-terminals.js have not drifted
beyond stated per-terminal tolerances.
"""

from __future__ import annotations

import pandas as pd
import pytest

from scripts.load_registry import load_terminal_registry
from scripts.task3_validate import TERMINALS, load_terminal_history


def compute_terminal_coverage_from_curated(term_key: str, window_days: int = 60) -> dict[str, float]:
    """Recompute empirical 60-day median coverage from curated parquet data.
    
    Applies:
      - Settled cycle rule: SQ only, hourly id{HH}00 excluded, cycle precedence.
      - Feed parity rule: only complete days where all active feeds reported.
    """
    hist, conf = load_terminal_history(term_key)
    if not hist:
        return {"days": 0, "median_mmcf": 0.0, "coverage_pct": 0.0}

    sorted_dates = sorted(hist.keys())
    # Use trailing complete days (up to window_days)
    sample_dates = sorted_dates[-window_days:]
    sample_mmcf = [hist[d]["value"] / 1.025 / 1000.0 for d in sample_dates]

    if not sample_mmcf:
        return {"days": 0, "median_mmcf": 0.0, "coverage_pct": 0.0}

    median_mmcf = float(pd.Series(sample_mmcf).median())
    nameplate = conf["nameplate"]
    coverage_pct = (median_mmcf / nameplate) * 100.0 if nameplate > 0 else 0.0

    return {
        "days": len(sample_mmcf),
        "median_mmcf": median_mmcf,
        "coverage_pct": coverage_pct,
    }


def test_terminal_coverage_guard_against_curated_parquets() -> None:
    """Anti-rot guard: asserts that registry claims match curated parquet reality."""
    registry = load_terminal_registry()
    assert len(registry) == 9, "All 9 terminals must be defined in registry"

    failures = []

    for term_key, reg in registry.items():
        if not reg["operational"]:
            assert reg["expectedCoveragePct"] == 0.0
            continue

        if term_key not in TERMINALS:
            continue

        # Recompute from curated parquet data
        # Note: Freeport uses its 100-day dual-feed overlap baseline
        window = 100 if term_key == "freeport" else 60
        res = compute_terminal_coverage_from_curated(term_key, window_days=window)

        measured_pct = res["coverage_pct"]
        claimed_pct = reg["expectedCoveragePct"]
        tolerance = reg["coverageTolerancePct"]
        drift = abs(measured_pct - claimed_pct)

        if drift > tolerance:
            failures.append(
                f"Terminal '{term_key}': claimed {claimed_pct:.1f}%, measured {measured_pct:.1f}%, "
                f"drift {drift:.1f}% exceeds tolerance ±{tolerance:.1f}% "
                f"(median {res['median_mmcf']:.1f} MMcf/d vs nameplate {reg['nameplate']:.0f})"
            )

    if failures:
        error_msg = "Coverage Anti-Rot Guard detected drift:\n" + "\n".join(f"  - {f}" for f in failures)
        raise AssertionError(error_msg)


def test_coverage_guard_rejects_perturbed_flattering_claim() -> None:
    """Proof of guard: rejects a deliberately falsified coverage claim (e.g. Freeport at 80%)."""
    # Recompute Freeport from curated
    res = compute_terminal_coverage_from_curated("freeport", window_days=100)
    measured_pct = res["coverage_pct"]  # ~52.9%

    # Simulate the flattering falsehood that Prompt L and M caught
    flattering_claim = 80.0
    tolerance = 10.0  # Freeport's verified tolerance
    drift = abs(measured_pct - flattering_claim)

    assert drift > tolerance, (
        f"Guard must fail when 80.0% is claimed for Freeport "
        f"(drift {drift:.1f}% must exceed tolerance {tolerance:.1f}%)"
    )
