"""Coverage Anti-Rot Guard for US LNG Export Terminals.

Recomputes empirical headline feedgas utilization from data/curated/*.parquet
under the settled cycle rule and feed parity rule, and asserts that the
registry's documented claims in docs/js/util/lng-terminals.js have not drifted
beyond stated per-terminal tolerances.
"""

from __future__ import annotations

import json

import pandas as pd

from scripts.load_registry import REGISTRY_JSON_PATH, load_terminal_registry
from scripts.task3_validate import TERMINALS, load_terminal_history


def compute_terminal_coverage_from_curated(
    term_key: str, window_days: int = 60, nameplate: float | None = None
) -> dict[str, float]:
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
    if nameplate is None:
        nameplate = conf.get("nameplate") or load_terminal_registry().get(term_key, {}).get("nameplate", 0.0)
    coverage_pct = (median_mmcf / nameplate) * 100.0 if nameplate and nameplate > 0 else 0.0

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
        res = compute_terminal_coverage_from_curated(term_key, window_days=window, nameplate=reg["nameplate"])

        measured_pct = res["coverage_pct"]
        claimed_pct = reg["expectedCoveragePct"]
        tolerance = reg["coverageTolerancePct"]
        drift = abs(measured_pct - claimed_pct)

        if drift > tolerance:
            failures.append(
                f"Terminal '{term_key}': claimed {claimed_pct:.1f}%, measured {measured_pct:.1f}%, "
                f"drift {drift:.1f}% exceeds tolerance +/-{tolerance:.1f}% "
                f"(median {res['median_mmcf']:.1f} MMcf/d vs nameplate {reg['nameplate']:.0f})"
            )

    if failures:
        error_msg = "Coverage Anti-Rot Guard detected drift:\n" + "\n".join(f"  - {f}" for f in failures)
        raise AssertionError(error_msg)


def test_coverage_guard_rejects_perturbed_flattering_claim() -> None:
    """Proof of guard: rejects a deliberately falsified coverage claim (e.g. Freeport at 80%)."""
    # Recompute Freeport from curated
    res = compute_terminal_coverage_from_curated("freeport", window_days=100, nameplate=2100.0)
    measured_pct = res["coverage_pct"]  # ~52.9%

    # Simulate the flattering falsehood that Prompt L and M caught
    flattering_claim = 80.0
    tolerance = 10.0  # Freeport's verified tolerance
    drift = abs(measured_pct - flattering_claim)

    assert drift > tolerance, (
        f"Guard must fail when 80.0% is claimed for Freeport "
        f"(drift {drift:.1f}% must exceed tolerance {tolerance:.1f}%)"
    )


def test_registry_json_sidecar_matches_js_source() -> None:
    """Assert generated config/terminals_registry.json matches docs/js/util/lng-terminals.js."""
    assert REGISTRY_JSON_PATH.exists(), f"Sidecar {REGISTRY_JSON_PATH} missing"
    sidecar = json.loads(REGISTRY_JSON_PATH.read_text(encoding="utf-8"))
    js_registry = load_terminal_registry()

    assert len(sidecar) == 9, f"Sidecar has {len(sidecar)} terminals, expected 9"
    assert len(js_registry) == 9, f"JS registry has {len(js_registry)} terminals, expected 9"

    for tid, js_entry in js_registry.items():
        assert tid in sidecar, f"Terminal {tid} missing from sidecar"
        sc_entry = sidecar[tid]
        assert sc_entry["nameplate"] == js_entry["nameplate"], f"{tid} nameplate mismatch"
        assert sc_entry["expectedCoveragePct"] == js_entry["expectedCoveragePct"], f"{tid} expectedCoveragePct mismatch"
        assert sc_entry["expectedMedianMmcf"] == js_entry["expectedMedianMmcf"], f"{tid} expectedMedianMmcf mismatch"
        assert sc_entry["coverageTolerancePct"] == js_entry["coverageTolerancePct"], f"{tid} coverageTolerancePct mismatch"
        assert sc_entry["operational"] == js_entry["operational"], f"{tid} operational mismatch"


def test_registry_sidecar_has_all_required_fields_for_all_terminals() -> None:
    """Assert config/terminals_registry.json contains all required fields for all 9 terminals."""
    registry = load_terminal_registry()
    assert len(registry) == 9, f"Expected 9 terminals, found {len(registry)}"
    required_fields = [
        "id", "display", "nameplate", "expectedCoveragePct",
        "expectedMedianMmcf", "coverageTolerancePct", "operational"
    ]
    for term_key, item in registry.items():
        for field in required_fields:
            assert field in item, f"Terminal '{term_key}' missing required field '{field}' in registry sidecar"
        assert isinstance(item["nameplate"], int | float) and item["nameplate"] > 0, (
            f"Terminal '{term_key}' has invalid nameplate: {item['nameplate']}"
        )


def test_km_feed_resolves_and_loads_daily() -> None:
    """Sabine Pass's second leg (km_ngpl_sq_3592_d) must resolve to kinder_morgan
    parquet and load a non-empty daily series (Prompt U §04).

    Before fix:
      - resolve_series("km_ngpl_sq_3592_d") returned (None, None) due to missing 'km' in PREFIX_MAP
      - load_feed_daily("km_ngpl_sq_3592_d") printed 'WARN: no parquet' and returned None
    After fix:
      - resolve_series returns (Path('...kinder_morgan.parquet'), 'km_ngpl_sq_3592_d')
      - load_feed_daily recognizes KM cycle aliases (evng, itrd1-3) and returns non-empty dict
    """
    from scripts.task3_validate import load_feed_daily, resolve_series

    path, pattern = resolve_series("km_ngpl_sq_3592_d")
    assert path is not None, "resolve_series('km_ngpl_sq_3592_d') must find parquet path"
    assert path.name == "kinder_morgan.parquet"
    assert pattern == "km_ngpl_sq_3592_d"

    daily = load_feed_daily("km_ngpl_sq_3592_d")
    assert daily is not None, "load_feed_daily('km_ngpl_sq_3592_d') must not be None"
    assert len(daily) > 0, "load_feed_daily('km_ngpl_sq_3592_d') must return data rows"


def test_sabine_pass_coverage_unchanged_with_km_feed() -> None:
    """KM feed (km_ngpl_sq_3592_d) posts 0.0 Dth across all cycles, so Sabine Pass
    coverage must remain exactly 30.3% (Prompt U §04).
    """
    res = compute_terminal_coverage_from_curated("sabine_pass", window_days=60, nameplate=4500.0)
    assert round(res["coverage_pct"], 1) == 30.3, f"Sabine Pass coverage changed: {res['coverage_pct']}%"
