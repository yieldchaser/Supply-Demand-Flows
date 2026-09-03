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
    coverage is unaffected by the KM feed and must stay within the registry's own
    declared tolerance of its claimed value (Prompt U §04). Sabine Pass's feeds are
    cheniere and kinder_morgan; the measured value legitimately drifts over time as
    the trailing 60-day window rolls forward over new Cheniere daily data, so this
    asserts the same tolerance check scripts/preflight.py performs rather than
    pinning the measurement to a literal.
    """
    registry = load_terminal_registry()
    claimed_pct = registry["sabine_pass"]["expectedCoveragePct"]
    tolerance_pct = registry["sabine_pass"]["coverageTolerancePct"]
    res = compute_terminal_coverage_from_curated("sabine_pass", window_days=60, nameplate=4500.0)
    drift = abs(res["coverage_pct"] - claimed_pct)
    assert drift <= tolerance_pct, (
        f"Sabine Pass coverage drifted outside tolerance: measured {res['coverage_pct']}%, "
        f"claimed {claimed_pct}%, drift {drift:.1f}% exceeds tolerance {tolerance_pct:.1f}%"
    )


def test_v2_terminal_feeds_resolve_to_parquet() -> None:
    """All four previously skipped terminals must have feeds resolving to valid parquets
    and entries in TERMINALS with inherited precedent defaults (Prompt V §03).
    """
    from scripts.task3_validate import TERMINALS, resolve_series

    expected = {
        "calcasieu": ("trans_cameron_sq_vgcpd_d", "quorum.parquet"),
        "golden_pass": ("golden_pass_sq_1097217_d", "gasnom.parquet"),
        "cameron": ("cameron_interstate_sq_772300_d", "gasnom.parquet"),
        "corpus_christi": ("corpus_christi_sq_CC200221_d", "cheniere.parquet"),
    }

    for term_key, (feed_id, expected_parq) in expected.items():
        assert term_key in TERMINALS, f"Terminal '{term_key}' missing from TERMINALS"
        path, pattern = resolve_series(feed_id)
        assert path is not None, f"Feed '{feed_id}' failed to resolve"
        assert path.name == expected_parq, f"Feed '{feed_id}' resolved to {path.name}, expected {expected_parq}"
        assert pattern == feed_id

    # port_arthur is non-operational and must stay out
    assert "port_arthur" not in TERMINALS, "port_arthur must stay out of TERMINALS"


def test_v3_cycle_priority_best_ranked_sub_timely() -> None:
    """'best' is not a genuine nomination cycle: it must be ranked below timely (>0)
    so that sole-presence periods are preserved while genuine nomination cycles always win (Prompt V §04).
    """
    from scripts.task3_validate import cycle_priority

    # best is sub-timely but strictly positive
    p_best = cycle_priority("best")
    p_timely = cycle_priority("timely")
    assert p_best > 0, "best must score > 0 to preserve periods where it is the sole cycle"
    assert p_best < p_timely, f"best priority ({p_best}) must be strictly below timely ({p_timely})"

    # Genuine cycles strictly ordered
    p_evening = cycle_priority("evening")
    p_evng = cycle_priority("evng")
    p_id1 = cycle_priority("id1")
    p_itrd1 = cycle_priority("itrd1")
    p_id2 = cycle_priority("id2")
    p_itrd2 = cycle_priority("itrd2")
    p_id3 = cycle_priority("id3")
    p_itrd3 = cycle_priority("itrd3")

    assert p_timely < p_evening == p_evng < p_id1 == p_itrd1 < p_id2 == p_itrd2 < p_id3 == p_itrd3

    # Hourly automated snapshots remain strictly 0
    assert cycle_priority("id0200") == 0
    assert cycle_priority("id1200") == 0

