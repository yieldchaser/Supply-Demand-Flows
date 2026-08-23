"""Tests for the Blue Tide meter classification rules.

Strategy:
    - Zero network calls: the classifier is pure logic over names, flows,
      and stats dicts; the parquet loader is exercised only through
      build_universe against the repo's own curated files (read-only), and
      every seed assertion below runs on synthetic stats so the rule engine
      itself is tested without any data dependency.
"""

from __future__ import annotations

from typing import Any

import pytest

from scripts.classify_meters import (
    RESEARCHED,
    classify_meter,
    parse_flow,
    parse_series_id,
)


def _stats(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "days": 90,
        "mean_dth": 100_000.0,
        "max_dth": 150_000.0,
        "std_dth": 5_000.0,
        "zero_frac": 0.0,
        "neg_frac": 0.0,
        "summer_mean_dth": None,
        "winter_mean_dth": None,
        "season_flip": False,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Frozen-seed assertions from the task brief.
# ---------------------------------------------------------------------------


def test_petal_is_storage_high() -> None:
    """50202 Petal Pipeline Injection/Withdrawal -> storage/high (seed)."""
    cls, conf, evidence, _ = classify_meter(
        "gulf_south", "50202", "Petal Pipeline Injection/Withdrawal", "B",
        _stats(mean_dth=198_016, max_dth=694_948),
    )
    assert cls == "storage"
    assert conf == "high"
    assert any("Injection/Withdrawal" in e for e in evidence)


def test_perryville_is_hub_not_basin_egress() -> None:
    """23201 Perryville Transportation Point -> hub/high via researched entry.

    Reasoning: RBN/CenterPoint document Perryville as a market hub with 21
    interstate interconnects — a trading point, not a producing-basin
    takeaway. It is the system's highest-volume meter, which is exactly why
    it must not silently count as basin production outflow.
    """
    assert ("gulf_south", "23201") in RESEARCHED
    cls, conf, evidence, attrs = classify_meter(
        "gulf_south", "23201", "Perryville Transportation Point", "?",
        _stats(mean_dth=1_685_634),
    )
    assert cls == "hub"
    assert conf == "high"
    assert attrs.get("facility") == "Perryville Hub"
    assert any("RBN" in e or "CenterPoint" in e for e in evidence)


def test_stratton_ridge_is_lng_export() -> None:
    """24329 Stratton Ridge (To Freeport Lng) -> lng_export/high (seed)."""
    cls, conf, _, _ = classify_meter(
        "gulf_south", "24329", "Stratton Ridge (To Freeport Lng)", "D",
        _stats(mean_dth=1_055_938),
    )
    assert cls == "lng_export"
    assert conf == "high"


def test_paybackgxp_is_system() -> None:
    """quorum PAYBACKGXP 'SYSTEM BALANCING' -> system (seed)."""
    cls, conf, evidence, _ = classify_meter(
        "quorum", "paybackgxp", "SYSTEM BALANCING", "?",
        _stats(mean_dth=0, zero_frac=1.0),
    )
    assert cls == "system"
    # All-zero window caps confidence at medium; the class call is the seed.
    assert conf in ("medium", "high")
    assert any("balancing" in e.lower() for e in evidence)


# ---------------------------------------------------------------------------
# Confidence guardrails.
# ---------------------------------------------------------------------------


def test_never_promotes_guess_to_high() -> None:
    """Profile-only inference stays medium; weak naming stays low."""
    cls, conf, _, _ = classify_meter(
        "gulf_south", "99999", "Mystery Point", "?",
        _stats(summer_mean_dth=-50_000, winter_mean_dth=80_000, season_flip=True),
    )
    assert cls == "storage"
    assert conf == "medium"

    cls2, conf2, _, _ = classify_meter(
        "gulf_south", "99998", "Some Bidirectional Thing Injection WD", "B", _stats(),
    )
    # 'INJECTION ... WD' matches the explicit storage rule -> high (the name
    # IS unambiguous); the weak-inference branch is exercised by the
    # flow=='B' fallback with a name that lacks both keywords.
    assert cls2 == "storage" and conf2 == "high"

    cls3, conf3, _, _ = classify_meter(
        "gulf_south", "99997", "Unlabeled Bidirectional Point", "B", _stats(),
    )
    assert cls3 == "storage"
    assert conf3 == "low"


def test_all_zero_window_caps_confidence_at_medium() -> None:
    """A dead meter keeps its name-based class but cannot be high."""
    cls, conf, evidence, _ = classify_meter(
        "gulf_south", "23602", "Jackson Storage Withdrawal", "D",
        _stats(mean_dth=0, zero_frac=1.0),
    )
    assert cls == "storage"
    assert conf == "medium"
    assert any("all-zero" in e for e in evidence)


def test_unknown_stays_unknown() -> None:
    """No signal at all -> unknown/unknown, never a guess."""
    for name in ("Widget Point", "Misc Area 99"):
        cls, conf, _, _ = classify_meter(
            "gulf_south", "88888", name, "?", _stats()
        )
        assert cls == "unknown"
        assert conf == "unknown"


# ---------------------------------------------------------------------------
# Rule-engine details.
# ---------------------------------------------------------------------------


def test_haynesville_gatherer_receipt_is_medium_basin_egress() -> None:
    """'(From Mark West)' style receipts with volume -> basin_egress/medium."""
    cls, conf, _, attrs = classify_meter(
        "gulf_south", "22492", "Bennington (From Mark West)", "R", _stats(),
    )
    assert cls == "basin_egress"
    assert conf == "medium"
    assert attrs.get("basin") == "haynesville"


def test_low_volume_gatherer_name_does_not_classify() -> None:
    """Materiality floor: tiny gatherer-named meters stay unknown."""
    cls, conf, _, _ = classify_meter(
        "gulf_south", "77777", "Markwest Tiny Cp", "?",
        _stats(mean_dth=500),
    )
    assert cls == "unknown"


def test_power_and_industrial_and_ldc_rules() -> None:
    cases = [
        ("Jack Watson Power Plant @ Gulfport", "power_burn"),
        ("Air Products Hydrogen @ Garyville", "industrial"),
        ("Ergon Refinery", "industrial"),
        ("Byram City Gate", "ldc"),
        ("Payback To Shipper", "system"),
    ]
    for name, expected in cases:
        cls, _, _, _ = classify_meter("gulf_south", "11111", name, "?", _stats())
        assert cls == expected, f"{name}: expected {expected}, got {cls}"


def test_season_flip_detection_is_profile_only() -> None:
    """Sign flip without STORAGE keyword -> storage/medium, never high."""
    cls, conf, _, _ = classify_meter(
        "x", "y", "Unnamed Seasonal Point", "?",
        _stats(summer_mean_dth=-10_000, winter_mean_dth=25_000, season_flip=True),
    )
    assert cls == "storage" and conf == "medium"


# ---------------------------------------------------------------------------
# Parsers.
# ---------------------------------------------------------------------------


def test_parse_series_id_shapes() -> None:
    assert parse_series_id("gulf_south_sq_24329_id3") == ("gulf_south", "24329", "sq")
    assert parse_series_id("golden_pass_sq_1097217_timely") == (
        "golden_pass", "1097217", "sq",
    )
    assert parse_series_id("creole_trail_design_CT109413_id3") == (
        "creole_trail", "CT109413", "design",
    )
    assert parse_series_id("not_a_series") == (None, None, None)


def test_parse_flow_precedence() -> None:
    assert parse_flow("Terminal") == "?"
    assert parse_flow("CC121033-TGP-SINTON-R (ID3)") == "R"
    assert parse_flow("CC200221-CORPUS CHRISTI-CCLIQ-D (ID3)") == "D"
    assert parse_flow("Cameron LNG (Rec)") == "R"
    assert parse_flow("Coastal Bend Gulf South Transfer Receipt") == "R"
    assert parse_flow("Wharton (From Enterprise Texas)") == "R"
    assert parse_flow("Holmesville (To Transco)") == "D"
    assert parse_flow("Petal Pipeline Injection/Withdrawal") == "B"


# ---------------------------------------------------------------------------
# Universe build (read-only over repo parquets).
# ---------------------------------------------------------------------------


def test_build_universe_covers_expected_totals() -> None:
    """The full run classifies the known universe with sane distributions."""
    from scripts.classify_meters import build_universe

    universe = build_universe()
    assert set(universe) == {"gulf_south", "gasnom", "quorum", "bhe", "cheniere"}
    counts = {s: len(ms) for s, ms in universe.items()}
    assert counts["gulf_south"] == 717
    assert counts["gasnom"] == 61
    assert counts["quorum"] == 11
    assert counts["bhe"] == 1
    assert counts["cheniere"] == 22

    classes = {m["class"] for ms in universe.values() for m in ms}
    assert classes <= {
        "lng_export", "storage", "basin_egress", "power_burn", "industrial",
        "ldc", "interconnect", "system", "hub", "export_pipeline", "unknown",
    }

    # Every classified meter carries non-empty evidence and valid confidence.
    for ms in universe.values():
        for m in ms:
            assert m["evidence"], m["loc_id"]
            assert m["confidence"] in ("high", "medium", "low", "unknown")


@pytest.mark.parametrize(
    ("source", "loc_id", "expected_class"),
    [
        ("gulf_south", "50202", "storage"),
        ("gulf_south", "23201", "hub"),
        ("gulf_south", "24329", "lng_export"),
        ("quorum", "paybackgxp", "system"),
        ("bhe", "40704", "lng_export"),
    ],
)
def test_end_to_end_seeds(source: str, loc_id: str, expected_class: str) -> None:
    """End-to-end seeds through the real parquet pipeline (no network)."""
    from scripts.classify_meters import build_universe

    by_loc = {m["loc_id"]: m for m in build_universe()[source]}
    assert by_loc[loc_id]["class"] == expected_class
