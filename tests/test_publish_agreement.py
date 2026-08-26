"""Negative + positive tests for the registry<->config<->bundle agreement audit.

Both arms of the assertion are exercised:
  (a) a measured feed whose loc is NOT high-confidence in its meter config must
      be reported (would be dropped by the prune),
  (b) a measured feed that ships ZERO non-zero rows in the built bundle must be
      reported (panel would render empty),
and a clean registry must pass.
"""
from __future__ import annotations

import importlib

PUB = importlib.import_module("publishers.export_dashboard_json")


# A minimal registry fragment with one terminal, three feeds exercising each arm.
GOOD_REG = """
  sabine_pass: {
    feeds: [
      { source: 'cheniere', series: 'creole_trail_sq_CT200111_d', kind: 'measured-partial' },
      { source: 'kinder_morgan', series: 'km_ngpl_sq_3592_d', kind: 'measured-partial' },
      { source: 'cheniere', series: 'creole_trail_sq_CT109413_r', kind: 'context' },
    ],
  },
"""

# Arm (a): headline loc 'ZZ999' is not in the high_conf allowlist -> must fail.
BAD_CONF_REG = """
  sabine_pass: {
    feeds: [
      { source: 'cheniere', series: 'creole_trail_sq_ZZ999_d', kind: 'measured' },
    ],
  },
"""

# Arm (b): headline series ships only zero rows -> must fail.
ZERO_ROWS_BUNDLE = {
    "cheniere": {
        "data": [
            {"series_id": "creole_trail_sq_CT200111_d_evening", "value": 0},
        ]
    }
}

# Good bundle: CT200111 + NGPL 3592 both present and non-zero.
GOOD_BUNDLE = {
    "cheniere": {
        "data": [
            {"series_id": "creole_trail_sq_CT200111_d_evening", "value": 1408700},
        ]
    },
    "kinder_morgan": {
        "data": [
            {"series_id": "km_ngpl_sq_3592_d_best", "value": 480674},
        ]
    },
}


def test_arm_a_low_confidence_reported():
    """A measured feed whose loc is not high-confidence must be flagged."""
    high_conf = {"cheniere": {"ct200111"}, "kinder_morgan": {"3592"}}
    problems = PUB._check_agreement(BAD_CONF_REG, GOOD_BUNDLE, high_conf)
    assert any("ZZ999" in p and "high-confidence" in p for p in problems), problems


def test_arm_b_zero_rows_reported():
    """A measured feed shipping only zero rows must be flagged."""
    high_conf = {"cheniere": {"ct200111"}, "kinder_morgan": {"3592"}}
    problems = PUB._check_agreement(GOOD_REG, ZERO_ROWS_BUNDLE, high_conf)
    assert any(
        "ZERO non-zero rows" in p and "CT200111" in p for p in problems
    ), problems


def test_clean_registry_passes():
    """A registry whose headlines are high-confidence AND ship non-zero rows."""
    high_conf = {"cheniere": {"ct200111", "ct109413"}, "kinder_morgan": {"3592"}}
    problems = PUB._check_agreement(GOOD_REG, GOOD_BUNDLE, high_conf)
    assert problems == [], problems


def test_context_feeds_exempt_from_bundle_check():
    """Context feeds must not trip the zero-row arm even if absent from bundle."""
    high_conf = {"cheniere": {"ct200111", "ct109413"}, "kinder_morgan": {"3592"}}
    # CT109413 (context) is high-conf but ships no rows here — must NOT be flagged.
    problems = PUB._check_agreement(GOOD_REG, GOOD_BUNDLE, high_conf)
    assert not any("CT109413" in p for p in problems), problems
