"""Tests for the Gulf South meter inventory and Freeport LNG matcher."""

from __future__ import annotations

from scrapers.energy_transfer.meter_inventory import identify_freeport_meters


def test_stratton_ridge_high_confidence() -> None:
    """Stratton Ridge (loc 24329) or names containing Freeport/Stratton Ridge match as high confidence."""
    # Match on name
    locs_name = [
        {"loc_id": 12345, "loc_name": "Freeport LNG Delivery", "flow_ind": "D"},
        {"loc_id": 54321, "loc_name": "Stratton Ridge Interconnect", "flow_ind": "D"},
        {"loc_id": 99999, "loc_name": "FLNG Feedgas", "flow_ind": "D"},
    ]
    matched = identify_freeport_meters(locs_name)
    assert len(matched) == 3
    for m in matched:
        assert m["confidence"] == "high"
        assert m["flow_ind"] == "D"

    # Match on loc_id seed
    locs_id = [{"loc_id": 24329, "loc_name": "Stratton Ridge (To Freeport Lng)", "flow_ind": "D"}]
    matched_id = identify_freeport_meters(locs_id)
    assert len(matched_id) == 1
    assert matched_id[0]["confidence"] == "high"
    assert matched_id[0]["loc_id"] == 24329


def test_coastal_bend_candidate_not_high() -> None:
    """Coastal Bend (case-insensitive) matches as candidate, not high confidence."""
    locs = [
        {"loc_id": 23700, "loc_name": "Gulf South Transfer To Coastal Bend", "flow_ind": "D"},
        {"loc_id": 88888, "loc_name": "COASTAL BEND INTERCONNECT", "flow_ind": "D"},
    ]
    matched = identify_freeport_meters(locs)
    assert len(matched) == 2
    for m in matched:
        assert m["confidence"] == "candidate"
        assert "regional transfer point" in m["note"]


def test_non_lng_meter_rejected() -> None:
    """Non-LNG or non-delivery meters are not matched."""
    locs = [
        # Receipt point matching Freeport names (should be rejected because flow_ind != 'D')
        {"loc_id": 24329, "loc_name": "Stratton Ridge Receipt", "flow_ind": "R"},
        {"loc_id": 12345, "loc_name": "Freeport LNG Receipt", "flow_ind": "R"},
        # Delivery point not matching LNG keywords
        {"loc_id": 11111, "loc_name": "Houston City Gate", "flow_ind": "D"},
        # None/empty names
        {"loc_id": 22222, "loc_name": "", "flow_ind": "D"},
    ]
    matched = identify_freeport_meters(locs)
    assert len(matched) == 0
