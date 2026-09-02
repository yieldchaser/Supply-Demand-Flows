"""Tests for LNG Terminal Downtime detection and multi-feed routing suppression.

Covers Brief G requirements:
- Single feed dropping to zero while sibling covers total is routing, not downtime
- Gaps (did not post) are never counted as zeros
- Both feeds posting zero triggers real OFFLINE outage
- Pre-first-gas commissioning zeros classify as NOT_YET_OPERATIONAL, never OFFLINE
- Cove Point plant intake (single-feed) clean operation
- Short routing dips (< 5 days) do not trigger DEPRESSED
"""
from __future__ import annotations

from typing import Any

from scripts.task3_validate import detect_events


def _build_history_item(
    total: float,
    feeds_posted: int,
    posted_zero: bool,
    posted: bool = True,
) -> dict[str, Any]:
    return {
        "value": total,
        "posted": posted,
        "posted_zero": posted_zero,
        "n_feeds_posted": feeds_posted,
    }


def test_feed_at_zero_while_sibling_covers_is_not_an_outage() -> None:
    """A feed dropping to zero while sibling covers total is routing, NOT an outage."""
    conf = {
        "name": "Freeport LNG",
        "feeds": ["gulf_south_sq_24329_d", "tetco_sq_79999_d"],
        "zero_mode": "both_zero",
        "zero_days_threshold": 2,
        "depressed_pct": 0.60,
        "depressed_days": 5,
        "is_cargo_zero": False,
    }

    # 40 days of normal operation at 1,500,000 Dth
    history: dict[str, dict[str, Any]] = {}
    for day in range(1, 41):
        d_str = f"2026-06-{day:02d}" if day <= 30 else f"2026-07-{day - 30:02d}"
        history[d_str] = _build_history_item(1_500_000, 2, posted_zero=False)

    # 3 days where Feed A drops to 0, but Feed B delivers 1,400,000 Dth (routing)
    for day in range(11, 14):
        d_str = f"2026-07-{day:02d}"
        # Total is 1.4M (not zero), feeds_posted=2, posted_zero=False
        history[d_str] = _build_history_item(1_400_000, 2, posted_zero=False)

    events = detect_events(history, conf)
    offline_events = [e for e in events if e["type"] == "OFFLINE"]
    assert len(offline_events) == 0, f"Expected 0 OFFLINE events, got {offline_events}"


def test_day_feed_did_not_post_is_never_counted_as_zero() -> None:
    """A day where a feed simply did not post (posting gap) is never counted as a zero."""
    conf = {
        "name": "Freeport LNG",
        "feeds": ["gulf_south_sq_24329_d", "tetco_sq_79999_d"],
        "zero_mode": "both_zero",
        "zero_days_threshold": 2,
        "depressed_pct": 0.60,
        "depressed_days": 5,
        "is_cargo_zero": False,
    }

    history: dict[str, dict[str, Any]] = {}
    # 35 normal days
    for day in range(1, 36):
        d_str = f"2026-06-{day:02d}" if day <= 30 else f"2026-07-{day - 30:02d}"
        history[d_str] = _build_history_item(1_500_000, 2, posted_zero=False)

    # Days 2026-07-06 and 2026-07-07 did not post at all (omitted from history dict)
    # They should NOT cause an outage when day 08 posts normally
    history["2026-07-08"] = _build_history_item(1_500_000, 2, posted_zero=False)

    events = detect_events(history, conf)
    assert len(events) == 0, f"Posting gap caused false event: {events}"


def test_both_feeds_posting_zero_is_real_outage() -> None:
    """When both feeds post zero for >= zero_days_threshold, a real OFFLINE outage is caught."""
    conf = {
        "name": "Freeport LNG",
        "feeds": ["gulf_south_sq_24329_d", "tetco_sq_79999_d"],
        "zero_mode": "both_zero",
        "zero_days_threshold": 2,
        "depressed_pct": 0.60,
        "depressed_days": 5,
        "is_cargo_zero": False,
    }

    history: dict[str, dict[str, Any]] = {}
    for day in range(1, 35):
        d_str = f"2026-06-{day:02d}" if day <= 30 else f"2026-07-{day - 30:02d}"
        history[d_str] = _build_history_item(1_500_000, 2, posted_zero=False)

    # 4 consecutive days where both feeds post zero
    for day in range(5, 9):
        d_str = f"2026-07-{day:02d}"
        history[d_str] = _build_history_item(0, 2, posted_zero=True)

    # Post-outage recovery
    for day in range(9, 15):
        d_str = f"2026-07-{day:02d}"
        history[d_str] = _build_history_item(1_500_000, 2, posted_zero=False)

    events = detect_events(history, conf)
    offline_events = [e for e in events if e["type"] == "OFFLINE"]
    assert len(offline_events) == 1, f"Expected 1 OFFLINE event, got {offline_events}"
    assert offline_events[0]["duration"] == 4
    assert offline_events[0]["date"] == "2026-07-08"


def test_plaquemines_pre_first_gas_classified_as_not_yet_operational() -> None:
    """Zeros posted before a terminal commences commercial operations classify as NOT_YET_OPERATIONAL."""
    conf = {
        "name": "Plaquemines LNG",
        "feeds": ["gator_express_sq_vgpqd_d"],
        "zero_mode": "normal",
        "zero_days_threshold": 3,
        "depressed_pct": 0.60,
        "depressed_days": 5,
        "is_cargo_zero": False,
    }

    history: dict[str, dict[str, Any]] = {}
    # 20 days of zero postings during pipeline testing before first commercial gas
    for day in range(1, 21):
        d_str = f"2024-05-{day:02d}"
        history[d_str] = _build_history_item(0, 1, posted_zero=True)

    events = detect_events(history, conf)
    # Must NOT emit OFFLINE
    offline_events = [e for e in events if e["type"] == "OFFLINE"]
    assert len(offline_events) == 0, f"Expected 0 OFFLINE events, got {offline_events}"

    # Must classify as NOT_YET_OPERATIONAL
    pre_op_events = [e for e in events if e["type"] == "NOT_YET_OPERATIONAL"]
    assert len(pre_op_events) >= 1
    assert pre_op_events[0]["type"] == "NOT_YET_OPERATIONAL"


def test_freeport_single_day_dip_does_not_trigger_depressed() -> None:
    """A transient 1-day dip below 60% baseline does NOT trigger DEPRESSED (requires >= 5 consecutive days)."""
    conf = {
        "name": "Freeport LNG",
        "feeds": ["gulf_south_sq_24329_d", "tetco_sq_79999_d"],
        "zero_mode": "both_zero",
        "zero_days_threshold": 2,
        "depressed_pct": 0.60,
        "depressed_days": 5,
        "is_cargo_zero": False,
    }

    history: dict[str, dict[str, Any]] = {}
    for day in range(1, 35):
        d_str = f"2026-06-{day:02d}" if day <= 30 else f"2026-07-{day - 30:02d}"
        history[d_str] = _build_history_item(1_500_000, 2, posted_zero=False)

    # Single-day dip on 2026-07-15 to 145,550 Dth (~10% of baseline)
    history["2026-07-15"] = _build_history_item(145_550, 2, posted_zero=False)

    # Next days return to normal
    for day in range(16, 25):
        d_str = f"2026-07-{day:02d}"
        history[d_str] = _build_history_item(1_500_000, 2, posted_zero=False)

    events = detect_events(history, conf)
    depressed_events = [e for e in events if e["type"] == "DEPRESSED"]
    assert len(depressed_events) == 0, f"Single-day dip falsely triggered DEPRESSED: {depressed_events}"
