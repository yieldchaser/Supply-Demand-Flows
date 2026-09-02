"""Blue Tide Preflight Audit & Health Suite.

Runs all core physical observatory checks in one deterministic pass:
1. Curated Parquet Data Inventory (rows, date spans, freshness)
2. Integrity Board (validators.run_integrity)
3. Section 8 Downtime Detection Cases (scripts/task3_validate.py)
4. Trailing 90-day Feedgas Alert Replay (scripts/replay_feedgas_alerts.py)
5. Coverage Anti-Rot Guard (tests/test_coverage_guard.py)
6. Final Comprehensive PASS / FAIL Verdict
"""

from __future__ import annotations

import glob
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from scripts.load_registry import load_terminal_registry
from scripts.task3_validate import TERMINALS, detect_events, load_terminal_history
from tests.test_coverage_guard import compute_terminal_coverage_from_curated
from validators.integrity import load_state, run_source_checks
from validators.run_integrity import DEFAULT_CONFIG_PATH, DEFAULT_STATE_PATH, load_config, _load_health, _availability_skipped


def run_inventory_check() -> tuple[bool, list[dict[str, Any]]]:
    """Audit all data/curated/*.parquet files."""
    parquets = sorted(glob.glob("data/curated/*.parquet"))
    results = []
    all_ok = True

    for p_str in parquets:
        p = Path(p_str)
        try:
            df = pd.read_parquet(p)
            n_rows = len(df)
            periods = sorted(df["period"].astype(str).unique()) if not df.empty else []
            min_p = periods[0] if periods else "N/A"
            max_p = periods[-1] if periods else "N/A"
            n_p = len(periods)
            ok = n_rows > 0
            if not ok:
                all_ok = False
            results.append({
                "source": p.stem,
                "rows": n_rows,
                "periods": n_p,
                "min_date": min_p,
                "max_date": max_p,
                "ok": ok,
            })
        except Exception as exc:
            all_ok = False
            results.append({
                "source": p.stem,
                "rows": 0,
                "periods": 0,
                "min_date": "ERR",
                "max_date": str(exc),
                "ok": False,
            })

    return all_ok, results


def run_integrity_board() -> tuple[bool, str, dict[str, str]]:
    """Run integrity validator suite across all configured pipeline sources."""
    defaults, sources = load_config(DEFAULT_CONFIG_PATH)
    now = datetime.now(UTC)
    state = load_state(DEFAULT_STATE_PATH)
    
    source_verdicts: dict[str, str] = {}
    any_fail = False

    for source_key, cfg in sources.items():
        parquet_path = Path("data/curated") / f"{source_key}.parquet"
        if not parquet_path.exists():
            res = _availability_skipped()
            source_verdicts[source_key] = res["severity"]
            continue

        try:
            df = pd.read_parquet(parquet_path)
        except Exception:
            source_verdicts[source_key] = "FAIL"
            any_fail = True
            continue

        health_payload = _load_health(cfg.get("health_file"))
        prior = state.get(source_key, {})
        source_rule = dict(defaults)
        source_rule.update(cfg.get("rules", {}))

        results = run_source_checks(
            source=source_key,
            df=df,
            now=now,
            rule=source_rule,
            health=health_payload,
            prior=prior,
        )

        # Max severity
        sev_rank = {"SKIPPED": 0, "PASS": 1, "WARN": 2, "FAIL": 3}
        max_sev = "PASS"
        for r in results:
            if sev_rank.get(r["severity"], 0) > sev_rank.get(max_sev, 0):
                max_sev = r["severity"]

        source_verdicts[source_key] = max_sev
        if max_sev == "FAIL":
            any_fail = True

    overall = "FAIL" if any_fail else ("WARN" if any(v == "WARN" for v in source_verdicts.values()) else "PASS")
    return not any_fail, overall, source_verdicts


def run_task3_cases() -> tuple[bool, list[tuple[str, bool, str]]]:
    """Execute the six Section 8 ground-truth validation cases."""
    cases = []

    # Case 1: Plaquemines pre-operational zero filtering
    hist_p, conf_p = load_terminal_history("plaquemines")
    ev_p = detect_events(hist_p, conf_p)
    offline_p = [e for e in ev_p if e["type"] == "OFFLINE"]
    c1_ok = len(offline_p) == 0
    cases.append(("Case 1: Plaquemines pre-operational zero filtering", c1_ok, f"{len(offline_p)} OFFLINE events"))

    # Case 2: TETCO 2024-04-11 outage detection (dur=7)
    ev_2 = [e for e in ev_p if e["type"] == "OFFLINE"] # check freeport
    hist_f, conf_f = load_terminal_history("freeport")
    ev_f = detect_events(hist_f, conf_f)
    outages_f = [e for e in ev_f if e["type"] == "OFFLINE" and e["date"] == "2024-04-17"]
    c2_ok = len(outages_f) == 1 and outages_f[0]["duration"] == 7
    c2_desc = f"dur={outages_f[0]['duration']}d" if outages_f else "no 2024-04-17 event"
    cases.append(("Case 2: TETCO 2024-04-11 outage detection (dur=7d)", c2_ok, c2_desc))

    # Case 3: Freeport 2026-07-15 multi-feed acute dip (294,850 Dth / 287.7 MMcf/d)
    h3 = hist_f.get("2026-07-15")
    c3_ok = False
    c3_desc = "missing"
    if h3:
        mmcf3 = h3["value"] / 1.025 / 1000.0
        c3_ok = 285.0 <= mmcf3 <= 290.0 and h3["n_feeds_posted"] == 2
        c3_desc = f"{mmcf3:.1f} MMcf/d ({h3['value']:,.0f} Dth), feeds={h3['n_feeds_posted']}/2"
    cases.append(("Case 3: Freeport 2026-07-15 multi-feed acute dip", c3_ok, c3_desc))

    # Case 4: Freeport 2026-07-14 routing check (GS flows, TETCO 0 -> no outage)
    h4 = hist_f.get("2026-07-14")
    c4_ok = False
    c4_desc = "missing"
    if h4:
        c4_ok = not h4["posted_zero"] and h4["value"] > 1_000_000
        c4_desc = f"value={h4['value']:,.0f} Dth, posted_zero={h4['posted_zero']}"
    cases.append(("Case 4: Freeport 2026-07-14 routing check", c4_ok, c4_desc))

    # Case 5: Cove Point plant intake negative case (10001-D, 0 OFFLINE)
    hist_c, conf_c = load_terminal_history("cove_point")
    ev_c = detect_events(hist_c, conf_c)
    offline_c = [e for e in ev_c if e["type"] == "OFFLINE"]
    c5_ok = len(offline_c) == 0
    cases.append(("Case 5: Cove Point plant intake (10001-D, 0 OFFLINE)", c5_ok, f"{len(offline_c)} OFFLINE events"))

    # Case 6: Plaquemines commissioning ramp (2 RAMPING events)
    ramp_p = [e for e in ev_p if e["type"] == "RAMPING"]
    c6_ok = len(ramp_p) == 2
    cases.append(("Case 6: Plaquemines commissioning ramp (2 RAMPING events)", c6_ok, f"{len(ramp_p)} RAMPING events"))

    all_ok = all(c[1] for c in cases)
    return all_ok, cases


def run_alert_replay_90d() -> tuple[bool, dict[str, int]]:
    """Replay alert generation over the trailing 90 days."""
    alert_counts: dict[str, int] = {}
    cutoff_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    for term_key, conf in TERMINALS.items():
        hist, _ = load_terminal_history(term_key)
        if not hist:
            alert_counts[term_key] = 0
            continue

        sorted_dates = [d for d in sorted(hist.keys()) if d >= cutoff_date]
        term_alerts = 0
        last_acute: str | None = None
        last_events_seen: set[tuple[str, str]] = set()

        all_sorted = sorted(hist.keys())

        for d_str in sorted_dates:
            idx = all_sorted.index(d_str)
            if idx < 15:
                continue

            current = hist[d_str]
            flow_mmcf = current["value"] / 1.025 / 1000.0

            # 30-day baseline
            window_dates = all_sorted[max(0, idx - 30):idx]
            trailing_vals = [
                hist[d]["value"] / 1.025 / 1000.0
                for d in window_dates
                if hist[d]["value"] > 0
            ]
            if not trailing_vals:
                continue
            baseline = float(pd.Series(trailing_vals).median())

            # Downtime events
            sub_hist = {d: hist[d] for d in all_sorted[:idx + 1]}
            events = detect_events(sub_hist, conf)
            for ev in events:
                if ev["type"] in ("OFFLINE", "DEPRESSED") and ev["date"] == d_str:
                    key = (ev["type"], ev["date"])
                    if key not in last_events_seen:
                        last_events_seen.add(key)
                        term_alerts += 1

            # Acute drop (>=40%) with 7-day onset suppression
            if baseline >= 100.0 and flow_mmcf > 0.0:
                drop_pct = (baseline - flow_mmcf) / baseline
                if drop_pct >= 0.40:
                    days_since = 999
                    if last_acute:
                        days_since = (datetime.fromisoformat(d_str) - datetime.fromisoformat(last_acute)).days
                    if days_since >= 7:
                        last_acute = d_str
                        term_alerts += 1

        alert_counts[term_key] = term_alerts

    return True, alert_counts


def run_coverage_guard_check() -> tuple[bool, list[dict[str, Any]]]:
    """Run coverage anti-rot guard against curated parquet files."""
    registry = load_terminal_registry()
    results = []
    all_ok = True

    for term_key, reg in registry.items():
        if not reg["operational"]:
            results.append({
                "terminal": term_key,
                "nameplate": reg["nameplate"],
                "claimed_pct": 0.0,
                "measured_pct": 0.0,
                "drift_pct": 0.0,
                "tolerance_pct": 0.0,
                "status": "PASS (non-op)",
            })
            continue

        window = 100 if term_key == "freeport" else 60
        cov_res = compute_terminal_coverage_from_curated(term_key, window_days=window)
        measured_pct = cov_res["coverage_pct"]
        claimed_pct = reg["expectedCoveragePct"]
        tolerance = reg["coverageTolerancePct"]
        drift = abs(measured_pct - claimed_pct)
        passed = drift <= tolerance

        if not passed:
            all_ok = False

        results.append({
            "terminal": term_key,
            "nameplate": reg["nameplate"],
            "claimed_pct": claimed_pct,
            "measured_pct": measured_pct,
            "drift_pct": drift,
            "tolerance_pct": tolerance,
            "status": "PASS" if passed else "FAIL",
        })

    return all_ok, results


def main() -> int:
    print("=" * 75)
    print("BLUE TIDE OBSERVATORY — PREFLIGHT VERIFICATION & HEALTH AUDIT")
    print("=" * 75)
    print(f"Timestamp: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC\n")

    overall_pass = True

    # 1. Curated Data Inventory
    print("[1/5] CURATED PARQUET INVENTORY")
    print("-" * 75)
    inv_ok, inventory = run_inventory_check()
    if not inv_ok:
        overall_pass = False
    print(f"{'Source':<20} | {'Rows':<8} | {'Periods':<8} | {'Span':<25} | Status")
    print("-" * 75)
    for inv in inventory:
        span_str = f"{inv['min_date']} → {inv['max_date']}"
        stat_str = "OK" if inv['ok'] else "EMPTY/MISSING"
        print(f"{inv['source']:<20} | {inv['rows']:<8d} | {inv['periods']:<8d} | {span_str:<25} | {stat_str}")
    print()

    # 2. Integrity Board
    print("[2/5] INTEGRITY BOARD (validators.run_integrity)")
    print("-" * 75)
    board_ok, overall_sev, verdicts = run_integrity_board()
    if not board_ok:
        overall_pass = False
    for src, sev in verdicts.items():
        print(f"  {src:<22}: {sev}")
    print(f"  -> Overall Integrity Board: {overall_sev}")
    print()

    # 3. Section 8 Downtime Validator Cases
    print("[3/5] SECTION 8 DOWNTIME VALIDATION CASES (task3_validate.py)")
    print("-" * 75)
    t3_ok, cases = run_task3_cases()
    if not t3_ok:
        overall_pass = False
    for name, ok, desc in cases:
        status_str = "OK CORRECT" if ok else "FAIL"
        print(f"  {name:<55} [{status_str}] ({desc})")
    print()

    # 4. Trailing 90-Day Feedgas Alert Replay
    print("[4/5] FEEDGAS ALERT REPLAY (Trailing 90 Days)")
    print("-" * 75)
    _, alert_counts = run_alert_replay_90d()
    for term, count in alert_counts.items():
        print(f"  {term:<20}: {count} alerts in last 90 days")
    print()

    # 5. Coverage Anti-Rot Guard
    print("[5/5] COVERAGE ANTI-ROT GUARD (Curated Parquet vs Registry Claims)")
    print("-" * 75)
    guard_ok, guard_results = run_coverage_guard_check()
    if not guard_ok:
        overall_pass = False
    print(f"{'Terminal':<16} | {'NP':<5} | {'Claimed':<8} | {'Measured':<8} | {'Drift':<6} | {'Tol':<6} | Status")
    print("-" * 75)
    for g in guard_results:
        print(
            f"{g['terminal']:<16} | {g['nameplate']:<5.0f} | "
            f"{g['claimed_pct']:>6.1f}% | {g['measured_pct']:>7.1f}% | "
            f"{g['drift_pct']:>5.1f}% | ±{g['tolerance_pct']:<4.1f}% | {g['status']}"
        )
    print()

    # Final Verdict
    print("=" * 75)
    if overall_pass:
        print("PREFLIGHT VERDICT: PASS — ALL SYSTEMS VERIFIED AND READY TO MERGE")
        print("=" * 75)
        return 0
    else:
        print("PREFLIGHT VERDICT: FAIL — ONE OR MORE OBSERVATORY CHECKS FAILED")
        print("=" * 75)
        return 1


if __name__ == "__main__":
    sys.exit(main())
