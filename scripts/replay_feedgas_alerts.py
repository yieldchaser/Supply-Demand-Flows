"""
Replay Feedgas Alert Logic over Full Curated History.

Simulates running feedgas alert detection on each gas day across all terminals.
Evaluates alert rate, monthly distribution, and suppresses alert fatigue.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

import pandas as pd

from publishers.alerts import format_feedgas_alert
from scripts.task3_validate import TERMINALS, detect_events, load_terminal_history


def replay_alerts(suppress_ongoing_drop: bool = True) -> dict:
    """
    Replay alert generation over history for each terminal.

    If suppress_ongoing_drop=True:
        Only alert on the ONSET of an acute drop (day t is >= 40% drop while day t-1 was not),
        or enforce a 7-day dedup TTL on acute drop alerts.
    If suppress_ongoing_drop=False:
        Naive per-day alert (fires every day flow remains >= 40% below baseline).
    """
    monthly_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total_alerts_per_terminal: dict[str, int] = defaultdict(int)
    alert_details: list[dict] = []

    for term_key, conf in TERMINALS.items():
        hist, _ = load_terminal_history(term_key)
        if not hist:
            continue

        sorted_dates = sorted(hist.keys())
        last_acute_alert_date: str | None = None
        last_events_seen: set[tuple[str, str]] = set()

        for idx, d_str in enumerate(sorted_dates):
            # Need at least 15 days to form a meaningful baseline
            if idx < 15:
                continue

            current = hist[d_str]
            flow_mmcf = current["value"] / 1.025 / 1000.0

            # Trailing 30-day baseline median
            window_dates = sorted_dates[max(0, idx - 30):idx]
            trailing_vals = [
                hist[d]["value"] / 1.025 / 1000.0
                for d in window_dates
                if hist[d]["value"] > 0
            ]
            if not trailing_vals:
                continue
            baseline_mmcf = float(pd.Series(trailing_vals).median())

            # 1. Check for Downtime Events starting or peaking on this day
            # Sub-history up to current date
            sub_hist = {d: hist[d] for d in sorted_dates[:idx + 1]}
            events = detect_events(sub_hist, conf)

            for ev in events:
                if ev["type"] in ("OFFLINE", "DEPRESSED"):
                    # Alert when the event date matches current date
                    event_key = (ev["type"], ev["date"])
                    if ev["date"] == d_str and event_key not in last_events_seen:
                        last_events_seen.add(event_key)
                        month_key = d_str[:7]
                        monthly_counts[term_key][f"{month_key} ({ev['type']})"] += 1
                        total_alerts_per_terminal[term_key] += 1
                        alert_details.append({
                            "terminal": term_key,
                            "date": d_str,
                            "type": ev["type"],
                            "flow": flow_mmcf,
                            "baseline": baseline_mmcf,
                            "duration": ev["duration"],
                        })

            # 2. Check for Acute Drop (>= 40% drop against baseline)
            if baseline_mmcf >= 100.0 and flow_mmcf > 0.0:
                drop_pct = (baseline_mmcf - flow_mmcf) / baseline_mmcf
                if drop_pct >= 0.40:
                    fire_acute = False
                    if not suppress_ongoing_drop:
                        fire_acute = True
                    else:
                        # Onset check: was prior day NOT in acute drop, OR did 7 days pass since last acute alert?
                        prior_d = sorted_dates[idx - 1]
                        prior_flow = hist[prior_d]["value"] / 1.025 / 1000.0
                        prior_drop = (baseline_mmcf - prior_flow) / baseline_mmcf if baseline_mmcf > 0 else 0

                        days_since_last = 999
                        if last_acute_alert_date:
                            days_since_last = (datetime.fromisoformat(d_str) - datetime.fromisoformat(last_acute_alert_date)).days

                        if prior_drop < 0.40 or days_since_last >= 7:
                            fire_acute = True

                    if fire_acute:
                        last_acute_alert_date = d_str
                        month_key = d_str[:7]
                        monthly_counts[term_key][f"{month_key} (ACUTE_DROP)"] += 1
                        total_alerts_per_terminal[term_key] += 1
                        alert_details.append({
                            "terminal": term_key,
                            "date": d_str,
                            "type": "ACUTE_DROP",
                            "flow": flow_mmcf,
                            "baseline": baseline_mmcf,
                            "duration": 1,
                        })

    return {
        "monthly_counts": dict(monthly_counts),
        "total_alerts": dict(total_alerts_per_terminal),
        "alert_details": alert_details,
    }


def main():
    print("=" * 60)
    print("FEEDGAS ALERT HISTORICAL REPLAY")
    print("=" * 60)

    print("\n--- RUN 1: NAIVE (Per-Day Key, No Dedup on Ongoing Drop) ---")
    res_naive = replay_alerts(suppress_ongoing_drop=False)
    for term, tot in res_naive["total_alerts"].items():
        print(f"  {term}: {tot} total alerts")

    print("\n--- RUN 2: ONSET-FILTERED (Alert on onset + 7-day TTL cooldown) ---")
    res_filtered = replay_alerts(suppress_ongoing_drop=True)
    for term, tot in res_filtered["total_alerts"].items():
        print(f"  {term}: {tot} total alerts")

    print("\n--- MONTHLY BREAKDOWN (Filtered Alert Path) ---")
    for term, m_counts in res_filtered["monthly_counts"].items():
        print(f"\n  Terminal: {term.upper()} (Total: {res_filtered['total_alerts'].get(term, 0)})")
        for m, cnt in sorted(m_counts.items()):
            print(f"    {m}: {cnt} alert(s)")

    print("\n--- SAMPLE RENDERED MESSAGES ---")
    sample_alerts = [
        ("Freeport", "OFFLINE", "2024-04-17", 7, 0.0, 1582.4, "TETCO Stratton Ridge multi-day outage"),
        ("Freeport", "ACUTE_DROP", "2026-07-15", 1, 287.7, 1582.4, "Single-day supply restriction: 81.8% drop against 30-day baseline"),
        ("Freeport", "DEPRESSED", "2024-08-10", 6, 620.0, 1550.0, "Flow below 60% of baseline for 6 consecutive days"),
    ]
    for term, ev_type, gday, dur, flow, base, detail in sample_alerts:
        msg = format_feedgas_alert(term, ev_type, gday, dur, flow, base, detail)
        print(f"\n[Type: {ev_type}]")
        print(msg)
        print("-" * 40)


if __name__ == "__main__":
    main()
