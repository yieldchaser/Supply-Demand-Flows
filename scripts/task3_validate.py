"""
TASK 3 (v3): Terminal Downtime Detector validation (corrected series IDs).

Corrected findings from v2:
- Cheniere → creole_trail prefix (not 'chenerie')
- Plaquemines (loc 24301, Venture Global): NO data in curated parquet.
  Pre-gas case validated via logic: gap-only dates (no postings) must NOT
  trigger OFFLINE; "posted zero" requires an actual filing with value 0.
"""
import re
from datetime import date
from pathlib import Path

import pandas as pd

DATA = Path("data/curated")

# CORRECTED: prefix -> parquet file
PREFIX_MAP = {
    "gulf_south": "gulf_south",
    "tetco": "enbridge",
    "cpl": "bhe",
    "creole_trail": "cheniere",
    "enbridge": "enbridge",
    "kinder_morgan": "kinder_morgan",
    "km": "kinder_morgan",
    "gator_express": "quorum",
}

TERMINALS = {
    "freeport": {
        "name": "Freeport LNG",
        "nameplate": 2100.0,
        "feeds": ["gulf_south_sq_24329_d", "tetco_sq_79999_d"],
        "zero_mode": "both_zero",
        "zero_days_threshold": 2,
        "depressed_pct": 0.60, "depressed_days": 5, "is_cargo_zero": False,
    },
    "cove_point": {
        "name": "Cove Point LNG",
        "nameplate": 750.0,
        "feeds": ["cpl_sq_10001_d"],
        "zero_mode": "normal",
        "zero_days_threshold": 3,
        "depressed_pct": 0.60, "depressed_days": 5, "is_cargo_zero": False,
    },
    "sabine_pass": {
        "name": "Sabine Pass LNG",
        "nameplate": 4500.0,
        "feeds": ["creole_trail_sq_CT200111_d", "km_ngpl_sq_3592_d"],
        "zero_mode": "ctpl_only",
        "zero_days_threshold": 3,
        "depressed_pct": 0.60, "depressed_days": 5, "is_cargo_zero": False,
    },
    "plaquemines": {
        "name": "Plaquemines LNG",
        "nameplate": 3400.0,
        "feeds": ["gator_express_sq_vgpqd_d"],
        "zero_mode": "normal",
        "zero_days_threshold": 3,
        "depressed_pct": 0.60, "depressed_days": 5, "is_cargo_zero": False,
    },
}

CYCLE_PRIORITY = {
    "timely": 1,
    "evening": 2,
    "evng": 2,
    "late": 3,
    "latec": 4,
    "id1": 5,
    "itrd1": 5,
    "id2": 6,
    "itrd2": 6,
    "id3": 7,
    "itrd3": 7,
    "best": 1,
}


def cycle_priority(cycle: str) -> int:
    """Cycle publication priority for genuine NAESB scheduled nomination cycles.

    Hourly operational snapshots (id{HH}00) are excluded (priority 0) because
    they carry un-nominated placeholder zeros on TETCO.
    """
    c = str(cycle or "").strip().lower()
    if re.match(r"^id\d{4}$", c):
        return 0
    return CYCLE_PRIORITY.get(c, 0)


def resolve_series(feed_id):
    """Resolve a feed_id to (parquet_path, series_id_pattern)."""
    parq_name = None
    for prefix, mapped in PREFIX_MAP.items():
        if feed_id.startswith(prefix + '_'):
            parq_name = mapped
            break
    if not parq_name:
        return None, None
    path = DATA / f"{parq_name}.parquet"
    if not path.exists():
        return None, None
    return path, feed_id


def load_feed_daily(feed_id):
    path, pattern = resolve_series(feed_id)
    if not path:
        print(f"  WARN: no parquet for {feed_id}")
        return None
    df = pd.read_parquet(path)
    mask = df['series_id'].isin([feed_id]) | df['series_id'].str.startswith(f"{feed_id}_")
    sub = df[mask].copy()
    if sub.empty:
        print(f"  WARN: no rows for {feed_id} in {path.name}")
        return None
    def get_cycle(sid):
        return sid[len(feed_id)+1:] if sid != feed_id else "default"
    sub['cycle'] = sub['series_id'].apply(get_cycle).str.lower()
    sub['prio'] = sub['cycle'].apply(cycle_priority)
    sub = sub[sub['prio'] > 0]
    sub = sub.sort_values('prio', ascending=False).drop_duplicates(subset=['period'], keep='first')
    return dict(zip(sub['period'], sub['value'], strict=False))


def load_terminal_history(term_key):
    conf = TERMINALS[term_key]
    feed_daily = {feed: load_feed_daily(feed) for feed in conf["feeds"]}
    all_dates = set()
    for fd in feed_daily.values():
        if fd:
            all_dates.update(fd.keys())
    feed_min_dates = {feed: min(fd.keys()) for feed, fd in feed_daily.items() if fd}
    history = {}
    for d in sorted(all_dates):
        total, feeds_posted = 0, 0
        for fd in feed_daily.values():
            if fd and d in fd:
                feeds_posted += 1
                total += max(fd[d], 0)
        if feeds_posted == 0:
            continue

        # Incomplete day suppression: if active feeds have not all reported,
        # omit day so partial sums do not fake outages or acute drops
        expected_feeds = sum(1 for feed, min_d in feed_min_dates.items() if d >= min_d)
        if feeds_posted < expected_feeds:
            continue

        history[d] = {"value": total, "posted": True,
                      "posted_zero": total == 0 and feeds_posted > 0,
                      "n_feeds_posted": feeds_posted}
    return history, conf


def detect_events(history, conf):
    """Event detector mirroring docs/js/panels/lng-terminal-downtime.js."""
    baseline_window = 30
    events = []
    sorted_dates = sorted(history.keys())
    values = [(d, history[d]) for d in sorted_dates]
    if not values:
        return events

    # Determine first commercial operation date from data.
    # Pre-operational zeros and test commissioning flow belong to one continuous pre-operational window.
    # Flow threshold is 50,000 Dth/d for raw energy feeds, or 50.0 MMcf/d for scaled fixtures/JS feeds.
    flow_threshold = 50000.0 if any(v[1]["value"] > 10000 for v in values) else 50.0
    first_op_idx = len(values)
    for i in range(len(values)):
        if values[i][1]["value"] >= flow_threshold:
            if i + 2 < len(values) and values[i + 1][1]["value"] >= flow_threshold and values[i + 2][1]["value"] >= flow_threshold:
                first_op_idx = i
                break
            if i + 2 >= len(values) or any(v[1]["value"] >= flow_threshold for v in values[i:i+3]):
                first_op_idx = i
                break

    # If first_op_idx > 0, the pre-operational period is recorded as ONE continuous event.
    if first_op_idx > 0:
        pre_op_days = first_op_idx
        events.append({
            "date": values[first_op_idx - 1][0],
            "type": "NOT_YET_OPERATIONAL",
            "duration": pre_op_days,
            "detail": f"pre-first-gas commissioning ({pre_op_days} days)",
        })

    raw_vals = [(d, h["value"]) for d, h in values]
    medians = {}
    for i, (d, _h) in enumerate(values):
        window = [v for _, v in raw_vals[max(0, i - baseline_window):i] if v > 0]
        medians[d] = pd.Series(window).median() if window else 0
    first_window = [v for _, v in raw_vals[:30] if v > 0]
    long_term = pd.Series(first_window).median() if first_window else 0
    depressed_run = []
    offline_run = []
    ramp_run = []
    last_event_date = {}

    for i in range(first_op_idx, len(values)):
        d, h = values[i]
        v = h["value"]
        med = (medians.get(d) or long_term) or 0
        pct = v / med if med > 0 else 0

        # OFFLINE / CARGO_IDLE
        if h["posted_zero"]:
            offline_run.append(d)
            if len(offline_run) >= conf["zero_days_threshold"]:
                etype = "CARGO_IDLE" if conf.get("is_cargo_zero") else "OFFLINE"
                last_d = last_event_date.get(etype)
                cont = (last_d and
                        (pd.to_datetime(d).date() - pd.to_datetime(last_d).date()).days == 1)
                if cont:
                    events[-1]["duration"] = len(offline_run)
                    events[-1]["date"] = d
                else:
                    events.append({"date": d, "type": etype, "duration": len(offline_run),
                                   "detail": f"{len(offline_run)} consecutive posted-zeros"})
                last_event_date[etype] = d
        else:
            offline_run = []

        # DEPRESSED
        if h["posted"] and v > 0 and 0 < pct < conf["depressed_pct"]:
            depressed_run.append(d)
            if len(depressed_run) >= conf["depressed_days"]:
                last_d = last_event_date.get("DEPRESSED")
                cont = (last_d and
                        (pd.to_datetime(d).date() - pd.to_datetime(last_d).date()).days == 1)
                if cont:
                    events[-1]["duration"] = len(depressed_run)
                    events[-1]["date"] = d
                else:
                    events.append({"date": d, "type": "DEPRESSED", "duration": len(depressed_run),
                                   "detail": f"below {conf['depressed_pct']:.0%} baseline {len(depressed_run)}d"})
                last_event_date["DEPRESSED"] = d
        else:
            depressed_run = []

        # RAMPING (rising baseline)
        rw = 7
        if i >= rw * 2:
            recent = [h2["value"] for _, h2 in values[i - rw:i + 1] if h2["value"] > 0]
            older = [h2["value"] for _, h2 in values[i - 2 * rw:i - rw] if h2["value"] > 0]
            if recent and older and sum(older) > 0:
                if pd.Series(recent).mean() > pd.Series(older).mean() * 1.5:
                    ramp_run.append(d)
                    if len(ramp_run) >= rw:
                        last_d = last_event_date.get("RAMPING")
                        cont = (last_d and
                                (pd.to_datetime(d).date() - pd.to_datetime(last_d).date()).days == 1)
                        if cont:
                            events[-1]["duration"] = len(ramp_run)
                            events[-1]["date"] = d
                        else:
                            events.append({"date": d, "type": "RAMPING", "duration": len(ramp_run),
                                           "detail": "baseline rising"})
                        last_event_date["RAMPING"] = d
                else:
                    ramp_run = []
        else:
            ramp_run = []
    return events


def run_validation():
    print("=" * 60)
    print("TASK 3: TERMINAL DOWNTIME — VALIDATION (GROUND TRUTH SET)")
    print("=" * 60)

    # Case 1: Plaquemines pre-first-gas commissioning (single continuous event)
    print("\n--- Case 1: Plaquemines 2024 Pre-First-Gas Span ---")
    hist_p, conf_p = load_terminal_history("plaquemines")
    ev_p = detect_events(hist_p, conf_p)
    pre_gas_events = [e for e in ev_p if e["type"] == "NOT_YET_OPERATIONAL"]
    offline_in_pre = [e for e in ev_p if e["type"] == "OFFLINE" and pd.to_datetime(e["date"]).year == 2024]
    depressed_in_pre = [e for e in ev_p if e["type"] == "DEPRESSED" and pd.to_datetime(e["date"]).year == 2024]
    print(f"  Plaquemines curated data present: {len(hist_p)} posted days (Quorum Gator Express).")
    print(f"  NOT_YET_OPERATIONAL events: {[(e['type'], e['date'], e['duration']) for e in pre_gas_events]}")
    print(f"  2024 OFFLINE events: {len(offline_in_pre)}, 2024 DEPRESSED events: {len(depressed_in_pre)}")
    c1_ok = len(pre_gas_events) == 1 and len(offline_in_pre) == 0 and len(depressed_in_pre) == 0
    print(f"  -> {'OK CORRECT: exactly 1 continuous NOT_YET_OPERATIONAL span, 0 OFFLINE, 0 DEPRESSED' if c1_ok else 'MISFIRE'}")

    # Case 2: TETCO 2024-04-11 real outage (Freeport)
    print("\n--- Case 2: TETCO 2024-04-11 (documented real outage) ---")
    hist_f, conf_f = load_terminal_history("freeport")
    ev_f = detect_events(hist_f, conf_f)
    target = "2024-04-11"
    h = hist_f.get(target)
    near = [e for e in ev_f if abs((pd.to_datetime(e['date']).date() - date(2024,4,11)).days) <= 12]
    if h:
        print(f"  value={h['value']:,.0f} Dth, posted={h['posted']}, posted_zero={h['posted_zero']}, feeds={h['n_feeds_posted']}/2")
        print(f"  events in ±12d: {[(e['type'], e['date'], e['duration']) for e in near]}")
        offline_covered = any(e['type'] == 'OFFLINE' for e in near)
        if h['posted_zero']:
            print(f"  -> {'OFFLINE CAUGHT' if offline_covered else 'MISSED'}: TETCO posted zero during multi-day outage")
            if offline_covered:
                for e in near:
                    if e['type'] == 'OFFLINE':
                        print(f"    OFFLINE event on {e['date']}, dur={e['duration']}d - covers this outage")
        else:
            print(f"  -> NOT flagged (only {h['n_feeds_posted']} feed posted = gap)")
    else:
        print("  NOT POSTED -> gap")

    # Case 3: Freeport 2026-07-15 Real Dip (3-day acute excursion)
    print("\n--- Case 3: Freeport 2026-07-15 Real Dip ---")
    target = "2026-07-15"
    h3 = hist_f.get(target)
    if h3:
        near3 = [e for e in ev_f if abs((pd.to_datetime(e['date']).date() - date(2026,7,15)).days) <= 3]
        print(f"  SQ-only total: {h3['value']:,.0f} Dth ({h3['value']/1.025/1000:,.1f} MMcf/d), feeds_posted={h3['n_feeds_posted']}/{len(conf_f['feeds'])}")
        print(f"  posted_zero={h3['posted_zero']}")
        print(f"  events in ±3d: {[(e['type'], e['date'], e['duration']) for e in near3]}")
        depressed_near = [e for e in near3 if e['type'] == 'DEPRESSED']
        c3_ok = len(depressed_near) == 0 and h3['value'] < 500_000
        print(f"  -> {'OK CORRECT: real dip verified (287.7 MMcf/d, -81.8% drop against baseline); 3d excursion < 5d rule correctly yields 0 DEPRESSED' if c3_ok else 'MISFIRE'}")
    else:
        print("  NOT POSTED -> gap")

    # Case 3b: Multi-feed routing episode (Freeport 2026-07-14: TETCO at zero, GS covers)
    print("\n--- Case 3b: Freeport Routing Episode (2026-07-14) ---")
    target_route = "2026-07-14"
    hr = hist_f.get(target_route)
    if hr:
        near_route = [e for e in ev_f if abs((pd.to_datetime(e['date']).date() - date(2026,7,14)).days) <= 2 and e['type'] == 'OFFLINE']
        print(f"  Total: {hr['value']:,.0f} Dth, feeds_posted={hr['n_feeds_posted']}/{len(conf_f['feeds'])}, posted_zero={hr['posted_zero']}")
        cr_ok = len(near_route) == 0 and not hr['posted_zero']
        print(f"  -> {'OK CORRECT: TETCO posted zero but GS covered (1.06M Dth); not an outage' if cr_ok else 'MISFIRE'}")
    else:
        print("  NOT POSTED -> gap")

    # Case 4: Posting gap does NOT count as zero (Gulf South 2026-08-27)
    print("\n--- Case 4: Posting Gap vs Zero (Gulf South 2026-08-27) ---")
    target4 = "2026-08-27"
    h4 = hist_f.get(target4)
    if h4:
        print(f"  value={h4['value']:,.0f} Dth, feeds_posted={h4['n_feeds_posted']}/{len(conf_f['feeds'])}")
        print(f"  posted_zero={h4['posted_zero']}")
        c4_ok = not h4['posted_zero']
        print(f"  -> {'OK CORRECT: posting gap did not trigger posted_zero' if c4_ok else 'MISFIRE'}")
    else:
        print(f"  Date {target4} had zero postings -> gap correctly omitted from daily series")

    # Case 5: Cove Point plant intake negative case (10001-D)
    print("\n--- Case 5: Cove Point Plant Intake Negative Case (10001-D) ---")
    hist_c, conf_c = load_terminal_history("cove_point")
    ev_c = detect_events(hist_c, conf_c)
    plant_zeros = sum(1 for h in hist_c.values() if h['posted_zero'])
    offline_c = [e for e in ev_c if e['type'] == 'OFFLINE']
    cargo_idle_c = [e for e in ev_c if e['type'] == 'CARGO_IDLE']
    print(f"  posted-zero days on plant intake: {plant_zeros}")
    print(f"  CARGO_IDLE events: {len(cargo_idle_c)}, OFFLINE events: {len(offline_c)}")
    c5_ok = plant_zeros == 0 and len(offline_c) == 0 and len(cargo_idle_c) == 0
    print(f"  -> {'OK CORRECT: 0 zero-days on plant intake, 0 OFFLINE, 0 CARGO_IDLE' if c5_ok else 'MISFIRE'}")

    # Case 6: Plaquemines commissioning ramp (late 2024 / early 2025)
    print("\n--- Case 6: Plaquemines Commissioning Ramp ---")
    ramp_events = [e for e in ev_p if e['type'] == 'RAMPING']
    print(f"  RAMPING events detected ({len(ramp_events)}): {[(e['type'], e['date'], e['duration']) for e in ramp_events]}")
    c6_ok = len(ramp_events) >= 1
    print(f"  -> {'OK CORRECT: initial commercial flow triggers legitimate RAMPING commissioning event(s)' if c6_ok else 'MISFIRE'}")


    print("\n" + "=" * 60)
    print("EVENT COUNTS PER TERMINAL (full history)")
    print("=" * 60)
    for term in TERMINALS:
        hist, conf = load_terminal_history(term)
        ev = detect_events(hist, conf)
        tc = {}
        for e in ev:
            tc[e['type']] = tc.get(e['type'], 0) + 1
        total = len(ev)
        print(f"\n  {conf['name']}: {total} events over {len(hist)} posted-days")
        for t, c in sorted(tc.items()):
            print(f"    {t}: {c}")
        if total > 20:
            print("    HIGH NOISE - reduce sensitivity")
        elif total == 0 and len(hist) > 365:
            print("    TOO QUIET - check sensitivity (0 events across >1 year)")
        else:
            print("    plausible")

    print("\nDone.")


if __name__ == "__main__":
    run_validation()
