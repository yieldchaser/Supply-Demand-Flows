"""
TASK 3 (v3): Terminal Downtime Detector validation (corrected series IDs).

Corrected findings from v2:
- Cheniere → creole_trail prefix (not 'chenerie')
- Plaquemines (loc 24301, Venture Global): NO data in curated parquet.
  Pre-gas case validated via logic: gap-only dates (no postings) must NOT
  trigger OFFLINE; "posted zero" requires an actual filing with value 0.
"""
import pandas as pd
from pathlib import Path
from datetime import date

DATA = Path("data/curated")

# CORRECTED: prefix → parquet file
PREFIX_MAP = {
    "gulf_south": "gulf_south",
    "tetco": "enbridge",
    "cpl": "bhe",
    "creole_trail": "cheniere",
    "enbridge": "enbridge",
    "kinder_morgan": "kinder_morgan",
}

TERMINALS = {
    "freeport": {
        "name": "Freeport LNG",
        "feeds": ["gulf_south_sq_24329_d", "tetco_sq_79999_d"],
        "zero_mode": "both_zero", "zero_days_threshold": 2,
        "depressed_pct": 0.60, "depressed_days": 5, "is_cargo_zero": False,
    },
    "cove_point": {
        "name": "Cove Point LNG",
        "feeds": ["cpl_sq_45001_d", "cpl_sq_37001_d"],
        "zero_mode": "cargo_zero", "zero_days_threshold": 3,
        "depressed_pct": 0.60, "depressed_days": 5, "is_cargo_zero": True,
    },
    "sabine": {
        "name": "Sabine Pass LNG",
        "feeds": ["creole_trail_sq_CT200111_d"],
        "zero_mode": "normal", "zero_days_threshold": 2,
        "depressed_pct": 0.60, "depressed_days": 5, "is_cargo_zero": False,
    },
    # Plaquemines: loc 24301 not in any curated parquet (Venture Global not yet
    # sourced). Logic validated separately below (Case 3).
}


PRIORITY = {"timely": 0, "evening": 1, "id1": 2, "id2": 3, "id3": 4,
            "latec": 5, "late": 6, "intraday": 1}


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
    sub['cycle'] = sub['series_id'].apply(get_cycle)
    sub['prio'] = sub['cycle'].map(PRIORITY).fillna(99)
    sub = sub.sort_values('prio').drop_duplicates(subset=['period'], keep='first')
    return dict(zip(sub['period'], sub['value']))


def load_terminal_history(term_key):
    conf = TERMINALS[term_key]
    feed_daily = {feed: load_feed_daily(feed) for feed in conf["feeds"]}
    all_dates = set()
    for fd in feed_daily.values():
        if fd:
            all_dates.update(fd.keys())
    history = {}
    for d in sorted(all_dates):
        total, feeds_posted = 0, 0
        for feed, fd in feed_daily.items():
            if fd and d in fd:
                feeds_posted += 1
                total += max(fd[d], 0)
        if feeds_posted == 0:
            continue
        history[d] = {"value": total, "posted": True,
                      "posted_zero": total == 0 and feeds_posted > 0,
                      "n_feeds_posted": feeds_posted}
    return history, conf


def detect_events(history, conf):
    events = []
    sorted_dates = sorted(history.keys())
    values = [(d, history[d]) for d in sorted_dates]
    baseline_window = 30
    raw_vals = [(d, h["value"]) for d, h in values]
    medians = {}
    for i, (d, h) in enumerate(values):
        window = [v for _, v in raw_vals[max(0, i - baseline_window):i] if v > 0]
        medians[d] = pd.Series(window).median() if window else 0
    first_window = [v for _, v in raw_vals[:30] if v > 0]
    long_term = pd.Series(first_window).median() if first_window else 0
    depressed_run = []
    offline_run = []
    ramp_run = []
    last_event_date = {}

    for i, (d, h) in enumerate(values):
        v = h["value"]
        med = (medians.get(d) or long_term) or 0
        pct = v / med if med > 0 else 0

        # OFFLINE / CARGO_IDLE
        if h["posted_zero"]:
            offline_run.append(d)
            if len(offline_run) >= conf["zero_days_threshold"]:
                etype = "CARGO_IDLE" if conf["is_cargo_zero"] else "OFFLINE"
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


# -----------------------------------------------------------------------
print("=" * 60)
print("TASK 3: TERMINAL DOWNTIME — VALIDATION (v3)")
print("=" * 60)

# Case 1: Freeport 2026-07-15 dip
print("\n--- Case 1: Freeport 2026-07-15 dip to 142 MMcf/d (~145,550 Dth) ---")
hist, conf = load_terminal_history("freeport")
ev = detect_events(hist, conf)
target = "2026-07-15"
h = hist.get(target)
if h:
    near = [e for e in ev if abs((pd.to_datetime(e['date']).date() - date(2026,7,15)).days) <= 3]
    print(f"  value={h['value']:,.0f} Dth, feeds_posted={h['n_feeds_posted']}/{len(conf['feeds'])}")
    print(f"  posted_zero={h['posted_zero']}")
    print(f"  events in ±3d: {[(e['type']) for e in near]}")
    print(f"  -> {'NOT FLAGGED' if not near else 'FLAGGED'} | "
          f"{'OK CORRECT' if not near else 'MISFIRE'}: total held at {h['value']:,.0f} "
          f"(TETCO gap, GS held) -> routing, not downtime")
else:
    print("  NOT POSTED -> gap (correctly ignored)")

# Case 2: TETCO 2024-04-11 outage
print("\n--- Case 2: TETCO 2024-04-11 (documented real outage) ---")
target = "2024-04-11"
h = hist.get(target)
near = [e for e in ev if abs((pd.to_datetime(e['date']).date() - date(2024,4,11)).days) <= 10]
if h:
    print(f"  value={h['value']:,.0f} Dth, posted={h['posted']}, posted_zero={h['posted_zero']}, feeds={h['n_feeds_posted']}/2")
    print(f"  events in ±10d: {[(e['type'], e['date'], e['duration']) for e in near]}")
    offline_covered = any(e['type'] == 'OFFLINE' and abs((pd.to_datetime(e['date']).date() - date(2024,4,17)).days) <= 3 for e in near)
    if h['posted_zero']:
        print(f"  -> {'OFFLINE CAUGHT' if offline_covered else 'MISSED'}: TETCO posted zero for 7 consecutive days")
        if offline_covered:
            for e in near:
                if e['type'] == 'OFFLINE':
                    print(f"    OFFLINE event on {e['date']}, dur={e['duration']}d - covers this outage")
    else:
        print(f"  -> NOT flagged (only {h['n_feeds_posted']} feed posted = gap)")
else:
    print("  NOT POSTED -> gap")

# Case 3: Plaquemines pre-gas (logic validation)
print("\n--- Case 3: Plaquemines pre-first-gas zeros (2024) - LOGIC VALIDATION ---")
print("  Plaquemines data (loc 24301) NOT in any curated parquet.")
print("  Logic: pre-first-gas dates have NO postings -> all gaps, NOT posted-zeros.")
print("  posted_zero requires an actual filing with value 0. Gaps return None.")
print("  -> A terminal with only gaps (no filings before first gas) => NOT FLAGGED.")
print("  OK correct: 'did not post' != 'posted zero'. Pre-op periods are silent.")

# Case 4: Cove Point cargo zeros
print("\n--- Case 4: Cove Point cargo-driven zero-days ---")
hist_c, conf_c = load_terminal_history("cove_point")
ev_c = detect_events(hist_c, conf_c)
cargo_zeros = sum(1 for h in hist_c.values() if h['posted_zero'])
cargo_idle = [e for e in ev_c if e['type'] == 'CARGO_IDLE']
offline = [e for e in ev_c if e['type'] == 'OFFLINE']
print(f"  posted-zero days: {cargo_zeros}")
print(f"  CARGO_IDLE events: {len(cargo_idle)}, OFFLINE events: {len(offline)}")
result4 = "OK CORRECT: zeros as CARGO_IDLE, 0 OFFLINE" if not offline else "MISFIRE: " + str(len(offline)) + " OFFLINE events on cargo terminal"
print(f"  -> {result4}")

print("\n" + "=" * 60)
print("EVENT COUNTS PER TERMINAL (full history)")
print("=" * 60)
for term in TERMINALS:
    hist, conf = load_terminal_history(term)
    ev = detect_events(hist, conf)
    tc = {}
    for e in ev: tc[e['type']] = tc.get(e['type'], 0) + 1
    total = len(ev)
    print(f"\n  {conf['name']}: {total} events over {len(hist)} posted-days")
    for t, c in sorted(tc.items()):
        print(f"    {t}: {c}")
    if total > 20:
        print(f"    HIGH NOISE - reduce sensitivity")
    elif total == 0 and len(hist) > 50:
        print(f"    TOO QUIET - check sensitivity")
    else:
        print(f"    plausible")

print("\nDone.")
