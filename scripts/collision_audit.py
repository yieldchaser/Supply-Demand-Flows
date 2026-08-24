"""TASK 2 — collision audit: enumerate source dimensions vs series_id encoding.

For every curated source, rebuild the pre-dedup key space from RAW payloads,
map each raw row through the transformer's key derivation, and count how many
raw rows collapse onto one (series_id, period) key — the silent-overwrite
condition merge_into_curated's dedup turns into data loss.
"""

from __future__ import annotations

import glob
import json
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

NOWCAST_RAW = Path(r"C:\Users\Dell\Github\Supply-Demand-Flows\data\raw")
CURATED = Path(r"C:\Users\Dell\Github\bt-curation\data\curated")

report: list[str] = []


def say(text: str = "") -> None:
    print(text)
    report.append(text)


# ---------------------------------------------------------------------------
# gulf_south (boardwalk): dims = loc, cycle, flow_ind, loc_purp, qti, ...
# transformer series_id = f"gulf_south_{sq|oac}_{loc}_{cycle}"  → flow_ind DROPPED
# ---------------------------------------------------------------------------

def audit_gulf_south() -> None:
    keys: Counter = Counter()
    values: dict[tuple, set[float]] = defaultdict(set)
    files = sorted(glob.glob(str(NOWCAST_RAW / "gulf_south" / "*.json")))
    n_raw = 0
    for f in files:
        stem = Path(f).stem
        if stem.startswith("_"):
            continue
        cycle = stem.rsplit("_", 1)[1]
        with open(f, encoding="utf-8") as fh:
            payload = json.load(fh)
        gas_day_file = stem.rsplit("_", 1)[0]
        for r in payload.get("data", []):
            loc = str(r.get("Loc") or "").strip()
            if not loc:
                continue
            period = str(r.get("Effective Gas Day") or "")
            if len(period) == 8 and period.isdigit():
                period = f"{period[:4]}-{period[4:6]}-{period[6:]}"
            else:
                period = gas_day_file
            str(r.get("Flow Ind") or "").strip().upper()
            str(r.get("Loc Purp Desc") or "").strip()
            n_raw += 1
            for kind, col in (("sq", "Total Scheduled Quantity"), ("oac", "Operationally  Available Capacity")):
                v = r.get(col)
                if v in (None, ""):
                    continue
                key = (f"gulf_south_{kind}_{loc}_{cycle.lower()}", period)
                keys[key] += 1
                values[key].add(float(v))
    dup_keys = {k: c for k, c in keys.items() if c > 1}
    diff_val = {k: len(values[k]) for k in dup_keys if len(values[k]) > 1}
    say("### gulf_south (boardwalk)")
    say("")
    say(f"- raw rows parsed: {n_raw:,} across {len(files)} cycle files")
    say("- dimensions in source: loc, cycle, flow_ind, loc_purp_desc, loc_qti_desc, "
        "meas_basis, IT flags")
    say("- series_id encodes: loc + cycle + metric-kind. **DROPPED: flow_ind** "
        "(and loc_purp which mirrors it)")
    say(f"- distinct (series_id, period) keys: {len(keys):,}")
    say(f"- keys with >1 raw row mapping in: **{len(dup_keys):,}**")
    say(f"- of those, keys where the colliding values DIFFER (silent overwrite): "
        f"**{len(diff_val):,}**")
    if diff_val:
        worst = sorted(diff_val.items(), key=lambda kv: -kv[1])[:5]
        examples = []
        for (sid, per), ndiff in worst:
            vals = sorted(values[(sid, per)])
            examples.append(f"`{sid}` @ {per}: {ndiff} distinct values {vals[:3]}")
        say("- worst offenders:")
        for e in examples:
            say(f"  - {e}")
    # quantify: how many dual-leg locs exist
    dual = {k[0] for k in dup_keys}
    locs = sorted({k.split("_")[3] for k in dual})
    say(f"- affected meters (locs with collisions): {len(locs)}"
        )
    say("")


audit_gulf_south()


# ---------------------------------------------------------------------------
# quorum / gasnom / cheniere / bhe: check CURATED parquet itself for residual
# duplicates on (series_id, period) and enumerate name-encoded dims.
# ---------------------------------------------------------------------------

def audit_curated(name: str, dims_note: str) -> None:
    df = pd.read_parquet(CURATED / f"{name}.parquet")
    dup_mask = df.duplicated(subset=["series_id", "period"], keep=False)
    n_dup_rows = int(dup_mask.sum())
    n_dup_keys = int(df.loc[dup_mask].groupby(["series_id", "period"]).ngroups)
    names = df["series_name"].astype(str).head(3).tolist()
    say(f"### {name}")
    say("")
    say(f"- rows: {len(df):,} | distinct series_id: {df['series_id'].nunique():,} | "
        f"window {str(df['period'].min())[:10]} → {str(df['period'].max())[:10]}")
    say(f"- sample names: {names}")
    say(f"- dims note: {dims_note}")
    if n_dup_rows:
        ex = (
            df.loc[dup_mask, ["series_id", "period", "value"]]
            .groupby(["series_id", "period"])["value"]
            .nunique()
        )
        bad = ex[ex > 1]
        say(f"- duplicate (series_id, period) keys IN curated: **{n_dup_keys:,}** "
            f"covering {n_dup_rows} rows; with differing values: {len(bad)}")
        if len(bad):
            for (sid, per), nv in bad.head(5).items():
                say(f"  - `{sid}` @ {per}: {nv} differing values")
    else:
        say("- duplicate (series_id, period) keys in curated: **0** "
            "(dedup already collapsed whatever collided upstream)")
    say("")


audit_curated(
    "quorum",
    "series_id = {pipe_prefix}_{kind}_{loc}_{cycle}; source OAC rows keyed by loc+cycle+flow_ind — flow_ind/purpose not visible in curated names",
)
audit_curated(
    "gasnom",
    "same template as quorum ({pipe}_{kind}_{loc}_{cycle}); Golden Pass/Cameron interconnects",
)
audit_curated(
    "bhe",
    "egts_{sq|oac|opcap}_{loc}_{cycle}; EGTS CSV carries flow_ind + Interconnect Party Name — filter selects Cove Point R-leg only by construction, but opcap/sq of OTHER shippers at 40704 are dropped silently",
)
audit_curated(
    "cheniere",
    "{creole_trail|corpus_christi}_{oac|sq|design}_{loc}_{cycle}; API returns one row per loc/cycle/flow_ind — flow_ind folded into loc suffix (…-R), so no collision observed",
)

Path(r"C:\Users\Dell\Github\bt-curation\analysis\_collision_audit.txt").write_text(
    "\n".join(report), encoding="utf-8"
)
print("\nwrote analysis/_collision_audit.txt")
