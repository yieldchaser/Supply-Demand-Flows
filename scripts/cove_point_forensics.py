"""Cove Point double-count forensics (v2 — corrected series handling).

Builds daily best-cycle series for the CPL feedgas meters + EGTS 40704 and
plant/storage meters from RAW payloads (bypassing curated's meter filter so
10001/10002/47001 are included), then runs:
  TEST 1: twin check — egts 40704-D vs cpl 47001-R
  TEST 2: feeder independence correlations
  TEST 3: mass balance receipts vs plant intake + storage + local deliveries
  TEST 4: totals distribution vs nameplate
"""
from __future__ import annotations

import glob
import json
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, ".")

NAMEPLATE_DTH = 750 * 1025
RANK = {"ID3": 5, "EVENING": 4, "ID2": 3, "ID1": 2, "TIMELY": 1}


def load_raw_daily() -> pd.DataFrame:
    """date -> {stem: value} using the best cycle per day, all CPL/EGTS locs."""
    files = sorted(glob.glob("data/raw/bhe/*.json"))
    # stem -> {period: (rank, tsq)}
    data: dict[str, dict[str, tuple[int, float]]] = defaultdict(dict)
    for fp in files:
        try:
            d = json.load(open(fp, encoding="utf-8"))
        except Exception:
            continue
        cycle = str(d.get("cycle") or "").upper()
        rank = RANK.get(cycle, 0)
        period = str(d.get("gas_day") or "")
        if not period or not rank:
            continue
        for r in d.get("data", []):
            loc = str(r.get("Loc", "")).strip()
            flow = str(r.get("Flow Ind", "")).strip().upper()
            try:
                tsq = float(r.get("Total Scheduled Quantity") or 0)
            except (TypeError, ValueError):
                continue
            key = f"{loc}_{flow}"
            cur = data[key].get(period)
            if cur is None or rank > cur[0]:
                data[key][period] = (rank, tsq)
    rows = {}
    for key, per in data.items():
        rows[key] = pd.Series({p: v for p, (rank_, v) in sorted(per.items())}, name=key)
    t = pd.DataFrame(rows).sort_index()
    return t


def main() -> None:
    t = load_raw_daily()
    print(f"days: {len(t)} | {t.index.min()} -> {t.index.max()}")
    print(f"stems: {sorted(t.columns)}")
    print()

    tsc_pv = t.get("45001_R", pd.Series(0, index=t.index))
    col_lou = t.get("37001_R", pd.Series(0, index=t.index))
    egts_cpl = t.get("47001_R", pd.Series(0, index=t.index))
    egts40704_d = t.get("40704_D", pd.Series(0, index=t.index))
    plant_d = t.get("10001_D", pd.Series(0, index=t.index))
    plant_si = t.get("10001_SI", pd.Series(0, index=t.index))
    stor_r = t.get("10002_R", pd.Series(0, index=t.index))

    local_stems = [c for c in t.columns if c.endswith("_D") and c.split("_")[0] in
                   {"77010", "77020", "77030", "77040", "77050", "77060",
                    "88000", "89000", "97000", "87000"}]
    local = t[local_stems].sum(axis=1) if local_stems else pd.Series(0, index=t.index)

    print("=== per-meter stats (Dth/d) ===")
    for name, s in [
        ("45001_R Transco PV", tsc_pv), ("37001_R Columbia", col_lou),
        ("47001_R EGTS(CPL)", egts_cpl), ("40704_D EGTS-side", egts40704_d),
        ("10001_D plant", plant_d), ("10001_SI plant SI", plant_si),
        ("10002_R storage inj", stor_r), ("local deliveries", local),
    ]:
        print(f"  {name:<22} mean {s.mean():>10,.0f} med {s.median():>10,.0f} "
              f"max {s.max():>10,.0f} nonzero {int((s > 0).sum()):>3}/{len(s)}")
    print()

    print("=== TEST 1: twin check — cpl 47001-R vs egts 40704-D ===")
    both = t[(egts_cpl > 0) & (egts40704_d > 0)]
    print(f"  days both > 0: {len(both)}")
    if len(both):
        diff = (both["47001_R"] - both["40704_D"]).abs()
        rel = (diff / both["40704_D"].clip(lower=1)) * 100
        print(f"  mean |47001R - 40704D| = {diff.mean():,.0f} Dth/d ({rel.mean():.2f}% rel)")
        print(f"  pearson corr = {both['47001_R'].corr(both['40704_D']):.4f}")
        print("  sample days:")
        for p in list(both.index)[:5]:
            i = list(t.index).index(p)
            print(f"    {p}: cpl47001={t.loc[p, '47001_R']:,.0f} vs egts40704D={t.loc[p, '40704_D']:,.0f}")
    else:
        print("  no overlap — check whether one side posts zeros when other flows")
        z = t[((egts_cpl == 0) & (egts40704_d > 0)) | ((egts_cpl > 0) & (egts40704_d == 0))]
        print(f"  mismatched-zero days: {len(z)}")
    print()

    print("=== TEST 2: independence correlations ===")
    pairs = [
        ("45001_R", "37001_R"), ("45001_R", "40704_D"), ("37001_R", "40704_D"),
        ("45001_R", "47001_R"), ("37001_R", "47001_R"),
        ("45001_R", "10001_D"), ("37001_R", "10001_D"), ("40704_D", "10001_D"),
        ("45001_R", "10002_R"), ("10002_R", "10001_D"),
    ]
    for a, b in pairs:
        c = t[a].corr(t[b])
        tag = ("LOCKSTEP" if abs(c) > 0.95 else
               "high" if abs(c) > 0.8 else
               "moderate" if abs(c) > 0.5 else "low")
        print(f"  corr({a:<10},{b:<10}) = {c:+.3f} [{tag}]")
    print()

    print("=== TEST 3: mass balance ===")
    bal = pd.DataFrame(index=t.index)
    bal["receipts_3f"] = t["45001_R"] + t["37001_R"] + t["47001_R"]
    bal["receipts_2f"] = t["45001_R"] + t["37001_R"]
    bal["plant_intake"] = t["10001_D"]
    bal["storage_inj"] = t["10002_R"]
    bal["local_deliv"] = local
    bal["resid_2f"] = bal["receipts_2f"] - bal["plant_intake"] - bal["storage_inj"] - bal["local_deliv"]
    bal["resid_3f"] = bal["receipts_3f"] - bal["plant_intake"] - bal["storage_inj"] - bal["local_deliv"]
    for name in ["resid_2f", "resid_3f"]:
        r = bal[name]
        denom = bal["receipts_2f"].replace(0, pd.NA)
        print(f"  {name}: mean {r.mean():>10,.0f} | med {r.median():>10,.0f} | std {r.std():>9,.0f}")
    m = bal.copy()
    m["month"] = [p[:7] for p in m.index]
    mm = m.groupby("month")[["receipts_3f", "receipts_2f", "plant_intake", "storage_inj", "local_deliv"]].mean().round(0)
    print(mm.to_string())
    print()

    print("=== TEST 4: totals vs nameplate (768,750 Dth/d) ===")
    for label, col in [("receipts_3f (45001+37001+47001)", "receipts_3f"),
                       ("receipts_2f (45001+37001)", "receipts_2f"),
                       ("plant_intake (10001_D)", "plant_intake")]:
        u = bal[col] / NAMEPLATE_DTH * 100
        print(f"  {label:<34} mean {u.mean():6.1f}% | max {u.max():6.1f}% | "
              f"days>105%: {(u > 105).sum():>3}/{len(u)}")


if __name__ == "__main__":
    main()
