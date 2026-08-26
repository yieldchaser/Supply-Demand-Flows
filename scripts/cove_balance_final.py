"""Final decisive balance: demand / receipts_3f over the full overlap window."""
import glob
import json
from collections import defaultdict

import pandas as pd

RANK = {"ID3": 5, "EVENING": 4, "ID2": 3, "ID1": 2, "TIMELY": 1}
data: dict[str, dict[str, tuple[int, float]]] = defaultdict(dict)
for fp in sorted(glob.glob("data/raw/bhe/cpl_*.json")):
    d = json.load(open(fp, encoding="utf-8"))
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

t = pd.DataFrame({k: {p: v for p, (rk, v) in sorted(per.items())} for k, per in data.items()}).sort_index()
t = t[t.index >= "2026-05-27"]
local_cols = [c for c in t.columns if c.split("_")[0] in
              {"77010", "77020", "77030", "77040", "77050", "77060",
               "88000", "89000", "97000", "87000"}]
local = t[local_cols].sum(axis=1)
demand = t["10001_D"] + local
r = demand / (t["45001_R"] + t["37001_R"] + t["47001_R"])
print(f"FINAL: demand / receipts_3f over {len(t)} days")
print(f"  mean {r.mean():.3f} | std {r.std():.3f} | min {r.min():.3f} | max {r.max():.3f}")
print(f"  days within ±5% of 1.0: {((r - 1).abs() < 0.05).sum()}/{len(t)}")

# And the utilization picture for the correct sum
np_ = 750 * 1025
feed3 = t["45001_R"] + t["37001_R"] + t["47001_R"]
u = feed3 / np_ * 100
print()
print(f"receipts_3f utilization vs 750 MMcf/d nameplate:")
print(f"  mean {u.mean():.1f}% | median {u.median():.1f}% | max {u.max():.1f}% | min {u.min():.1f}%")
print(f"  latest day ({t.index[-1]}): {feed3.iloc[-1]:,.0f} Dth/d = {feed3.iloc[-1]/1025:.1f} MMcf/d = {u.iloc[-1]:.1f}%")
