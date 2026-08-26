"""FINAL Cove Point verdict — mass balance closes with receipts_3f = plant + local.

receipts_3f / (plant_intake + local_deliveries) = 1.017 ± 0.036 over 92 days.
Storage (10002) is NOT part of the balance: it's a separate injection stream
whose gas does not come from the three feeder receipts.

So the three feeders ARE independent parallel feeds, and their sum IS total
CPL throughput. But CPL throughput ≠ LNG liquefaction feedgas: most of the
gas goes to local LDC/power deliveries, NOT to the plant.
"""
from __future__ import annotations

import glob
import json
from collections import defaultdict

import pandas as pd

RANK = {"ID3": 5, "EVENING": 4, "ID2": 3, "ID1": 2, "TIMELY": 1}
NP = 750 * 1025


def load() -> pd.DataFrame:
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
    t = pd.DataFrame(
        {k: {p: v for p, (rk, v) in sorted(per.items())} for k, per in data.items()}
    ).sort_index()
    return t[t.index >= "2026-05-27"]


def main() -> None:
    t = load()
    local_cols = [c for c in t.columns if c.split("_")[0] in
                  {"77010", "77020", "77030", "77040", "77050", "77060",
                   "88000", "89000", "97000", "87000"}]
    local = t[local_cols].sum(axis=1)

    receipts_3f = t["45001_R"] + t["37001_R"] + t["47001_R"]
    plant = t["10001_D"]

    # Feedgas share: what fraction of receipt gas actually feeds liquefaction
    # vs. going straight through to local deliveries? Use the residual method:
    #   plant_intake = feedgas that became LNG (+/- storage swings we can't see)
    #   local_deliv  = pass-through gas to WGL/power (never touches the plant)
    # So of the receipts, the plant takes (plant_intake / receipts) and local
    # takes (local / receipts); they sum to ~1.017 which is our closure error.
    print(f"days: {len(t)}")
    print()
    print("=== FLOW SPLIT OF RECEIPT GAS (means over 92 days) ===")
    print(f"  total receipts (45001+37001+47001): {receipts_3f.mean():>10,.0f} Dth/d "
          f"= {receipts_3f.mean()/1025:.1f} MMcf/d")
    print(f"  → to liquefaction (plant intake):   {plant.mean():>10,.0f} Dth/d "
          f"= {plant.mean()/1025:.1f} MMcf/d ({(plant/receipts_3f).mean()*100:.0f}% of receipts)")
    print(f"  → straight to local delivery:       {local.mean():>10,.0f} Dth/d "
          f"= {local.mean()/1025:.1f} MMcf/d ({(local/receipts_3f).mean()*100:.0f}% of receipts)")
    print()
    print(f"closure: receipts / (plant + local) = {(receipts_3f/(plant+local)).mean():.3f} ± "
          f"{(receipts_3f/(plant+local)).std():.3f}")
    print()

    print("=== THE TWO HONEST METRICS ===")
    u_liq = plant / NP * 100
    u_thr = receipts_3f / NP * 100
    print(f"A) LNG FEEDGAS (plant intake 10001-D): mean {u_liq.mean():.1f}% | max {u_liq.max():.1f}% "
          f"| latest {(u_liq.iloc[-1]):.1f}% of 750 MMcf/d nameplate")
    print(f"   latest value: {plant.iloc[-1]:,.0f} Dth/d = {plant.iloc[-1]/1025:.1f} MMcf/d")
    print()
    print(f"B) TOTAL CPL THROUGHPUT (all 3 feeders): mean {u_thr.mean():.1f}% | max {u_thr.max():.1f}% "
          f"| latest {(u_thr.iloc[-1]):.1f}% of nameplate — NOT comparable to nameplate")
    print(f"   latest value: {receipts_3f.iloc[-1]:,.0f} Dth/d = {receipts_3f.iloc[-1]/1025:.1f} MMcf/d")
    print()

    # Per-feeder contribution to LIQUEFACTION feedgas specifically:
    # allocate plant_intake pro-rata by each feeder's share of receipts.
    shares = pd.DataFrame({
        "Transco PV (45001)": t["45001_R"] / receipts_3f,
        "Columbia Loudoun (37001)": t["37001_R"] / receipts_3f,
        "EGTS Loudoun (47001/40704)": t["47001_R"] / receipts_3f,
    }).mean()
    print("average share of total receipts per feeder:")
    for k, v in shares.items():
        liq_mmcf = v * (plant.mean()) / 1025
        print(f"  {k:<28} {v*100:.1f}% of receipts ≈ {liq_mmcf:.0f} MMcf/d toward plant")


if __name__ == "__main__":
    main()
