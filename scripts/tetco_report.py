"""Final TETCO integration report — Freeport anchor, floor, rows, combined utilization.

Reads data/curated/enbridge.parquet (run transformers.enbridge first).
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

CURATED = Path("data/curated/enbridge.parquet")

GULF_SOUTH_MMCF_D = 898.0  # Blue Tide's existing Gulf South capture (per brief)
FREEPORT_NAMEPLATE_MMCF_D = 2100.0


def main() -> None:
    df = pd.read_parquet(CURATED)
    sq = df[df["series_id"].str.contains("_sq_")]
    oac = df[df["series_id"].str.contains("_oac_")]

    print("=== CURATED HISTORY ===")
    print(f"total rows      : {len(df):,}")
    print(f"period range    : {df['period'].min()} .. {df['period'].max()}")
    print(f"distinct days   : {df['period'].nunique():,}")
    print(f"sq rows         : {len(sq):,}   oac rows: {len(oac):,}")
    print(f"distinct meters : {sq['series_id'].str.split('_').str[2].nunique()}")

    print("\n=== FREEPORT ANCHOR — tetco_sq_79999_d_timely ===")
    fp = sq[sq["series_id"] == "tetco_sq_79999_d_timely"].sort_values("period")
    if len(fp) == 0:
        print("NO TIMELY SERIES FOUND — available 79999 ids:")
        print(sorted(sq[sq["series_id"].str.startswith("tetco_sq_79999")]["series_id"].unique())[:20])
        return
    vals = fp["value"]
    latest = fp.iloc[-1]
    print(f"rows            : {len(fp):,}")
    print(f"range           : {fp['period'].min()} .. {fp['period'].max()}")
    print(f"true floor      : {vals.min():,.0f} Dth/d on {fp.loc[vals.idxmin(), 'period']}")
    print(f"median / mean   : {vals.median():,.0f} / {vals.mean():,.0f} Dth/d")
    print(f"latest          : {latest['value']:,.0f} Dth/d ({latest['period']})")

    # All-cycle view of the most recent gas day with timely data.
    day = sq[(sq["period"] == latest["period"]) & (sq["series_id"].str.startswith("tetco_sq_79999"))]
    print("\nall cycles, latest gas day:")
    for _, r in day.sort_values("series_id").iterrows():
        print(f"  {r['series_id']:42s} {r['value']:>12,.0f}")

    tetco_latest_mmcf = float(latest["value"]) / 1000.0
    combined = GULF_SOUTH_MMCF_D + tetco_latest_mmcf
    util_old = GULF_SOUTH_MMCF_D / FREEPORT_NAMEPLATE_MMCF_D * 100
    util_new = combined / FREEPORT_NAMEPLATE_MMCF_D * 100
    print("\n=== COMBINED FREEPORT FEEDGAS ===")
    print(f"Gulf South (existing)   : {GULF_SOUTH_MMCF_D:.0f} MMcf/d")
    print(f"TETCO Stratton Ridge    : {tetco_latest_mmcf:,.1f} MMcf/d  ({latest['period']} timely)")
    print(f"COMBINED                : {combined:,.1f} MMcf/d")
    print(f"utilization vs nameplate: {util_new:.1f}%  (was {util_old:.1f}% before TETCO)")

    print("\n=== sanity ===")
    z = fp[vals == 0]
    print(f"zero-TSQ timely days: {len(z)} ({len(z)/len(fp)*100:.1f}%)")

    summary = {
        "total_rows": int(len(df)),
        "period_min": str(df["period"].min()),
        "period_max": str(df["period"].max()),
        "freeport_timely_rows": int(len(fp)),
        "freeport_floor_dth": float(vals.min()),
        "freeport_floor_day": str(fp.loc[vals.idxmin(), "period"]),
        "freeport_median_dth": float(vals.median()),
        "freeport_latest_dth": float(latest["value"]),
        "freeport_latest_day": str(latest["period"]),
        "combined_mmcf_d": round(combined, 1),
        "combined_utilization_pct": round(util_new, 1),
        "previous_utilization_pct": round(util_old, 1),
    }
    Path("data/tetco_report.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("\nwrote data/tetco_report.json")


if __name__ == "__main__":
    main()
