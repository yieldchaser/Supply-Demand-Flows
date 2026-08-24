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

    # Final-cycle-per-day view: prefer latec > late > timely (corrections win).
    anchor_cycles = ("latec", "late", "timely")
    parts = []
    for cyc in anchor_cycles:
        s = sq[sq["series_id"] == f"tetco_sq_79999_d_{cyc}"][["period", "value"]]
        s = s.rename(columns={"value": f"v_{cyc}"})
        parts.append(s)
    from functools import reduce

    daily = reduce(
        lambda left, right: left.merge(right, on="period", how="outer"),
        parts,
    )
    daily["final_tsq"] = (
        daily["v_latec"].fillna(daily["v_late"]).fillna(daily["v_timely"])
    )
    daily = daily.sort_values("period")

    print("\n=== FREEPORT ANCHOR — loc 79999 STRATTON RIDGE (final cycle per day) ===")
    vals = daily["final_tsq"]
    latest_day = str(daily["period"].iloc[-1])
    print(f"days covered    : {len(daily):,}")
    print(f"range           : {daily['period'].min()} .. {latest_day}")
    print(f"true floor      : {vals.min():,.0f} Dth/d on {daily.loc[vals.idxmin(), 'period']}")
    print(f"median / mean   : {vals.median():,.0f} / {vals.mean():,.0f} Dth/d")
    p5 = vals.quantile(0.05)
    print(f"5th percentile  : {p5:,.0f} Dth/d")
    print(f"max             : {vals.max():,.0f} Dth/d")
    zero_days = int((vals == 0).sum())
    print(f"zero-flow days  : {zero_days} ({zero_days/len(vals)*100:.1f}%)")

    latest_row = daily.iloc[-1]
    latest_day = str(latest_row["period"])
    print(f"\nlatest day ({latest_day}): latec={_fmt(latest_row.get('v_latec'))}, "
          f"late={_fmt(latest_row.get('v_late'))}, timely={_fmt(latest_row.get('v_timely'))} "
          f"-> final {latest_row['final_tsq']:,.0f} Dth/d")

    tetco_mmcf = float(latest_row["final_tsq"]) / 1000.0
    combined = GULF_SOUTH_MMCF_D + tetco_mmcf
    util_old = GULF_SOUTH_MMCF_D / FREEPORT_NAMEPLATE_MMCF_D * 100
    util_new = combined / FREEPORT_NAMEPLATE_MMCF_D * 100
    med_combined = GULF_SOUTH_MMCF_D + float(vals.median()) / 1000.0
    print("\n=== COMBINED FREEPORT FEEDGAS ===")
    print(f"Gulf South (existing)      : {GULF_SOUTH_MMCF_D:.0f} MMcf/d")
    print(f"TETCO Stratton Ridge latest: {tetco_mmcf:,.1f} MMcf/d ({latest_day})")
    print(f"TETCO Stratton Ridge median: {float(vals.median())/1000:,.1f} MMcf/d")
    print(f"COMBINED (latest)          : {combined:,.1f} MMcf/d -> {util_new:.1f}% of nameplate")
    print(f"COMBINED (median TETCO)    : {med_combined:,.1f} MMcf/d -> "
          f"{med_combined/FREEPORT_NAMEPLATE_MMCF_D*100:.1f}% of nameplate")
    print(f"(panel before TETCO        : {util_old:.1f}% — understated by "
          f"{util_new - util_old:.1f}+ points)")

    summary = {
        "total_rows": int(len(df)),
        "period_min": str(df["period"].min()),
        "period_max": str(df["period"].max()),
        "distinct_days": int(df["period"].nunique()),
        "anchor_days": int(len(daily)),
        "freeport_floor_dth": float(vals.min()),
        "freeport_floor_day": str(daily.loc[vals.idxmin(), "period"]),
        "freeport_p5_dth": float(p5),
        "freeport_median_dth": float(vals.median()),
        "freeport_latest_dth": float(latest_row["final_tsq"]),
        "freeport_latest_day": latest_day,
        "combined_latest_mmcf_d": round(combined, 1),
        "combined_median_tetco_mmcf_d": round(med_combined, 1),
        "combined_utilization_pct": round(util_new, 1),
        "median_combined_utilization_pct": round(med_combined / FREEPORT_NAMEPLATE_MMCF_D * 100, 1),
        "previous_utilization_pct": round(util_old, 1),
        "zero_flow_days": zero_days,
    }
    Path("data/tetco_report.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("\nwrote data/tetco_report.json")


def _fmt(v: object) -> str:
    return f"{float(v):,.0f}" if pd.notna(v) else "-"


if __name__ == "__main__":
    main()
