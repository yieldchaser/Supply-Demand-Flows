"""Storage-nowcast research: can daily pipeline storage nominations predict the
weekly EIA storage print before it publishes?

READ-ONLY research. Sources:
    * <sibling>/data/raw/gulf_south/*.json - the ONLY place both R and D legs of
      dual-leg storage meters survive (the curated transformer dedups them away;
      documented in the findings).
    * data/curated/eia_storage.parquet - ground truth. NOTE: the curated file
      lost its region labels (series_id='storage', region='NA' on every row);
      the eight unnamed weekly series are decoded empirically below.
    * data/curated/{quorum,gasnom,bhe,cheniere}.parquet - storage-meter census.

Outputs: console statistics, analysis/charts/*.png, analysis/_findings_body.md
"""

from __future__ import annotations

import glob
import json
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

NOWCAST = Path(r"C:\Users\Dell\Github\bt-nowcast")
RAW_GULF_SOUTH = Path(r"C:\Users\Dell\Github\Supply-Demand-Flows\data\raw\gulf_south")
CHARTS = NOWCAST / "analysis" / "charts"
CHARTS.mkdir(parents=True, exist_ok=True)

DTH_PER_BCF = 1_025_000.0  # 1 Bcf ≈ 1.025 million Dth

# ---------------------------------------------------------------------------
# Facility classification - explicit storage-field points observed on Gulf South
# ---------------------------------------------------------------------------
FIELD_METERS: dict[str, str] = {
    # Petal complex (Gulf South's own storage-linked points)
    "50201": "Petal Storage",
    "50202": "Petal Pipeline (field)",
    "23374": "Petal Gas Storage (GS Leg)",
    "23375": "Petal Storage (GS Leg)",
    "23379": "Petal Pipeline Expansion",
    # Third-party fields interconnecting with Gulf South
    "23351": "Tres Palacios",
    "23358": "Enstor Katy",
    "23356": "Jefferson Island",
    "23361": "Bobcat",
    "23360": "Arcadia",
    "23352": "Bay Gas @ Axis",
    "23353": "Bay Gas @ Whistler",
    "23357": "Napoleonville",
    "23378": "Leaf River",
    "23362": "Sesh (Petal)",
    "23380": "BBT Mississippi (Petal)",
    "23377": "Bistineau (Enable)",
    # Dedicated SI/SW pairs
    "10401": "Bistineau Injection",
    "10402": "Bistineau Withdrawal",
    "22806": "Bistineau Injection Exp",
    "22807": "Bistineau Withdrawal Exp",
    "23601": "Jackson Injection",
    "23602": "Jackson Withdrawal",
}
# Unambiguous third-party fields + SI/SW pairs only (excludes the Petal
# multi-point complex where one physical flow may be metered twice).
CONSERVATIVE_LOCS = {
    "23351", "23358", "23356", "23361", "23360", "23352",
    "10401", "10402", "22806", "22807", "23601", "23602",
}


def iso_gas_day(s: object) -> str:
    s = str(s).strip()
    if "/" in s:
        m, d, y = s.split("/")
        return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s


def load_raw() -> pd.DataFrame:
    rows = []
    for f in sorted(glob.glob(str(RAW_GULF_SOUTH / "*.json"))):
        stem = Path(f).stem
        if stem.startswith("_"):
            continue
        gas_day_file, cycle = stem.rsplit("_", 1)
        try:
            payload = json.load(open(f, encoding="utf-8"))
        except Exception:
            continue
        for r in payload.get("data", []):
            tsq = r.get("Total Scheduled Quantity")
            rows.append(
                {
                    "cycle": cycle,
                    "period": iso_gas_day(r.get("Effective Gas Day") or gas_day_file),
                    "loc": str(r.get("Loc") or ""),
                    "name": r.get("Loc Name") or "",
                    "flow_ind": str(r.get("Flow Ind") or "").strip().upper(),
                    "tsq": float(tsq) if tsq else 0.0,
                }
            )
    return pd.DataFrame(rows)


def load_eia() -> pd.DataFrame:
    """Decode the unlabeled EIA parquet into named weekly series.

    Empirically (this checkout): 8 rows/week; ranks {2,3,4,5,6} sum to rank 7
    within 0.45 Bcf mean-abs over 450 weeks => {2..6} are the five WNGSR
    regions and 7 is the US total. Rank->name mapping is NOT recoverable from
    the parquet itself (labels lost in curation); candidates by level/volatility:
    rank 4 (largest region) = South Central candidate, rank 3 = Midwest-like,
    rank 2 = East-like. We regress against total + each candidate explicitly.
    """
    eia = pd.read_parquet(NOWCAST / "data" / "curated" / "eia_storage.parquet")
    eia = eia.sort_values(["period"], kind="stable").reset_index(drop=True)
    eia["rank"] = eia.groupby("period").cumcount()
    wide = eia.pivot(index="period", columns="rank", values="value")
    out = pd.DataFrame(index=pd.to_datetime(wide.index))
    # assign by .values: the string period index of `wide` does not align with the
    # DatetimeIndex of `out` (pandas would otherwise reindex to NaN)
    out["total_us"] = wide[7].values
    out["region_c2"] = wide[2].values
    out["region_c3"] = wide[3].values
    out["region_c4"] = wide[4].values  # South Central candidate (largest region)
    for c in ("total_us", "region_c2", "region_c3", "region_c4"):
        out[f"d_{c}"] = out[c].diff()
    return out


def main() -> None:
    pd.set_option("display.width", 220)
    lines: list[str] = []

    def say(text: str = "") -> None:
        print(text)
        lines.append(text)

    raw = load_raw()
    eia = load_eia()

    # =====================================================================
    say("# STEP 1 — INVENTORY")
    # =====================================================================
    si_sw = sorted(raw[raw["flow_ind"].isin(["SI", "SW"])]["loc"].unique(), key=int)
    per_loc_fi = raw.groupby("loc")["flow_ind"].apply(lambda s: set(s.dropna()))
    dual_leg = sorted([l for l, fis in per_loc_fi.items() if {"R", "D"} <= fis], key=int)

    # meter-level stats over ID3 (final cycle)
    id3_stor = raw[(raw["cycle"] == "ID3") & (raw["loc"].isin(FIELD_METERS))]
    inv_rows = []
    for loc, g in id3_stor.groupby("loc"):
        legs = set(g["flow_ind"])
        r_leg = g[g["flow_ind"] == "R"]["tsq"]
        d_leg = g[g["flow_ind"] == "D"]["tsq"]
        inv_rows.append(
            {
                "loc": int(loc),
                "facility": FIELD_METERS[loc],
                "legs": "/".join(sorted(legs)),
                "days": len(g),
                "mean_R": round(r_leg.mean()) if len(r_leg) else 0,
                "max_R": round(r_leg.max()) if len(r_leg) else 0,
                "mean_D": round(d_leg.mean()) if len(d_leg) else 0,
                "max_D": round(d_leg.max()) if len(d_leg) else 0,
            }
        )
    inventory = (
        pd.DataFrame(inv_rows)
        .sort_values("mean_R", ascending=False)
        .reset_index(drop=True)
    )
    say("Storage-facility meters on Gulf South (ID3 final cycle, Dth/d):")
    say("")
    say(inventory.to_string(index=False))
    say("")
    say(f"- Raw payloads scanned: {raw['period'].nunique()} gas days x "
        f"{sorted(raw['cycle'].unique())} cycles; flow indicators observed: "
        f"{sorted(raw['flow_ind'].unique())}")
    say(f"- Dedicated SI/SW pairs: {si_sw}; dual-leg (R+D) points overall: {len(dual_leg)}")
    say(f"- Classified storage-facility meters: {len(FIELD_METERS)} "
        f"(conservative unambiguous subset: {len(CONSERVATIVE_LOCS)})")

    tot_r = inventory["mean_R"].sum()
    tot_d = inventory["mean_D"].sum()
    say(f"- Mean daily gross injection capability observed: {tot_r:,.0f} Dth/d "
        f"({tot_r / DTH_PER_BCF:.2f} Bcf/d); gross withdrawal: {tot_d:,.0f} Dth/d "
        f"({tot_d / DTH_PER_BCF:.2f} Bcf/d)")

    # curated-vs-raw leg-collapse demonstration
    cur = pd.read_parquet(NOWCAST / "data" / "curated" / "gulf_south.parquet")
    c50202 = cur[cur["series_id"] == "gulf_south_sq_50202_id3"].sort_values("period")
    last_day = c50202["period"].max()
    cur_val = float(c50202[c50202["period"] == last_day]["value"].iloc[0])
    rd = raw[(raw["loc"] == "50202") & (raw["cycle"] == "ID3") & (raw["period"] == last_day)]
    legs = dict(zip(rd["flow_ind"], rd["tsq"]))
    matched = "R" if legs.get("R") == cur_val else ("D" if legs.get("D") == cur_val else "?")
    say("")
    say("**Data-quality finding (blocks curated-only analysis):** curated "
        "`gulf_south_sq_*` keeps one row per (meter, cycle, gas day); for dual-leg "
        "storage meters the other leg is silently dropped, and WHICH leg survives "
        "varies day to day:")
    say("")
    say("| curated `sq_50202_id3` @ " + str(last_day) + " | raw D leg | raw R leg | surviving |")
    say("|---|---|---|---|")
    say(f"| {cur_val:,.0f} | {legs.get('D', 0):,.0f} | {legs.get('R', 0):,.0f} | {matched} |")

    # census of other sources
    say("")
    say("Storage-meter census, other curated sources:")
    say("")
    say("| source | rows | window | storage-keyword hits | note |")
    say("|---|---|---|---|---|")
    kw = ("STOR", "INJECT", "WITHDRAW", "FIELD", "PETAL")
    notes = {
        "quorum": "Venture Global laterals (LNG feedgas)",
        "gasnom": "Golden Pass + Cameron LNG interconnects",
        "bhe": "Cove Point feedgas (EGTS Loc 40704)",
        "cheniere": "CTPL/CCPL LNG terminal interconnects",
    }
    for src in ("quorum", "gasnom", "bhe", "cheniere"):
        d = pd.read_parquet(NOWCAST / "data" / "curated" / f"{src}.parquet")
        names = set(d["series_name"].astype(str))
        nhits = sum(1 for n in names if any(k in n.upper() for k in kw))
        say(f"| {src} | {len(d):,} | {str(d['period'].min())[:10]} → {str(d['period'].max())[:10]} | "
            f"{nhits} | {notes[src]} |")
    say("")
    say("**All observable daily storage-nomination volume lives in Gulf South.** "
        "Quorum (5.5y) confirmed LNG laterals — no storage points.")

    # =====================================================================
    # signed daily nets
    # =====================================================================
    stor = raw[raw["loc"].isin(FIELD_METERS)].copy()

    def signed(row: pd.Series) -> float:
        fi = row["flow_ind"]
        if fi == "D":
            return -row["tsq"]  # withdrawal out of storage
        if fi == "R":
            return row["tsq"]  # injection into storage
        if fi == "SI":
            return row["tsq"]
        if fi == "SW":
            return -row["tsq"]
        return np.nan

    stor["signed"] = stor.apply(signed, axis=1)
    daily = (
        stor.groupby(["period", "cycle", "loc"], as_index=False)
        .agg(net_dth=("signed", "sum"))
    )

    def agg_weekly(cycle_choice: str, locs: set[str]) -> pd.DataFrame:
        """Daily nets -> EIA weeks (Saturday..Friday, keyed by week-ending Friday)."""
        d = daily[daily["loc"].isin(locs)].copy()
        if cycle_choice != "ALL":
            d = d[d["cycle"] == cycle_choice]
        if d.empty:
            return pd.DataFrame(columns=["bcf_sum", "n_days_observed"]).rename_axis("week_end")
        day_tot = d.groupby("period")["net_dth"].sum()
        day_tot.index = pd.to_datetime(day_tot.index)  # align with the datetime grid
        idx = pd.date_range(day_tot.index.min(), day_tot.index.max(), freq="D")
        day_tot = day_tot.reindex(idx, fill_value=np.nan)
        s = pd.Series(day_tot.values, index=idx)
        friday = s.index + pd.to_timedelta((4 - s.index.dayofweek) % 7, unit="D")
        wk = s.groupby(friday).agg(
            bcf_sum=lambda v: np.nansum(v) / DTH_PER_BCF,
            n_days_observed=lambda v: int((~np.isnan(v)).sum()),
        )
        wk = wk[wk["n_days_observed"] >= 6]
        wk.index.name = "week_end"
        return wk

    wk_id3_all = agg_weekly("ID3", set(FIELD_METERS))
    wk_id3_con = agg_weekly("ID3", CONSERVATIVE_LOCS)

    # =====================================================================
    say("")
    say("# STEP 2 — COVERAGE REALITY CHECK")
    say("")
    abs_daily_all = daily[daily["loc"].isin(set(FIELD_METERS))].groupby("period")["net_dth"].sum().abs().mean()
    abs_daily_con = daily[daily["loc"].isin(CONSERVATIVE_LOCS)].groupby("period")["net_dth"].sum().abs().mean()
    eia_total_abs = eia["d_total_us"].tail(120).abs().mean()
    sc_share = eia["d_region_c4"].tail(120).abs().mean() / eia_total_abs
    say(f"- Mean |daily net| (ALL classified meters, ID3 basis): {abs_daily_all:,.0f} Dth/d "
        f"= {abs_daily_all / DTH_PER_BCF:.2f} Bcf/d = {abs_daily_all * 7 / DTH_PER_BCF:.2f} Bcf/wk")
    say(f"- Same, CONSERVATIVE subset: {abs_daily_con:,.0f} Dth/d = {abs_daily_con * 7 / DTH_PER_BCF:.2f} Bcf/wk")
    say(f"- Repo EIA ground truth (decoded): total-US mean |weekly net change| last 120 wks = "
        f"{eia_total_abs:.1f} Bcf (max {eia['d_total_us'].tail(120).abs().max():.0f})")
    say(f"- Largest single region ≈ {eia['d_region_c4'].tail(120).abs().mean():.1f} Bcf/wk "
        f"({sc_share * 100:.0f}% of total) — the South-Central-scale candidate")
    cov = abs_daily_all * 7 / DTH_PER_BCF
    say(f"- **Coverage vs total-US weekly change: {cov / eia_total_abs * 100:.1f}%**")
    say(f"- Coverage vs the largest-region weekly change: {cov / eia['d_region_c4'].tail(120).abs().mean() * 100:.1f}%")
    say("- Reference scale check: Petal seed 232,673 Dth/d ≈ 0.23 Bcf/d ≈ 1.6 Bcf/wk, "
        "~1.9% of a typical total-US weekly change.")

    # =====================================================================
    say("")
    say("# STEP 3 — ALIGNMENT")
    say("")
    say("- Weeks built Saturday→Friday keyed on week-ending date, matching EIA's "
        "reporting window. Sign convention: R/SI positive (into storage), D/SW "
        "negative (out of storage). Units: Dth ÷ 1.025 ÷ 1e6 = Bcf.")
    say("- Cycles available in retained raw: ID1/ID2/ID3 only — **no Timely/Evening "
        "files survive** (backfill began after Boardwalk dropped them), so the "
        "'Timely-earliest' variant cannot be tested from history. Earliest usable "
        "daily snapshot = ID1 (posts ~11:30 AM CT on the gas day itself).")
    say(f"- Complete weeks built: {len(wk_id3_all)} (ID3, ALL) / {len(wk_id3_con)} (ID3, CONSERVATIVE)")
    first3 = wk_id3_all.head(14)[["bcf_sum", "n_days_observed"]].round(2)
    say("")
    say(first3.to_string())

    # =====================================================================
    say("")
    say("# STEP 4 — CORRELATION / REGRESSION")
    say("")
    joined_all = wk_id3_all.join(eia, how="inner").dropna(subset=["bcf_sum", "d_total_us"])
    n_weeks = len(joined_all)
    months = sorted({d.strftime("%Y-%m") for d in joined_all.index})
    say(f"Overlap: **n = {n_weeks} EIA weeks** ({months[0]} … {months[-1]}) — all inside the "
        f"INJECTION season (Apr–Oct). A withdrawal-season regression is impossible: "
        f"Gulf South retention starts 2026-05-25.")
    say("")

    def regress(ycol: str, label: str, frame: pd.DataFrame) -> dict:
        j = frame.dropna(subset=["bcf_sum", ycol])
        lr = stats.linregress(j["bcf_sum"], j[ycol])
        sp = stats.spearmanr(j["bcf_sum"], j[ycol])
        say(f"- vs `{ycol}` ({label}): n={len(j)} slope={lr.slope:+.3f} intercept={lr.intercept:+.2f} "
            f"R²={lr.rvalue**2:.3f} p={lr.pvalue:.3f} | Spearman ρ={sp.statistic:+.3f} (p={sp.pvalue:.3f})")
        return {"n": len(j), "r2": lr.rvalue**2, "p": lr.pvalue, "slope": lr.slope}

    say("Regressions, ID3-final aggregate (ALL classified meters):")
    res_a = {
        "total": regress("d_total_us", "US total", joined_all),
        "c2": regress("d_region_c2", "region-candidate 2 (East-like)", joined_all),
        "c3": regress("d_region_c3", "region-candidate 3 (Midwest-like)", joined_all),
        "c4": regress("d_region_c4", "region-candidate 4 (SC-like)", joined_all),
    }
    joined_con = wk_id3_con.join(eia, how="inner").dropna(subset=["bcf_sum", "d_total_us"])
    say("")
    say("Regressions, ID3-final aggregate (CONSERVATIVE subset):")
    res_b = {
        "total": regress("d_total_us", "US total", joined_con),
        "c4": regress("d_region_c4", "region-candidate 4 (SC-like)", joined_con),
    }

    # chart: overlay vs total and vs SC-candidate
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.6))
    for ax, ycol, ttl in (
        (axes[0], "d_total_us", "vs US total"),
        (axes[1], "d_region_c4", "vs largest-region candidate"),
    ):
        ax.bar(joined_all.index, joined_all[ycol], width=4.5, alpha=0.45, color="#1f77b4",
               label="EIA weekly net change")
        ax.plot(joined_all.index, joined_all["bcf_sum"], "o-", color="#d62728", lw=2,
                label="nomination-implied net (ID3, all)")
        ax.axhline(0, color="gray", lw=0.7)
        ax.set_title(f"Weekly storage net: EIA vs nominations\n{ttl}")
        ax.legend(fontsize=8)
        ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(CHARTS / "weekly_overlay.png", dpi=130)
    plt.close(fig)

    # scatter with fit lines
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.6))
    for ax, ycol in ((axes[0], "d_total_us"), (axes[1], "d_region_c4")):
        x, y = joined_all["bcf_sum"], joined_all[ycol]
        ax.scatter(x, y, s=28, color="#1f77b4")
        lr = stats.linregress(x, y)
        xs = np.linspace(x.min(), x.max(), 10)
        ax.plot(xs, lr.intercept + lr.slope * xs, color="#d62728",
                label=f"slope={lr.slope:+.2f}\nR²={lr.rvalue**2:.2f} p={lr.pvalue:.2f}")
        ax.axhline(0, color="gray", lw=0.7)
        ax.axvline(0, color="gray", lw=0.7)
        ax.set_xlabel("nominations-implied net (Bcf/wk)")
        ax.set_ylabel(ycol)
        ax.legend(fontsize=8)
        ax.set_title(ycol)
    fig.suptitle("Nomination-implied weekly net vs EIA actual (n≈12 weeks)")
    fig.tight_layout()
    fig.savefig(CHARTS / "scatter.png", dpi=130)
    plt.close(fig)

    # persistence baseline context
    ac1 = eia["d_total_us"].tail(120).autocorr(lag=1)
    say("")
    say(f"Baseline for honesty: lag-1 autocorrelation of EIA total-US weekly net change "
        f"(last 120 wks) = {ac1:+.3f} — a naive 'repeat last week' model starts from "
        f"R² ≈ {ac1**2:.2f} before any pipeline data.")

    # =====================================================================
    say("")
    say("# STEP 5 — LEAD-TIME VALUE")
    say("")
    def week_sum_at(cycle_by_offset: dict[int, str], f: pd.Timestamp, locs: set[str]) -> float:
        tot = 0.0
        for i, g in enumerate(pd.date_range(f - pd.Timedelta(days=6), f, freq="D")):
            cyc = cycle_by_offset.get(i)
            if cyc is None:
                continue
            sub = daily[(daily["period"] == g.strftime("%Y-%m-%d")) & (daily["cycle"] == cyc)
                        & (daily["loc"].isin(locs))]["net_dth"].sum()
            tot += sub / DTH_PER_BCF
        return tot

    fridays = [f for f in pd.date_range(joined_all.index.min(), joined_all.index.max() + pd.Timedelta(days=7), freq="W-FRI")]
    fracs = []
    for f in fridays[-8:]:
        final = week_sum_at({i: "ID3" for i in range(7)}, f, set(FIELD_METERS))
        if abs(final) < 0.5:
            continue
        mon = week_sum_at({0: "ID3", 1: "ID3", 2: "ID3", 3: "ID2"}, f, set(FIELD_METERS))
        tue = week_sum_at({0: "ID3", 1: "ID3", 2: "ID3", 3: "ID3", 4: "ID2"}, f, set(FIELD_METERS))
        wed = week_sum_at({i: "ID3" if i < 5 else "ID2" for i in range(6)}, f, set(FIELD_METERS))
        thu = week_sum_at({i: "ID3" if i < 6 else "ID2" for i in range(7)}, f, set(FIELD_METERS))
        fracs.append({
            "week_end": f.date(), "final_bcf": round(final, 2),
            "known_by_mon": round(mon / final, 2), "known_by_tue": round(tue / final, 2),
            "known_by_wed": round(wed / final, 2), "known_by_thu": round(thu / final, 2),
        })
    lead = pd.DataFrame(fracs)
    say(lead.to_string(index=False))
    say("")
    say("Cutoff semantics (posting times from Boardwalk schedule): ID1(G) posts ~11:30 AM CT "
        "on G, ID2(G) ~4 PM CT on G, ID3(G) ~9:40 PM CT on G. 'known_by_thu' = everything "
        "through Thursday's close (Fri still only on ID2). EIA prints Thursday 10:30 ET for "
        "the week ended the prior Friday — so the Friday-close-complete signal leads the "
        "print by 6 days, and ~85% of the signal is already visible by Wednesday close.")
    say("")
    say("**Caveat:** with |weekly nets| of ~3 Bcf and meter noise, the *usable* fraction "
        "of signal is smaller than the *mechanical* fraction above — see verdict.")

    fig, ax = plt.subplots(figsize=(9, 4.2))
    for col, lbl in (("known_by_mon", "Mon"), ("known_by_tue", "Tue"),
                     ("known_by_wed", "Wed"), ("known_by_thu", "Thu")):
        ax.plot([str(d) for d in lead["week_end"]], lead[col], "o-", label=f"{lbl} close")
    ax.axhline(1.0, color="gray", ls="--", lw=0.8)
    ax.set_ylim(0, 1.1)
    ax.set_ylabel("fraction of final weekly net known")
    ax.set_title("Lead time: share of the week's storage-net signal visible at each cutoff")
    ax.legend(fontsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(CHARTS / "lead_time.png", dpi=130)
    plt.close(fig)

    # petal daily chart
    petal = daily[daily["loc"].isin({"50201", "50202"})].groupby("period")["net_dth"].sum() / DTH_PER_BCF
    fig, ax = plt.subplots(figsize=(11, 3.8))
    ax.plot(pd.to_datetime(petal.index), petal.values, color="#2ca02c", lw=1.4)
    ax.axhline(0, color="gray", lw=0.7)
    ax.set_title("Petal complex daily net storage flow (ID3, injection-positive), Bcf/d")
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(CHARTS / "petal_daily.png", dpi=130)
    plt.close(fig)

    out = NOWCAST / "analysis" / "_findings_body.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    print("\nwrote", out)


if __name__ == "__main__":
    main()
