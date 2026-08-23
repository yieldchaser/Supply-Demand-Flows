"""Verify the rank->region mapping hypothesis against the labeled raw file."""

import pandas as pd

RAW = r"C:\Users\Dell\Github\Supply-Demand-Flows\data\raw\eia_storage\2026\04\eia_storage_2026-04-10.json"
CUR = r"C:\Users\Dell\Github\bt-curation\data\curated\eia_storage.parquet"

import json

p = json.load(open(RAW, encoding="utf-8"))
rows = []
for r in p["response"]["data"]:
    rows.append(
        {
            "period": r["period"],
            "series": r["series"],
            "duoarea": r["duoarea"],
            "desc": r["series-description"],
            "value": float(r["value"]),
        }
    )
raw_df = pd.DataFrame(rows)

# region label from series id: SWO_R31..R35, SNO/SSO_R33
def label(s: str) -> str:
    return {
        "NW2_EPG0_SWO_R48_BCF": "Lower 48 (total)",
        "NW2_EPG0_SWO_R31_BCF": "East",
        "NW2_EPG0_SWO_R32_BCF": "Midwest",
        "NW2_EPG0_SWO_R33_BCF": "South Central",
        "NW2_EPG0_SWO_R34_BCF": "Mountain",
        "NW2_EPG0_SWO_R35_BCF": "Pacific",
        "NW2_EPG0_SNO_R33_BCF": "South Central Nonsalt",
        "NW2_EPG0_SSO_R33_BCF": "South Central Salt",
    }.get(s, s)


raw_df["region"] = raw_df["series"].map(label)

# curated ranks
cur = pd.read_parquet(CUR)
cur = cur.sort_values(["period"], kind="stable").reset_index(drop=True)
cur["rank"] = cur.groupby("period").cumcount()

# join raw to curated on period+value, collect which rank each series lands in
m = raw_df.merge(cur[["period", "rank", "value"]], on=["period", "value"], how="inner")
mapping = m.groupby("series")["rank"].agg(["min", "max", "count"])
print("series -> curated rank(s):")
print(mapping.to_string())

# regions sum vs total check using RAW data directly
wide = raw_df.pivot_table(index="period", columns="region", values="value")
regions5 = ["East", "Midwest", "South Central", "Mountain", "Pacific"]
diff = (wide[regions5].sum(axis=1) - wide["Lower 48 (total)"]).abs()
salt_check = (wide[["South Central Salt", "South Central Nonsalt"]].sum(axis=1) - wide["South Central"]).abs()
print("\nregions-sum vs Lower48 total: max |diff| =", round(diff.max(), 3), "| weeks:", len(diff))
print("SC salt+nonsalt vs SC:        max |diff| =", round(salt_check.max(), 3), "| weeks:", len(salt_check))
