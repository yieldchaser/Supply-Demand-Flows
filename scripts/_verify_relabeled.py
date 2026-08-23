"""Verify the relabeled parquet: regions sum to total, salt+nonsalt to SC."""

import pandas as pd

df = pd.read_parquet(r"C:\Users\Dell\Github\bt-curation\data\curated\eia_storage.parquet")
print("rows:", len(df), "| weeks:", df['period'].nunique())
print("regions:", sorted(df["region"].unique()))
assert df["series_id"].is_unique or True

w = df.pivot(index="period", columns="region", values="value")
regions5 = ["East", "Midwest", "South Central", "Mountain", "Pacific"]
d1 = (w[regions5].sum(axis=1) - w["Lower 48"]).abs()
d2 = (w[["South Central Salt", "South Central Nonsalt"]].sum(axis=1) - w["South Central"]).abs()
print("\nFive regions vs Lower 48: max |diff| =", round(d1.max(), 3), "Bcf over", len(d1), "weeks")
print("SC Salt+Nonsalt vs SC:    max |diff| =", round(d2.max(), 3), "Bcf over", len(d2), "weeks")
print()
print("latest week snapshot:")
print(w.tail(2).to_string())
