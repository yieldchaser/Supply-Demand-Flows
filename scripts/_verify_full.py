"""Full-history mapping verification: labeled raw vs all 450 curated weeks.

The raw file covers ~1 year (51 weeks). For the full 450-week verification we
verify internal consistency of the curated parquet itself:
  ranks {2,3,4,5,6} == rank 7  (five regions sum to US total)
  rank 0 + rank 1 == rank 4    (SC salt + SC nonsalt == South Central)
and confirm the label assignment via the raw overlap above.
"""

import pandas as pd

CUR = r"C:\Users\Dell\Github\bt-curation\data\curated\eia_storage.parquet"

cur = pd.read_parquet(CUR)
cur = cur.sort_values(["period"], kind="stable").reset_index(drop=True)
cur["rank"] = cur.groupby("period").cumcount()
wide = cur.pivot(index="period", columns="rank", values="value")

regions_sum = wide[[2, 3, 4, 5, 6]].sum(axis=1)
total = wide[7]
d1 = (regions_sum - total).abs()

salt_pair = wide[[0, 1]].sum(axis=1)
sc = wide[4]
d2 = (salt_pair - sc).abs()

print("weeks:", len(wide))
print("five regions {2,3,4,5,6} vs total {7}: max |diff| =", round(d1.max(), 3),
      "| weeks with |diff| > 0.55:", int((d1 > 0.55).sum()))
print("salt{nonsalt {0,1} vs SC {4}:          max |diff| =", round(d2.max(), 3),
      "| weeks with |diff| > 0.55:", int((d2 > 0.55).sum()))

# also check the alternative region set {0,...} doesn't fit better anywhere
alt = wide[list(set(range(7)) - {2, 3, 4, 5, 6})].sum(axis=1)
print("(sanity) complement-of-regions vs total max |diff|:", round((alt - total).abs().max(), 1))
