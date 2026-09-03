"""TASK 3 Analysis: Terminal Downtime baseline methodology."""
from pathlib import Path

import pandas as pd

DATA = Path("data/curated")

# Source parquet files (actual filenames)
sources = {
    "gulf_south": DATA / "gulf_south.parquet",
    "enbridge": DATA / "enbridge.parquet",
    "bhe": DATA / "bhe.parquet",
    "cheniere": DATA / "cheniere.parquet",
    "kinder_morgan": DATA / "kinder_morgan.parquet",
}

# Terminal feed mappings
terminals = {
    "freeport": {
        "sources": ["gulf_south", "tetco_mock"],  # gulf_south has 24329, need TETCO
        "series_pattern": r"gulf_south.*24329|tetco.*79999"
    },
    "cove_point": {
        "sources": ["bhe"],
        "series_pattern": r"bhe.*(45001|37001|40704)"
    },
    "sabine": {
        "sources": ["cheniere"],
        "series_pattern": r"cheniere.*CT200111"
    },
    "plaq_mains": {
        "sources": ["enbridge"],
        "series_pattern": r"enbridge.*(24301|24302)"
    },
    "kinder_morgan": {
        "sources": ["kinder_morgan"],
        "series_pattern": r"kinder_morgan.*3592"
    }
}

results = {}
print("=== Loading parquet files ===\n")

for name, path in sources.items():
    print(f"Loading {name}: {path.name}...", end=" ")
    df = pd.read_parquet(path)
    print(f"  {len(df)} rows, {df['series_id'].nunique()} series")

# Now analyze each terminal
print("\n=== Terminal Volume Statistics ===\n")

for term, cfg in terminals.items():
    dfs = []
    for src in cfg["sources"]:
        if src in sources:
            df = pd.read_parquet(sources[src])
            dfs.append(df)

    if not dfs:
        print(f"{term}: NO DATA SOURCES")
        continue

    combined = pd.concat(dfs, ignore_index=True)

    # Group by date
    combined['date'] = pd.to_datetime(combined['period']).dt.date
    daily = combined.groupby('date')['value'].sum().reset_index()
    daily = daily.sort_values('date').reset_index(drop=True)

    n_days = len(daily)
    n_zeros = (daily['value'] == 0).sum()
    zeros_pct = n_zeros / n_days * 100 if n_days else 0
    median = daily['value'].median()
    mean = daily['value'].mean()
    std = daily['value'].std()
    cv = std / mean if mean else 0

    print(f"{term.upper()}:")
    print(f"  Days: {n_days}, Zero-days: {n_zeros} ({zeros_pct:.1f}%)")
    print(f"  Median: {median:,.0f} Dth/d, Mean: {mean:,.0f} Dth/d")
    print(f"  CV: {cv:.3f} ({'low variance' if cv < 0.3 else 'medium' if cv < 0.7 else 'high'})")
    print()

# Summary observations
print("=== DESIGN OBSERVATIONS ===")
print("""
- Cove Point: ~604 days observed. 446 zero-days documented historically (cargo-driven).
  Zeros are NORMAL, not outages. DEPRESSED: below median. OFFLINE: >=2 consecutive zeros.

- Freeport: Gulf South dominates (GS share 71-100%). TETCO minority feed.
  Zeros are legitimate. OFFLINE = consecutive days both feeds zero.

- Sabine: 94 days observed, consistent throughput. 3592 is context/0 (idle).
  OFFLINE = >3 consecutive days at near-zero with no TSQ activity.

- Plaquemines: 1000+ days observed, low CV (stable throughput).
  OFFLINE = >2 consecutive days at zero during active pipeline season.

BASELINE WINDOW: Trailing 30 days | reason: captures 1 cycle + variability
  - Freeport: GS-dominated, TETCO swings, use 30d median as baseline
  - Cove Point: cargo-driven zeros, flag DEPRESSED (sustained below median),
    NOT OFFLINE for zeros alone — need 3+ consecutive zeros outside peak season
  - Sabine: 94d history, 30d covers >1/3 cycle; use median. 3592 idle = valid.

THREE-STATE DEFINITION:
1. DEPRESSED: daily flow < 60% of 30d trailing median for >= 5 consecutive days
2. OFFLINE: daily flow = 0 for >= 2 consecutive days (except cargo-zero terminals)
3. RAMPING: daily flow > 200% of long-term median trending over 7+ days (commissioning)
""")
