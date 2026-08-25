"""Latent header-variant audit: scan raw payloads for row-key spelling drift.

For each source, collect the distinct RAW row-key sets that reached curated
ingestion. Multiple normalized-equal-but-textually-different shapes = latent
variant that the old exact-match lookups would have silently dropped.
"""
import glob
import json
from collections import defaultdict

from scrapers.base.headers import normalize_header

keysets: dict[str, dict[tuple[str, ...], int]] = defaultdict(lambda: defaultdict(int))
files_seen: dict[str, int] = defaultdict(int)
MAX_FILES_PER_SOURCE = 150
for path in sorted(glob.glob("data/raw/*/*.json")):
    parts = path.replace("\\", "/").split("/")
    src = parts[2]
    if files_seen[src] >= MAX_FILES_PER_SOURCE:
        continue
    files_seen[src] += 1
    try:
        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)
        for row in (payload.get("data") or [])[:5]:
            keysets[src][tuple(sorted(row.keys()))] += 1
    except Exception:
        continue

for src in sorted(keysets):
    shapes = keysets[src]
    if len(shapes) > 1:
        print(f"== {src}: {len(shapes)} DISTINCT key-shapes")
        for ks, n in sorted(shapes.items(), key=lambda kv: -kv[1]):
            print(f"   x{n}: {list(ks)[:9]}{'...' if len(ks) > 9 else ''}")
        # check whether any two shapes collide after normalization
        norms: set[tuple[str, ...]] = set()
        for ks in shapes:
            t = tuple(sorted(normalize_header(k) for k in ks))
            if t in norms:
                print("   ^^^ NORMALIZED COLLISION — same columns, different spellings")
            norms.add(t)
    elif shapes:
        only = next(iter(shapes))
        print(f"== {src}: single shape ({len(only)} keys), files={sum(shapes.values())}")
