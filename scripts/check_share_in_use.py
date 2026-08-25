"""Sanity-check share-in-use = TSQ/(TSQ+OAC) on the live bundle for Lonewa."""
import json
import statistics

with open("docs/data/src.gulf_south.f7fa603f.json", encoding="utf-8") as fh:
    d = json.load(fh)
rows = d['data']


def series_map(loc, kind, flow='d', cycle='id3'):
    out = {}
    pre = f'gulf_south_{kind}_{loc}_{flow}_{cycle}'
    for r in rows:
        if r['series_id'].lower() == pre:
            out[r['period']] = float(r['value'])
    return out


tsq = series_map(3362, 'sq')
oac = series_map(3362, 'oac')
days = sorted(set(tsq) & set(oac))[-90:]
shares = [tsq[d] / (tsq[d] + oac[d]) * 100 for d in days if (tsq[d] + oac[d]) > 0]
print(f"Lonewa share-in-use last 90d: min={min(shares):.1f}% max={max(shares):.1f}% med={statistics.median(shares):.1f}%")
ratios = [tsq[d] / oac[d] for d in days if oac[d] > 0]
print(f"(broken TSQ/OAC would read: median {statistics.median(ratios):.2f}x, max {max(ratios):.2f}x)")
print(f"OAC level range: {min(oac[d] for d in days):,.0f} -> {max(oac[d] for d in days):,.0f} Dth/d")
