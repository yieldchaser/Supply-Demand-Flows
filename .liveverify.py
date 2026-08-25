"""Live verification: kinder_morgan + basin_egress series in served bundle."""

import json
import os
import urllib.request

url = "https://yieldchaser.github.io/Supply-Demand-Flows/data/bundle.8685e66b.json"
path = os.environ.get("LOCALAPPDATA", "/tmp") + "/live5.json"
urllib.request.urlretrieve(url, path)
size = os.path.getsize(path) / 1048576
print(f"bundle size: {size:.1f} MB")

b = json.load(open(path, encoding="utf-8"))
print("sources:", sorted(b["sources"]))

km = b["sources"].get("kinder_morgan", {}).get("data", [])
print("kinder_morgan rows:", len(km))
for r in km:
    print("  ", r["series_id"], r["period"], r["value"])

be = b["sources"].get("basin_egress", {})
if be:
    data = be.get("data", [])
    oac = [r for r in data if "_oac_" in r["series_id"]]
    print("basin_egress rows:", len(data), "| with _oac_:", len(oac))
else:
    print("basin_egress source: ABSENT")

gs = b["sources"]["gulf_south"]["data"]
oac_gs = [r for r in gs if "_oac_" in r["series_id"]]
print("gulf_south _oac_ rows in bundle:", len(oac_gs))
os.remove(path)
