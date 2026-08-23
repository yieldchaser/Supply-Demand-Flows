"""TASK 3 output: run the collision guard against every curated source + the
gulf_south PRE-dedup reconstruction (the proof it catches the original bug)."""

import glob
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, r"C:\Users\Dell\Github\bt-curation")
logging.disable(logging.CRITICAL)

from validators.collision import check_collision  # noqa: E402

BASE = Path(r"C:\Users\Dell\Github\bt-curation")
cfg = yaml.safe_load((BASE / "config/integrity_rules.yaml").read_text(encoding="utf-8"))

lines = []


def say(t=""):
    print(t)
    lines.append(t)


say("# Collision-guard output against current data")
say("")
say("## Curated parquets (post-dedup — residual duplicates)")
say("")
say("| source | verdict | detail |")
say("|---|---|---|")
for key, src in cfg["sources"].items():
    pq = Path(str(src.get("parquet", "")))
    if not pq.exists():
        say(f"| {key} | SKIPPED | parquet absent |")
        continue
    df = pd.read_parquet(pq)
    res = check_collision(df, src, cfg["defaults"])
    msg = res["message"].replace("\n", " ")[:160]
    say(f"| {key} | **{res['severity']}** | {msg} |")

say("")
say("## gulf_south pre-dedup reconstruction (proves the guard catches the original bug)")
say("")

# Rebuild the pre-dedup frame exactly as transformers/gulf_south.py would emit it,
# but WITHOUT the drop_duplicates step.
FIELD_RAW = Path(r"C:\Users\Dell\Github\Supply-Demand-Flows\data\raw\gulf_south")
rows = []
for f in sorted(glob.glob(str(FIELD_RAW / "*.json"))):
    stem = Path(f).stem
    if stem.startswith("_"):
        continue
    cycle = stem.rsplit("_", 1)[1]
    gas_day_file = stem.rsplit("_", 1)[0]
    payload = json.load(open(f, encoding="utf-8"))
    for r in payload.get("data", []):
        loc = str(r.get("Loc") or "").strip()
        if not loc:
            continue
        period_raw = str(r.get("Effective Gas Day") or "")
        if len(period_raw) == 8 and period_raw.isdigit():
            period = f"{period_raw[:4]}-{period_raw[4:6]}-{period_raw[6:]}"
        else:
            period = gas_day_file
        posted_at = str(r.get("Post Date/Time") or payload.get("posted_at") or "")
        for kind, col in (
            ("sq", "Total Scheduled Quantity"),
            ("oac", "Operationally  Available Capacity"),
        ):
            v = r.get(col)
            if v in (None, ""):
                continue
            rows.append(
                {
                    "series_id": f"gulf_south_{kind}_{loc}_{cycle.lower()}",
                    "period": period,
                    "value": float(v),
                }
            )
pre = pd.DataFrame(rows)

# emulate the batch dedup (sort by _posted_at equivalent: file order) — here we
# simply run the guard on ALL pre-dedup rows for one representative month to
# show what the accumulator receives.
res = check_collision(pre, {}, cfg.get("defaults", {}))
n_keys = res["details"].get("keys") or res["details"].get("keys_total") or 0
say(f"- pre-dedup rows: {len(pre):,} → keys: {n_keys:,}")
say(f"- verdict: **{res['severity']}**")
say(f"- {res['message']}")
say("")
if res["severity"] == "FAIL":
    sample = res["details"].get("colliding_sample", [])
    if sample:
        say("Named keys (first 25 of the FAIL details):")
        say("")
        say("| series_id | period | distinct values |")
        say("|---|---|---|")
        for s in sample:
            say(f"| {s['series_id']} | {s['period']} | {s['n_values']} |")

out = BASE / "analysis" / "_collision_guard_report.md"
out.write_text("\n".join(lines), encoding="utf-8")
print("\nwrote", out)
