"""ENTSOG raw point flows -> curated EU imports by route (GWh/d, daily).

Why:
    The dashboard needs EU gas imports by supply route, not 75 meter series.
    Two traps make a naive sum wrong: several EU operators report the same
    physical meter (Dornum appears three times with identical values, which
    would triple-count part of Norway), and a day on which one normally
    reporting point is missing would silently understate the route.

What:
    1. Maps each (operator, point) to its route (and, for LNG, its country)
       from ``config/entsog_points.json``.
    2. Within a route, drops any point whose series is identical to an earlier
       point in config order (|diff| < 0.5 GWh on >= 80% of the common days
       where either is non-zero, with at least 30 such days).
    3. Interpolates reporting gaps of up to 3 days inside a point's span, then
       sums each route per gas day, but only on days where every kept point
       that reported anything within +/-15 days has a value.
    4. Emits:
         entsog_route_<route>   route total, GWh/d
         entsog_lng_<cc>        LNG send-out by country, GWh/d
         entsog_imports_total   sum of all routes, GWh/d (days with every route)
    and accumulates them into ``data/curated/entsog.parquet``.

Failure modes:
    TransformError when the raw file is missing, when the scraper recorded
    failed batches (partial point coverage would understate totals), or when
    no rows survive.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from transformers.base.accumulate import merge_into_curated
from transformers.errors import TransformError

log = logging.getLogger(__name__)

RAW_PATH = Path("data/raw/entsog/flows.json")
CURATED_PATH = Path("data/curated/entsog.parquet")
CONFIG_PATH = Path("config/entsog_points.json")

DUP_TOL_GWH = 0.5
DUP_SHARE = 0.8
DUP_MIN_DAYS = 30
PRESENCE_WINDOW_DAYS = 15
MAX_FILL_DAYS = 3


def _config(path: Path = CONFIG_PATH) -> tuple[dict[str, str], list[dict[str, str]]]:
    cfg = json.loads(path.read_text(encoding="utf-8"))
    labels = {k: v["label"] for k, v in cfg["routes"].items()}
    points = []
    for route, spec in cfg["routes"].items():
        for e in spec["points"]:
            points.append({"route": route, "key": f"{e[0]}|{e[1]}", "label": e[2], "country": e[3] if len(e) > 3 else ""})
    return labels, points


def find_duplicates(wide: pd.DataFrame, ordered_keys: list[str]) -> dict[str, str]:
    """Map each duplicate column to the earlier column it repeats."""
    dup: dict[str, str] = {}
    kept: list[str] = []
    for k in ordered_keys:
        if k not in wide:
            continue
        twin = None
        for j in kept:
            a, b = wide[k], wide[j]
            both = a.notna() & b.notna() & ((a.abs() > 0) | (b.abs() > 0))
            n = int(both.sum())
            if n < DUP_MIN_DAYS:
                continue
            same = int(((a[both] - b[both]).abs() < DUP_TOL_GWH).sum())
            if same / n >= DUP_SHARE:
                twin = j
                break
        if twin:
            dup[k] = twin
        else:
            kept.append(k)
    return dup


def complete_sum(wide: pd.DataFrame, cols: list[str]) -> pd.Series:
    """Row sum over *cols*, NaN on days where an active column is missing.

    A column is 'active' on a day if it has any observation within
    +/-PRESENCE_WINDOW_DAYS; a gap inside that window is a missing report,
    not a zero flow.
    """
    if not cols:
        return pd.Series(dtype=float)
    sub = wide[cols]
    present = sub.notna()
    window = 2 * PRESENCE_WINDOW_DAYS + 1
    near = present.astype(int).rolling(window, center=True, min_periods=1).max().astype(bool)
    # Only within the point's own reporting span: the weeks before a new
    # terminal's first report are 'not yet built', not 'missing'.
    started = present.cummax()
    not_ended = present[::-1].cummax()[::-1]
    # A point silent for only the last few days has not 'ended': its operator
    # has not published yet, so those newest days stay incomplete.
    recent = present.iloc[-(PRESENCE_WINDOW_DAYS + 1):].any()
    not_ended.loc[:, recent[recent].index] = True
    active = near & started & not_ended
    missing = (active & ~present).any(axis=1)
    total = sub.sum(axis=1, min_count=1)
    total[missing] = float("nan")
    return total


def build(rows: list[dict[str, Any]], config_path: Path = CONFIG_PATH) -> tuple[pd.DataFrame, dict[str, Any]]:
    labels, points = _config(config_path)
    df = pd.DataFrame(rows)
    df["key"] = df["operator"] + "|" + df["point"]
    df["gwh"] = df["kwh"] / 1e6
    wide = df.pivot_table(index="gas_day", columns="key", values="gwh", aggfunc="last")
    wide.index = pd.to_datetime(wide.index)
    wide = wide.asfreq("D")
    # Short reporting gaps (<= MAX_FILL_DAYS) inside a point's reporting span
    # are interpolated: one terminal skipping a day would otherwise blank the
    # whole route for that day. Longer gaps stay missing, and a gap at the end
    # of a series is never extrapolated (the newest days stay incomplete until
    # every operator has reported).
    wide = wide.interpolate(limit=MAX_FILL_DAYS, limit_area="inside")
    # A longer gap between two idle readings (< 1 GWh/d on both sides) is an
    # idle terminal that stopped reporting zeros, not missing data.
    prev = wide.ffill()
    nxt = wide.bfill()
    idle_gap = wide.isna() & (prev.abs() < 1.0) & (nxt.abs() < 1.0)
    wide = wide.mask(idle_gap, 0.0)

    out: dict[str, pd.Series] = {}
    report: dict[str, Any] = {"duplicates": {}, "routes": {}}
    for route in labels:
        keys = [p["key"] for p in points if p["route"] == route]
        dups = find_duplicates(wide, keys)
        report["duplicates"].update(dups)
        kept = [k for k in keys if k in wide and k not in dups]
        out[f"entsog_route_{route}"] = complete_sum(wide, kept)
        report["routes"][route] = kept
        if route == "lng":
            for cc in sorted({p["country"] for p in points if p["route"] == "lng"}):
                ccols = [p["key"] for p in points if p["route"] == "lng" and p["country"] == cc and p["key"] in kept]
                if ccols:
                    out[f"entsog_lng_{cc.lower()}"] = complete_sum(wide, ccols)

    routes = pd.DataFrame({k: v for k, v in out.items() if k.startswith("entsog_route_")})
    out["entsog_imports_total"] = routes.sum(axis=1, min_count=len(routes.columns)).where(routes.notna().all(axis=1))

    ingested_at = datetime.now(UTC).isoformat()
    names = {f"entsog_route_{r}": f"EU imports: {lab}" for r, lab in labels.items()}
    names["entsog_imports_total"] = "EU imports: all routes"
    recs = []
    for sid, s in out.items():
        for day, v in s.dropna().items():
            recs.append({
                "source": "entsog",
                "series_id": sid,
                "series_name": names.get(sid, f"EU LNG send-out: {sid.rsplit('_', 1)[-1].upper()}"),
                "period": day.strftime("%Y-%m-%d"),
                "value": round(float(v), 3),
                "unit": "GWh/d",
                "region": "EU",
                "ingested_at": ingested_at,
            })
    return pd.DataFrame(recs), report


def transform(raw_path: Path = RAW_PATH, curated_path: Path = CURATED_PATH) -> dict[str, Any]:
    if not raw_path.exists():
        raise TransformError(f"Raw file not found: {raw_path}")
    payload = json.loads(raw_path.read_text(encoding="utf-8"))
    if payload.get("batches_failed"):
        raise TransformError(f"refusing partial ENTSOG payload: {payload['batches_failed']}")
    rows = payload.get("data", [])
    if not rows:
        raise TransformError("ENTSOG raw payload has no rows")
    df, report = build(rows)
    if df.empty:
        raise TransformError("ENTSOG transformer produced zero rows")
    merged = merge_into_curated(df, curated_path)
    log.info("ENTSOG: %d rows, duplicates dropped: %s", len(merged), report["duplicates"])
    return {
        "rows": len(merged),
        "series": int(merged["series_id"].nunique()),
        "period_range": (merged["period"].min(), merged["period"].max()),
        "duplicates_dropped": report["duplicates"],
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
    print(json.dumps(transform(), indent=2, default=str))
