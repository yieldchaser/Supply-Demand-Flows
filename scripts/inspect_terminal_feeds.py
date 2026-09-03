"""Inspect all LNG terminals from registry, derive feed IDs, and query curated parquets."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
REGISTRY_JS = REPO_ROOT / "docs" / "js" / "util" / "lng-terminals.js"
CURATED_DIR = REPO_ROOT / "data" / "curated"

# Prefix map including V2 additions
PREFIX_MAP = {
    "gulf_south": "gulf_south",
    "tetco": "enbridge",
    "cpl": "bhe",
    "creole_trail": "cheniere",
    "enbridge": "enbridge",
    "kinder_morgan": "kinder_morgan",
    "km": "kinder_morgan",
    "gator_express": "quorum",
    "trans_cameron": "quorum",
    "golden_pass": "gasnom",
    "cameron_interstate": "gasnom",
    "corpus_christi": "cheniere",
}


def resolve_parquet(feed_id: str) -> Path | None:
    """Resolve feed_id to curated parquet file path."""
    for prefix, mapped in PREFIX_MAP.items():
        if feed_id.startswith(prefix + "_"):
            p = CURATED_DIR / f"{mapped}.parquet"
            if p.exists():
                return p
    return None


def parse_registry_terminals() -> list[dict[str, Any]]:
    """Extract terminal metadata and feeds from lng-terminals.js."""
    content = REGISTRY_JS.read_text(encoding="utf-8")
    term_re = re.compile(r"^\s{2}(\w+):\s*\{", re.MULTILINE)
    terminals = []

    for m in term_re.finditer(content):
        term_key = m.group(1)
        start = m.end()
        depth = 1
        i = start
        while i < len(content) and depth > 0:
            if content[i] == "{":
                depth += 1
            elif content[i] == "}":
                depth -= 1
            i += 1
        block = content[m.start() : i]

        operational = "operational: false" not in block
        display_m = re.search(r"display:\s*'([^']+)'", block)
        display = display_m.group(1) if display_m else term_key

        feeds = []
        # Multi-feed block
        if "feeds:" in block:
            # Match feed objects
            feed_matches = re.finditer(r"\{\s*source:\s*'([^']+)',\s*series:\s*'([^']+)'(?:[^}]+)?\}", block)
            for fm in feed_matches:
                f_block = fm.group(0)
                series = fm.group(2)
                kind_m = re.search(r"kind:\s*'([^']+)'", f_block)
                kind = kind_m.group(1) if kind_m else "measured"
                # Exclude context and comparison feeds from headline summation
                if kind in ("context", "comparison"):
                    continue
                feeds.append(series)
        else:
            # Single-source terminal
            prefix_m = re.search(r"seriesPrefix:\s*'([^']+)'", block)
            loc_m = re.search(r"loc:\s*'([^']+)'", block)
            flow_m = re.search(r"flow:\s*'([^']+)'", block)
            if prefix_m and loc_m and flow_m:
                feed_id = f"{prefix_m.group(1)}_sq_{loc_m.group(1)}_{flow_m.group(1)}"
                feeds.append(feed_id)

        terminals.append({
            "key": term_key,
            "display": display,
            "operational": operational,
            "feeds": feeds,
        })

    return terminals


def inspect_all() -> list[dict[str, Any]]:
    terminals = parse_registry_terminals()
    results = []

    for term in terminals:
        for feed_id in term["feeds"]:
            parquet_path = resolve_parquet(feed_id)
            if not parquet_path or not parquet_path.exists():
                results.append({
                    "terminal": term["key"],
                    "display": term["display"],
                    "operational": term["operational"],
                    "feed_id": feed_id,
                    "parquet": parquet_path.name if parquet_path else "None",
                    "rows": 0,
                    "period_range": "N/A",
                    "cycles": [],
                })
                continue

            df = pd.read_parquet(parquet_path)
            mask = df["series_id"].isin([feed_id]) | df["series_id"].str.startswith(f"{feed_id}_")
            sub = df[mask]
            rows = len(sub)
            if rows == 0:
                results.append({
                    "terminal": term["key"],
                    "display": term["display"],
                    "operational": term["operational"],
                    "feed_id": feed_id,
                    "parquet": parquet_path.name,
                    "rows": 0,
                    "period_range": "N/A",
                    "cycles": [],
                })
                continue

            periods = sub["period"].dropna().astype(str)
            min_p = periods.min()
            max_p = periods.max()

            cycles = sorted(
                set(
                    sid[len(feed_id) + 1 :].lower()
                    for sid in sub["series_id"]
                    if len(sid) > len(feed_id)
                )
            )

            results.append({
                "terminal": term["key"],
                "display": term["display"],
                "operational": term["operational"],
                "feed_id": feed_id,
                "parquet": parquet_path.name,
                "rows": rows,
                "period_range": f"{min_p} -> {max_p}",
                "cycles": cycles,
            })

    return results


if __name__ == "__main__":
    res = inspect_all()
    print("| Terminal | Feed ID | Curated Parquet | Matching Rows | Period Range | Distinct Cycles |")
    print("|---|---|---|---|---|---|")
    for r in res:
        c_str = ", ".join(r["cycles"]) if r["cycles"] else "(none)"
        print(f"| {r['display']} (`{r['terminal']}`) | `{r['feed_id']}` | `{r['parquet']}` | {r['rows']} | {r['period_range']} | {c_str} |")
