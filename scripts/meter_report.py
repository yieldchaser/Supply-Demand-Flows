"""Blue Tide meter classification report generator.

Reads config/meters/classification.json (produced by scripts/classify_meters.py)
and renders analysis/meter_classification_report.md: per-source class/confidence
counts, the full storage inventory, the full basin_egress inventory, the top 25
meters by mean volume, high-volume unknowns for manual review, and the
lng_export cross-check against the existing config/meters/*.json seed maps.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
CLASSIFICATION = REPO_ROOT / "config" / "meters" / "classification.json"
REPORT = REPO_ROOT / "analysis" / "meter_classification_report.md"
SOURCES = ("gulf_south", "gasnom", "quorum", "bhe", "cheniere")

# Expected lng_export loc ids from the existing seed configs.
SEED_EXPECTATIONS: dict[str, set[str]] = {
    "gulf_south": {"24329"},          # Stratton Ridge -> Freeport LNG
    "gasnom": {"1097217"},            # Golden Pass Terminal
    "bhe": {"40704"},                 # EGTS Loudoun -> Cove Point
    # quorum.json lists VGPQD as THE feedgas meter; VGCPD is a sibling
    # Venture Global delivery (Calcasieu Pass).
    "quorum": {"vgpqd"},
    # cheniere.json lists CT200111 + CC100221 as its two seeds.
    "cheniere": {"CT200111", "CC100221"},
}


def _fmt(v: float | None) -> str:
    if v is None:
        return "-"
    return f"{v:,.0f}"


def main() -> None:
    """Render analysis/meter_classification_report.md."""
    doc: dict[str, Any] = json.loads(CLASSIFICATION.read_text(encoding="utf-8"))
    meta = doc["_meta"]
    lines: list[str] = []

    lines.append("# Blue Tide Meter Classification Report")
    lines.append("")
    lines.append(f"*Generated:* `{meta['generated']}` — deterministic output of "
                 "`scripts/classify_meters.py` over `data/curated/*.parquet`.")
    lines.append("")
    lines.append(f"**{meta['total_meters']} meters classified across {len(SOURCES)} EBB sources.** "
                 "One primary class and one confidence per meter; evidence strings are "
                 "carried in `config/meters/classification.json`.")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## Class distribution")
    lines.append("")
    lines.append("| Source | " + " | ".join(k for k in meta["by_class"]) + " | Total |")
    header_classes = list(meta["by_class"].keys())
    lines.append("|---|" + "---|" * (len(header_classes) + 1))
    for src in SOURCES:
        counts = Counter(m["class"] for m in doc[src].values())
        cells = [str(counts.get(c, 0)) for c in header_classes]
        lines.append(f"| **{src}** | " + " | ".join(cells) + f" | {len(doc[src])} |")
    totals = ["**" + str(v) + "**" for v in meta["by_class"].values()]
    lines.append("| **all** | " + " | ".join(totals) + f" | **{meta['total_meters']}** |")
    lines.append("")
    lines.append("## Confidence distribution")
    lines.append("")
    conf_header = list(meta["by_confidence"].keys())
    lines.append("| Source | " + " | ".join(conf_header) + " |")
    lines.append("|---|" + "---|" * len(conf_header))
    for src in SOURCES:
        counts = Counter(m["confidence"] for m in doc[src].values())
        lines.append(f"| **{src}** | "
                     + " | ".join(str(counts.get(c, 0)) for c in conf_header) + " |")
    lines.append("| **all** | "
                 + " | ".join("**" + str(v) + "**" for v in meta["by_confidence"].values())
                 + " |")
    lines.append("")

    def table(rows: list[tuple[str, str, dict[str, Any]]], title: str, note: str) -> None:
        """Render one inventory section."""
        if not rows:
            lines.append(f"## {title}")
            lines.append("")
            lines.append("_None found._")
            lines.append("")
            return
        lines.append(f"## {title}")
        lines.append("")
        lines.append(note)
        lines.append("")
        lines.append("| Source | Loc | Name | Flow | Mean Dth/d | Max Dth/d | Zero % | Conf | Facility/Operator/Capacity | Basin |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|")
        for src, loc, m in rows:
            st = m["stats"]
            attrs = m["attrs"]
            facility = attrs.get("facility") or "-"
            operator = attrs.get("operator") or ""
            bcf = attrs.get("working_gas_bcf")
            fac_cell = facility if operator == "" else f"{facility} ({operator})"
            if bcf is not None:
                fac_cell += f" · {bcf} Bcf WG · {attrs.get('net_or_gross')}"
            basin = attrs.get("basin") or "-"
            lines.append(
                f"| {src} | {loc} | {m['loc_name'][:46]} | {m['flow_ind']} "
                f"| {_fmt(st['mean_dth'])} | {_fmt(st['max_dth'])} "
                f"| {st['zero_frac']*100:.0f}% | {m['confidence']} | {fac_cell} | {basin} |"
            )
        lines.append("")

    storage_rows: list[tuple[str, str, dict[str, Any]]] = []
    egress_rows: list[tuple[str, str, dict[str, Any]]] = []
    all_rows: list[tuple[float, str, str, dict[str, Any]]] = []
    unknowns: list[tuple[float, str, str, dict[str, Any]]] = []
    for src in SOURCES:
        for loc, m in doc[src].items():
            row = (src, loc, m)
            mean = float(m["stats"]["mean_dth"])
            all_rows.append((mean, src, loc, m))
            if m["class"] == "storage":
                storage_rows.append(row)
            elif m["class"] == "basin_egress":
                egress_rows.append(row)
            elif m["class"] == "unknown" and mean > 100_000:
                unknowns.append((mean, src, loc, m))

    storage_rows.sort(key=lambda r: -float(r[2]["stats"]["mean_dth"]))
    egress_rows.sort(key=lambda r: -float(r[2]["stats"]["mean_dth"]))
    table(
        storage_rows,
        f"Storage inventory ({len(storage_rows)} meters)",
        "Feeds the EIA weekly-print nowcast. Capacity/operator where researchable "
        "(citations inline in classification.json evidence). Petal/Bistineau/Jackson are "
        "Gulf South's own fields; the '(Petal ...)' suffixed points are lateral "
        "counterparty points at those facilities.",
    )
    table(
        egress_rows,
        f"Basin-egress inventory ({len(egress_rows)} meters)",
        "Haynesville-attributed takeaways and producer/gatherer receipts. All researched "
        "high-confidence entries cite public sources; gatherer-name matches are medium.",
    )

    # ------------------------------------------------------------------
    lines.append("## Top 25 meters by mean scheduled volume")
    lines.append("")
    lines.append("| Mean Dth/d | Source | Loc | Name | Class | Conf |")
    lines.append("|---|---|---|---|---|---|")
    for mean, src, loc, m in sorted(all_rows, reverse=True)[:25]:
        lines.append(
            f"| {mean:,.0f} | {src} | {loc} | {m['loc_name'][:44]} "
            f"| {m['class']} | {m['confidence']} |"
        )
    lines.append("")

    lines.append(f"## High-volume unknowns needing manual review ({len(unknowns)})")
    lines.append("")
    lines.append("Mean > 100,000 Dth/d with no rule/research coverage. These carry real ")
    lines.append("analytical weight and deserve a human pass:")
    lines.append("")
    lines.append("| Mean Dth/d | Source | Loc | Name | Current evidence |")
    lines.append("|---|---|---|---|---|")
    for mean, src, loc, m in sorted(unknowns, reverse=True):
        first_ev = next((e for e in m["evidence"] if not e.startswith("[researched]")), "")
        lines.append(f"| {mean:,.0f} | {src} | {loc} | {m['loc_name'][:42]} | {first_ev[:70]} |")
    lines.append("")

    # ------------------------------------------------------------------
    lines.append("## lng_export cross-check vs existing seed maps")
    lines.append("")
    lines.append("Sum of mean TSQ over meters classified lng_export, compared against the ")
    lines.append("high-confidence entries in the pre-existing `config/meters/{gulf_south → ")
    lines.append("lng_meter_map.json}, gasnom.json, quorum.json, bhe.json, cheniere.json`:")
    lines.append("")
    lines.append("| Source | n lng_export | Σ mean Dth/d (high conf) | Σ mean Dth/d (all) | Seed-map agreement |")
    lines.append("|---|---|---|---|---|")
    findings: list[str] = []
    for src in SOURCES:
        members = [(loc, m) for loc, m in doc[src].items() if m["class"] == "lng_export"]
        high_sum = sum(float(m["stats"]["mean_dth"]) for _, m in members if m["confidence"] == "high")
        all_sum = sum(float(m["stats"]["mean_dth"]) for _, m in members)
        expected = SEED_EXPECTATIONS[src]
        got = {loc for loc, _ in members}
        missing = expected - got
        extra = got - expected
        agree = "✅ superset of seeds"
        if missing:
            agree = f"⚠️ MISSING {sorted(missing)}"
            findings.append(f"{src}: seed map expects {sorted(missing)} as LNG but classifier disagrees")
        elif extra:
            agree = f"➕ extends seeds by {len(extra)}"
        lines.append(
            f"| {src} | {len(members)} | {high_sum:,.0f} | {all_sum:,.0f} | {agree} |"
        )
    lines.append("")

    if findings:
        lines.append("**Findings / disagreements:**")
        lines.append("")
        for f in findings:
            lines.append(f"- {f}")
        lines.append("")

    lines.append("Notes on intentional extensions beyond the seed maps (each carries cited ")
    lines.append("evidence in classification.json): Cameron Interstate's TENN-CIP/TETCO-CIP/")
    lines.append("CIP receipts and Gulf Run (Golden Pass supply), TransCameron (Cameron LNG), ")
    lines.append("the Coastal Bend Header trio (Freeport LNG), Gator Express TGP/TETCO/CGT ")
    lines.append("(Plaquemines), Cheniere CTPL/CCPL receipt sets, and Cove Point via EGTS. ")
    lines.append("The Cameron LNG (Rec) and Sabine Pass LNG Rec points are terminal return/")
    lines.append("placeholder legs that post zero; they classify as lng_export by name but ")
    lines.append("carry no volume and must be excluded from feedgas sums.")
    lines.append("")

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {REPORT} ({len(lines)} lines)")


if __name__ == "__main__":
    main()
