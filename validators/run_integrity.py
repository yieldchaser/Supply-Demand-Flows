"""CLI runner wiring validators/integrity.py to disk, clock, and Telegram.

Why:
    ``validators/integrity.py`` is pure by design — zero I/O, injected ``now``
    and priors.  Something has to load ``config/integrity_rules.yaml``, feed
    each curated parquet through the check suite, persist the verdict, and
    page a human when the dataset rots while the scraper smiles.  This module
    is that something.

What:
    ``python -m validators.run_integrity [--config PATH] [--state PATH]
    [--report PATH] [--alert]`` audits every source stanza in config order,
    writes ``data/health/_integrity_report.json`` and
    ``_integrity_state.json`` atomically, prints a fixed-width table, and —
    with ``--alert`` — dispatches the WARN/FAIL digest via
    ``publishers.alerts.send_integrity_alert_if_needed``.  Exit code is 1
    when any source ends FAIL, else 0, so GHA gates on it directly.

Failure modes:
    * Parquet absent → one ``availability`` SKIPPED entry, no crash, and the
      prior baseline is carried through untouched (nothing was measured, so
      nothing may be overwritten).
    * CRITICAL caller contract (see ``build_source_state``): a source whose
      shrinkage check FAILED keeps its OLD baseline in state — recovery
      visibility depends on the pre-corruption numbers surviving.
    * Health JSON missing/corrupt → ``None`` (checks degrade to SKIPPED).
    * ``--alert`` is opt-in to loud failure: any ``publishers.alerts.AlertError``
      (missing Telegram env, unreadable report) propagates out of ``main`` so
      the GHA step fails visibly instead of paging nobody.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from scrapers.base.safe_writer import safe_write_json
from validators.integrity import (
    CheckResult,
    build_source_state,
    load_state,
    run_source_checks,
)

log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/integrity_rules.yaml")
DEFAULT_STATE_PATH = Path("data/health/_integrity_state.json")
DEFAULT_REPORT_PATH = Path("data/health/_integrity_report.json")

_SEVERITY_RANK: dict[str, int] = {"SKIPPED": 0, "PASS": 1, "WARN": 2, "FAIL": 3}


def load_config(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Split the YAML rules file into ``(defaults, sources)`` preserving order."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        msg = f"integrity config {path} is not a mapping"
        raise ValueError(msg)
    defaults: dict[str, Any] = dict(raw.get("defaults") or {})
    sources: dict[str, Any] = dict(raw.get("sources") or {})
    return defaults, sources


def _load_health(raw_path: Any) -> dict[str, Any] | None:
    """Read a scraper health stamp; missing/unreadable payloads become ``None``."""
    if not raw_path:
        return None
    path = Path(str(raw_path))
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("health file %s unreadable (%s) — ignoring", path, exc)
        return None
    return payload if isinstance(payload, dict) else None


def _availability_skipped() -> CheckResult:
    return {
        "check": "availability",
        "severity": "SKIPPED",
        "message": "parquet absent (source not live yet)",
        "details": {},
    }


def _next_consecutive_flat(prior: dict[str, Any], rows_now: int) -> int:
    """Bookkeeping for the divergence flat arm (see ``check_divergence``)."""
    prior_rows = prior.get("rows")
    if isinstance(prior_rows, int) and prior_rows == rows_now:
        return int(prior.get("consecutive_flat", 0)) + 1
    return 0


def build_report(
    defaults: dict[str, Any],
    sources: dict[str, Any],
    prior_state: dict[str, Any],
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Audit every source stanza; return ``(report, next_state)``.

    State policy: sources with an absent parquet or a shrinkage FAIL carry
    their prior baseline forward unchanged; every other source receives a
    fresh ``build_source_state`` snapshot augmented with the
    ``consecutive_flat`` counter the divergence flat arm reads next run.
    """
    entries: dict[str, Any] = {}
    next_state: dict[str, Any] = {}

    for key, raw_cfg in sources.items():
        src_cfg: dict[str, Any] = dict(raw_cfg) if isinstance(raw_cfg, dict) else {}
        raw_prior = prior_state.get(key)
        prior: dict[str, Any] = dict(raw_prior) if isinstance(raw_prior, dict) else {}

        parquet = Path(str(src_cfg.get("parquet", "")))
        if not parquet.exists():
            log.info("integrity[%s]: parquet %s absent — source not audited", key, parquet)
            entries[key] = {
                "overall": "SKIPPED",
                "results": [_availability_skipped()],
                "stats": {"rows": None, "days": None, "latest": None},
            }
            if prior:
                next_state[key] = dict(prior)
            continue

        df: pd.DataFrame = pd.read_parquet(parquet)
        results, overall = run_source_checks(
            key,
            df,
            src_cfg,
            defaults,
            prior or None,
            now,
            _load_health(src_cfg.get("health_file")),
        )

        has_periods = "period" in df.columns and not df.empty
        entries[key] = {
            "overall": overall,
            "results": results,
            "stats": {
                "rows": int(len(df)),
                "days": int(df["period"].nunique()) if has_periods else 0,
                "latest": str(df["period"].max()) if has_periods else "",
            },
        }

        shrunk = any(r["check"] == "shrinkage" and r["severity"] == "FAIL" for r in results)
        if shrunk:
            log.warning("integrity[%s]: shrinkage FAILED — prior baseline kept: %s", key, prior)
            next_state[key] = dict(prior)
        else:
            fresh = build_source_state(df, now)
            fresh["consecutive_flat"] = _next_consecutive_flat(prior, int(len(df)))
            next_state[key] = fresh

    worst_overall = ""
    for entry in entries.values():
        sev = str(entry["overall"])
        if not worst_overall or _SEVERITY_RANK.get(sev, 0) > _SEVERITY_RANK.get(worst_overall, 0):
            worst_overall = sev
    report = {
        "generated_at": now.isoformat(),
        "overall": worst_overall or "PASS",
        "sources": entries,
    }
    return report, next_state


def _cell(value: Any, width: int, *, left: bool = False) -> str:
    text = "-" if value in (None, "") else str(value)
    return f"{text:<{width}}" if left else f"{text:>{width}}"


def render_table(report: dict[str, Any]) -> str:
    """Fixed-width console summary: source | rows | days | latest | status | findings.

    Findings list only WARN/FAIL checks — SKIPPED/PASS are noise — so an
    all-clear row shows ``-``.
    """
    lines = [
        f"{'source':<22} {'rows':>9} {'days':>6} {'latest':<12} {'status':<8} findings",
        "-" * 74,
    ]
    sources: dict[str, Any] = report.get("sources") or {}
    for key, entry in sources.items():
        stats: dict[str, Any] = entry.get("stats") or {}
        findings = ",".join(
            str(res.get("check"))
            for res in entry.get("results") or []
            if isinstance(res, dict) and res.get("severity") in ("WARN", "FAIL")
        )
        lines.append(
            f"{str(key):<22} {_cell(stats.get('rows'), 9)} {_cell(stats.get('days'), 6)} "
            f"{_cell(stats.get('latest'), 12, left=True)} {_cell(entry.get('overall'), 8, left=True)} "
            f"{findings or '-'}"
        )
    return "\n".join(lines)


def _maybe_alert(report_path: Path) -> None:
    """Dispatch the WARN/FAIL digest; AlertError propagates (--alert opted in)."""
    from publishers.alerts import send_integrity_alert_if_needed

    sent = send_integrity_alert_if_needed(report_path)
    print(f"alert: {'SENT' if sent else 'NOT-SENT (all-clear or already sent today)'}")


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="validators.run_integrity",
        description="Audit curated parquets for silent dataset degradation.",
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument(
        "--alert",
        action="store_true",
        help="send the Telegram digest when WARN/FAIL findings exist",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the audit; return 1 when any source verdict is FAIL, else 0."""
    args = _parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    now = datetime.now(UTC)
    defaults, sources = load_config(args.config)
    report, next_state = build_report(defaults, sources, load_state(args.state), now)

    safe_write_json(args.report, report)
    safe_write_json(args.state, next_state)

    print(render_table(report))
    print()
    print(f"OVERALL: {report['overall']} (generated_at={report['generated_at']})")

    if args.alert:
        _maybe_alert(args.report)
    return 1 if report["overall"] == "FAIL" else 0


if __name__ == "__main__":
    sys.exit(main())
