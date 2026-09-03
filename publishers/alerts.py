"""
Telegram alert dispatcher with idempotency.

Why: we send alerts from multiple workflows (scrapers, transformers, events)
and must never duplicate. Each message carries a deterministic dedup_key
(e.g., 'storage_print_2026-04-17'). The key + timestamp are recorded in
data/sent_alerts.json. Before sending, we check if the key already exists
and was sent within the dedup TTL.

What: send_alert(dedup_key, html_body) returns True if sent, False if
deduplicated. Also provides build_health_prefix() which scans data/health/
and returns the "🚨 SYSTEM HEALTH ALERTS 🚨" prefix if any source is in
a failed state.

Failure modes:
- Telegram API returns 4xx/5xx → raise AlertError, do NOT record in sent_alerts
  (so retry on next run can succeed)
- sent_alerts.json corrupt/missing → treat as empty, continue
- env vars missing → raise AlertError early (caller decides whether to
  suppress or bubble up)
"""

from __future__ import annotations

import html
import json
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from scrapers.base.safe_writer import safe_write_json

logger = logging.getLogger(__name__)

SENT_ALERTS_PATH = Path("data/sent_alerts.json")
HEALTH_DIR = Path("data/health")
DEFAULT_DEDUP_TTL = timedelta(days=7)
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


class AlertError(Exception):
    """Failure to dispatch a Telegram alert."""


def _load_sent_alerts() -> dict[str, str]:
    """Load sent alerts map {dedup_key: iso_timestamp}. Missing/corrupt → empty."""
    if not SENT_ALERTS_PATH.exists():
        return {}
    try:
        loaded: dict[str, str] = json.loads(SENT_ALERTS_PATH.read_text(encoding="utf-8"))
        return loaded
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(f"sent_alerts.json unreadable ({exc}), treating as empty")
        return {}


def _save_sent_alerts(data: dict[str, str]) -> None:
    safe_write_json(SENT_ALERTS_PATH, data)


def _is_duplicate(dedup_key: str, ttl: timedelta) -> bool:
    sent = _load_sent_alerts()
    ts = sent.get(dedup_key)
    if ts is None:
        return False
    try:
        sent_at = datetime.fromisoformat(ts)
    except ValueError:
        return False
    now = datetime.now(UTC)
    return (now - sent_at) < ttl


def _record_sent(dedup_key: str) -> None:
    sent = _load_sent_alerts()
    sent[dedup_key] = datetime.now(UTC).isoformat()
    # Prune entries older than 30 days to keep file lean
    cutoff = datetime.now(UTC) - timedelta(days=30)
    sent = {k: v for k, v in sent.items() if datetime.fromisoformat(v) >= cutoff}
    _save_sent_alerts(sent)


def _post_telegram(token: str, chat_id: str, html_body: str) -> dict[str, Any]:
    url = TELEGRAM_API.format(token=token)
    payload = {
        "chat_id": chat_id,
        "text": html_body,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        resp = httpx.post(url, json=payload, timeout=15.0)
    except httpx.HTTPError as exc:
        raise AlertError(f"Telegram HTTP error: {exc}") from exc
    if resp.status_code >= 400:
        raise AlertError(f"Telegram API returned {resp.status_code}: {resp.text[:500]}")
    decoded: dict[str, Any] = resp.json()
    return decoded


def build_health_prefix() -> str:
    """
    Scan data/health/*.json (excluding .prev and internal ``_*.json``
    integrity artifacts). If any source has status='failed' since the last
    success, return a prefix block. Otherwise empty string.
    """
    if not HEALTH_DIR.exists():
        return ""
    failures: list[str] = []
    for health_file in sorted(HEALTH_DIR.glob("*.json")):
        if health_file.stem.startswith("_") or ".prev" in health_file.stem:
            continue
        try:
            data = json.loads(health_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("status") == "failed":
            source = html.escape(data.get("source", health_file.stem))
            err = html.escape(str(data.get("error", "unknown"))[:200])
            failures.append(f"  • <b>{source}</b>: <code>{err}</code>")
    if not failures:
        return ""
    header = "🚨 <b>SYSTEM HEALTH ALERTS</b> 🚨\n"
    return header + "\n".join(failures) + "\n\n────────\n\n"


def send_alert(
    dedup_key: str,
    html_body: str,
    include_health_prefix: bool = True,
    dedup_ttl: timedelta = DEFAULT_DEDUP_TTL,
) -> bool:
    """
    Why: centralized Telegram send with idempotency + health context.
    Returns True if sent, False if deduplicated.
    Raises AlertError on API failure (so GHA step fails loudly).
    """
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise AlertError("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set")

    if _is_duplicate(dedup_key, dedup_ttl):
        logger.info(f"Alert '{dedup_key}' deduplicated (within {dedup_ttl})")
        return False

    body = html_body
    if include_health_prefix:
        prefix = build_health_prefix()
        if prefix:
            body = prefix + body

    _post_telegram(token, chat_id, body)
    _record_sent(dedup_key)
    logger.info(f"Sent alert: {dedup_key}")
    return True


def send_health_only_if_failing() -> bool:
    """
    For health-watchdog workflow: only sends if there's an actual failure.
    Returns True if an alert was dispatched.
    """
    prefix = build_health_prefix()
    if not prefix:
        return False
    dedup = f"health_snapshot_{datetime.now(UTC).strftime('%Y-%m-%d-%H')}"
    return send_alert(dedup, prefix, include_health_prefix=False, dedup_ttl=timedelta(hours=1))


INTEGRITY_ALERT_SEVERITIES: frozenset[str] = frozenset({"WARN", "FAIL"})


def _integrity_finding_results(results: Any) -> list[dict[str, Any]]:
    """Extract WARN/FAIL results from one report entry; SKIPPED/PASS are noise."""
    return [
        res
        for res in results or []
        if isinstance(res, dict) and res.get("severity") in INTEGRITY_ALERT_SEVERITIES
    ]


def format_integrity_findings(report: dict[str, Any]) -> str:
    """
    Render the Telegram integrity digest: one row per source in report order.

    Mirrors the console table's findings column (validators.run_integrity.
    render_table): each source lists only its WARN/FAIL checks; sources with
    none show ``-`` so all-clear rows carry no SKIPPED noise. WARN/FAIL check
    messages follow their source line — they embed the deciding numbers.
    """
    overall = html.escape(str(report.get("overall", "?")))
    generated_at = html.escape(str(report.get("generated_at", "")))
    lines = [
        "<b>DATASET INTEGRITY DIGEST</b>",
        f"overall <b>{overall}</b> at {generated_at}",
        "",
    ]
    sources = report.get("sources") or {}
    for key, raw_entry in sources.items():
        entry = raw_entry if isinstance(raw_entry, dict) else {}
        name = html.escape(str(key))
        status = html.escape(str(entry.get("overall", "?")))
        findings = _integrity_finding_results(entry.get("results"))
        if findings:
            checks = ", ".join(str(res.get("check", "?")) for res in findings)
            detail_lines = [
                f"  • <i>{html.escape(str(res.get('check', '?')))}</i>: "
                f"{html.escape(str(res.get('message', ''))[:300])}"
                for res in findings
            ]
        else:
            checks = "-"
            detail_lines = []
        lines.append(f"<b>{name}</b> [{status}] — {html.escape(checks)}")
        lines.extend(detail_lines)
    return "\n".join(lines)


def send_integrity_alert_if_needed(report_path: Path | str) -> bool:
    """
    Dispatch the dataset-integrity digest once per day when WARN/FAIL exist.

    Reads the report written by validators.run_integrity. All-clear (every
    source PASS/SKIPPED) → False, nothing sent. Otherwise formats via
    format_integrity_findings and sends under a date-scoped dedup key, so a
    single day can never page twice. Raises AlertError on an unreadable
    report so the caller can print the reason instead of guessing.
    """
    try:
        payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AlertError(f"integrity report unreadable: {exc}") from exc
    if not isinstance(payload, dict):
        raise AlertError(f"integrity report {report_path} is not a JSON object")

    sources = payload.get("sources") or {}
    has_findings = any(
        isinstance(entry, dict) and str(entry.get("overall")) in INTEGRITY_ALERT_SEVERITIES
        for entry in sources.values()
    )
    if not has_findings:
        logger.info("integrity digest suppressed: no WARN/FAIL findings")
        return False

    generated_at = str(payload.get("generated_at") or "")
    day_key = generated_at[:10] or datetime.now(UTC).date().isoformat()
    dedup_key = f"integrity_digest_{day_key}"
    body = format_integrity_findings(payload)
    return send_alert(dedup_key, body, include_health_prefix=False, dedup_ttl=timedelta(days=1))


def format_feedgas_alert(
    terminal: str,
    event_type: str,
    gas_day: str,
    duration: int,
    flow_mmcf: float,
    baseline_mmcf: float,
    detail: str = "",
) -> str:
    """
    Format Telegram HTML alert for an LNG terminal downtime or feedgas drop event.
    """
    icon = "🔴" if event_type == "OFFLINE" else ("🟠" if event_type == "DEPRESSED" else "⚠️")
    pct_drop = ((baseline_mmcf - flow_mmcf) / baseline_mmcf * 100.0) if baseline_mmcf > 0 else 0.0
    lines = [
        f"{icon} <b>LNG TERMINAL ALERT: {html.escape(terminal.upper())}</b>",
        f"Event: <b>{html.escape(event_type)}</b>",
        f"Gas Day: <code>{html.escape(gas_day)}</code>" + (f" (duration: {duration}d)" if duration > 1 else ""),
        f"Flow: <b>{flow_mmcf:,.1f} MMcf/d</b> (baseline: {baseline_mmcf:,.1f} MMcf/d, drop: {pct_drop:.1f}%)",
    ]
    if detail:
        lines.append(f"Detail: <i>{html.escape(detail)}</i>")
    return "\n".join(lines)


def send_feedgas_alerts_if_needed(dry_run: bool = False) -> list[dict[str, Any]]:
    """
    Check configured LNG terminals for active OFFLINE/DEPRESSED events or >40% baseline drops.

    Why:
        Analytical alert bridge for Section 8 LNG downtime. Baseload LNG facilities
        routinely swing 10-25% from cargo loading and ambient cycles. A 40% drop
        (flow < 60% of trailing 30-day baseline) signals an acute restriction or outage
        requiring operational notification without false-alarm fatigue.

    Returns:
        List of alert dictionaries. If dry_run=True, prints message bodies without sending.
    """
    import pandas as pd

    from scripts.task3_validate import TERMINALS, detect_events, load_terminal_history

    alerts: list[dict[str, Any]] = []
    for term_key, conf in TERMINALS.items():
        try:
            hist, _ = load_terminal_history(term_key)
        except Exception as exc:
            logger.warning(f"Could not load history for {term_key}: {exc}")
            continue
        if not hist:
            continue

        events = detect_events(hist, conf)
        sorted_dates = sorted(hist.keys())
        latest_date = sorted_dates[-1]
        latest_hist = hist[latest_date]
        latest_flow_mmcf = latest_hist["value"] / 1.025 / 1000.0

        # Calculate 30-day baseline median in MMcf/d
        trailing_30 = [
            hist[d]["value"] / 1.025 / 1000.0
            for d in sorted_dates[-31:-1]
            if hist[d]["value"] > 0
        ]
        baseline_mmcf = float(pd.Series(trailing_30).median()) if trailing_30 else latest_flow_mmcf

        # Check for active downtime event (within last 3 days)
        for ev in events:
            if ev["type"] in ("OFFLINE", "DEPRESSED"):
                days_ago = (datetime.fromisoformat(latest_date) - datetime.fromisoformat(ev["date"])).days
                if 0 <= days_ago <= 3:
                    dedup_key = f"feedgas_{term_key}_{ev['type']}_{ev['date']}"
                    body = format_feedgas_alert(
                        terminal=conf["name"],
                        event_type=ev["type"],
                        gas_day=ev["date"],
                        duration=ev["duration"],
                        flow_mmcf=latest_flow_mmcf,
                        baseline_mmcf=baseline_mmcf,
                        detail=ev.get("detail", ""),
                    )
                    payload = {"dedup_key": dedup_key, "html_body": body, "terminal": term_key}
                    alerts.append(payload)
                    if dry_run:
                        print(f"--- [DRY RUN ALERT: {dedup_key}] ---")
                        print(body)
                        print("-" * 40)
                    else:
                        has_creds = bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))
                        if not has_creds:
                            logger.info("Telegram credentials not configured; cleanly skipping alert dispatch")
                        else:
                            send_alert(dedup_key, body, dedup_ttl=timedelta(days=7))

        # Check for acute single-day drop >= 40% against baseline
        if baseline_mmcf > 100 and latest_flow_mmcf > 0:
            drop_pct = (baseline_mmcf - latest_flow_mmcf) / baseline_mmcf
            if drop_pct >= 0.40:
                dedup_key = f"feedgas_{term_key}_acute_drop"
                body = format_feedgas_alert(
                    terminal=conf["name"],
                    event_type="ACUTE_DROP",
                    gas_day=latest_date,
                    duration=1,
                    flow_mmcf=latest_flow_mmcf,
                    baseline_mmcf=baseline_mmcf,
                    detail=f"Single-day supply restriction: {drop_pct:.1%} drop against 30-day baseline",
                )
                payload = {"dedup_key": dedup_key, "html_body": body, "terminal": term_key}
                alerts.append(payload)
                if dry_run:
                    print(f"--- [DRY RUN ALERT: {dedup_key}] ---")
                    print(body)
                    print("-" * 40)
                else:
                    has_creds = bool(os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID"))
                    if not has_creds:
                        logger.info("Telegram credentials not configured; cleanly skipping alert dispatch")
                    else:
                        send_alert(dedup_key, body, dedup_ttl=timedelta(days=7))

    return alerts



if __name__ == "__main__":
    import sys

    # Smoke-test helper: send a test message
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Missing TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID", file=sys.stderr)
        sys.exit(1)
    body = "<b>Blue Tide smoke test</b>\nPipeline alive at " + datetime.now(UTC).isoformat()
    try:
        sent = send_alert("smoke_test_" + datetime.now(UTC).strftime("%Y%m%d%H%M"), body)
        print(f"Sent: {sent}")
    except AlertError as e:
        print(f"Failed to send alert: {e}", file=sys.stderr)
        sys.exit(1)
