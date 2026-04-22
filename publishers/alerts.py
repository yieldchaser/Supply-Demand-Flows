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
        return json.loads(SENT_ALERTS_PATH.read_text(encoding="utf-8"))
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


def _post_telegram(token: str, chat_id: str, html_body: str) -> dict:
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
    return resp.json()


def build_health_prefix() -> str:
    """
    Scan data/health/*.json (excluding .prev). If any source has status='failed'
    since the last success, return a prefix block. Otherwise empty string.
    """
    if not HEALTH_DIR.exists():
        return ""
    failures: list[str] = []
    for health_file in sorted(HEALTH_DIR.glob("*.json")):
        if ".prev" in health_file.stem:
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
