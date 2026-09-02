"""Tests for Telegram alert dispatcher."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from publishers.alerts import (
    AlertError,
    build_health_prefix,
    format_feedgas_alert,
    format_integrity_findings,
    send_alert,
    send_feedgas_alerts_if_needed,
    send_integrity_alert_if_needed,
)


@pytest.fixture
def clean_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Setup mock data and health directories."""
    sent_alerts = tmp_path / "data" / "sent_alerts.json"
    health_dir = tmp_path / "data" / "health"

    sent_alerts.parent.mkdir(parents=True)
    health_dir.mkdir(parents=True)

    import publishers.alerts as alerts

    monkeypatch.setattr(alerts, "SENT_ALERTS_PATH", sent_alerts)
    monkeypatch.setattr(alerts, "HEALTH_DIR", health_dir)

    return {"sent_alerts": sent_alerts, "health_dir": health_dir}


def test_send_alert_deduplicates(clean_data, monkeypatch: pytest.MonkeyPatch):
    """Verify that alerts with the same key within TTL are not sent twice."""
    sent_alerts = clean_data["sent_alerts"]
    key = "test_dedup"
    now_iso = datetime.now(UTC).isoformat()
    sent_alerts.write_text(json.dumps({key: now_iso}), encoding="utf-8")

    # Mock environment variables
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "fake_id")

    # Mock httpx.post so it shouldn't even be called
    import httpx

    def mock_post(*args, **kwargs):
        pytest.fail("httpx.post should not be called for a duplicate alert")

    monkeypatch.setattr(httpx, "post", mock_post)

    sent = send_alert(key, "hello")
    assert sent is False


def test_send_alert_records_on_success(clean_data, monkeypatch: pytest.MonkeyPatch):
    """Verify that successful sends are recorded in sent_alerts.json."""
    sent_alerts = clean_data["sent_alerts"]
    key = "test_success"

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "fake_id")

    # Mock httpx.post success
    import httpx

    class MockResponse:
        status_code = 200

        def json(self):
            return {"ok": True}

    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: MockResponse())

    sent = send_alert(key, "hello")
    assert sent is True

    data = json.loads(sent_alerts.read_text(encoding="utf-8"))
    assert key in data


def test_send_alert_does_not_record_on_failure(clean_data, monkeypatch: pytest.MonkeyPatch):
    """Verify that failed sends are NOT recorded (to allow retry)."""
    sent_alerts = clean_data["sent_alerts"]
    key = "test_failure"

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "fake_id")

    # Mock httpx.post failure
    import httpx

    def mock_post_fail(*args, **kwargs):
        raise httpx.ConnectError("timeout")

    monkeypatch.setattr(httpx, "post", mock_post_fail)

    with pytest.raises(AlertError):
        send_alert(key, "hello")

    if sent_alerts.exists():
        data = json.loads(sent_alerts.read_text(encoding="utf-8"))
        assert key not in data


def test_build_health_prefix_empty_when_all_ok(clean_data):
    """Prefix should be empty if no files have status='failed'."""
    health_dir = clean_data["health_dir"]
    (health_dir / "scraper1.json").write_text(json.dumps({"status": "ok"}), encoding="utf-8")

    prefix = build_health_prefix()
    assert prefix == ""


def test_build_health_prefix_formats_failures(clean_data):
    """Prefix should contain error details for failed sources."""
    health_dir = clean_data["health_dir"]
    (health_dir / "scraper1.json").write_text(
        json.dumps({"status": "failed", "source": "EIA", "error": "API key expired"}),
        encoding="utf-8",
    )

    prefix = build_health_prefix()
    assert "🚨 <b>SYSTEM HEALTH ALERTS</b> 🚨" in prefix
    assert "EIA" in prefix
    assert "API key expired" in prefix


def test_build_health_prefix_ignores_internal_integrity_files(clean_data):
    """``_*.json`` integrity artifacts must never page anyone."""
    health_dir = clean_data["health_dir"]
    (health_dir / "_integrity_state.json").write_text(
        json.dumps({"status": "failed"}), encoding="utf-8"
    )
    (health_dir / "_integrity_report.json").write_text(
        json.dumps({"overall": "FAIL"}), encoding="utf-8"
    )

    assert build_health_prefix() == ""


def _integrity_report():
    return {
        "generated_at": "2026-08-23T06:00:00+00:00",
        "overall": "FAIL",
        "sources": {
            "eia_lng_exports": {
                "overall": "FAIL",
                "results": [
                    {"check": "divergence", "severity": "FAIL", "message": "DIVERGENCE: rot"},
                    {"check": "gaps", "severity": "SKIPPED", "message": "no gap_rule configured"},
                ],
                "stats": {},
            },
            "gulf_south": {
                "overall": "PASS",
                "results": [
                    {"check": "schema", "severity": "PASS", "message": "ok"},
                    {"check": "divergence", "severity": "SKIPPED", "message": "no health payload"},
                ],
                "stats": {},
            },
        },
    }


def test_format_integrity_findings_lists_only_warn_fail():
    """Digest rows carry WARN/FAIL checks only; SKIPPED noise stays out."""
    out = format_integrity_findings(_integrity_report())

    lng_line = next(line for line in out.splitlines() if "eia_lng_exports" in line)
    assert "divergence" in lng_line
    assert "gaps" not in lng_line
    assert "DIVERGENCE: rot" in out  # deciding numbers ride along as detail

    gulf_line = next(line for line in out.splitlines() if "gulf_south" in line)
    assert gulf_line.rstrip().endswith("-")  # all-clear row shows '-'
    assert "schema" not in gulf_line and "divergence" not in gulf_line


def test_send_integrity_alert_suppressed_on_all_clear(clean_data, tmp_path, monkeypatch):
    """No WARN/FAIL findings → nothing posted, returns False."""
    import httpx

    def mock_post(*args, **kwargs):
        pytest.fail("httpx.post must not fire on an all-clear integrity report")

    monkeypatch.setattr(httpx, "post", mock_post)

    report = _integrity_report()
    report["overall"] = "PASS"
    report["sources"]["eia_lng_exports"] = {
        "overall": "SKIPPED",
        "results": [{"check": "availability", "severity": "SKIPPED"}],
        "stats": {},
    }
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report), encoding="utf-8")

    assert send_integrity_alert_if_needed(path) is False


def test_send_integrity_alert_dispatches_once_per_day(clean_data, tmp_path, monkeypatch):
    """WARN/FAIL report → digest sent under a date-scoped dedup key."""
    import httpx

    calls = []

    class MockResponse:
        status_code = 200

        def json(self):
            return {"ok": True}

    def fake_post(*args, **kwargs):
        calls.append(kwargs)
        return MockResponse()

    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "fake_id")

    path = tmp_path / "report.json"
    path.write_text(json.dumps(_integrity_report()), encoding="utf-8")

    assert send_integrity_alert_if_needed(path) is True
    assert len(calls) == 1
    text = calls[0]["json"]["text"]
    assert "DATASET INTEGRITY DIGEST" in text
    assert "eia_lng_exports" in text and "DIVERGENCE: rot" in text
    # Dedup key derives from the report's generated_at date.
    data = json.loads(clean_data["sent_alerts"].read_text(encoding="utf-8"))
    assert "integrity_digest_2026-08-23" in data

    # Same-day rerun deduplicates even though findings persist.
    assert send_integrity_alert_if_needed(path) is False
    assert len(calls) == 1


def test_send_integrity_alert_unreadable_report_raises(clean_data, tmp_path):
    """Corrupt report surfaces AlertError (caller prints, never guesses)."""
    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")

    with pytest.raises(AlertError):
        send_integrity_alert_if_needed(bad)


def test_format_feedgas_alert_renders_correctly() -> None:
    """Verify HTML formatting of LNG feedgas alerts."""
    # OFFLINE alert
    offline_body = format_feedgas_alert(
        terminal="Freeport LNG",
        event_type="OFFLINE",
        gas_day="2024-04-17",
        duration=7,
        flow_mmcf=0.0,
        baseline_mmcf=1900.0,
        detail="7 consecutive posted-zeros",
    )
    assert "🔴 <b>LNG TERMINAL ALERT: FREEPORT LNG</b>" in offline_body
    assert "Event: <b>OFFLINE</b>" in offline_body
    assert "<code>2024-04-17</code> (duration: 7d)" in offline_body
    assert "Flow: <b>0.0 MMcf/d</b>" in offline_body
    assert "drop: 100.0%" in offline_body
    assert "7 consecutive posted-zeros" in offline_body

    # DEPRESSED alert
    depressed_body = format_feedgas_alert(
        terminal="Freeport LNG",
        event_type="DEPRESSED",
        gas_day="2026-07-20",
        duration=5,
        flow_mmcf=800.0,
        baseline_mmcf=1900.0,
        detail="below 60% baseline 5d",
    )
    assert "🟠 <b>LNG TERMINAL ALERT: FREEPORT LNG</b>" in depressed_body
    assert "Event: <b>DEPRESSED</b>" in depressed_body
    assert "drop: 57.9%" in depressed_body


def test_send_feedgas_alerts_dry_run(monkeypatch: pytest.MonkeyPatch) -> None:
    """dry_run mode prints rendered message body and makes zero network calls."""
    import httpx
    import scripts.task3_validate as t3

    # Mock terminal history with an active outage
    mock_history = {
        "2026-08-30": {"value": 1_900_000, "posted": True, "posted_zero": False, "n_feeds_posted": 2},
        "2026-08-31": {"value": 0, "posted": True, "posted_zero": True, "n_feeds_posted": 2},
        "2026-09-01": {"value": 0, "posted": True, "posted_zero": True, "n_feeds_posted": 2},
        "2026-09-02": {"value": 0, "posted": True, "posted_zero": True, "n_feeds_posted": 2},
    }
    monkeypatch.setattr(t3, "load_terminal_history", lambda term: (mock_history, t3.TERMINALS[term]))

    # Ensure no network post happens
    def fail_post(*args, **kwargs):
        pytest.fail("Network call attempted during dry_run")

    monkeypatch.setattr(httpx, "post", fail_post)

    alerts = send_feedgas_alerts_if_needed(dry_run=True)
    assert len(alerts) >= 1
    assert any(a["terminal"] == "freeport" and "OFFLINE" in a["html_body"] for a in alerts)


def test_send_feedgas_alerts_missing_credentials_degrades_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing Telegram credentials degrade to clean skip without raising AlertError or crashing."""
    import scripts.task3_validate as t3

    mock_history = {
        "2026-08-30": {"value": 1_900_000, "posted": True, "posted_zero": False, "n_feeds_posted": 2},
        "2026-08-31": {"value": 0, "posted": True, "posted_zero": True, "n_feeds_posted": 2},
        "2026-09-01": {"value": 0, "posted": True, "posted_zero": True, "n_feeds_posted": 2},
        "2026-09-02": {"value": 0, "posted": True, "posted_zero": True, "n_feeds_posted": 2},
    }
    monkeypatch.setattr(t3, "load_terminal_history", lambda term: (mock_history, t3.TERMINALS[term]))

    # Explicitly clear credentials
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    # Must not raise AlertError
    alerts = send_feedgas_alerts_if_needed(dry_run=False)
    assert len(alerts) >= 1
    assert any(a["terminal"] == "freeport" for a in alerts)


def test_feedgas_alert_dedup_ttl_suppresses_repeat(clean_data, monkeypatch: pytest.MonkeyPatch) -> None:
    """Dedup TTL suppresses duplicate alert of same event within TTL window."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "fake_id")

    import httpx

    post_count = 0

    class MockResponse:
        status_code = 200
        def json(self): return {"ok": True}

    def count_post(*args, **kwargs):
        nonlocal post_count
        post_count += 1
        return MockResponse()

    monkeypatch.setattr(httpx, "post", count_post)

    key = "feedgas_freeport_OFFLINE_2024-04-17"
    # First send succeeds and calls post
    first = send_alert(key, "Freeport offline", include_health_prefix=False)
    assert first is True
    assert post_count == 1

    # Second send within TTL deduplicates without calling post
    second = send_alert(key, "Freeport offline repeat", include_health_prefix=False)
    assert second is False
    assert post_count == 1


def test_new_event_on_terminal_with_active_alert_gets_through(clean_data, monkeypatch: pytest.MonkeyPatch) -> None:
    """A new event on a terminal with an active alert still gets dispatched."""
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "fake_token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "fake_id")

    import httpx

    post_count = 0

    class MockResponse:
        status_code = 200
        def json(self): return {"ok": True}

    def count_post(*args, **kwargs):
        nonlocal post_count
        post_count += 1
        return MockResponse()

    monkeypatch.setattr(httpx, "post", count_post)

    # First event: OFFLINE on 2024-04-17
    ev1 = "feedgas_freeport_OFFLINE_2024-04-17"
    res1 = send_alert(ev1, "Freeport offline", include_health_prefix=False)
    assert res1 is True
    assert post_count == 1

    # Second distinct event on same terminal: ACUTE_DROP or DEPRESSED
    ev2 = "feedgas_freeport_acute_drop"
    res2 = send_alert(ev2, "Freeport acute drop", include_health_prefix=False)
    assert res2 is True
    assert post_count == 2


