"""Tests for Telegram alert dispatcher."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from publishers.alerts import AlertError, build_health_prefix, send_alert


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
