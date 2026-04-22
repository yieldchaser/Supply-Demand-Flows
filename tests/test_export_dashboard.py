"""Tests for dashboard bundle exporter."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from publishers.export_dashboard_json import build


def test_build_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Verify bundle aggregation, hashing, and manifest generation."""
    curated_dir = tmp_path / "data" / "curated"
    health_dir = tmp_path / "data" / "health"
    docs_dir = tmp_path / "docs" / "data"

    curated_dir.mkdir(parents=True)
    health_dir.mkdir(parents=True)

    # Create mock curated data
    df = pd.DataFrame([{"period": "2024-04-12", "value": 100.0, "source": "eia"}])
    df.to_parquet(curated_dir / "eia_storage.parquet")

    # Create mock health data
    health_data = {"status": "ok", "last_run": "2024-04-12T15:00:00Z"}
    (health_dir / "eia_storage.json").write_text(json.dumps(health_data))

    # Monkeypatch paths in the module
    import publishers.export_dashboard_json as export

    monkeypatch.setattr(export, "CURATED_DIR", curated_dir)
    monkeypatch.setattr(export, "HEALTH_DIR", health_dir)
    monkeypatch.setattr(export, "DOCS_DATA_DIR", docs_dir)

    result = build()

    assert result["sources_count"] == 1
    assert (docs_dir / "bundle.json").exists()
    assert (docs_dir / "manifest.json").exists()

    manifest = json.loads((docs_dir / "manifest.json").read_text())
    assert manifest["bundle_url"].startswith("bundle.")
    assert manifest["bundle_url"].endswith(".json")

    bundle = json.loads((docs_dir / manifest["bundle_url"]).read_text())
    assert "eia_storage" in bundle["sources"]
    assert "eia_storage" in bundle["health"]
    assert bundle["sources"]["eia_storage"]["data"][0]["value"] == 100.0


def test_bundle_handles_date_objects():
    """Verify the default serializer handles pandas/transformer date outputs."""
    # Simulate a bundle with a date object
    import json
    from datetime import date

    from publishers.export_dashboard_json import _json_default

    result = json.dumps({"period": date(2026, 4, 22)}, default=_json_default)
    assert "2026-04-22" in result


def test_bundle_filters_prev_health_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Verify that .prev health files are ignored by the builder."""
    health_dir = tmp_path / "data" / "health"
    health_dir.mkdir(parents=True)

    # Create one real and one .prev file
    (health_dir / "eia_storage.json").write_text(json.dumps({"status": "ok"}), encoding="utf-8")
    (health_dir / "eia_storage.prev.json").write_text(
        json.dumps({"status": "ok"}), encoding="utf-8"
    )

    import publishers.export_dashboard_json as export

    monkeypatch.setattr(export, "HEALTH_DIR", health_dir)
    monkeypatch.setattr(export, "CURATED_DIR", tmp_path / "empty_curated")
    monkeypatch.setattr(export, "DOCS_DATA_DIR", tmp_path / "empty_docs")

    # build() uses real directories if not exists, but we want to test the loop
    # We don't need to call build() if we just test the logic inside build or
    # mock the glob. Let's just run build().
    from publishers.export_dashboard_json import build

    build()
    # Read the written bundle.json
    bundle_path = tmp_path / "empty_docs" / "bundle.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))

    assert "eia_storage" in bundle["health"]
    assert "eia_storage.prev" not in bundle["health"]
