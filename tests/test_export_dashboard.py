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
