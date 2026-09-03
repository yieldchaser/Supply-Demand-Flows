"""Test retention policy for published dashboard bundle artifacts in docs/data/.

Ensures dead hashes are pruned so docs/data/ does not accumulate gigabytes of
dead json payloads across builds.
"""
from __future__ import annotations

import os
from pathlib import Path

from publishers.export_dashboard_json import _prune_stale_bundles


def test_prune_stale_bundles_retention_policy(tmp_path: Path) -> None:
    """Verify that _prune_stale_bundles retains only current_hash + keep_previous prior hashes."""
    hashes = ["11111111", "22222222", "33333333", "44444444", "55555555"]

    # Create mock bundle, index, and source files for each hash with spaced mtimes
    for idx, h in enumerate(hashes):
        bundle_f = tmp_path / f"bundle.{h}.json"
        bundle_f.write_text("{}", encoding="utf-8")
        index_f = tmp_path / f"index.{h}.json"
        index_f.write_text("{}", encoding="utf-8")
        shard_f = tmp_path / f"src.gasnom.{h}.json"
        shard_f.write_text("{}", encoding="utf-8")

        # Space out mtime so ranking is deterministic
        mtime = 1000000 + idx * 100
        os.utime(bundle_f, (mtime, mtime))
        os.utime(index_f, (mtime, mtime))
        os.utime(shard_f, (mtime, mtime))

    # Also create non-hashed files (load-bearing manifest and root bundle)
    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    root_bundle = tmp_path / "bundle.json"
    root_bundle.write_text("{}", encoding="utf-8")

    # Current hash is 55555555, keep_previous = 2
    # Should keep: 55555555 (current), plus 44444444 and 33333333 (most recent)
    # Should delete: 22222222 and 11111111 (6 files total)
    pruned_count = _prune_stale_bundles(tmp_path, current_hash="55555555", keep_previous=2)
    assert pruned_count == 6, f"Expected 6 files pruned, got {pruned_count}"

    # Verify non-hashed files survive untouched
    assert manifest.exists(), "manifest.json must never be pruned"
    assert root_bundle.exists(), "bundle.json must never be pruned"

    # Verify kept hashes
    for h in ["33333333", "44444444", "55555555"]:
        assert (tmp_path / f"bundle.{h}.json").exists(), f"Hash {h} should be kept"
        assert (tmp_path / f"index.{h}.json").exists(), f"Hash {h} index should be kept"
        assert (tmp_path / f"src.gasnom.{h}.json").exists(), f"Hash {h} shard should be kept"

    # Verify deleted hashes
    for h in ["11111111", "22222222"]:
        assert not (tmp_path / f"bundle.{h}.json").exists(), f"Hash {h} should be deleted"
        assert not (tmp_path / f"index.{h}.json").exists(), f"Hash {h} index should be deleted"
        assert not (tmp_path / f"src.gasnom.{h}.json").exists(), f"Hash {h} shard should be deleted"
