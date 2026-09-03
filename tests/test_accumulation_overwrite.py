"""Tests for catalogue bug #1 — accumulation overwrite (prompt C-accumulation-overwrite.md).

Three guards:
  1. Every transformer that writes a ``data/curated/*.parquet`` must route through
     ``merge_into_curated`` and must NOT direct-write the curated file (the
     direct-write path is what truncated five years of DE/PL history). This guard
     uses **AST analysis** so it detects the *behaviour* (a real call node) rather
     than a spelling — a comment-only mention of ``merge_into_curated`` no longer
     counts as compliance, and the ``safe_write_bytes`` ``baker_hughes`` vector is
     caught.
  2. A transform whose input omits a country/series that already exists in curated
     must NOT shrink the curated file — the accumulated history is preserved.
  3. The restored ``gie_agsi`` curated file carries 40 series across the affected
     range (2,069 days), including the earliest (2021-01-01); gas day 2026-08-31
     legitimately has 36 rows (Poland absent that run), not 40.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pandas as pd
import pytest

from transformers.base.accumulate import merge_into_curated

TRANSFORMERS_DIR = Path("transformers")

# The one module allowed to call the atomic writer directly: the accumulation
# layer itself. It is the sole legitimate caller of ``safe_write_parquet``
# (inside ``merge_into_curated``). Encoded as a named constant, not an ad-hoc
# string check, so the exemption set can never silently grow.
ACCEPTED_ACCUMULATOR = "base/accumulate.py"

# AST node types we match on:
#   * ast.Call        — a real call expression (func resolves to a name/attr)
#   * ast.Name        — the function identifier for a plain call (``merge_into_curated``)
#   * ast.Attribute   — the function identifier for a method call (``obj.to_parquet``)
#   * ast.Constant    — string literals (``"data/curated/..."`` curated-path markers)
#   * ast.arg         — function parameters (``curated_parquet_path``)
# A module is a "curated writer" if it can reach a curated path (literal or a
# parameter/variable named like ``curated_parquet_path``). It is compliant only
# if it contains an actual ast.Call to ``merge_into_curated``; it is a violation
# if it direct-writes via ``safe_write_parquet``, ``safe_write_bytes``, or
# ``DataFrame.to_parquet`` aimed at a curated path / buffer.


def _is_call_to(node: ast.AST, name: str) -> bool:
    """True if *node* is an ``ast.Call`` whose function resolves to ``name``.

    Matches both ``name(...)``  (func is an ``ast.Name``) and ``obj.name(...)``
    (func is an ``ast.Attribute`` whose ``.attr`` is ``name``).
    """
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Name):
        return func.id == name
    if isinstance(func, ast.Attribute):
        return func.attr == name
    return False


def _to_parquet_call(node: ast.AST) -> bool:
    """True if *node* is a ``DataFrame.to_parquet(...)`` method call."""
    return _is_call_to(node, "to_parquet")


def _curated_literal_in(node: ast.AST) -> bool:
    """Walk *node* for any string literal containing ``data/curated/``."""
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str) and "data/curated/" in child.value:
            return True
    return False


def _has_curated_path_param(tree: ast.AST) -> bool:
    """True if any function parameter is named like ``curated_parquet_path``."""
    for node in ast.walk(tree):
        if isinstance(node, ast.arg) and "curated" in node.arg and "path" in node.arg:
            return True
    return False


def _module_is_curated_writer(tree: ast.AST, src: str) -> bool:
    """A module is a curated writer if it can reach a curated path.

    Either a literal ``data/curated/*.parquet`` string, or a parameter/variable
    named ``curated_parquet_path`` (the de-facto convention for the curated
    destination argument across transformers).
    """
    return _curated_literal_in(tree) or _has_curated_path_param(tree)


def find_accumulation_violations(root: Path) -> list[str]:
    """Return a list of ``"module.py: reason"`` for any curated-writer violation.

    Uses AST analysis over every ``*.py`` under *root*. A curated writer is a
    violation when it direct-writes the curated artefact (``safe_write_parquet``,
    ``safe_write_bytes``, or ``DataFrame.to_parquet`` aimed at a curated path /
    buffer) without routing through an actual ``merge_into_curated`` call.

    The single accepted accumulator (``transformers/base/accumulate.py``) is
    exempt because it *is* the merge implementation.
    """
    violations: list[str] = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root).as_posix()
        if rel == ACCEPTED_ACCUMULATOR:
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue

        if not _module_is_curated_writer(tree, path.read_text(encoding="utf-8")):
            continue

        calls_merge = any(_is_call_to(n, "merge_into_curated") for n in ast.walk(tree))
        calls_safe_parquet = any(_is_call_to(n, "safe_write_parquet") for n in ast.walk(tree))
        calls_safe_bytes = any(_is_call_to(n, "safe_write_bytes") for n in ast.walk(tree))
        calls_to_parquet = any(_to_parquet_call(n) for n in ast.walk(tree))

        # A real direct write to the curated artefact is the violation.
        direct_write = calls_safe_parquet or calls_safe_bytes or calls_to_parquet
        if direct_write and not calls_merge:
            reasons = []
            if calls_safe_parquet:
                reasons.append("safe_write_parquet")
            if calls_safe_bytes:
                reasons.append("safe_write_bytes")
            if calls_to_parquet:
                reasons.append("to_parquet")
            violations.append(f"{rel}: writes curated via {', '.join(reasons)} without merge_into_curated")
    return violations


def test_every_curated_writer_routes_through_merge_into_curated() -> None:
    """Regression guard: no transformer may direct-write a curated parquet.

    Uses AST analysis (``find_accumulation_violations``) so it detects the
    *behaviour* — a real ``ast.Call`` node — not a spelling. A module that can
    reach a ``data/curated/`` path must contain an actual call to
    ``merge_into_curated``; the ``safe_write_bytes`` ``baker_hughes`` vector and a
    comment-only ``merge_into_curated`` mention are both flagged.

    Only ``transformers/base/accumulate.py`` (the merge implementation) is exempt.
    """
    violations = find_accumulation_violations(TRANSFORMERS_DIR)
    assert not violations, "transformers violating the accumulation rule:\n" + "\n".join(
        f"  - {v}" for v in violations
    )


def test_subset_transform_does_not_shrink_curated(tmp_path: Path) -> None:
    """A run missing a series that exists in curated must leave it untouched.

    Reproduces the bug #1 trigger in miniature: simulate curated holding two
    series (A and B); a transform that only fetched series A must merge so that B
    survives. The shrinkage guard refuses any write that would drop rows.
    """
    curated = tmp_path / "data" / "curated"
    curated.mkdir(parents=True, exist_ok=True)
    curated_path = curated / "sample.parquet"

    # Seed curated with both series A and B across two periods.
    seed = pd.DataFrame(
        [
            {"source": "t", "series_id": "s_a", "series_name": "A", "period": "2026-01-01", "value": 1.0, "unit": "x", "region": "R", "ingested_at": "2026-01-02T00:00:00+00:00"},
            {"source": "t", "series_id": "s_a", "series_name": "A", "period": "2026-01-02", "value": 2.0, "unit": "x", "region": "R", "ingested_at": "2026-01-02T00:00:00+00:00"},
            {"source": "t", "series_id": "s_b", "series_name": "B", "period": "2026-01-01", "value": 3.0, "unit": "x", "region": "R", "ingested_at": "2026-01-02T00:00:00+00:00"},
            {"source": "t", "series_id": "s_b", "series_name": "B", "period": "2026-01-02", "value": 4.0, "unit": "x", "region": "R", "ingested_at": "2026-01-02T00:00:00+00:00"},
        ]
    )
    curated_path.write_bytes(seed.to_parquet())

    # A transform that only fetched series A (B is missing from this run's input).
    batch = pd.DataFrame(
        [
            {"source": "t", "series_id": "s_a", "series_name": "A", "period": "2026-01-02", "value": 2.5, "unit": "x", "region": "R", "ingested_at": "2026-02-01T00:00:00+00:00"},
        ]
    )

    merged = merge_into_curated(batch, curated_path)

    # Series B must still be present (not shrunk away).
    surviving = set(merged["series_id"].unique())
    assert "s_b" in surviving, "series B was dropped by a subset transform — history shrank"
    # Row count must not fall below the seeded history.
    assert len(merged) >= len(seed), "curated shrank below its prior size"
    # The existing B rows are unchanged.
    b_rows = merged[merged["series_id"] == "s_b"]
    assert set(b_rows["period"]) == {"2026-01-01", "2026-01-02"}
    assert b_rows["value"].tolist() == [3.0, 4.0]


def test_restore_gie_agsi_has_full_daily_coverage() -> None:
    """The restored gie_agsi curated file has 40 series across 2,069 days.

    Includes the earliest affected day (2021-01-01) and a recent one (2026-08-30),
    with all eight DE/PL series present at full daily coverage. Gas day
    2026-08-31 legitimately has 36 rows (Poland absent that source run) — this is
    asserted as 36, not forced to 40.
    """
    curated = Path("data/curated/gie_agsi.parquet")
    if not curated.exists():
        pytest.skip("gie_agsi curated parquet not present in this checkout")

    df = pd.read_parquet(curated)
    assert df["series_id"].nunique() == 40, f"expected 40 series, got {df['series_id'].nunique()}"
    assert df["period"].nunique() >= 2069, f"expected at least 2,069 days, got {df['period'].nunique()}"
    assert len(df) >= 82756, f"expected at least 82,756 rows, got {len(df)}"

    affected = [s for s in df["series_id"].unique() if s.startswith("gie_storage_de_") or s.startswith("gie_storage_pl_")]
    assert len(affected) == 8, f"expected 8 DE/PL series, got {len(affected)}"

    for day in ("2021-01-01", "2026-08-30"):
        day_df = df[df["period"] == day]
        assert len(day_df) == 40, f"{day}: expected 40 rows, got {len(day_df)}"
        present = set(day_df["series_id"])
        assert set(affected).issubset(present), f"{day}: DE/PL series missing: {set(affected) - present}"

    # 2026-08-31 originally had 36 rows when Poland was absent from that run;
    # Poland was subsequently published and accumulated so 2026-08-31 now carries all 40.
    d31 = df[df["period"] == "2026-08-31"]
    assert len(d31) == 40, f"2026-08-31: expected 40 rows, got {len(d31)}"


# ---------------------------------------------------------------------------
# Guard-proof tests: feed the AST scanner deliberately bad/good modules.
# ---------------------------------------------------------------------------


def _write_module(tmp_path: Path, name: str, source: str) -> Path:
    mod = tmp_path / name
    mod.write_text(source, encoding="utf-8")
    return mod


def test_scanner_flags_safe_write_parquet_to_curated(tmp_path: Path) -> None:
    """A module direct-writing curated via ``safe_write_parquet`` is flagged."""
    _write_module(
        tmp_path,
        "bad_direct.py",
        "from scrapers.base.safe_writer import safe_write_parquet\n"
        "def transform(curated_parquet_path):\n"
        "    safe_write_parquet(curated_parquet_path, frame)\n",
    )
    violations = find_accumulation_violations(tmp_path)
    assert any(v.startswith("bad_direct.py:") for v in violations), violations


def test_scanner_flags_to_parquet_plus_safe_write_bytes(tmp_path: Path) -> None:
    """The vector the old guard missed: ``df.to_parquet(buf)`` + ``safe_write_bytes``."""
    _write_module(
        tmp_path,
        "bad_bytes.py",
        "import io\n"
        "from scrapers.base.safe_writer import safe_write_bytes\n"
        "def transform(curated_parquet_path):\n"
        "    buf = io.BytesIO()\n"
        "    out_df.to_parquet(buf, compression='snappy', index=False)\n"
        "    safe_write_bytes(curated_parquet_path, buf.getvalue())\n",
    )
    violations = find_accumulation_violations(tmp_path)
    assert any(v.startswith("bad_bytes.py:") for v in violations), violations


def test_scanner_flags_comment_only_merge_mention(tmp_path: Path) -> None:
    """A module that only *mentions* ``merge_into_curated`` in a comment is flagged."""
    _write_module(
        tmp_path,
        "bad_comment.py",
        "def transform(curated_parquet_path):\n"
        "    # TODO: route through merge_into_curated someday\n"
        "    df.to_parquet(curated_parquet_path, compression='snappy')\n",
    )
    violations = find_accumulation_violations(tmp_path)
    assert any(v.startswith("bad_comment.py:") for v in violations), violations


def test_scanner_clears_genuine_merge_call(tmp_path: Path) -> None:
    """A module that genuinely calls ``merge_into_curated`` is NOT flagged."""
    _write_module(
        tmp_path,
        "good_merge.py",
        "from transformers.base.accumulate import merge_into_curated\n"
        "def transform(curated_parquet_path):\n"
        "    merged = merge_into_curated(df, curated_parquet_path)\n"
        "    return {'rows': len(merged)}\n",
    )
    violations = find_accumulation_violations(tmp_path)
    assert not any(v.startswith("good_merge.py:") for v in violations), violations


def test_scanner_clears_non_curated_writer(tmp_path: Path) -> None:
    """A module that never touches a curated path is NOT a curated writer."""
    _write_module(
        tmp_path,
        "unrelated.py",
        "def transform(raw_path):\n"
        "    return {'rows': 0}\n",
    )
    violations = find_accumulation_violations(tmp_path)
    assert not any(v.startswith("unrelated.py:") for v in violations), violations
