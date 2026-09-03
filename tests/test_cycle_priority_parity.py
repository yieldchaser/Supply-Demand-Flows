"""Parity test: Verify JS and Python cycle priority definitions agree on relative ordering.

Prompt W §02 requirement:
Assert that CYCLE_PRIORITY in docs/js/util/lng-downtime.js and CYCLE_PRIORITY in
scripts/task3_validate.py produce identical relative ordering over the union of keys.
"""

from __future__ import annotations

from pathlib import Path

from scripts.task3_validate import CYCLE_PRIORITY as PY_CYCLE_PRIORITY
from scripts.task3_validate import cycle_priority as py_cycle_priority

JS_DOWNTIME_PATH = Path("docs/js/util/lng-downtime.js")


def parse_js_cycle_priority(path: Path = JS_DOWNTIME_PATH) -> dict[str, int]:
    """Parse export const CYCLE_PRIORITY from lng-downtime.js using robust brace tracking."""
    text = path.read_text(encoding="utf-8")
    marker = "export const CYCLE_PRIORITY = {"
    start_idx = text.find(marker)
    if start_idx == -1:
        raise ValueError(f"Could not find '{marker}' in {path}")

    brace_start = text.find("{", start_idx)
    depth = 0
    end_idx = -1
    for i in range(brace_start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                end_idx = i
                break

    if end_idx == -1:
        raise ValueError(f"Unmatched braces parsing CYCLE_PRIORITY in {path}")

    block = text[brace_start + 1 : end_idx]
    result: dict[str, int] = {}
    for line in block.splitlines():
        # Strip comments
        clean = line.split("//")[0].strip()
        if not clean or ":" not in clean:
            continue
        parts = clean.split(":", 1)
        key = parts[0].strip().strip("'\"")
        val_str = parts[1].strip().rstrip(",")
        try:
            result[key] = int(val_str)
        except ValueError:
            continue
    return result


def test_cycle_priority_vocabulary_and_ordering_parity() -> None:
    """JS and Python must have identical cycle vocabularies and identical relative orderings."""
    js_priority = parse_js_cycle_priority()

    # 1. Exact vocabulary agreement (union of keys must match exactly)
    js_keys = set(js_priority.keys())
    py_keys = set(PY_CYCLE_PRIORITY.keys())
    assert js_keys == py_keys, (
        f"Cycle vocabulary mismatch:\n"
        f"In JS only: {sorted(js_keys - py_keys)}\n"
        f"In Python only: {sorted(py_keys - js_keys)}"
    )

    # 2. Relative ordering parity over all pairs of keys
    keys = sorted(js_keys)
    for i, ka in enumerate(keys):
        for kb in keys[i + 1 :]:
            js_cmp = (js_priority[ka] > js_priority[kb]) - (js_priority[ka] < js_priority[kb])
            py_cmp = (PY_CYCLE_PRIORITY[ka] > PY_CYCLE_PRIORITY[kb]) - (PY_CYCLE_PRIORITY[ka] < PY_CYCLE_PRIORITY[kb])
            assert js_cmp == py_cmp, (
                f"Relative ordering mismatch between '{ka}' and '{kb}':\n"
                f"JS ranks: {ka}={js_priority[ka]}, {kb}={js_priority[kb]} (cmp={js_cmp})\n"
                f"Python ranks: {ka}={PY_CYCLE_PRIORITY[ka]}, {kb}={PY_CYCLE_PRIORITY[kb]} (cmp={py_cmp})"
            )

    # 3. best must be sub-timely (> 0 and < timely) on both sides
    assert js_priority["best"] > 0
    assert js_priority["best"] < js_priority["timely"]
    assert PY_CYCLE_PRIORITY["best"] > 0
    assert PY_CYCLE_PRIORITY["best"] < PY_CYCLE_PRIORITY["timely"]

    # 4. Hourly snapshots are strictly 0 on Python side
    assert py_cycle_priority("id0200") == 0
    assert py_cycle_priority("id1200") == 0
