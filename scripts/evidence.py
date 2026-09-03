"""Automated Evidence Harness for Blue Tide (Prompt S §02).

Executes all observatory gates in one pass, writes attestation-stamped log files
into logs/, removes stale/unverified logs, prunes superseded bundle generations,
and emits logs/EVIDENCE.json with sha256 checksums of every generated log.

If a gate cannot be spawned, it writes `NOT RUN: <exception>`, records
`status: "not_run"`, and continues without omitting the gate.

Usage:
    python scripts/evidence.py
"""
from __future__ import annotations

import contextlib
import glob
import hashlib
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    with contextlib.suppress(Exception):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REPO_ROOT = Path(__file__).resolve().parent.parent
LOGS_DIR = REPO_ROOT / "logs"
DATA_DIR = REPO_ROOT / "docs" / "data"

# Seven stale/unverified logs that must be permanently absent (Prompt S §02)
STALE_LOG_FILES = [
    "Q0-preflight.txt",
    "final-node.txt",
    "P1-prune.txt",
    "P2-load.txt",
    "final-preflight.txt",
    "N1-preflight.txt",
    "N3-preflight.txt",
]

# Empirical finding-count baselines (Prompt AA §04).
# Ratchet rule: thresholds may go down as tech debt is paid down, NEVER up.
BASELINE_THRESHOLDS: dict[str, int] = {
    "ruff": 17,  # 17 accepted baseline findings (E402 imports not at top, N806 mock names)
    "mypy": 53,  # 53 accepted baseline findings in targeted scripts and tests
}

# Committed minimum collected test counts (Prompt AA §05).
# Ratchet rule: counts may rise; a fall fails the gate loudly naming both numbers.
EXPECTED_MIN_TESTS: dict[str, int] = {
    "pytest": 448,     # 448 passed unit tests
    "node_tests": 42,  # 42 node test runner assertions
}


def clean_stale_logs() -> list[str]:
    """Delete stale logs that no longer describe reality (Prompt U §03)."""
    removed = []
    # Purge all legacy logs from earlier rounds (N*, P*, Q*, final-*)
    for p in LOGS_DIR.glob("*.txt"):
        if p.name.startswith(("N", "P", "Q", "final-")):
            try:
                p.unlink()
                removed.append(p.name)
            except OSError as exc:
                print(f"WARN: could not remove stale log {p.name}: {exc}")
    for name in STALE_LOG_FILES:
        target = LOGS_DIR / name
        if target.exists() and name not in removed:
            try:
                target.unlink()
                removed.append(name)
            except OSError as exc:
                print(f"WARN: could not remove stale log {name}: {exc}")
    if removed:
        print(f"Cleaned {len(removed)} stale log file(s): {', '.join(removed)}")
    return removed


def prune_superseded_bundles() -> dict[str, Any]:
    """Prune superseded bundle generations from docs/data (Prompt S §03)."""
    from publishers.export_dashboard_json import KEEP_PREVIOUS, _prune_stale_bundles

    manifest_file = DATA_DIR / "manifest.json"
    if not manifest_file.exists():
        return {"status": "error", "error": "manifest.json missing"}

    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    live_hash = manifest.get("hash", "")

    files_before = list(DATA_DIR.glob("*.json"))
    bytes_before = sum(f.stat().st_size for f in files_before)

    pruned_count = _prune_stale_bundles(DATA_DIR, current_hash=live_hash, keep_previous=KEEP_PREVIOUS)

    files_after = list(DATA_DIR.glob("*.json"))
    bytes_after = sum(f.stat().st_size for f in files_after)

    res = {
        "status": "ok",
        "live_hash": live_hash,
        "keep_previous": KEEP_PREVIOUS,
        "pruned_files": pruned_count,
        "files_before": len(files_before),
        "bytes_before": bytes_before,
        "files_after": len(files_after),
        "bytes_after": bytes_after,
        "bytes_saved": bytes_before - bytes_after,
    }
    print(
        f"Data Prune: pruned {pruned_count} files ({res['bytes_saved'] / (1024 * 1024):.1f} MB saved). "
        f"Retained {len(files_after)} files ({res['bytes_after'] / (1024 * 1024):.1f} MB)."
    )
    return res


def get_git_head_sha() -> str:
    """Read git commit SHA directly from .git/HEAD without invoking git commands."""
    git_dir = REPO_ROOT / ".git"
    if not git_dir.exists():
        return "UNKNOWN_NO_GIT"
    head_file = git_dir / "HEAD"
    if not head_file.exists():
        return "UNKNOWN_NO_HEAD"
    head_content = head_file.read_text(encoding="utf-8").strip()
    if head_content.startswith("ref: "):
        ref_name = head_content[5:].strip()
        ref_path = git_dir / ref_name
        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8").strip()
        packed_refs = git_dir / "packed-refs"
        if packed_refs.exists():
            for line in packed_refs.read_text(encoding="utf-8", errors="replace").splitlines():
                if line.startswith("#") or line.startswith("^"):
                    continue
                parts = line.split()
                if len(parts) == 2 and parts[1] == ref_name:
                    return parts[0]
    return head_content


def run_ts_grep() -> tuple[int, str]:
    """Audit docs/js for forbidden TypeScript syntax in executable code."""
    ts_pattern = re.compile(
        r"(:\s*(?:string|number|boolean|any)\b|interface\s+[A-Z]|\bas\s+(?:string|number|HTMLElement))"
    )
    js_files = sorted(glob.glob("docs/js/**/*.js", recursive=True))
    violations: list[str] = []

    for js_path in js_files:
        p = Path(js_path)
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        in_block_comment = False
        for i, line in enumerate(lines, 1):
            s = line.strip()
            if "/*" in s:
                in_block_comment = True
            if in_block_comment:
                if "*/" in s:
                    in_block_comment = False
                continue
            if s.startswith("//") or s.startswith("*"):
                continue
            if ts_pattern.search(line):
                violations.append(f"{js_path}:{i}: {line.strip()}")

    if violations:
        output = "TypeScript syntax detected in executable JS:\n" + "\n".join(violations)
        return 1, output
    return 0, f"CLEAN -- verified {len(js_files)} JS files with zero TypeScript syntax in executable code."


def run_gate(
    name: str,
    cmd: list[str],
    log_file: str,
    git_sha: str,
    custom_fn: Any | None = None,
) -> dict[str, Any]:
    """Execute a single gate, write attestation header and output, and hash log.

    Hardened against spawn failures: records status='not_run' if subprocess cannot run.
    """
    print(f"\n[{name}] Running: {' '.join(cmd) if cmd else 'in-process audit'} ...", flush=True)
    start_iso = datetime.now(UTC).isoformat()
    status = "ok"

    if custom_fn:
        try:
            exit_code, output = custom_fn()
            status = "ok" if exit_code == 0 else "failed"
        except Exception as exc:
            output = f"In-process execution error: {exc}\n"
            exit_code = 99
            status = "error"
    else:
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(REPO_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            output = proc.stdout
            exit_code = proc.returncode
            status = "ok" if exit_code == 0 else "failed"
        except Exception as exc:
            output = f"NOT RUN: {exc}\n"
            exit_code = 127
            status = "not_run"

    extra_meta: dict[str, Any] = {}

    # Determine summary line
    non_empty_lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    summary = non_empty_lines[-1] if non_empty_lines else "NO_OUTPUT"
    for ln in reversed(non_empty_lines):
        if any(w in ln.lower() for w in ("passed", "verdict", "clean", "all checks pass", "failed", "not run", "found ")):
            summary = ln
            break

    # 1. Test-count mechanical guard (§05 / AA4)
    if name in EXPECTED_MIN_TESTS:
        expected = EXPECTED_MIN_TESTS[name]
        actual_count: int | None = None
        if name == "pytest":
            m_test = re.search(r"(\d+)\s+passed", output)
            if m_test:
                actual_count = int(m_test.group(1))
        elif name == "node_tests":
            m_test = re.search(r"(?:ℹ\s+tests|pass)\s+(\d+)", output)
            if m_test:
                actual_count = int(m_test.group(1))

        if actual_count is not None:
            extra_meta["collected_count"] = actual_count
            extra_meta["expected_min"] = expected
            if actual_count < expected:
                status = "failed"
                exit_code = 1
                summary = (
                    f"collected tests fell {expected} → {actual_count}; "
                    "a test was removed or destroyed. If this was deliberate, "
                    "raise it in the brief and lower the floor."
                )
            else:
                status = "ok" if exit_code == 0 else "failed"

    # 2. Finding-count baseline ratchet (§04 / AA3)
    elif name in BASELINE_THRESHOLDS:
        threshold = BASELINE_THRESHOLDS[name]
        finding_count: int | None = None
        if name == "ruff":
            m_find = re.search(r"Found\s+(\d+)\s+errors?", output) or re.search(r"(\d+)\s+errors?", output)
            if m_find:
                finding_count = int(m_find.group(1))
        elif name == "mypy":
            m_find = re.search(r"Found\s+(\d+)\s+errors?", output)
            if m_find:
                finding_count = int(m_find.group(1))

        if finding_count is not None:
            extra_meta["finding_count"] = finding_count
            extra_meta["baseline_threshold"] = threshold
            if exit_code == 0:
                status = "ok"
            elif finding_count <= threshold:
                status = "at baseline"
                if finding_count < threshold:
                    summary += f" [GOOD NEWS: {finding_count} < threshold {threshold} — lower the ratchet!]"
            else:
                status = "failed"
        else:
            status = "ok" if exit_code == 0 else "failed"

    # Attestation header written AFTER command returns and status is determined
    header = (
        f"{'=' * 80}\n"
        f"GATE: {name}\n"
        f"TIMESTAMP: {start_iso}\n"
        f"COMMAND: {' '.join(cmd) if cmd else name}\n"
        f"GIT HEAD SHA: {git_sha}\n"
        f"STATUS: {status}\n"
        f"EXIT CODE: {exit_code}\n"
        f"{'=' * 80}\n\n"
    )
    full_text = header + output
    log_path = LOGS_DIR / log_file
    log_path.write_text(full_text, encoding="utf-8")

    sha256 = hashlib.sha256(full_text.encode("utf-8")).hexdigest()

    print(f"[{name}] Status: {status} (exit {exit_code}) -> {summary}")

    ret = {
        "name": name,
        "command": cmd,
        "status": status,
        "exit_code": exit_code,
        "summary": summary,
        "log_file": str(log_path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "log_sha256": sha256,
    }
    ret.update(extra_meta)
    return ret


def main() -> int:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    git_sha = get_git_head_sha()

    print("=" * 80)
    print("BLUE TIDE AUTOMATED EVIDENCE HARNESS (scripts/evidence.py)")
    print("=" * 80)
    print(f"Git HEAD: {git_sha}")
    print(f"Timestamp: {datetime.now(UTC).isoformat()}\n")

    # 1. Clean stale logs
    clean_stale_logs()

    # 2. Prune dead data bundles
    prune_info = prune_superseded_bundles()

    # Discover test files
    node_tests = sorted(glob.glob("tests/*.test.mjs"))
    py_executable = sys.executable

    gates: list[tuple[str, list[str], str, Any | None]] = [
        # Gate 1: pytest
        ("pytest", [py_executable, "-m", "pytest", "-q", "-m", "not network"], "R-pytest.txt", None),
        # Gate 2: node tests
        ("node_tests", ["node", "--test", *node_tests], "R-node.txt", None),
        # Gate 3: ruff check
        (
            "ruff",
            [
                py_executable,
                "-m",
                "ruff",
                "check",
                "scripts/",
                "tests/",
                "publishers/",
                "validators/",
                "scrapers/",
            ],
            "R-ruff.txt",
            None,
        ),
        # Gate 4: mypy
        (
            "mypy",
            [
                py_executable,
                "-m",
                "mypy",
                "scripts/preflight.py",
                "scripts/evidence.py",
                "scripts/prune_bundles.py",
                "publishers/export_dashboard_json.py",
                "tests/test_bundle_retention.py",
                "tests/test_bundle_coverage_audit.py",
            ],
            "R-mypy.txt",
            None,
        ),
        # Gate 5: TypeScript grep
        ("ts_grep", [], "R-tsgrep.txt", run_ts_grep),
        # Gate 6: preflight audit
        ("preflight", [py_executable, "scripts/preflight.py"], "R-preflight.txt", None),
        # Gate 7: measure load
        ("measure_load", ["node", "scripts/measure_load.mjs"], "R-load.txt", None),
    ]

    results = []
    overall_exit = 0

    for name, cmd, log_file, custom_fn in gates:
        res = run_gate(name, cmd, log_file, git_sha, custom_fn)
        results.append(res)
        if res["status"] in ("failed", "error", "not_run"):
            overall_exit = 1

    manifest = {
        "_meta": {
            "generated_at": datetime.now(UTC).isoformat(),
            "git_sha": git_sha,
            "overall_exit": overall_exit,
            "prune_summary": prune_info,
        },
        "gates": results,
    }

    evidence_json_path = LOGS_DIR / "EVIDENCE.json"
    evidence_json_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"\nWrote manifest to {evidence_json_path.relative_to(REPO_ROOT)}")

    print("\n" + "=" * 80)
    print("EVIDENCE BOARD")
    print("=" * 80)
    for g in results:
        status_text = (
            "PASS"
            if g["status"] == "ok"
            else ("AT BASELINE" if g["status"] == "at baseline" else f"{g['status'].upper()} (exit {g['exit_code']})")
        )
        print(f"  {g['name']:<15} [{status_text:<14}] -> {g['summary']}")
    print("=" * 80)

    return overall_exit


if __name__ == "__main__":
    sys.exit(main())
