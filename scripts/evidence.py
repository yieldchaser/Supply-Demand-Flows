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

import glob
import hashlib
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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


def clean_stale_logs() -> list[str]:
    """Delete stale logs that no longer describe reality (Prompt S §02)."""
    removed = []
    for name in STALE_LOG_FILES:
        target = LOGS_DIR / name
        if target.exists():
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
        ref_path = git_dir / head_content[5:]
        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8").strip()
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

    # Attestation header written AFTER command returns
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

    # Determine summary line
    non_empty_lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    summary = non_empty_lines[-1] if non_empty_lines else "NO_OUTPUT"
    for ln in reversed(non_empty_lines):
        if any(w in ln.lower() for w in ("passed", "verdict", "clean", "all checks pass", "failed", "not run")):
            summary = ln
            break

    print(f"[{name}] Status: {status} (exit {exit_code}) -> {summary}")

    return {
        "name": name,
        "command": cmd,
        "status": status,
        "exit_code": exit_code,
        "summary": summary,
        "log_file": str(log_path.relative_to(REPO_ROOT)).replace("\\", "/"),
        "log_sha256": sha256,
    }


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
                "scripts/preflight.py",
                "scripts/evidence.py",
                "scripts/prune_bundles.py",
                "publishers/export_dashboard_json.py",
                "validators/integrity.py",
                "scrapers/eia_api/storage.py",
                "tests/test_coverage_guard.py",
                "tests/test_integrity.py",
                "tests/test_bundle_retention.py",
                "tests/test_bundle_coverage_audit.py",
                "tests/test_classify_meters.py",
                "tests/test_eia_storage_scraper.py",
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
        if res["exit_code"] != 0:
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
        status_text = "PASS" if g["exit_code"] == 0 else f"{g['status'].upper()} (exit {g['exit_code']})"
        print(f"  {g['name']:<15} [{status_text:<14}] -> {g['summary']}")
    print("=" * 80)

    return overall_exit


if __name__ == "__main__":
    sys.exit(main())
