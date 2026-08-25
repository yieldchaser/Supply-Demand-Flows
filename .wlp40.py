"""Dump probe v26 output."""

import subprocess
import sys

run_id = sys.argv[1] if len(sys.argv) > 1 else "32783235674"
log = subprocess.run(
    ["gh", "run", "view", run_id, "--log"],
    capture_output=True,
    text=True,
    timeout=180,
).stdout
for ln in log.splitlines():
    body = ln.split("\t")[-1]
    if ("->" in body and ("json" in body.lower() or "html" in body.lower())) or body.startswith("   "):
        print(body.strip()[:500])
