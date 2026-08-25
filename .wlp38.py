"""Dump probe v24 output."""

import subprocess
import sys

run_id = sys.argv[1] if len(sys.argv) > 1 else "32782337851"
log = subprocess.run(
    ["gh", "run", "view", run_id, "--log"],
    capture_output=True,
    text=True,
    timeout=180,
).stdout
for ln in log.splitlines():
    body = ln.split("\t")[-1]
    if body.startswith(("CAPTURES:", "CAP ")) or "Traceback" in body:
        print(body.replace("\x1b[36;1m", "").replace("\x1b[0m", "").strip()[:300])
