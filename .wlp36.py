"""Dump probe v22 output."""

import subprocess
import sys

run_id = sys.argv[1] if len(sys.argv) > 1 else "32781577721"
log = subprocess.run(
    ["gh", "run", "view", run_id, "--log"],
    capture_output=True,
    text=True,
    timeout=180,
).stdout
for ln in log.splitlines():
    body = ln.split("\t")[-1]
    if "->" in body and ("JSON" in body or "HTML" in body or "ERR" in body) and "httpx" not in body:
        print(body.strip()[:400])
