"""Dump probe v29 output."""

import subprocess
import sys

run_id = sys.argv[1] if len(sys.argv) > 1 else "32785097546"
log = subprocess.run(
    ["gh", "run", "view", run_id, "--log"],
    capture_output=True,
    text=True,
    timeout=180,
).stdout
for ln in log.splitlines():
    body = ln.split("\t")[-1]
    if body.startswith(("OAC ROUTES:", "MFE:", "CTX:", "Traceback", "SyntaxError")):
        print(body.replace("\x1b[36;1m", "").replace("\x1b[0m", "").strip()[:700])
