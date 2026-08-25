"""Dump probe v21 output."""

import subprocess
import sys

run_id = sys.argv[1] if len(sys.argv) > 1 else "32781134802"
log = subprocess.run(
    ["gh", "run", "view", run_id, "--log"],
    capture_output=True,
    text=True,
    timeout=180,
).stdout
lines = log.splitlines()
idx = None
for i, ln in enumerate(lines):
    if "python /tmp/probe.py" in ln:
        idx = i
        break
if idx:
    for ln in lines[idx + 1 : idx + 12]:
        body = ln.split("\t")[-1]
        if any(
            k in body
            for k in ("CYCLE-IND:", "QUERY:", "BODY:", "Traceback", "deprecated", "Post ")
        ):
            continue
        print(body.strip()[:700])
else:
    print("marker not found")
