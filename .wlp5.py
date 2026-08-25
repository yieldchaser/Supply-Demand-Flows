"""Dump every scraper stdout JSON + HTTP lines from the latest williams run."""

import re
import subprocess

log = subprocess.run(
    ["gh", "run", "view", "32770532787", "--log"],
    capture_output=True,
    text=True,
    timeout=180,
).stdout
for ln in log.splitlines():
    body = ln.split("\t")[-1]
    body = body.replace("\x1b[36;1m", "").replace("\x1b[0m", "").rstrip()
    if "httpx" in body or '"status"' in body or "WARNING" in body or "ERROR" in body:
        print(body[:230])
