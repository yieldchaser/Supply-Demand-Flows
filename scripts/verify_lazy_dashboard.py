"""End-to-end verification of the lazy-loading dashboard in headless Chromium.

Boots the real site from a local http.server, captures:
- boot console timing (core sources parsed)
- per-source lazy-load events
- panel render success/failure counts
"""

from __future__ import annotations

import http.server
import json
import socketserver
import threading
import functools
from pathlib import Path

from playwright.sync_api import sync_playwright

DOCS = Path("docs").resolve()
PORT = 8765


def main() -> None:
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(DOCS))
    httpd = socketserver.TCPServer(("127.0.0.1", PORT), handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()

    logs: list[str] = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            ctx = browser.new_context()
            page = ctx.new_page()
            page.on("console", lambda m: logs.append(f"{m.type}: {m.text}"))
            page.goto(f"http://127.0.0.1:{PORT}/index.html", wait_until="load")
            page.wait_for_timeout(15000)  # let all sections settle

            state = page.evaluate(
                """() => ({
                  title: document.title,
                  fleetCards: document.querySelectorAll('.fleet-card').length,
                  aggregateText: document.querySelector('.fleet-aggregate')?.innerText || null,
                  caveatPresent: !!document.querySelector('.fleet-footnote--caveat'),
                  errorPanels: Array.from(document.querySelectorAll('.panel-error')).length,
                  heroTitle: document.querySelector('.hero-terminal, .feedgas-hero h1, .panel-title')?.innerText || null,
                })"""
            )
            browser.close()
    finally:
        httpd.shutdown()

    print("== PAGE STATE ==")
    print(json.dumps(state, indent=2))
    print("\n== CONSOLE LOGS (bluetide/errors) ==")
    for line in logs:
        if "bluetide" in line or line.startswith("error") or "fail" in line.lower():
            print(" ", line)


if __name__ == "__main__":
    main()
