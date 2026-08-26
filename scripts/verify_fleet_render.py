"""Verify the live-rendered fleet aggregate + Cove Point card after the fix."""
from __future__ import annotations

import functools
import http.server
import json
import socketserver
import threading
from pathlib import Path

from playwright.sync_api import sync_playwright

DOCS = Path("docs").resolve()
PORT = 8771


def main() -> None:
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(DOCS))
    socketserver.TCPServer.allow_reuse_address = True
    httpd = socketserver.TCPServer(("127.0.0.1", PORT), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:160]))
            page.goto(f"http://127.0.0.1:{PORT}/index.html", wait_until="load")
            page.wait_for_timeout(15000)
            state = page.evaluate(
                """() => ({
                  aggregateText: document.querySelector('.fleet-aggregate')?.innerText || null,
                  coveCard: Array.from(document.querySelectorAll('.fleet-card, .lng-card'))
                    .map((c) => c.innerText.replace(/\\n/g, ' | '))
                    .filter((t) => t.includes('Cove'))[0] || null,
                  sabineCard: Array.from(document.querySelectorAll('.fleet-card, .lng-card'))
                    .map((c) => c.innerText.replace(/\\n/g, ' | '))
                    .filter((t) => t.includes('Sabine'))[0] || null,
                  errorPanels: document.querySelectorAll('.panel-error').length,
                })"""
            )
            browser.close()
    finally:
        httpd.shutdown()
    print(json.dumps(state, indent=2))
    if errors:
        print("PAGE ERRORS:", errors[:3])


if __name__ == "__main__":
    main()
