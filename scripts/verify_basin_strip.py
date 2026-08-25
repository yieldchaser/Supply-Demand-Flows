"""Verify share-in-use + capacity lines render with REAL values (in-browser)."""
from __future__ import annotations

import functools
import http.server
import json
import socketserver
import threading
from pathlib import Path

from playwright.sync_api import sync_playwright

DOCS = Path("docs").resolve()
PORT = 8767


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
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto(f"http://127.0.0.1:{PORT}/index.html", wait_until="load")
            page.wait_for_timeout(16000)
            state = page.evaluate(
                """async () => {
                  const strip = document.querySelector('.basin-egress-congestion');
                  return {
                    stripHeader: strip?.querySelector('h3')?.innerText || null,
                    badge: strip?.querySelector('.congestion-status')?.innerText || null,
                    renderedRows: Array.from(document.querySelectorAll('.congestion-row')).map((r) =>
                      r.innerText.replace(/\\n/g, ' ')),
                    capLines: document.querySelectorAll('.basin-egress-wrapper svg path[stroke-dasharray]').length,
                    capLabels: Array.from(document.querySelectorAll('.basin-egress-wrapper svg text'))
                      .map((t) => t.textContent).filter((t) => t && t.startsWith('cap ')),
                    noteResidual: (strip?.querySelector('.congestion-note')?.innerText || '').includes('RESIDUAL'),
                    footnote: document.querySelector('.basin-egress-footnote')?.innerText.slice(0, 160) || null,
                  };
                }"""
            )
            browser.close()
    finally:
        httpd.shutdown()

    print(json.dumps(state, indent=2))
    if errors:
        print("PAGE ERRORS:", errors[:3])


if __name__ == "__main__":
    main()
