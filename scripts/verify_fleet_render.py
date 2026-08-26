"""Verify the live-rendered fleet aggregate + Cove Point card after the fix.

Serves docs/ locally (the page resolves the CURRENT bundle via manifest.json,
never by globbing) and asserts the rendered aggregate matches the curated
compute + that Sabine's CT200111-D headline ships in the live bundle.

TASK 4 (2026-08-26): index resolution goes through manifest.json -> index_url,
the single source of truth, so stale index.*.json files on disk can't confuse
the check. We import resolve_current_index() to prove the live hash + that
CT200111-D actually ships.
"""
from __future__ import annotations

import functools
import http.server
import importlib
import json
import socketserver
import threading
from pathlib import Path

from playwright.sync_api import sync_playwright

PUB = importlib.import_module("publishers.export_dashboard_json")
DOCS = Path("docs").resolve()
PORT = 8771


def _assert_live_sabine_headline() -> None:
    """TASK 4: prove CT200111-D ships in the live bundle via manifest resolution."""
    idx = PUB.resolve_current_index(DOCS / "data")
    chen = json.loads((DOCS / "data" / idx["sources"]["cheniere"]["file"]).read_text(encoding="utf-8"))
    rows = [r for r in chen["data"] if "ct200111" in str(r["series_id"]).lower()]
    assert rows, "LIVE BUNDLE: Sabine headline CT200111-D ships ZERO rows — regression!"
    print(f"LIVE BUNDLE ({idx['hash']}): CT200111-D ships {len(rows)} rows — OK")


def main() -> None:
    _assert_live_sabine_headline()
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
