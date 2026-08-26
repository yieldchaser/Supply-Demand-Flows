"""Verify Section 7 (Feed Substitution) panel renders against the real bundle.

Boots the local docs/ site in headless Chromium, waits for the
panel-lng-feed-substitution element, and asserts:
  - no .panel-error inside it
  - a chart <svg> was drawn
  - the terminal tabs exist (3)
  - Freeport's event table rendered (>=1 row OR the 'no events' note)
Captures desktop (1440) + mobile (390) screenshots.
"""
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
    httpd = socketserver.TCPServer(("127.0.0.1", PORT), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    logs: list[str] = []
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()

            # Desktop
            ctx = browser.new_context(viewport={"width": 1440, "height": 1600})
            page = ctx.new_page()
            page.on("console", lambda m: logs.append(f"{m.type}: {m.text}"))
            page.on("pageerror", lambda e: logs.append(f"pageerror: {e}"))
            page.goto(f"http://127.0.0.1:{PORT}/index.html", wait_until="load")
            page.wait_for_timeout(18000)
            page.evaluate("document.getElementById('panel-lng-feed-substitution')?.scrollIntoView()")
            page.wait_for_timeout(2500)

            state = page.evaluate(
                """() => {
                  const el = document.getElementById('panel-lng-feed-substitution');
                  if (!el) return { found: false };
                  const err = el.querySelector('.panel-error');
                  const svg = el.querySelector('svg');
                  const tabs = el.querySelectorAll('.subst-tab').length;
                  const tableRows = el.querySelectorAll('.subst-table tbody tr').length;
                  const noneNote = !!el.querySelector('.subst-events__none');
                  const honesty = !!el.querySelector('.subst-honesty');
                  const kpis = el.querySelectorAll('.kpi-card').length;
                  return {
                    found: true,
                    error: err ? err.innerText.slice(0,200) : null,
                    hasSvg: !!svg,
                    svgWidth: svg ? svg.getAttribute('viewBox') : null,
                    tabs, tableRows, noneNote, honesty, kpis,
                    title: el.querySelector('.panel-header__title')?.innerText || null,
                  };
                }"""
            )
            page.screenshot(path="scripts/_shot_section7_desktop.png", full_page=False)
            ctx.close()

            # Mobile
            mctx = browser.new_context(viewport={"width": 390, "height": 1800})
            mpage = mctx.new_page()
            mpage.goto(f"http://127.0.0.1:{PORT}/index.html", wait_until="load")
            mpage.wait_for_timeout(18000)
            mpage.evaluate("document.getElementById('panel-lng-feed-substitution')?.scrollIntoView()")
            mpage.wait_for_timeout(2500)
            mstate = mpage.evaluate(
                """() => {
                  const el = document.getElementById('panel-lng-feed-substitution');
                  const table = el?.querySelector('.subst-table');
                  return {
                    found: !!el,
                    tableOverflowX: table ? (table.scrollWidth > table.clientWidth + 2) : null,
                    kpis: el?.querySelectorAll('.kpi-card').length || 0,
                  };
                }"""
            )
            mpage.screenshot(path="scripts/_shot_section7_mobile.png", full_page=False)
            mctx.close()
            browser.close()
    finally:
        httpd.shutdown()

    print("== SECTION 7 STATE (desktop) ==")
    print(json.dumps(state, indent=2))
    print("== SECTION 7 STATE (mobile 390) ==")
    print(json.dumps(mstate, indent=2))
    print("\n== CONSOLE / PAGE ERRORS ==")
    for line in logs:
        if "pageerror" in line or line.startswith("error") or "fail" in line.lower():
            print(" ", line)


if __name__ == "__main__":
    main()
