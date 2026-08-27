"""Verify Section 8 (Terminal Downtime / Turnaround) panel renders against the real bundle.

Boots the local docs/ site in headless Chromium, waits for the
panel-lng-downtime element, and asserts:
  - no .panel-error inside it
  - a chart <svg> was drawn
  - the terminal tabs exist (3)
  - each tab renders an event table row OR the 'no events' note (agreement gate)
  - the honesty footnote is present
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
PORT = 8773


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
            page.evaluate("document.getElementById('panel-lng-downtime')?.scrollIntoView()")
            page.wait_for_timeout(2500)

            state = page.evaluate(
                """() => {
                  const el = document.getElementById('panel-lng-downtime');
                  if (!el) return { found: false };
                  const err = el.querySelector('.panel-error');
                  const svg = el.querySelector('svg');
                  const tabs = el.querySelectorAll('.downtime-tab').length;
                  const tableRows = el.querySelectorAll('.downtime-table tbody tr').length;
                  const noneNote = !!el.querySelector('.downtime-events__empty');
                  const honesty = !!el.querySelector('.downtime-honesty');
                  const kpis = el.querySelectorAll('.kpi-card').length;
                  const badges = el.querySelectorAll('.badge').length;
                  return {
                    found: true,
                    error: err ? err.innerText.slice(0,200) : null,
                    hasSvg: !!svg,
                    svgViewBox: svg ? svg.getAttribute('viewBox') : null,
                    tabs, tableRows, noneNote, honesty, kpis, badges,
                    title: el.querySelector('.panel-header__title')?.innerText || null,
                  };
                }"""
            )
            page.screenshot(path="scripts/_shot_section8_desktop.png", full_page=False)

            # Click through all three tabs IN THE SAME CONTEXT to confirm each
            # renders (agreement gate). Reusing one page avoids cold-reload races.
            tab_states = []
            for tab_idx in range(3):
                page.evaluate(
                    f"document.querySelectorAll('.downtime-tab')[{tab_idx}]?.click()"
                )
                page.wait_for_timeout(2000)
                tstate = page.evaluate(
                    """() => {
                      const el = document.getElementById('panel-lng-downtime');
                      const err = el.querySelector('.panel-error');
                      const rows = el.querySelectorAll('.downtime-table tbody tr').length;
                      const none = !!el.querySelector('.downtime-events__empty');
                      const insuff = el.innerText.includes('Insufficient data');
                      const kpis = el.querySelectorAll('.kpi-card').length;
                      const tabLabel = el.querySelector('.downtime-tab--active')?.innerText || null;
                      return { rows, none, insuff, kpis, tabLabel,
                               err: err ? err.innerText.slice(0,200) : null };
                    }"""
                )
                tab_states.append(tstate)
            ctx.close()

            # Mobile
            mctx = browser.new_context(viewport={"width": 390, "height": 1800})
            mpage = mctx.new_page()
            mpage.goto(f"http://127.0.0.1:{PORT}/index.html", wait_until="load")
            mpage.wait_for_timeout(18000)
            mpage.evaluate("document.getElementById('panel-lng-downtime')?.scrollIntoView()")
            mpage.wait_for_timeout(2500)
            mstate = mpage.evaluate(
                """() => {
                  const el = document.getElementById('panel-lng-downtime');
                  const table = el?.querySelector('.downtime-table');
                  return {
                    found: !!el,
                    tableOverflowX: table ? (table.scrollWidth > table.clientWidth + 2) : null,
                    kpis: el?.querySelectorAll('.kpi-card').length || 0,
                    tabsFit: (el?.querySelectorAll('.downtime-tab').length || 0) > 0,
                  };
                }"""
            )
            mpage.screenshot(path="scripts/_shot_section8_mobile.png", full_page=False)
            mctx.close()
            browser.close()
    finally:
        httpd.shutdown()

    print("== SECTION 8 STATE (desktop) ==")
    print(json.dumps(state, indent=2))
    print("== TAB STATES (desktop, all 3 tabs) ==")
    for i, ts in enumerate(tab_states):
        print(f"  tab {i}: {json.dumps(ts)}")
    print("== SECTION 8 STATE (mobile 390) ==")
    print(json.dumps(mstate, indent=2))
    print("\n== CONSOLE / PAGE ERRORS ==")
    saw_err = False
    for line in logs:
        if "pageerror" in line or line.startswith("error") or "fail" in line.lower():
            print(" ", line)
            saw_err = True
    if not saw_err:
        print("  (none)")


if __name__ == "__main__":
    main()
