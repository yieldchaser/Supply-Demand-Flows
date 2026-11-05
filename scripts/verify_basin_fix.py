"""Verify basin egress chart rendering and hover interaction."""
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
    socketserver.TCPServer.allow_reuse_address = True
    httpd = socketserver.TCPServer(("127.0.0.1", PORT), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            page = browser.new_page()
            page.goto(f"http://127.0.0.1:{PORT}/index.html?chapter=flows", wait_until="networkidle")
            page.wait_for_timeout(4000)

            chart_info = page.evaluate("""() => {
                const wrap = document.querySelector('.basin-egress-wrapper');
                const svg = wrap ? wrap.querySelector('.panel-chart svg') : null;
                if (!svg) return { error: 'No svg found' };
                const texts = Array.from(svg.querySelectorAll('text')).map(t => t.textContent.trim());
                const paths = Array.from(svg.querySelectorAll('path')).map(p => ({
                    fill: p.getAttribute('fill'),
                    stroke: p.getAttribute('stroke'),
                    dasharray: p.getAttribute('stroke-dasharray'),
                    dLength: (p.getAttribute('d') || '').length
                }));
                const callout = svg.querySelector('.latest-callout text')?.textContent || null;
                return {
                    texts: texts,
                    pathCount: paths.length,
                    callout: callout,
                    areaBands: paths.filter(p => p.fill && p.fill !== 'none').length,
                    dashedLines: paths.filter(p => p.dasharray).length,
                    monthTicks: texts.filter(t => ['Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Jan'].includes(t))
                };
            }""")
            print("Chart info:")
            print(json.dumps(chart_info, indent=2))

            # Test hover interaction
            rect = page.locator('.basin-egress-wrapper .panel-chart svg rect[pointer-events="all"]')
            if rect.count() > 0:
                rect.hover(position={"x": 100, "y": 100})
                page.wait_for_timeout(500)
                tt_info = page.evaluate("""() => {
                    const tt = document.querySelector('.basin-egress-wrapper .panel-chart .chart-tooltip');
                    const cross = document.querySelector('.basin-egress-wrapper .panel-chart svg line[stroke-dasharray="2,2"]');
                    return {
                        tooltipFound: !!tt,
                        tooltipOpacity: tt ? window.getComputedStyle(tt).opacity : null,
                        tooltipText: tt ? tt.innerText.replace(/\\n/g, ' | ') : null,
                        crosshairOpacity: cross ? window.getComputedStyle(cross).opacity : null,
                        crosshairX1: cross ? cross.getAttribute('x1') : null
                    };
                }""")
                print("Tooltip info on hover:")
                print(json.dumps(tt_info, indent=2))

            browser.close()
    finally:
        httpd.shutdown()

if __name__ == "__main__":
    main()
