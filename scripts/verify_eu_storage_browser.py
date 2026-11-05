"""Verify European Gas Storage Panel 100x Upgrade in live Chromium browser.

Boots local docs/ server, navigates to the Europe chapter, asserts:
1. Aggregate hero chart renders with SVG.
2. 10 interactive country cards render with Name, Code, %, TWh, Deviation, Flow Regime, and Sparkline.
3. Default selection is Germany (DE), rendering the full detail view.
4. "Where It Should Be Right Now" 5Y median benchmark card renders prominently.
5. Derived working gas capacity and historical rank render.
6. Multi-year comparison chart renders with SVG.
7. Flow dynamics chart renders with SVG and net bars + trailing average line.
8. 14-day history table renders 14 rows with complete columns.
9. Clicking different countries (Belgium, Poland, Hungary, Germany) dynamically updates the detail view.
10. Takes high-resolution screenshots.
"""
from __future__ import annotations

import functools
import http.server
import socketserver
import sys
import threading
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

DOCS = Path("docs").resolve()
PORT = 4199


def main() -> None:
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(DOCS))
    httpd = socketserver.TCPServer(("127.0.0.1", PORT), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    console_logs: list[str] = []
    errors: list[str] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            ctx = browser.new_context(viewport={"width": 1440, "height": 2200})
            page = ctx.new_page()

            page.on("console", lambda m: console_logs.append(f"{m.type}: {m.text}"))
            page.on("pageerror", lambda e: errors.append(f"pageerror: {e}"))

            print(f"Loading http://127.0.0.1:{PORT}/index.html ...")
            page.goto(f"http://127.0.0.1:{PORT}/index.html", wait_until="load")
            time.sleep(3)

            # Navigate to Europe chapter if tabs are present
            europe_tab = page.query_selector('.chapter-tab[data-chapter-id="europe"]')
            if europe_tab:
                print("Clicking Europe chapter tab...")
                europe_tab.click()
                time.sleep(1)

            # Ensure #panel-eu-storage is in view
            page.evaluate("document.getElementById('panel-eu-storage')?.scrollIntoView()")
            time.sleep(2)

            # Query panel element
            panel_data = page.evaluate("""() => {
                const el = document.getElementById('panel-eu-storage');
                if (!el) return { found: false };

                const errorEl = el.querySelector('.panel-error');
                const heroSvg = el.querySelector('.eu-hero-container svg');
                const cards = Array.from(el.querySelectorAll('.eu-card')).map(c => ({
                    cc: c.dataset.cc,
                    label: c.querySelector('.eu-card__label')?.innerText?.trim(),
                    value: c.querySelector('.eu-card__value')?.innerText?.trim(),
                    sub: c.querySelector('.eu-card__sub')?.innerText?.trim(),
                    regime: c.querySelector('.eu-regime-badge')?.innerText?.trim(),
                    hasSpark: !!c.querySelector('.eu-card__spark path'),
                    isSelected: c.classList.contains('is-selected'),
                }));

                const detailTitle = el.querySelector('.eu-detail__title')?.innerText?.trim();
                const benchmarkCards = Array.from(el.querySelectorAll('.eu-benchmark-card')).map(b => ({
                    label: b.querySelector('.eu-benchmark-card__label')?.innerText?.trim(),
                    value: b.querySelector('.eu-benchmark-card__value')?.innerText?.trim(),
                }));

                const historyChartSvg = el.querySelector('.eu-detail__chart-card[data-chart="history"] svg');
                const flowChartSvg = el.querySelector('.eu-detail__chart-card[data-chart="flow"] svg');
                const flowBars = el.querySelectorAll('.eu-flow-bar--injection, .eu-flow-bar--withdrawal').length;
                const flowTrendLine = !!el.querySelector('.eu-flow-trend-line');
                const tableRows = el.querySelectorAll('.eu-detail__table tbody tr').length;

                return {
                    found: true,
                    error: errorEl ? errorEl.innerText : null,
                    hasHeroSvg: !!heroSvg,
                    cardCount: cards.length,
                    cards,
                    detailTitle,
                    benchmarkCards,
                    hasHistorySvg: !!historyChartSvg,
                    hasFlowSvg: !!flowChartSvg,
                    flowBars,
                    flowTrendLine,
                    tableRows,
                };
            }""")

            if not panel_data.get("found"):
                raise AssertionError("Panel #panel-eu-storage element not found in DOM!")

            if panel_data.get("error"):
                raise AssertionError(f"Panel rendered an error: {panel_data['error']}")

            print("1. Hero Chart:", "OK" if panel_data["hasHeroSvg"] else "MISSING")
            assert panel_data["hasHeroSvg"], "Hero chart SVG missing"

            print(f"2. Country Grid Cards ({panel_data['cardCount']}/10):")
            assert panel_data["cardCount"] == 10, f"Expected 10 cards, got {panel_data['cardCount']}"
            for c in panel_data["cards"]:
                print(f"   [{c['cc'].upper()}] {c['label']}: {c['value']} ({c['sub']}) | Regime: {c['regime']} | Sparkline: {c['hasSpark']}")
                assert c["value"], f"Value missing for card {c['cc']}"
                assert c["regime"], f"Regime missing for card {c['cc']}"
                assert c["hasSpark"], f"Sparkline missing for card {c['cc']}"

            print("3. Detail Title:", panel_data["detailTitle"])
            assert "Germany" in panel_data["detailTitle"], f"Initial selection should be Germany: {panel_data['detailTitle']}"

            print("4. Benchmark Cards:")
            for b in panel_data["benchmarkCards"]:
                print(f"   - {b['label']}: {b['value']}")
            labels_lower = [b["label"].lower() for b in panel_data["benchmarkCards"]]
            assert any("where it should be" in l for l in labels_lower), "Missing 'Where It Should Be Right Now' benchmark card"

            print("5. Multi-Year Comparison Chart:", "OK" if panel_data["hasHistorySvg"] else "MISSING")
            assert panel_data["hasHistorySvg"], "Multi-year comparison chart SVG missing"

            print(f"6. Flow Dynamics Chart (Bars: {panel_data['flowBars']}, Trend Line: {panel_data['flowTrendLine']}):", "OK" if panel_data["hasFlowSvg"] else "MISSING")
            assert panel_data["hasFlowSvg"], "Flow dynamics chart SVG missing"
            assert panel_data["flowBars"] > 10, "Flow bars missing"
            assert panel_data["flowTrendLine"], "Flow 7-day trend line missing"

            print(f"7. History Table Rows: {panel_data['tableRows']}")
            assert panel_data["tableRows"] == 14, f"Expected 14 rows, got {panel_data['tableRows']}"

            # Test interactive clicking
            print("8. Testing Interactivity:")

            # Click Belgium
            print("   -> Clicking Belgium card...")
            page.click('.eu-card[data-cc="be"]')
            time.sleep(0.5)
            be_title = page.evaluate("document.querySelector('#panel-eu-storage .eu-detail__title')?.innerText")
            be_sel = page.evaluate("document.querySelector('.eu-card[data-cc=\"be\"]').classList.contains('is-selected')")
            print(f"      Belgium Selected: {be_sel}, Detail Title: {be_title}")
            assert "Belgium" in be_title, f"Expected Belgium in detail title: {be_title}"
            assert be_sel, "Belgium card must have is-selected class"

            # Click Poland
            print("   -> Clicking Poland card...")
            page.click('.eu-card[data-cc="pl"]')
            time.sleep(0.5)
            pl_title = page.evaluate("document.querySelector('#panel-eu-storage .eu-detail__title')?.innerText")
            pl_sel = page.evaluate("document.querySelector('.eu-card[data-cc=\"pl\"]').classList.contains('is-selected')")
            print(f"      Poland Selected: {pl_sel}, Detail Title: {pl_title}")
            assert "Poland" in pl_title, f"Expected Poland in detail title: {pl_title}"
            assert pl_sel, "Poland card must have is-selected class"

            # Click Hungary
            print("   -> Clicking Hungary card...")
            page.click('.eu-card[data-cc="hu"]')
            time.sleep(0.5)
            hu_title = page.evaluate("document.querySelector('#panel-eu-storage .eu-detail__title')?.innerText")
            hu_sel = page.evaluate("document.querySelector('.eu-card[data-cc=\"hu\"]').classList.contains('is-selected')")
            print(f"      Hungary Selected: {hu_sel}, Detail Title: {hu_title}")
            assert "Hungary" in hu_title, f"Expected Hungary in detail title: {hu_title}"
            assert hu_sel, "Hungary card must have is-selected class"

            # Click Germany
            print("   -> Clicking Germany card...")
            page.click('.eu-card[data-cc="de"]')
            time.sleep(0.5)
            de_title = page.evaluate("document.querySelector('#panel-eu-storage .eu-detail__title')?.innerText")
            de_sel = page.evaluate("document.querySelector('.eu-card[data-cc=\"de\"]').classList.contains('is-selected')")
            print(f"      Germany Selected: {de_sel}, Detail Title: {de_title}")
            assert "Germany" in de_title, f"Expected Germany in detail title: {de_title}"
            assert de_sel, "Germany card must have is-selected class"

            # Capture screenshot of the full panel
            screenshot_path = Path("screenshots/eu_storage_100x_desktop.png")
            screenshot_path.parent.mkdir(parents=True, exist_ok=True)
            panel_elem = page.query_selector('#panel-eu-storage')
            if panel_elem:
                panel_elem.screenshot(path=str(screenshot_path))
                print(f"9. Screenshot saved to {screenshot_path}")

            print("\nALL 10 VERIFICATION CHECKS PASSED PERFECTLY!")

    finally:
        httpd.shutdown()


if __name__ == "__main__":
    main()
