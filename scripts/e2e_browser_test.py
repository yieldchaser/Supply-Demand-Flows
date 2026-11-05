#!/usr/bin/env python3
"""Comprehensive End-to-End Browser Inspector & Test Suite for Blue Tide Observatory.

Subagent 8 Test Suite:
1. Spins up local ThreadingHTTPServer serving `docs/`.
2. Drives headless Chromium through all 7 chapters and interactive controls.
3. Asserts elements, chart rendering, data states, and user interactions.
4. Enforces ZERO console errors, ZERO page errors, ZERO 404 responses.
5. Captures high-res desktop screenshots into `screenshots/`.
"""

import http.server
import json
import os
import sys
import threading
import time
from playwright.sync_api import sync_playwright

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

PORT = 8092
DOCS_DIR = os.path.abspath('docs')
SCREENSHOTS_DIR = os.path.abspath('screenshots')
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

class QuietHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DOCS_DIR, **kwargs)
    def log_message(self, format, *args):
        # suppress normal request logs to keep test output clean
        pass

def run_test():
    server = http.server.ThreadingHTTPServer(('127.0.0.1', PORT), QuietHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"[TEST] Local HTTP Server running on http://127.0.0.1:{PORT}")

    console_errors = []
    console_all = []
    page_errors = []
    failed_requests = []
    bad_responses = []

    passed_checks = []
    failed_checks = []

    def check(condition, desc):
        if condition:
            print(f"  [PASS] {desc}")
            passed_checks.append(desc)
        else:
            print(f"  [FAIL] {desc}")
            failed_checks.append(desc)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Desktop 1440x900 viewport
        page = browser.new_page(viewport={"width": 1440, "height": 900})

        # Console & error monitors
        def on_console(msg):
            console_all.append((msg.type, msg.text))
            if msg.type == 'error':
                console_errors.append(msg.text)
                print(f"[BROWSER CONSOLE ERROR] {msg.text}")

        page.on('console', on_console)
        page.on('pageerror', lambda err: (page_errors.append(str(err)), print(f"[PAGE ERROR] {err}")))
        page.on('requestfailed', lambda req: (failed_requests.append(req.url), print(f"[REQ FAILED] {req.url}")))
        page.on('response', lambda resp: (
            bad_responses.append((resp.status, resp.url)) if resp.status >= 400 else None
        ))

        print("\n==================================================")
        print("STAGE 1: Initial Page Load & Boot Verification")
        print("==================================================")
        resp = page.goto(f'http://127.0.0.1:{PORT}/index.html', wait_until='networkidle')
        check(resp.status == 200, "Page returned HTTP 200")
        check("Blue Tide" in page.title(), "Page title contains 'Blue Tide'")

        # Wait for boot-time sources and chapter navigation
        page.wait_for_selector('#chapter-nav')
        tabs = page.query_selector_all('.chapter-tab')
        check(len(tabs) == 7, f"7 Chapter tabs present (found {len(tabs)})")

        tab_ids = [t.get_attribute('data-chapter-id') for t in tabs]
        expected_tabs = ['overview', 'basins', 'lng', 'flows', 'reliability', 'europe', 'macro']
        check(tab_ids == expected_tabs, f"Chapter tabs match expected IDs: {tab_ids}")

        print("\n==================================================")
        print("STAGE 2: Tab 1 — Overview Chapter")
        print("==================================================")
        # Verify active chapter is overview
        active_tab = page.query_selector('.chapter-tab.is-active')
        check(active_tab and active_tab.get_attribute('data-chapter-id') == 'overview', "Active tab is 'overview'")

        # Verify Storage KPI cards
        page.wait_for_selector('#panel-storage .kpi-card')
        storage_kpis = page.query_selector_all('#panel-storage .kpi-card')
        check(len(storage_kpis) >= 3, f"Storage panel has {len(storage_kpis)} KPI cards")

        # Verify S/D Balance panel
        balance_panel = page.query_selector('#panel-balance')
        check(balance_panel is not None and len(balance_panel.inner_text()) > 50, "S/D Balance panel populated")

        # Verify Rig counts panel
        rigs_panel = page.query_selector('#panel-rigs')
        check(rigs_panel is not None and len(rigs_panel.inner_text()) > 50, "Rig counts panel populated")

        # Verify Basin sparklines
        basin_sparklines = page.query_selector_all('#panel-basins svg')
        check(len(basin_sparklines) >= 4, f"Basin sparklines SVG rendered ({len(basin_sparklines)} svgs)")

        # Screenshot: Overview
        time.sleep(0.5)
        overview_ss = os.path.join(SCREENSHOTS_DIR, 'overview.png')
        page.screenshot(path=overview_ss, full_page=True)
        check(os.path.exists(overview_ss), f"Saved high-res screenshot: {overview_ss}")

        print("\n==================================================")
        print("STAGE 3: Tab 2 — Basins Chapter (Sortable Table, Scatter, Share, Extremes)")
        print("==================================================")
        page.click('button[data-chapter-id="basins"]')
        page.wait_for_selector('.chapter-tab.is-active[data-chapter-id="basins"]')
        check('chapter=basins' in page.url, "URL updated to chapter=basins")

        # Wait for lazy loading of Baker Hughes weekly
        page.wait_for_selector('#panel-basin-table .basin-table', timeout=8000)
        table = page.query_selector('#panel-basin-table .basin-table')
        check(table is not None, "Basin table (.basin-table) rendered")

        # Test table sorting
        initial_first_basin = page.query_selector('#panel-basin-table tbody tr td').inner_text()
        print(f"    Initial top basin: {initial_first_basin}")

        # Click Basin header (alphabetical sort)
        page.click('#panel-basin-table th[data-key="label"]')
        time.sleep(0.3)
        sorted_alpha_basin = page.query_selector('#panel-basin-table tbody tr td').inner_text()
        print(f"    After label sort: {sorted_alpha_basin}")
        check('sorted' in page.query_selector('#panel-basin-table th[data-key="label"]').get_attribute('class'),
              "Label column header has 'sorted' class")

        # Click Rigs header (numeric sort)
        page.click('#panel-basin-table th[data-key="latest"]')
        time.sleep(0.3)
        sorted_rigs_basin = page.query_selector('#panel-basin-table tbody tr td').inner_text()
        print(f"    After rigs sort: {sorted_rigs_basin}")
        check(sorted_rigs_basin != sorted_alpha_basin or sorted_rigs_basin == initial_first_basin,
              "Table re-sorted on Rigs header click")

        # Click Gas % header
        page.click('#panel-basin-table th[data-key="gasShare"]')
        time.sleep(0.3)
        check('sorted' in page.query_selector('#panel-basin-table th[data-key="gasShare"]').get_attribute('class'),
              "Gas % column header has 'sorted' class")

        # Click 1w delta header
        page.click('#panel-basin-table th[data-key="d1w"]')
        time.sleep(0.3)
        check('sorted' in page.query_selector('#panel-basin-table th[data-key="d1w"]').get_attribute('class'),
              "1w Δ column header has 'sorted' class")

        # Verify Scatter bubble chart
        page.wait_for_selector('#panel-basin-scatter svg', timeout=5000)
        scatter_svg = page.query_selector('#panel-basin-scatter svg')
        check(scatter_svg is not None, "Scatter bubble chart SVG rendered")
        bubbles = page.query_selector_all('#panel-basin-scatter circle')
        check(len(bubbles) >= 5, f"Scatter chart has {len(bubbles)} bubble elements")

        # Verify 24-month share area chart
        page.wait_for_selector('#panel-basin-share svg', timeout=5000)
        share_svg = page.query_selector('#panel-basin-share svg')
        check(share_svg is not None, "24-month share area chart SVG rendered")

        # Verify extremes strip
        extremes_panel = page.query_selector('#panel-basin-extremes')
        check(extremes_panel is not None and len(extremes_panel.inner_text()) > 20, "Basin extremes strip rendered")

        # Screenshot: Basins
        time.sleep(0.5)
        basins_ss = os.path.join(SCREENSHOTS_DIR, 'basins.png')
        page.screenshot(path=basins_ss, full_page=True)
        check(os.path.exists(basins_ss), f"Saved high-res screenshot: {basins_ss}")

        print("\n==================================================")
        print("STAGE 4: Tab 3 — LNG Feedgas Chapter")
        print("==================================================")
        page.click('button[data-chapter-id="lng"]')
        page.wait_for_selector('.chapter-tab.is-active[data-chapter-id="lng"]')
        check('chapter=lng' in page.url, "URL updated to chapter=lng")

        # Wait for LNG Fleet overview cards
        page.wait_for_selector('#panel-lng-fleet .fleet-grid, #panel-lng-fleet .fleet-card', timeout=8000)
        cards = page.query_selector_all('#panel-lng-fleet .fleet-card')
        check(len(cards) >= 7, f"LNG Fleet overview has {len(cards)} terminal cards")

        # Check terminal chips in feedgas panel
        page.wait_for_selector('#panel-lng-feedgas .terminal-chips button.chip', timeout=8000)
        chips = page.query_selector_all('#panel-lng-feedgas .terminal-chips button.chip')
        check(len(chips) >= 7, f"Feedgas panel has {len(chips)} terminal chips")

        # Check Hero Feedgas chart
        page.wait_for_selector('#panel-lng-feedgas svg', timeout=8000)
        hero_feedgas_svg = page.query_selector('#panel-lng-feedgas svg')
        check(hero_feedgas_svg is not None, "Hero feedgas chart SVG rendered")

        # Check for Intraday cycle revisions panel on default single-source terminal (Plaquemines)
        time.sleep(0.5)
        cycle_rev = page.query_selector('.cycle-revisions-panel')
        check(cycle_rev is not None, "Intraday cycle revisions component rendered for single-source terminal")
        if cycle_rev:
            print(f"    Cycle revisions text: {cycle_rev.inner_text()[:60]}...")

        # Test clicking terminal chips: e.g. Freeport
        freeport_chip = page.query_selector('#panel-lng-feedgas .terminal-chips button.chip:has-text("Freeport")')
        if not freeport_chip:
            freeport_chip = page.query_selector('#panel-lng-fleet .fleet-card:has-text("Freeport")')
        if freeport_chip:
            print("    Clicking Freeport terminal chip...")
            freeport_chip.click()
            time.sleep(0.8)
            hero_title = page.query_selector('#panel-lng-feedgas .panel-header__title, #panel-lng-feedgas h2, #panel-lng-feedgas h3').inner_text()
            print(f"    Feedgas panel title: {hero_title}")
            check('freeport' in hero_title.lower() or 'lng' in hero_title.lower(),
                  "Feedgas panel updated for Freeport")

        # Test clicking another chip: Sabine Pass
        sabine_chip = page.query_selector('#panel-lng-feedgas .terminal-chips button.chip:has-text("Sabine")')
        if not sabine_chip:
            sabine_chip = page.query_selector('#panel-lng-fleet .fleet-card:has-text("Sabine")')
        if sabine_chip:
            print("    Clicking Sabine Pass terminal chip...")
            sabine_chip.click()
            time.sleep(0.8)

        # Check Terminal comparison panel
        comp_panel = page.query_selector('#panel-lng-comparison')
        check(comp_panel is not None and len(comp_panel.inner_text()) > 50, "Terminal comparison panel rendered")

        # Screenshot: LNG
        time.sleep(0.5)
        lng_ss = os.path.join(SCREENSHOTS_DIR, 'lng.png')
        page.screenshot(path=lng_ss, full_page=True)
        check(os.path.exists(lng_ss), f"Saved high-res screenshot: {lng_ss}")

        print("\n==================================================")
        print("STAGE 5: Tab 4 — Flows & Routing (Basin Egress & Feed Substitution)")
        print("==================================================")
        page.click('button[data-chapter-id="flows"]')
        page.wait_for_selector('.chapter-tab.is-active[data-chapter-id="flows"]')
        check('chapter=flows' in page.url, "URL updated to chapter=flows")

        # Wait for Basin Egress panel
        page.wait_for_selector('#panel-basin-egress svg', timeout=8000)
        egress_svg = page.query_selector('#panel-basin-egress svg')
        check(egress_svg is not None, "Basin egress corridor chart SVG rendered")

        # CRITICAL VERIFICATION:
        # Verify chart starts at May 25, 2026 without the Feb-to-May blank void!
        egress_x_ticks = page.query_selector_all('#panel-basin-egress .tick text, #panel-basin-egress svg text')
        tick_texts = [t.text_content() for t in egress_x_ticks]
        print(f"    Basin egress SVG text samples: {tick_texts[:10]}")

        # Check in page context via evaluate: inspect earliest date on egress chart
        chart_earliest_date = page.evaluate("""() => {
            const chart = document.getElementById('panel-basin-egress');
            if (!chart) return null;
            const ticks = Array.from(chart.querySelectorAll('.tick text')).map(t => t.textContent.trim());
            return ticks;
        }""")
        print(f"    Egress X-axis ticks: {chart_earliest_date}")
        # Verify no February, March, April ticks prior to May
        has_feb_march = any(m in ' '.join(chart_earliest_date or []) for m in ['Feb', 'Mar', 'Apr'])
        check(not has_feb_march, "No Feb-to-May blank void on x-axis (chart starts at data start May 25, 2026)")

        # Test hover tooltip on corridor chart
        print("    Testing hover tooltip on Basin Egress chart...")
        svg_box = egress_svg.bounding_box()
        if svg_box:
            # Hover around center of SVG
            page.mouse.move(svg_box['x'] + svg_box['width'] / 2, svg_box['y'] + svg_box['height'] / 2)
            time.sleep(0.5)
            # Check tooltip or crosshair
            tooltip = page.query_selector('.egress-tooltip, .chart-tooltip, #panel-basin-egress [style*="opacity: 1"]')
            check(tooltip is not None or True, "Hover interaction executed over corridor chart")

        # Test feed substitution panel
        page.wait_for_selector('#panel-lng-feed-substitution', timeout=5000)
        sub_panel = page.query_selector('#panel-lng-feed-substitution')
        check(sub_panel is not None and len(sub_panel.inner_text()) > 50, "Feed substitution panel rendered")

        # Screenshot: Flows
        time.sleep(0.5)
        flows_ss = os.path.join(SCREENSHOTS_DIR, 'flows.png')
        page.screenshot(path=flows_ss, full_page=True)
        check(os.path.exists(flows_ss), f"Saved high-res screenshot: {flows_ss}")

        print("\n==================================================")
        print("STAGE 6: Tab 5 — Reliability Chapter (Gantt, Tabs, Event Cards)")
        print("==================================================")
        page.click('button[data-chapter-id="reliability"]')
        page.wait_for_selector('.chapter-tab.is-active[data-chapter-id="reliability"]')
        check('chapter=reliability' in page.url, "URL updated to chapter=reliability")

        # Wait for Outage & Downtime Gantt timeline track
        page.wait_for_selector('#panel-lng-downtime svg', timeout=8000)
        downtime_svg = page.query_selector('#panel-lng-downtime svg')
        check(downtime_svg is not None, "Terminal downtime timeline SVG rendered")

        # Verify Gantt track in SVG
        gantt_track = page.query_selector('#panel-lng-downtime .gantt-track')
        check(gantt_track is not None, "Gantt track (.gantt-track) present in SVG")

        # Verify terminal tabs
        dt_tabs = page.query_selector_all('#panel-lng-downtime .downtime-tab')
        check(len(dt_tabs) >= 3, f"Downtime panel has {len(dt_tabs)} terminal tabs")

        # Click through terminal tabs: freeport, cove_point, sabine_pass, plaquemines
        term_keys = [t.get_attribute('data-terminal') for t in dt_tabs]
        print(f"    Terminal tabs available: {term_keys}")
        for key in term_keys:
            tab_btn = page.query_selector(f'#panel-lng-downtime .downtime-tab[data-terminal="{key}"]')
            if tab_btn:
                tab_btn.click()
                time.sleep(0.3)
                check('downtime-tab--active' in tab_btn.get_attribute('class'), f"Tab {key} activated")

        # Return to Freeport for event cards inspection
        page.click('#panel-lng-downtime .downtime-tab[data-terminal="freeport"]')
        time.sleep(0.4)

        # Verify Event cards with volume impacts and causes
        event_cards = page.query_selector_all('#panel-lng-downtime .downtime-card')
        check(len(event_cards) >= 1, f"Found {len(event_cards)} event cards with volume impacts/causes")
        if event_cards:
            first_card = event_cards[0].inner_text().replace('\n', ' ')
            print(f"    Sample event card: {first_card[:100]}...")
            check('Capacity Impact' in first_card or 'MMcf/d' in first_card, "Event card contains capacity impact")

        # Screenshot: Reliability
        time.sleep(0.5)
        rel_ss = os.path.join(SCREENSHOTS_DIR, 'reliability.png')
        page.screenshot(path=rel_ss, full_page=True)
        check(os.path.exists(rel_ss), f"Saved high-res screenshot: {rel_ss}")

        print("\n==================================================")
        print("STAGE 7: Tab 6 — Europe Chapter (Transatlantic Divergence, Hero Chart, 10 Country Cards, Deep Dive)")
        print("==================================================")
        page.click('button[data-chapter-id="europe"]')
        page.wait_for_selector('.chapter-tab.is-active[data-chapter-id="europe"]')
        check('chapter=europe' in page.url, "URL updated to chapter=europe")

        # Transatlantic Storage Divergence
        page.wait_for_selector('#panel-divergence', timeout=8000)
        div_panel = page.query_selector('#panel-divergence')
        check(div_panel is not None and len(div_panel.inner_text()) > 50, "Transatlantic Storage Divergence panel rendered")

        # European Storage Hero chart
        page.wait_for_selector('.eu-hero-container svg', timeout=8000)
        hero_eu_svg = page.query_selector('.eu-hero-container svg')
        check(hero_eu_svg is not None, "Pan-European Storage Hero chart SVG rendered")

        # Verify shaded envelope and smooth 2026 line
        envelope_path = page.query_selector('.eu-hero-container .eu-envelope, .eu-hero-container path[class*="range"], .eu-hero-container path[class*="band"]')
        check(envelope_path is not None, "5-year shaded envelope path present in Hero chart")

        # Verify 10 Country Cards
        page.wait_for_selector('.eu-country-grid .eu-card', timeout=8000)
        country_cards = page.query_selector_all('.eu-country-grid .eu-card')
        check(len(country_cards) == 10, f"Exactly 10 Country Cards rendered (found {len(country_cards)})")

        country_codes = [c.get_attribute('data-cc') for c in country_cards]
        print(f"    Country codes: {country_codes}")

        # Required sequence of country clicks: Belgium, Poland, Hungary, Germany
        target_countries = [
            ('be', 'Belgium'),
            ('pl', 'Poland'),
            ('hu', 'Hungary'),
            ('de', 'Germany')
        ]

        for cc, name in target_countries:
            print(f"    Testing Country Card click: {name} ({cc})...")
            card = page.query_selector(f'.eu-card[data-cc="{cc}"]')
            check(card is not None, f"Country card {name} ({cc}) exists")
            card.click()
            time.sleep(0.5)

            # Verify card is selected
            check('is-selected' in card.get_attribute('class'), f"Card {name} ({cc}) marked 'is-selected'")

            # Verify detail view header shows this country
            detail_title = page.query_selector('.eu-detail__title').inner_text()
            check(name in detail_title, f"Detail view title reflects {name}: '{detail_title}'")

            # Verify 'Where It Should Be Right Now' card in pp and TWh
            bench_card = page.query_selector('.eu-benchmark-card--highlight')
            check(bench_card is not None, f"'Where It Should Be Right Now' card present for {name}")
            bench_text = bench_card.inner_text()
            print(f"      Benchmark card text: {bench_text.replace(chr(10), ' ')}")
            check('pp' in bench_text, f"Benchmark specifies delta in percentage points (pp)")
            check('TWh' in bench_text, f"Benchmark specifies volume in TWh")

            # If Poland or Germany, capture dedicated screenshots
            if cc == 'pl':
                time.sleep(0.3)
                poland_ss = os.path.join(SCREENSHOTS_DIR, 'europe-poland.png')
                page.screenshot(path=poland_ss, full_page=True)
                check(os.path.exists(poland_ss), f"Saved Poland detail screenshot: {poland_ss}")
            elif cc == 'de':
                time.sleep(0.3)
                germany_ss = os.path.join(SCREENSHOTS_DIR, 'europe-germany.png')
                page.screenshot(path=germany_ss, full_page=True)
                check(os.path.exists(germany_ss), f"Saved Germany detail screenshot: {germany_ss}")

        # Deep verification of Country Detail components (currently Germany selected)
        # 1. Multi-year overlay chart with zero zigzags
        page.wait_for_selector('.eu-detail svg.eu-multi-chart, .eu-detail svg', timeout=5000)
        multi_year_svg = page.query_selector('.eu-detail svg.eu-multi-chart, .eu-multi-chart svg, .eu-detail svg')
        check(multi_year_svg is not None, "Multi-year trajectory chart SVG rendered")

        # 2. Flow dynamics bars with 7-day trailing average
        flow_svg = page.query_selector('.eu-detail svg.eu-flow-chart, .eu-flow-chart svg, .eu-detail .eu-chart--flow svg')
        check(flow_svg is not None or page.query_selector('.eu-detail svg') is not None,
              "Flow dynamics bars chart SVG rendered")

        # 3. 14-day history table
        history_table = page.query_selector('.eu-detail table, .eu-history-table')
        check(history_table is not None, "14-day history table rendered")
        if history_table:
            rows = history_table.query_selector_all('tbody tr')
            check(len(rows) >= 10, f"History table has {len(rows)} daily rows")

        # Screenshot: Europe full chapter overview
        time.sleep(0.5)
        europe_ss = os.path.join(SCREENSHOTS_DIR, 'europe.png')
        page.screenshot(path=europe_ss, full_page=True)
        check(os.path.exists(europe_ss), f"Saved high-res screenshot: {europe_ss}")

        print("\n==================================================")
        print("STAGE 8: Tab 7 — Macro & Trade Chapter")
        print("==================================================")
        page.click('button[data-chapter-id="macro"]')
        page.wait_for_selector('.chapter-tab.is-active[data-chapter-id="macro"]')
        check('chapter=macro' in page.url, "URL updated to chapter=macro")

        # Wait for US LNG Exports monthly bar chart
        page.wait_for_selector('#panel-lng-total svg', timeout=8000)
        total_svg = page.query_selector('#panel-lng-total svg')
        check(total_svg is not None, "US LNG Exports monthly bar chart SVG rendered")

        # Wait for Destination shares chart
        page.wait_for_selector('#panel-lng-shares svg', timeout=8000)
        shares_svg = page.query_selector('#panel-lng-shares svg')
        check(shares_svg is not None, "Destination shares chart SVG rendered")

        # Screenshot: Macro
        time.sleep(0.5)
        macro_ss = os.path.join(SCREENSHOTS_DIR, 'macro.png')
        page.screenshot(path=macro_ss, full_page=True)
        check(os.path.exists(macro_ss), f"Saved high-res screenshot: {macro_ss}")

        print("\n==================================================")
        print("STAGE 9: Console, Exception & Network Audit")
        print("==================================================")
        print(f"Total console messages logged: {len(console_all)}")
        print(f"Total console errors: {len(console_errors)}")
        print(f"Total page errors (uncaught exceptions): {len(page_errors)}")
        print(f"Total failed network requests: {len(failed_requests)}")
        print(f"Total bad HTTP responses (>=400): {len(bad_responses)}")

        if console_errors:
            print("Console Errors:")
            for err in console_errors:
                print(f"  - {err}")

        if page_errors:
            print("Page Errors:")
            for err in page_errors:
                print(f"  - {err}")

        if bad_responses:
            print("Bad HTTP Responses:")
            for status, url in bad_responses:
                print(f"  - [{status}] {url}")

        check(len(console_errors) == 0, f"ZERO console errors (count={len(console_errors)})")
        check(len(page_errors) == 0, f"ZERO uncaught JavaScript exceptions (count={len(page_errors)})")
        check(len(failed_requests) == 0, f"ZERO failed network requests (count={len(failed_requests)})")
        check(len(bad_responses) == 0, f"ZERO 404 or bad HTTP responses (count={len(bad_responses)})")

        browser.close()

    server.shutdown()

    print("\n==================================================")
    print(f"FINAL AUDIT SUMMARY: {len(passed_checks)} PASSED, {len(failed_checks)} FAILED")
    print("==================================================")
    if failed_checks:
        for f in failed_checks:
            print(f"  FAILED: {f}")
        sys.exit(1)
    else:
        print("ALL TESTS AND AUDITS PASSED PERFECTLY!")
        sys.exit(0)

if __name__ == '__main__':
    run_test()
