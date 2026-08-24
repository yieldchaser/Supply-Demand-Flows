# RECON — Measured Feedgas Paths for Sabine Pass & Corpus Christi LNG

**Date:** 2026-08-24 · **Agent:** recon-only · **Clone:** `bt-recon-sabine` · **Duration:** ~50 min
**Verdict up front: YES for Sabine Pass (measured, anonymous, live-verified) — PARTIAL/YES for Corpus Christi (measured TGP meter + measured NMP Nueces meter; combined they plausibly cover a majority of CCPL feedgas). KM's `pipeline2` EBB serves OAC *and* TSQ to anonymous users. The Elba Express "empty" result was NOT an auth wall — it was the wrong tenant code: unknown codes silently fall back to the TGP page.**

---

## 1. The decisive finding — Q4 answered

`pipeline2.kindermorgan.com` **does serve OAC and Total Scheduled Quantity unauthenticated**, via an ASP.NET WebForms image-button postback. Cold HTTP client proof:

1. `GET /Capacity/OpAvailPoint.aspx?code=NGPL` → 200, 167 KB — but the grid is an **empty shell** (0 data rows). This is exactly what the TETCO/Elba agent saw and misread as an auth wall.
2. Harvesting all `<input>` fields from that GET (`__VIEWSTATE`, `__VIEWSTATEGENERATOR`, `__EVENTVALIDATION`, Infragistics `clientState` hiddens) and POSTing them back to the same URL with the retrieve button's name as an image-button coordinate pair:

```
POST https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=NGPL
Content-Type: application/x-www-form-urlencoded   (cookies: plain ASP.NET session)

ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve.x=10
ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve.y=10
+ every <input name=value> echoed verbatim from the GET
```

→ **200, ~320 KB, full data grid server-rendered**: columns `Loc · Loc Name · Loc Zn · Loc (Segment) · Design Capacity · Operating Capacity · Total Scheduled Quantity · Operationally Available Capacity · IT · Flow Ind · All Qty Avail · Qty Reason`. 59 NGPL meters, 75 TGP meters, 15 KMLP meters retrieved this way.

**Why Elba Express looked empty:** `?code=ELBA` (and EEPL/EEX/ELBAEXPRESS…) does not 404 — the site **silently falls back to the Tennessee Gas Pipeline page** (title proves it). Elba Express does not publish on pipeline2 at all; the previous agent queried TGP's grid with no Elba-specific filter and got whatever TGP returned. Auth was never the issue.

Confidence: **very high** — reproduced across NGPL/TGP/KMLP in one session, cold client, no cookies beyond the anonymous session the GET sets, no login, no JS executed.

## 2. Sabine Pass — MEASURED path confirmed

### Primary: NGPL loc **3592** — `SABPL/NGPL HENRY HUB VERMILION`

Live capture (BEST AVAILABLE cycle, gas day ≈ 2026-08-22):

| loc | name | zone | design | operating | **TSQ** | OAC | flow_ind |
|---|---|---|---|---|---|---|---|
| 3592 | SABPL/NGPL HENRY HUB VERMILION | 05 | 500,000 | 500,000 | **472,702** | 27,298 | BD |

- `SABPL` = **Sabine Pass Liquefaction LLC** (shipper prefix convention on KM EBBs matches e.g. `CCCPL` = Cheniere Corpus Christi Pipeline, `GULFSTH` = Gulfstream).
- This is a **measured scheduled quantity at the interconnect**: SPL nominated 472,702 Dth/d through NGPL while OAC shrank to 27,298 — capacity is being consumed by the terminal, exactly the signal Blue Tide wants.
- Bidirectional flag `BD`; Henry Hub Vermilion placement means this meter sits on SPL's Louisiana header feed.
- Magnitude check: 472,702 Dth/d ≈ 473 MMcf/d ≈ **~10% of SPL's ~4,500 MMcf/d nameplate throughput** — one of several parallel feeds, but genuinely measured.

### Secondary (Sabine-corridor context, same platform)
- [TGP] loc 49524 `GULFSTH/TGP COASTAL BEND LNG WHARTON` — TSQ 92,547 / OAC 551,153 (Gulfstream is a Sabine Pass-area JV line; Wharton delivery point).
- [TGP] loc 412013 `ENTRPRSE/TGP SABINE RIVER TRANS` — TSQ 0 / OAC 257,258 (Enterprise Sabine Pass lateral; idle or post-cycle at snapshot).
- [KMLP] loc 44337 `SABPL/KMLP CALCA` — TSQ 0 / OAC 443,017 (**Calcasieu Pass area, not Sabine Pass** — do not confuse; SABPL prefix here refers to Sabine-related shipper entity but the CALCA location is the Calcasieu corridor).

### Not reachable anonymously
- **Transco Zone 3** (the largest SPL feed, Station 30 pool): `www.1line.williams.com` returns **403 from Azure Application Gateway** against this egress IP for both bot and browser UAs (IP-level WAF block, not UA filtering — `transco.williams.com`/`ebbs.williams.com` don't resolve). Zone 3 coverage could not be tested from this vantage. If Williams blocks CI runners the way it blocked this probe, Transco stays proxy-only unless scraped from a different IP/residential path.

## 3. Corpus Christi — MEASURED candidates

### Primary: TGP loc **49861** — `CCCPL/TGP SINTON SAN PATRICIO`
TSQ **169,489** / OAC 599,261 on design 768,750 (Zone 0, flow BD). `CCCPL` = **Cheniere Corpus Christi Pipeline L.P.** — the terminal's own pipe as a TGP shipper at Sinton, ~20 mi from the terminal. This is a measured nomination into the CCPL system.

### Supporting: TGP loc **47799** — `NMP/TGP GILLRINA ROAD NUECES`
TSQ **410,663** / OAC 101,837 on design 512,500 (Nueces County = Corpus Christi metro). Nueces County receipts are dominated by CCPL-related supply movement; treat as directional-measured rather than terminal-metered.

### Also relevant
- TGP loc 411306 `ENTRPRSE/TGP AGUA DULCE NUECES` — TSQ 0 / OAC 294,678 at snapshot (Agua Dulce hub nexus).
- NGPL has **no** Corpus-area LNG meter (its south Texas footprint ends at Banquete/KM Tejas interconnects); Agua Dulce deliveries to CCPL ride TGP/Enterprise, not NGPL.

Coverage math: CCPL ≈ 2,400 MMcf/d nameplate. CCCPL/Sinton alone shows 169 MMcf/d measured at snapshot (a low-cycle moment), plus Gillrina 411 MMcf/d of Nueces County flow. Combined measured-window visibility is meaningful but the Sinton meter is the only one *labeled* to Cheniere's pipeline; treat Corpus as **one hard meter + one strong regional proxy** until a multi-week correlation pins the mapping factor.

## 4. Platform & scraping contract (for the future scraper)

- **Platform:** ASP.NET WebForms + Infragistics Nautilus controls (WebSplitter/WebDataGrid/WebDropDown). Server-side rendering on postback — **no JSON API needed, no JS execution needed**.
- **Auth model:** fully anonymous for Informational Postings → Capacity → OpAvail pages. Login markers in the HTML are nav furniture only. Session cookie from the initial GET must be carried into the POST.
- **Request shape (the whole scraper):**
  1. `GET /Capacity/OpAvailPoint.aspx?code={NGPL|TGP|KMLP|EPNG|CIG…}` — harvest ALL `<input>` values.
  2. `POST` same URL, echo every field, append `…btnRetrieve.x=10&…btnRetrieve.y=10`.
  3. Parse `<tr>` rows with ≥12 `<td>`s whose second cell is a 4–6 digit loc.
- **Cycle selection:** the page defaults to **BEST AVAILABLE** (latest posted cycle). The dropdown carries TIMELY/EVNG/ITRD1/ITRD2/ITRD3 (values visible in its embedded config), but posting `ddlCycleDD_clientState` overrides did **not** change results in testing — per-cycle pinning needs the Infragistics drop-down postback format solved (medium difficulty, non-blocking since BEST AVAILABLE is the freshest anyway).
- **Rate/politeness:** responses are heavy (~165–320 KB each; three requests per source per cycle ≈ 1 MB). 1 rps is plenty; 3 sources × ~6 cycles/day ≈ tiny volume.
- **Non-US IP barriers:** none hit on pipeline2 from this vantage. Williams (Transco): hard 403 WAF block at IP level — likely needs residential/US egress if attempted later.
- **DevTools capture targets (if a human verifies):** the POST to `OpAvailPoint.aspx` after clicking Retrieve Data; response HTML contains the rendered grid directly.
- **Difficulty:** LOW-MEDIUM. Same family as the existing Boardwalk scraper skill set (form postback + HTML table parse), no Playwright required.

## 5. Anomalies & caveats

- Unknown `code=` values **fall back to TGP content with HTTP 200** — any scraper must assert the expected `<title>` per tenant or it will silently scrape the wrong pipe.
- `SABPL/KMLP CALCA` (loc 44337) is Calcasieu-corridor, not Sabine Pass; naming will trip naive filters.
- KMLP showed only 15 point meters and 2 nonzero TSQ — thin pipe, marginal value.
- NGPL loc names are truncated to ~32 chars (EBB convention), e.g. `SABPL/NGPL HENRY HUB VERMILION`.
- Snapshot values are single-cycle; multi-day sampling needed before trusting magnitudes as daily averages.
- DSSNSS pages ("Daily Scheduled System Nominal", type=D/N) exist but their grid stayed empty under the same postback — deprioritized; OpAvailPoint already carries the measured TSQ column.
- This egress IP is WAF-blocked by Williams — any Transco attempt needs a different network path first.

## Verdict

- **Sabine Pass: measurable NOW.** NGPL loc 3592 gives a real, anonymously-scrapable scheduled quantity on SPL's own shipper account (472,702 Dth/d observed). It is one feed among several (Transco Z3 being the big untested one), so it is a *partial* measurement — but a genuine measurement, strictly better than design−OAC inference, and sufficient to re-admit Sabine Pass into fleet aggregates with an explicit coverage caveat.
- **Corpus Christi: measurably covered at the margin.** TGP loc 49861 (`CCCPL/TGP SINTON`) is the terminal pipeline's own measured nomination point; TGP loc 47799 adds a large measured Nueces County flow. Recommend shipping both scrapers behind a short calibration window correlating measured TSQ against the existing OAC-proxy before switching the dashboard to measured values.
