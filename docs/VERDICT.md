# VERDICT — Terminal Feedgas Coverage Findings

Dated findings from deep-dive investigations. Newest last. Each verdict states
what is measured, what is inferred, and what was ruled out.

---

## 2026-08-25 — Corpus Christi & Sabine Pass data corrections

- **Corpus Christi**: Cheniere's LNG Connection site DOES publish real
  Scheduled Quantities (`corpus_christi_sq_CC200221_d`, ~2.46M Dth/d ≈ 100% of
  nameplate). The "OAC proxy" framing was retired; Corpus is fully MEASURED.
  KM's independent Sinton meter (49861) corroborates exactly on overlap days
  but posts TSQ=0 while CCPL runs near plate → comparison-only, never summed.
- **Sabine Pass**: the "~1,408 MMcf/d OAC proxy" at CT200111 was an artifact —
  design − OAC ≡ SQ identically, i.e. the same number restated. Sabine's
  visible laterals (KM NGPL 3592 + Creole Trail CT200111-D) were real but are
  *output-side* measurements.

## 2026-08-26 — Cove Point: multi-feed promotion via CPL's OWN EBB

**Finding**: Cove Point LNG LP posts its own Operationally Available CSVs on
infopost.bhegts.com under `/cpl/` (same `searchHistoricalData` POST contract as
EGTS). These enumerate receipts from **every feeder**, including Transco.

| CPL loc | Name | Leg | Role | In feedgas total? |
|---|---|---|---|---|
| 45001 | TRANSCO PLEASANT VALLEY | R | Third-party receipt (Transco Z6 via Pleasant Valley compressor) | **YES** |
| 37001 | COLUMBIA LOUDOUN | R | Third-party receipt (TCO Columbia Gas) | **YES** |
| 47001 | EGTS LOUDOUN | R/D | Third-party receipt (our original EGTS-40704 view, from CPL's side) | **YES** |
| 10002 | CVP STORAGE POINT (ST) | R | **LNG tank storage cycling — gas already at the terminal re-injected to sendout. NOT third-party feedgas.** | **NO** |
| 10001 | COVE POINT PLANT | SI/D | **Plant SENDOUT to Transco/domestic market — output, not input.** | **NO** |
| 37002 | TCO/CP LOUDOUN (COMMISSIONING) | R | Twin of 37001, TSQ=0 throughout retained history | Not yet — verify activation first |

**Why this matters (strategic)**: Transco's Cove Point volumes ARE publicly
visible through CPL's own postings (loc 45001). The shelved Williams 1Line
scraper is NOT needed for Cove Point feedgas visibility. Transco Zone 6 remains
unavailable for other purposes, but this terminal no longer depends on it.

**The Coastal Bend lesson applied**: 10001 and 10002 look like high-volume
feedgas meters but are not — one is sendout (output), one is internal tank
cycling. Summing either would double-count gas that never entered as new
supply.

## 2026-08-26 — Sabine Pass: receipt-side fully visible on Creole Trail

Cheniere's Creole Trail Pipeline (tspNo=200) posts **receipt-side meters for
every third-party feeder** at the Gillis hub — the exact pattern CPL uses:

| CTPL loc | Feeder | Leg | Status in Blue Tide |
|---|---|---|---|
| CT109413 | TETCO Gillis | R | Already captured (`creole_trail_sq_CT109413_r_*`) |
| CT109441 | Transco (Gillis) | R | Already captured |
| CT109451 | Trunkline | R | Already captured |
| CT109461 | LEAP | R | Already captured |
| CT109471 | Acadian | R | Already captured |

Cross-check: sum of the five receipt legs tracks CT200111-D (the SPL delivery)
within ±8% over 30 days — they are two views of the same pipe flow. Sabine can
be presented as FULLY MEASURED using the receipt-leg sum (≈1.42M Dth/d ≈ 1,388
MMcf/d), with CT200111-D as corroboration rather than headline.
