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

## 2026-08-26 — Cove Point DOUBLE-COUNT audit: receipts ≠ feedgas

The first multi-feed promotion summed feeder receipts (45001+37001+40704 ≈
1,044 MMcf/d = 139% of nameplate). Forensic audit found this WRONG — not
because the meters double-count each other, but because **CPL is more than
an LNG feedgas pipe**:

- **Mass balance closes**: receipts_3f / (plant_intake + local_deliv) =
  1.017 ± 0.036 over 92 days. All three receipt meters are INDEPENDENT
  parallel feeds (corr(45001,40704)=0.11, corr(37001,40704)=0.43).
- **Twin check**: cpl-47001-R ≡ egts-40704-D (mean diff 204 Dth/d = 0.12%,
  r=0.9991). Same molecules seen by two operators. Never sum both.
- **Flow split of receipt gas**: ~62% feeds liquefaction (plant intake
  10001-D, mean 767 MMcf/d = 102% of nameplate), ~37% goes STRAIGHT THROUGH
  to local LDC/power deliveries (WGL x7, Chalk Point, Possum Point,
  Woodville, St Charles) without touching the plant.
- CPL's own maintenance notice (ID 1011059) confirms the topology: it
  restricts receipts at 45001 while explicitly keeping "Primary Deliveries
  to the Cove Point Delivery Locations" flowing — i.e., pass-through is a
  designed, normal state.

**CORRECT FEEDGAS METRIC: loc 10001-D (plant intake)** — mean 767 MMcf/d,
102% of nameplate (max 110%). Feeder receipts are kept as `kind:'context'`
(documented, never summed). The old egts-40704 single-feed view (~124
MMcf/d) was capturing only ~16% of true plant intake.

## 2026-08-26 — Multi-feed independence audit (all terminals)

- **Freeport (GS 24329 + TETCO 79999, both "Stratton Ridge")**: the name
  collision is REAL but benign — Stratton Ridge is a physical hub where
  multiple pipes land; GS and TETCO each post their OWN delivery meter
  into Freeport there. Distinct pipelines, distinct meters, independent
  flows → sum is correct.
- **Sabine (Creole Trail receipt legs)**: CT109413/441/451/461/471 are five
  DIFFERENT third-party pipes delivering at the Gillis hub. Receipt sum vs
  CT200111-D tracks ±8% — no sequential re-measurement. Sum correct.
- **Corpus (CC200221 + Sinton comparison-only)**: already handled.
- **Registry schema note added**: any future `feeds[]` entry must state its
  independence evidence (distinct physical interconnect or mass-balance
  closure) before carrying `kind:'measured'`.

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
within ±8% over 30 days — they are two views of the same pipe flow.

## 2026-08-26 — REUSABLE TECHNIQUE: the TWIN-METER pattern (automatic audit trigger)

When two operators meter the same physical handoff — e.g. Cove Point's EGTS
interconnect — BOTH publish it:
- CPL posts it as `cpl_sq_47001_r` (CPL's own view of EGTS Loudoun)
- EGTS/BHE posts it as `egts_sq_40704_d` (EGTS's own view)

We proved they are the SAME molecules: mean |diff| 204 Dth/d (0.12%),
Pearson r = 0.9991 across 92 days. Summing both double-counts the gas.

**RULE (add to registry audit checklist):** any new multi-feed terminal MUST
be checked for twin meters — locate the interconnect in BOTH operators' EBBs
and verify they are not the same physical flow before summing. Corroborate
with a correlation/diff test, never by trust. This is now documented in the
MULTI-FEED INDEPENDENCE RULE header in lng-terminals.js.

## 2026-08-26 — Cove Point audit corrections (four-task follow-up)

1. **Golden Pass was stale in the fleet table** — an earlier report carried
   181 MMcf/d (pre-gasnom-ingestion-fix value). Curated gasnom now has
   `golden_pass_sq_1097217_d` = 343,070 Dth/d = **334.7 MMcf/d** (live, 08-26).
   Fleet recomputed with the live value. No other terminal carried a pre-fix
   value — verified against curated parquet (Cove Point 725.6, Sabine 1,408,
   Freeport 1,104, Corpus 1,735, Plaquemines 3,853, Calcasieu 1,613, Cameron 1,445).

2. **Nameplate denominators audited** — every entry in LNG_NAMEPLATE_MMCFD
   carries a FERC docket source (CP12-509 Freeport=2,100; CP13-25 Cameron=2,000;
   CP11-72 Sabine=4,500; CP12-507 Corpus=2,400; CP17-66 Plaquemines=3,400;
   CP15-550 Calcasieu=1,300; CP13-113 Cove Point=750; CP14-517 Golden Pass=2,600;
   CP17-20 Port Arthur=1,900). The 2,260/2,400 figures in the prior report were
   report typos, NOT registry values — registry was correct. A "do not silently
   change a denominator" warning + reconciliation block was added to the JSDoc.

3. **Sabine "fully measured" was overstated** — same class of error as the Cove
   Point 139%. The five Gillis receipt legs + NGPL summed to 1,388 MMcf/d = 30.8%
   of the 4,500 nameplate, which contradicts Cheniere's public cargo cadence.
   Creole Trail DOES post a consolidated plant-delivery meter CT200111-D (the
   Cove-Point-10001 analogue) but it reads only 1,408 MMcf/d — CTPL's EBB only
   sees feedgas CTPL itself delivers into the plant. NGI feeder-gas nominations
   put Sabine near 3.9 Bcf/d (Aug 2026, even during compressor maintenance),
   so CTPL's view is ~ONE THIRD of true terminal feedgas. Demoted to
   **MEASURED-PARTIAL**: CT200111-D + NGPL 3592 are the headline (summable),
   the five Gillis feeders demoted to `kind:'context'`, coverage gap footnoted.

4. **VERDICT.md** — this entry. The file exists and is being maintained; the
   prior claim that it "was never written" was incorrect (it carried 100 lines
   of findings). The Transco-via-CPL, CPL-topology, and twin-meter findings are
   now all documented for reuse.

## 2026-08-26 — STRUCTURAL FIX: registry↔config↔bundle agreement gate

The Sabine CT200111-D silent-loss (headline meter dropped by the prune because
cheniere.json marked it `comparison`, bundle shipped 0 rows, panel rendered
empty) was the **seventh bug of the same family**: two layers disagreeing with
nothing checking. Fixed at the root:

- **Publish-time assertion** `_audit_registry_bundle_agreement()` in
  `publishers/export_dashboard_json.py`. For every terminal feed declared
  `kind:'measured'` or `kind:'measured-partial'` in `lng-terminals.js`:
  (a) its loc MUST be high-confidence in the source's `config/meters/*.json`
  (else the prune drops it), and (b) it MUST ship ≥1 non-zero row in the built
  bundle (else the card renders empty). Context/comparison/proxy feeds are
  exempt from (b) but not (a). Fails the publish naming the terminal, series,
  and which condition broke. Pure core `_check_agreement()` is unit-tested in
  `tests/test_publish_agreement.py` (both arms, positive + context-exempt).
- **Why the old coverage audit missed it**: that audit only checked whole-source
  row counts and whether config loc_ids resolved to *some* curated series.
  CT200111-D DID resolve to a curated series, so "id-space drift" passed — only
  the *prune* excluded it. The new audit inspects the registry's declared
  headlines against what the prune actually kept + what the bundle actually ships.
- **TASK 4 — stale index resolution**: verification tooling now calls
  `resolve_current_index()` which follows `manifest.json → index_url` instead of
  globbing `index.*.json` (which alphabetically picked the stale `fb4f74be` over
  the fresh `4bd04b97`). The graveyard prune already removes superseded
  `index.{h}.json`; the resolution fix removes the *reader* ambiguity.
- **NGPL 3592 zero explained**: KM `pipeline2` OpAvail posts the `best_available`
  default cycle when NAESB cycle-pinning fails; for loc 3592 on 2026-08-25 that
  returned 0.0 — a **scraper-cycle artifact, not a real plant idle** (it showed
  ~480k during recon). Demoted to `kind:'context'` (diagnostic) so a structurally
  zero meter never masquerades as Sabine coverage. CT200111-D alone is the
  headline = 1,407.7 MMcf/d = **31.3%** of nameplate → MEASURED-PARTIAL, with the
  invisible ~69% (non-CTPL feedgas + Transco Z3 SPA migration) footnoted on the
  card at Freeport-caveat prominence.

## 2026-08-26 — SCRAPER FAULTS FIXED (the gate caught real bugs, not noise)

Two freshness/coverage faults surfaced this cycle. Both were REAL pipeline bugs
the agreement/integrity gates correctly flagged — fixed at the cause, not by
demoting meters.

1. **Cheniere went stale 2 days (08-24 to 08-26 block).** Root cause: the scraper's
   identity assertion expected marker "CTPL" (Creole Trail) / "CCPL" (Corpus),
   but Cheniere's API now returns tsP_NAME: "CHENIERE CREOLE TRAIL PIPELINE, L."
   — the legacy tickers no longer appear in the payload, so every pull raised
   TenantFallbackError and the dataset froze. Fix: match on distinctive
   pipeline words ("CREOLE TRAIL" / "CORPUS CHRISTI") in
   scrapers/cheniere/client.py. After the fix the scraper resumed and cheniere
   caught up to 08-26. The per-source staleness guard (config/integrity_rules.yaml
   already pins daily EBB sources at warn_days=2 / fail_days=4) now correctly
   shows cheniere PASS.

2. **NGPL 3592 returned 0 / KM per-cycle pulls never wrote.** Root cause: the KM
   scraper verified tenant identity on the AJAX POST delta response, which has
   NO title tag -> TenantFallbackError for every NGPL/TGP/KMLP per-cycle pull,
   so no raw file was written and only a stale best-available row (0.0) remained.
   Fix: verify identity on the GET (full page, carries the title), inherit it for
   the POST delta in scrapers/kinder_morgan.py. NGPL per-cycle pulls now succeed.
   BUT the live value is genuinely 0: KM's OpAvail reports loc 3592 as
   flow_ind:BD with total_scheduled_quantity:0 across all cycles/gas-days
   (verified 08-23 to 08-27). The ~480k Dth/d from recon is NOT reproducible from KM
   (design/operating capacity is 500,000, suggesting a capacity misread). So 3592
   stays kind:'context' (diagnostic) — held there because the DATA is 0, NOT to
   silence the gate. The gate did its job: it caught a real zero, the pipeline bug
   was fixed, and we reported the true state instead of fabricating flow.

3. **bhe value_sanity FAIL** (pre-existing, surfaced by the integrity run): the
   single cove_point_capacity band (max: 1,000,000) wrongly covered Cove
   Point's plant-SENDOUT OAC series (cpl_oac_10001_*), which genuinely reaches
   ~2.59M Dth/d (sendout != feedgas intake). Split into three bands: plant intake
   (cpl_sq_10001_*, max 1.1M), sendout OAC (cpl_oac_10001_*, max 3.0M), sendout
   opcap (cpl_opcap_10001_*, max 2.2M). bhe now PASS.

## 2026-09-02 — Sabine Pass plant intake exhaustive audit (Cheniere Creole Trail TSP 200 & Corpus Christi TSP 400)

Exhaustive enumeration of all locations published by Cheniere's public API for Creole Trail
(TSP 200) and Corpus Christi (TSP 400) was conducted across the full curated raw payload library
to investigate whether a consolidated plant-intake meter exists for Sabine Pass (analogous to
Cove Point's `cpl_sq_10001_d` or Corpus Christi's `CC200221`).

### 1. Creole Trail Pipeline (TSP 200) — complete 11-meter census
- `CT109413` (GILLIS-TETCO-R): Receipt, design 650k Dth/d. Gillis hub interconnect.
- `CT109441` (GILLIS-TRANSCO-R): Receipt, design 900k Dth/d. Gillis hub interconnect.
- `CT109451` (GILLIS-TRUNK-R): Receipt, design 1,000k Dth/d. Gillis hub interconnect.
- `CT109461` (GILLIS-LEAP-R): Receipt, design 1,000k Dth/d. Gillis hub interconnect.
- `CT109471` (GILLIS-ACADIAN-R): Receipt, design 1,000k Dth/d. Gillis hub interconnect.
- `CT200111` (CREOLE TRAIL-SPLIQ-D): Delivery, design 1,700,000 Dth/d. Pipeline delivery into Sabine Pass Liquefaction. Typically flows ~1,390,000 Dth/d (~1,356 MMcf/d).
- `CT209441` (GILLIS-TRANSCO-D): Delivery, design 900k Dth/d. Bi-directional delivery at Gillis. Flow 0.0.
- `SPLNG` (Sabine Pass LNG Rec): Receipt, design 25,000 Dth/d. Auxiliary terminal receipt point. Flow 0.0.
- `SPLNGD` (Sabine Pass LNG Del): Delivery, design 25,000 Dth/d. Small auxiliary delivery tap. Flow ~9,500 Dth/d (~9.2 MMcf/d).
- `TETCO` (TETCO Gillis): Delivery, design 650k Dth/d. Bi-directional delivery at Gillis. Flow 0.0.
- `TRUNK` (Trunkline Gillis): Delivery, design 1,000k Dth/d. Bi-directional delivery at Gillis. Flow 0.0.

### 2. Corpus Christi Pipeline (TSP 400) — complete 11-meter census
- `CC100221` (CORPUS CHRISTI-CCLIQ-R): Receipt, design 35k Dth/d. Terminal return. Flow 0.0.
- `CC108011` (TAFT RECEIPT-R): Receipt, design 50k Dth/d. Flow 0.0.
- `CC121033` (TGP-SINTON-R): Receipt, design 750k Dth/d. Interconnect at Sinton.
- `CC121041` (TRANSCO-SAN PAT CO-R): Receipt, design 400k Dth/d. Interconnect in San Patricio Co.
- `CC121053` (NGPL-SINTON-R): Receipt, design 1,000k Dth/d. Interconnect at Sinton.
- `CC121063` (EPROD-SAN PAT CO-R): Receipt, design 750k Dth/d. Interconnect in San Patricio Co.
- `CC121073` (KM TEJAS-SINTON-R): Receipt, design 1,000k Dth/d. Interconnect at Sinton.
- `CC121083` (TEJAS II-SINTON-R): Receipt, design 1,000k Dth/d. Interconnect at Sinton.
- `CC200221` (CORPUS CHRISTI-CCLIQ-D): Delivery, design 2,750,000 Dth/d. Consolidated delivery into CCLIQ. Captures ~100% of terminal feedgas (~1,868–2,450 MMcf/d).
- `CC221033` (TGP-SINTON-D): Delivery, design 500k Dth/d. Bi-directional delivery. Flow 0.0.
- `CC221073` (KM TEJAS-SINTON-D): Delivery, design 1,000k Dth/d. Bi-directional delivery. Flow 0.0.

### 3. Twin-meter check and pass-through audit
- **Creole Trail mass balance**: On TSP 200, the sum of the five Gillis receipts (TETCO + Transco + Trunkline + LEAP + Acadian) equals 1,423,371 Dth/d. Delivery meter `CT200111` equals 1,390,562 Dth/d. The 32,809 Dth/d difference is 2.3% pipeline fuel and shrinkage along the 94-mile lateral. Correlation is $r = 0.985$. The five receipt meters and `CT200111` are two ends of the same pipe. Summing them would double-count.
- **Pass-through**: Unlike Cove Point (where 37% bypasses the plant to regional utilities), Creole Trail has 0 local utility deliveries. 100% of non-fuel gas delivered by Creole Trail enters Sabine Pass Liquefaction.

### 4. Plant intake verdict for Sabine Pass
- **Verdict: NO consolidated plant-intake meter exists on Cheniere's EBB for Sabine Pass.**
- Unlike Corpus Christi (where Cheniere owns the primary transmission artery carrying ~100% of plant supply), Sabine Pass Liquefaction (4,500 MMcf/d nameplate) is an unbundled facility supplied by multiple independent pipeline systems (Creole Trail, Williams Transco Zone 3, Kinder Morgan NGPL, and Kinetica).
- Creole Trail's total design capacity is 1.7 Bcf/d (~37% of Sabine nameplate), and its operational delivery is ~1.4 Bcf/d (~31% of nameplate).
- Because Cheniere only publishes informational postings for its own pipeline (Creole Trail LP), Cheniere's API physically cannot see the remaining ~69% arriving via Transco and other operators.
- **Conclusion**: `CT200111-D` is already the maximum possible measurement of Creole Trail delivery into Sabine Pass. The observatory's classification of Sabine Pass as `MEASURED-PARTIAL (~31%)` with explicit UI caveats is the honest, optimal reporting structure. The remaining gas cannot be captured without separate scraper pipelines for Williams Transco and other third-party EBBs.

