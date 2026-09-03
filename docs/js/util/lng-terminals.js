/**
 * LNG Terminal Registry — single source of truth for every export terminal
 * rendered in the Feedgas Observatory (hero panel, fleet overview, chips).
 *
 * Every other module reads from this config; terminal metadata must never be
 * duplicated elsewhere. Nameplate values mirror LNG_NAMEPLATE_MMCFD in
 * lng-metrics.js (FERC docket sources documented there).
 *
 * SIGNAL SEMANTICS:
 *   signal: 'sq'        — Total Scheduled Quantity IS the feedgas measurement.
 *                         utilization = sq / nameplate.
 *   signal: 'oac-proxy' — Operator does not publish SQ (Cheniere). The plotted
 *                         number is INFERRED capacity consumption:
 *                           implied_flow = design_capacity − oac
 *                         falling back to operating capacity when design is
 *                         missing for a cycle. UI must label it as a proxy.
 *   operational: false  — terminal renders as a muted, non-clickable card.
 *
 * MULTI-FEED INDEPENDENCE RULE (added 2026-08-26 after the Cove Point audit):
 *   A `feeds[]` entry may carry `kind: 'measured'` ONLY with documented
 *   evidence that it is an independent parallel feed, i.e. at least one of:
 *     - distinct physical interconnect (different pipe delivering at its own
 *       meter — e.g. Freeport's GS 24329 vs TETCO 79999, both AT Stratton
 *       Ridge but from different pipelines), or
 *     - mass-balance closure: sum(feeds) ≈ a consolidated downstream meter
 *       within a few percent (e.g. Cove Point receipts_3f / plant_intake +
 *       local = 1.017 ± 0.036).
 *   Sequential re-measurements of the same gas (a receipt meter AND the
 *   downstream plant meter for the same molecules) must NEVER both be
 *   'measured' — one side becomes kind:'comparison'. Feeder receipts that
 *   include pass-through deliveries to OTHER customers (gas that never
 *   reaches liquefaction) must be kind:'context' and never enter terminal
 *   sums — see cove_point, whose honest feedgas number is plant intake
 *   10001-D, not the receipt total.
 *
 * FLEET AGGREGATE (60-day complete-day median, verified 2026-09-02):
 *   Fleet aggregate = 12,825.9 MMcf/d (67.3% of 19,050 MMcf/d operational nameplate;
 *   61.2% of 20,950 MMcf/d total nameplate including non-operational Port Arthur).
 *   Construction: daily sum of headline meters across all terminals on complete days,
 *   then median of those daily sums over the trailing 60 complete days.
 *   Latest 3 complete days:
 *     2026-08-30: 13,770.7 MMcf/d (72.3%)
 *     2026-08-31: 13,644.8 MMcf/d (71.6%)
 *     2026-09-01: 13,913.8 MMcf/d (73.0%)
 */

/**
 * @typedef {Object} LngTerminal
 * @property {string} id               — registry key (also used as chip/card id)
 * @property {string} display          — human-readable terminal name
 * @property {string} [source]         — bundle.sources key (absent for non-operational)
 * @property {string} [seriesPrefix]   — curated series_id prefix
 * @property {string} [loc]            — location id embedded in series_ids
 * @property {string} [locName]        — meter name for subtitles/footnotes
 * @property {number} nameplate        — MMcf/d
 * @property {'sq'|'oac-proxy'} [signal]
 * @property {boolean} [operational]   — defaults true
 * @property {string} [statusText]     — text shown on non-operational cards
 * @property {string[]} [cycles]       — cycle codes this platform publishes
 * @property {string} [platformNote]   — per-terminal cycle-publications note
 * @property {string} [platformLabel]  — short upstream platform name
 * @property {string} [methodLine]     — footer methodology sentence
 * @property {Array<{source: string, series: string, label: string, kind?: string, note?: string}>} [feeds]
 *   — MULTI-FEED terminals only: each pipeline that feeds the terminal, with
 *   the exact series-id stem (up to, but excluding, the cycle token) and a
 *   display label. When present, `feeds` supersedes the single-source
 *   fields for data extraction. `kind` marks feed semantics:
 *     'measured'        — full-terminal measurement, enters sums
 *     'measured-partial'— real measurement of ONE feed; terminal is
 *                         partial-coverage (see FLEET_PROXY_EXCLUSIONS)
 *     'proxy'           — inferred estimate shown alongside measured feeds
 *     'comparison'      — cross-check only; never summed into any headline
 *   Feeds with kind 'comparison' are excluded from card/hero aggregation.
 */

/**
 * Registry of all nine US LNG export terminals.
 *
 * @type {Object<string, LngTerminal>}
 */
export const LNG_TERMINALS = {
  freeport: {
    id: 'freeport',
    display: 'Freeport',
    feeds: [
      { source: 'gulf_south', series: 'gulf_south_sq_24329_d', label: 'Gulf South' },
      { source: 'enbridge', series: 'tetco_sq_79999_d', label: 'TETCO' },
    ],
    locName: 'Stratton Ridge — dual feed (Gulf South + TETCO)',
    nameplate: 2100,
    expectedCoveragePct: 52.9,
    expectedMedianMmcf: 1111.5,
    coverageTolerancePct: 10.0,
    coverageNote:
      'MEASURED-PARTIAL: interstate-visible feeds only (Gulf South + TETCO, 52.9% median coverage of 2,100 MMcf/d nameplate). KMTP intrastate lateral (~400–450 MMcf/d capacity) is unmetered on public EBBs.',
    signal: 'sq',
    cycles: ['id1', 'id2', 'id3'],
    platformLabel: 'Boardwalk OAC + Enbridge rtba',
    platformNote:
      'Gulf South posts ID1/ID2/ID3 only; TETCO (Enbridge rtba) posts Timely/Evening/Intraday. Figures are interstate-visible feedgas only (52.9% median of 2,100 MMcf/d nameplate over 100-day overlap). KMTP (intrastate) is not publicly posted.',
    methodLine:
      'Combined TSQ into Freeport LNG: Gulf South Stratton Ridge 24329 + TETCO Stratton Ridge 79999 (both delivery) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · Interstate-visible only',
  },

  plaquemines: {
    id: 'plaquemines',
    display: 'Plaquemines',
    source: 'quorum',
    seriesPrefix: 'gator_express',
    loc: 'vgpqd',
    flow: 'd',
    feeds: [
      { source: 'quorum', series: 'gator_express_sq_vgpqd_d', label: 'Gator Express', kind: 'measured' },
    ],
    locName: 'Venture Global Plaquemines LNG Delivery (VGPQD)',
    nameplate: 3400,
    expectedCoveragePct: 112.4,
    expectedMedianMmcf: 3820.9,
    coverageTolerancePct: 12.0,
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'Quorum myQuorumCloud',
    platformNote:
      'Plaquemines (Quorum IPWS) posts all five NAESB cycles — Timely, Evening, ID1, ID2, ID3.',
    methodLine:
      'TSQ at VGPQD (MQ/D delivery into Plaquemines LNG) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · Source: Quorum Gator Express OpAvail (TspNo=2)',
  },

  calcasieu: {
    id: 'calcasieu',
    display: 'Calcasieu Pass',
    source: 'quorum',
    seriesPrefix: 'trans_cameron',
    loc: 'vgcpd',
    flow: 'd',
    feeds: [
      { source: 'quorum', series: 'trans_cameron_sq_vgcpd_d', label: 'TransCameron', kind: 'measured' },
    ],
    locName: 'Venture Global Calcasieu Pass Delivery (VGCPD)',
    nameplate: 1300,
    expectedCoveragePct: 123.5,
    expectedMedianMmcf: 1605.8,
    coverageTolerancePct: 8.0,
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'Quorum myQuorumCloud',
    platformNote:
      'Calcasieu Pass (Quorum IPWS) posts all five NAESB cycles — Timely, Evening, ID1, ID2, ID3.',
    methodLine:
      'TSQ at VGCPD (MQ/D delivery into Calcasieu Pass LNG) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · Source: Quorum TransCameron OpAvail (TspNo=10)',
  },

  golden_pass: {
    id: 'golden_pass',
    display: 'Golden Pass',
    source: 'gasnom',
    seriesPrefix: 'golden_pass',
    loc: '1097217',
    flow: 'd',   // D leg = consolidated plant intake delivery meter
    feeds: [
      { source: 'gasnom', series: 'golden_pass_sq_1097217_d', label: 'Golden Pass Pipeline', kind: 'measured' },
    ],
    locName: 'Golden Pass Terminal (delivery meter)',
    nameplate: 2600,
    expectedCoveragePct: 12.7,
    expectedMedianMmcf: 330.4,
    coverageTolerancePct: 15.0,
    coverageNote:
      'MEASURED (commissioning ramp): loc 1097217 is the full-terminal consolidated plant intake meter with 2,600,910 Dth/d design capacity (matching 2,600 MMcf/d nameplate). Current ~330–359 MMcf/d flow is active Train 1 commissioning ramp, not partial pipeline visibility.',
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'GasNom ESG',
    platformNote:
      'Golden Pass headline = full-terminal consolidated delivery meter (loc 1097217). Current ~330–359 MMcf/d represents active Train 1 commissioning ramp (~40% of Train 1 capacity).',
    methodLine:
      'TSQ at Golden Pass Terminal (loc 1097217, delivery) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · Full plant intake gate · Commissioning ramp',
  },

  cameron: {
    id: 'cameron',
    display: 'Cameron',
    source: 'gasnom',
    seriesPrefix: 'cameron_interstate',
    loc: '772300',
    flow: 'd',
    feeds: [
      { source: 'gasnom', series: 'cameron_interstate_sq_772300_d', label: 'Cameron Interstate', kind: 'measured' },
    ],
    locName: 'Cameron LNG (Del)',
    nameplate: 2000,
    expectedCoveragePct: 72.9,
    expectedMedianMmcf: 1458.6,
    coverageTolerancePct: 8.0,
    coverageNote:
      'MEASURED-PARTIAL: Cameron Interstate Pipeline (CIP) loc 772300 design capacity is 1,560,000 Dth/d (1,522 MMcf/d) — runs at ~96% capacity delivering 1,458.6 MMcf/d median, covering 72.9% of Cameron LNG’s 2,000 MMcf/d nameplate. The remaining ~27% (~500 MMcf/d) is delivered via Columbia Gulf Transmission (CGT Cameron Extension, FERC CP15-514), not posted on GasNom.',
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'GasNom ESG',
    platformNote:
      'Cameron Interstate Pipeline (loc 772300 delivery) delivers ~1,459 MMcf/d (72.9% of 2,000 MMcf/d nameplate). This is CIP’s visible share running near its 1.56 Bcf/d design capacity; Columbia Gulf Transmission delivers the remaining ~27% (~500 MMcf/d) unmeasured.',
    methodLine:
      'TSQ at Cameron LNG complex via Cameron Interstate Pipeline (loc 772300, delivery) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · COVERAGE: ~73% of 2,000 nameplate; remaining ~27% arrives via unmeasured Columbia Gulf Transmission',
  },

  cove_point: {
    id: 'cove_point',
    display: 'Cove Point',
    source: 'bhe',
    feeds: [
      {
        source: 'bhe',
        series: 'cpl_sq_10001_d',
        label: 'Plant intake (measured)',
        kind: 'measured',
        note: 'Consolidated liquefaction feedgas at the plant meter. 97–102% of nameplate typical.',
      },
      {
        source: 'bhe',
        series: 'cpl_sq_45001_r',
        label: 'Transco Pleasant Valley receipts',
        kind: 'context',
        note: 'Largest feeder (~57% of CPL throughput). Pass-through to LDCs included in this meter.',
      },
      {
        source: 'bhe',
        series: 'cpl_sq_37001_r',
        label: 'Columbia Loudoun receipts',
        kind: 'context',
        note: 'TCO feeder (~32% of throughput).',
      },
      {
        source: 'bhe',
        series: 'egts_sq_40704_d',
        label: 'EGTS Loudoun receipts',
        kind: 'context',
        note: '~11% of throughput. Identical to cpl_sq_47001_r (r=0.9991) — never sum both.',
      },
    ],
    locName: 'Plant intake 10001-D + feeder receipts 45001/37001/47001',
    nameplate: 750,
    expectedCoveragePct: 97.1,
    expectedMedianMmcf: 728.5,
    coverageTolerancePct: 7.0,
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: "BHE GT&S EBB (EGTS + CPL's own postings)",
    platformNote:
      "MEASURED via Cove Point LNG LP's own postings. Headline = plant intake (10001-D): actual gas entering liquefaction (97–102% of nameplate). Feeder receipts shown as context — they include pass-through deliveries to local utilities that never reach the plant, so their sum EXCEEDS feedgas by design.",
    methodLine:
      'MEASURED: plant intake at CPL loc 10001-D · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · Feeder context: Transco PV 45001 + Columbia Loudoun 37001 + EGTS Loudoun 47001 (receipt legs; mass balance closes at 1.017±0.036 vs plant+local) · Excluded from sums: 10002 storage cycling, duplicate EGTS twin · Strategic: Transco volumes public via CPL — Williams scraper not needed',
  },

  sabine_pass: {
    id: 'sabine_pass',
    display: 'Sabine Pass',
    feeds: [
      {
        source: 'cheniere',
        series: 'creole_trail_sq_CT109413_r',
        label: 'TETCO Gillis (measured)',
        kind: 'context',
      },
      {
        source: 'cheniere',
        series: 'creole_trail_sq_CT109441_r',
        label: 'Transco Gillis (measured)',
        kind: 'context',
      },
      {
        source: 'cheniere',
        series: 'creole_trail_sq_CT109451_r',
        label: 'Trunkline Gillis (measured)',
        kind: 'context',
      },
      {
        source: 'cheniere',
        series: 'creole_trail_sq_CT109461_r',
        label: 'LEAP Gillis (measured)',
        kind: 'context',
      },
      {
        source: 'cheniere',
        series: 'creole_trail_sq_CT109471_r',
        label: 'Acadian Gillis (measured)',
        kind: 'context',
      },
      {
        source: 'cheniere',
        series: 'creole_trail_sq_CT200111_d',
        label: 'CTPL→SPL plant delivery (measured, partial)',
        kind: 'measured-partial',
        note: 'Consolidated CTPL delivery into SPL — ~30.3% of nameplate (1,365 MMcf/d); CTPL does not meter the other feedgas paths.',
      },
      {
        source: 'kinder_morgan',
        series: 'km_ngpl_sq_3592_d',
        label: 'NGPL lateral (diagnostic — live 0.0)',
        kind: 'context',
        note: 'Separate physical delivery into SPL via NGPL. The KM scraper cycle-pin was fixed so per-cycle pulls succeed. As of current data, KM reports TSQ 0 with OAC at full 500,000 (idle lateral). Held as kind:"context" at 0 because current data is 0.',
      },
    ],
    locName: 'CTPL plant delivery (CT200111-D) + five Gillis feeders + NGPL 3592',
    nameplate: 4500,
    expectedCoveragePct: 30.3,
    expectedMedianMmcf: 1365.2,
    coverageTolerancePct: 6.0,
    coverageNote: 'MEASURED-PARTIAL: only CTPL’s EBB-visible share (CT200111-D plant delivery ≈ 1,365 MMcf/d, ~30.3% of 4,500 MMcf/d nameplate) is public. INVISIBLE: (1) CTPL does not meter non-CTPL feedgas (Transco Z3, intrastate, other interconnects); (2) Transco Zone 3 deliveries are unavailable — Williams migrated that reporting to a Shipper-Posted Allocation (SPA), not public SQ. Per NGI feeder-gas nominations Sabine runs near 3.9 Bcf/d; the other ~2.5 Bcf/d is not in any public EBB we scrape.',
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'Cheniere LNG Connection + KM pipeline2',
    platformNote:
      'MEASURED-PARTIAL: consolidated plant-delivery meter CT200111-D (1,365 MMcf/d ≈ 30.3% of nameplate) plus the NGPL lateral. This is CTPL’s visible share only — Sabine runs near 3.9 Bcf/d per NGI feeder-gas nominations; the remainder is non-CTPL feedgas we cannot see. NOT full coverage.',
    methodLine:
      'MEASURED-PARTIAL: CTPL plant delivery CT200111-D + KM NGPL 3592 lateral · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · COVERAGE GAP: CTPL EBB ≈ 30.3% of 4,500 nameplate; other Sabine feedgas not public · Gillis feeders demoted to context',
  },

  corpus_christi: {
    id: 'corpus_christi',
    display: 'Corpus Christi',
    feeds: [
      {
        source: 'cheniere',
        series: 'corpus_christi_sq_CC200221_d',
        label: 'CCPL measured',
        kind: 'measured',
        note: 'Cheniere LNG Connection published TSQ at CCLIQ (full terminal, median 2.38M–2.46M Dth/d ≈ 99.4% of nameplate).',
      },
      {
        source: 'kinder_morgan',
        series: 'km_tgp_sq_49861_d',
        label: 'TGP Sinton comparison',
        kind: 'comparison',
        note: 'Independent interstate meter ~20 mi from the terminal. Currently posting TSQ=0 across cycles/days while the terminal runs — treated as a cross-check, never summed.',
      },
    ],
    locName: 'CCLIQ measured + TGP Sinton cross-check',
    nameplate: 2400,
    expectedCoveragePct: 99.4,
    expectedMedianMmcf: 2384.7,
    coverageTolerancePct: 7.0,
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'Cheniere LNG Connection + KM pipeline2',
    platformNote:
      'Corpus Christi headline = MEASURED published TSQ at CCPL interconnect CC200221 (all five cycles). The earlier capacity-proxy framing is retired: schedD_QTY is published on lngconnection.cheniere.com. TGP Sinton (49861) ships as a secondary comparison only.',
    methodLine:
      'MEASURED: Cheniere CCPL published TSQ at CC200221 (CCLIQ delivery), all cycles · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · Cross-check: KM TGP Sinton 49861 (independent meter)',
  },

  port_arthur: {
    id: 'port_arthur',
    display: 'Port Arthur',
    nameplate: 1900,
    expectedCoveragePct: 0.0,
    expectedMedianMmcf: 0.0,
    coverageTolerancePct: 0.0,
    signal: 'sq',
    operational: false,
    statusText: 'Not operational — Phase 1 expected 2027',
  },
};

/** Fleet rendering order (registry key sequence). */
export const LNG_FLEET_ORDER = [
  'plaquemines',
  'freeport',
  'calcasieu',
  'golden_pass',
  'cameron',
  'cove_point',
  'sabine_pass',
  'corpus_christi',
  'port_arthur',
];

/** Default hero terminal — Plaquemines: deepest history (5.5y), running hot. */
export const DEFAULT_TERMINAL_ID = 'plaquemines';

/**
 * Fleet aggregate semantics (2026-08-25):
 *   The headline total sums terminals whose number is genuinely MEASURED.
 *   A terminal with partial measured coverage stays IN the sum at its
 *   measured-lateral value but must be labeled partial (see the Sabine
 *   caveat in lng-fleet-overview) — silently presenting a partial sum as a
 *   terminal total is the failure mode these registries exist to prevent.
 *
 *   - corpus_christi: fully measured via Cheniere published SQ at CC200221.
 *   - sabine_pass: measured laterals summed (~40% of nameplate), labeled
 *     partial everywhere it renders.
 *
 * @type {string[]}
 */
export const FLEET_PROXY_EXCLUSIONS = [];

/** Feeds that must NEVER enter any summed headline (comparison-only views). */
export const COMPARISON_FEED_EXCLUSIONS = [
  // KM TGP Sinton (49861): independent cross-check of Corpus CCPL; currently
  // posting TSQ=0 across cycles/days while the terminal runs near nameplate.
  'km_tgp_sq_49861_d',
  // EGTS-Loudoun 40704: legacy single-feed view of Cove Point feedgas. The
  // same physical flow is measured CPL-side (loc 47001 twin) and is a SUBSET
  // of the cpl_45001 + cpl_37001 receipt sum — summing it would double-count.
  'egts_sq_40704',
];

/**
 * Resolve the series-id matcher strings for one terminal.
 *
 * Single-source terminals only — multi-feed terminals use `feeds` entries
 * directly (see LNG_TERMINALS.freeport).
 *
 * @param {LngTerminal} t
 * @returns {{sqPrefix: string|null, kindPrefixes: Object<string, string>|null}}
 *   sqPrefix  — prefix for direct-SQ terminals ("{prefix}_sq_{loc}_{flow}_")
 *   kindPrefixes — map of kind -> prefix for oac-proxy terminals
 */
export function terminalSeriesPrefixes(t) {
  if (!t.source || !t.seriesPrefix || !t.loc) return { sqPrefix: null, kindPrefixes: null };
  // Series layout (post dual-leg fix) is
  //   "{prefix}_{kind}_{loc}_{flow}_{cycle}"  with flow ∈ r|d
  // e.g. gator_express_sq_vgpqd_d_id3, creole_trail_design_CT200111_r_timely.
  // Loc tokens are lowercased in curated ids (CT200111 -> ct200111).
  // The registry's `flow` field selects WHICH leg is the feedgas signal:
  // delivery-side meters read 'd', receipt-side meters 'r'.
  const loc = String(t.loc).toLowerCase();
  const flow = t.flow || 'd';
  if (t.signal === 'oac-proxy') {
    return {
      sqPrefix: null,
      kindPrefixes: {
        sq: `${t.seriesPrefix}_sq_${loc}_${flow}_`,
        oac: `${t.seriesPrefix}_oac_${loc}_${flow}_`,
        design: `${t.seriesPrefix}_design_${loc}_${flow}_`,
        opcap: `${t.seriesPrefix}_opcap_${loc}_${flow}_`,
      },
    };
  }
  return { sqPrefix: `${t.seriesPrefix}_sq_${loc}_${flow}_`, kindPrefixes: null };
}

/**
 * Cycle publication summary used by the revisions sub-panel.
 *
 * @param {LngTerminal} t
 * @returns {{count: number, note: string}}
 */
export function terminalCycleInfo(t) {
  const count = t.cycles ? t.cycles.length : 5;
  return { count, note: t.platformNote || '' };
}
