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
    // Multi-feed: Gulf South (Boardwalk) + TETCO (Enbridge rtba) both feed
    // the Freeport lateral at Stratton Ridge. KMTP (intrastate) is not
    // publicly posted — figures are conservative.
    feeds: [
      { source: 'gulf_south', series: 'gulf_south_sq_24329_d', label: 'Gulf South' },
      { source: 'enbridge', series: 'tetco_sq_79999_d', label: 'TETCO' },
    ],
    locName: 'Stratton Ridge — dual feed (Gulf South + TETCO)',
    nameplate: 2100,
    signal: 'sq',
    cycles: ['id1', 'id2', 'id3'],
    platformLabel: 'Boardwalk OAC + Enbridge rtba',
    platformNote:
      'Gulf South posts ID1/ID2/ID3 only; TETCO (Enbridge rtba) posts Timely/Evening/Intraday. Figures are interstate-visible feedgas only — KMTP (intrastate) is not publicly posted, so totals are conservative.',
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
    locName: 'Venture Global Plaquemines LNG Delivery (VGPQD)',
    nameplate: 3400,
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
    locName: 'Venture Global Calcasieu Pass Delivery (VGCPD)',
    nameplate: 1300,
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
    flow: 'd',   // D leg = real ramp (R leg is 0 across all 90 days)
    locName: 'Golden Pass Terminal (delivery meter)',
    nameplate: 2600,
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'GasNom ESG',
    platformNote:
      'Golden Pass (GasNom ESG) posts all five NAESB cycles — Timely, Evening, ID1, ID2, ID3.',
    methodLine:
      'TSQ at Golden Pass Terminal (loc 1097217, delivery) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d',
  },

  cameron: {
    id: 'cameron',
    display: 'Cameron',
    source: 'gasnom',
    seriesPrefix: 'cameron_interstate',
    loc: '772300',
    flow: 'd',
    locName: 'Cameron LNG (Del)',
    nameplate: 2000,
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'GasNom ESG',
    platformNote:
      'Cameron (GasNom ESG) posts all five NAESB cycles — Timely, Evening, ID1, ID2, ID3.',
    methodLine:
      'TSQ at Cameron LNG complex (loc 772300, delivery) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d',
  },

  cove_point: {
    id: 'cove_point',
    display: 'Cove Point',
    source: 'bhe',
    seriesPrefix: 'egts',
    loc: '40704',
    flow: 'd',   // D leg carries cargo volumes; R leg ~0 (config legacy said R — corrected)
    locName: 'EGTS – Loudoun (Cove Point LNG LP interconnect)',
    nameplate: 750,
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'BHE GT&S EBB',
    platformNote:
      'Cove Point (BHE GT&S EBB) posts all five NAESB cycles. Zeros are legitimate — cargo-driven facility.',
    methodLine:
      'MEASURED-PARTIAL: TSQ at EGTS–Loudoun (loc 40704, delivery leg) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · COVERAGE GAP: this is one of several parallel feeds into the 750 MMcf/d terminal. CPL\'s own EBB (infopost.bhegts.com/cpl) additionally shows Transco Pleasant Valley (~600k Dth/d receipts) + Columbia Loudoun (~300k) + CPL storage draw (~750k) — none of which flow through EGTS-40704. The EGTS figure is real but covers only a fraction of total feedgas; never present it as the terminal total.',
  },

  sabine_pass: {
    id: 'sabine_pass',
    display: 'Sabine Pass',
    // MEASURED-BUT-PARTIAL (2026-08-25 audit): every VISIBLE lateral is
    // genuinely measured — NGPL 3592 (~470 MMcf/d) + Cheniere's own
    // Creole Trail CT200111 published SQ (~1,408 MMcf/d). The old
    // "OAC proxy implies full-terminal ~1,408" framing was WRONG:
    // design − OAC equals schedD_QTY identically at CT200111, so the
    // proxy was just this lateral's flow restated, never a terminal
    // estimate. The true unknown is Transco Z3 + other unposted feeds
    // (~half of a 4,500 MMcf/d terminal). Measured laterals are summed;
    // the card/hero label says what fraction of nameplate is visible.
    feeds: [
      {
        source: 'kinder_morgan',
        series: 'km_ngpl_sq_3592_d',
        label: 'NGPL lateral (measured)',
        kind: 'measured-partial',
        note: 'Real TSQ at the NGPL interconnect.',
      },
      {
        source: 'cheniere',
        series: 'creole_trail_sq_CT200111_d',
        label: 'Creole Trail lateral (measured)',
        kind: 'measured-partial',
        note: 'Cheniere-published TSQ at SPLIQ. Supersedes the retired OAC-proxy framing (identical values, better provenance).',
      },
    ],
    locName: 'SPL laterals — NGPL 3592 + Creole Trail CT200111 (both measured)',
    nameplate: 4500,
    signal: 'sq',
    cycles: ['id1', 'id2', 'id3'],
    platformLabel: 'KM pipeline2 + Cheniere LNG Connection',
    platformNote:
      'MEASURED-BUT-PARTIAL: figures sum the two publicly-visible laterals (~1.88 Bcf/d combined, roughly 40% of the 4,500 MMcf/d terminal). Transco Z3 and other SPL feeds do not publicly post (Transco 1Line SPA migration shelved that scraper), so terminal-wide feedgas is not measurable from public data. Never present the visible-lateral sum as the terminal total.',
    methodLine:
      'MEASURED laterals: KM NGPL loc 3592 + Cheniere Creole Trail CT200111 (published SQ) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · COVERAGE GAP: Transco Z3 + other SPL feeds not public — figure undercounts the 4,500 MMcf/d terminal by the invisible share · Former OAC-proxy framing retired 2026-08-25 (design−OAC ≡ SQ at CT200111)',
  },

  corpus_christi: {
    id: 'corpus_christi',
    display: 'Corpus Christi',
    // PROMOTED to MEASURED 2026-08-25: Cheniere's own LNG Connection site
    // PUBLISHES Scheduled Quantities (schedD_QTY) — the "no public SQ"
    // premise behind the oac-proxy holding was wrong. corpus_christi_sq_
    // CC200221_d carries 90+ days of full-terminal history (median
    // 2.46M Dth/d ≈ 100% of nameplate) and cross-corroborates with KM's
    // independent TGP Sinton meter (identical 169,489 Dth/d on overlap).
    // Cycle pinning resolved the old "169k vs 79k" swing as cycle-sampling:
    // per-cycle values are stable; the 08-22 ~50k drop was a genuine
    // mid-day revision.
    feeds: [
      {
        source: 'cheniere',
        series: 'corpus_christi_sq_CC200221_d',
        label: 'CCPL measured',
        kind: 'measured',
        note: 'Cheniere LNG Connection published TSQ at CCLIQ (full terminal).',
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
    signal: 'sq',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'Cheniere LNG Connection + KM pipeline2',
    platformNote:
      'Corpus Christi headline = MEASURED published TSQ at CCPL interconnect CC200221 (all five cycles). The earlier capacity-proxy framing is retired: schedD_QTY is published on lngconnection.cheniere.com. TGP Sinton (49861) ships as a secondary comparison only.',
    methodLine:
      'MEASURED: Cheniere CCPL published TSQ at CC200221 (CCLIQ delivery), all cycles · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · Cross-check: KM TGP Sinton 49861 (independent meter) · Former OAC-proxy framing retired 2026-08-25 after cycle pinning showed per-cycle stability',
  },

  port_arthur: {
    id: 'port_arthur',
    display: 'Port Arthur',
    nameplate: 1900,
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
