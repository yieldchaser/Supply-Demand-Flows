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
    source: 'gulf_south',
    seriesPrefix: 'gulf_south',
    loc: '24329',
    flow: 'd',
    locName: 'Stratton Ridge (To Freeport Lng)',
    nameplate: 2100,
    signal: 'sq',
    cycles: ['id1', 'id2', 'id3'],
    platformLabel: 'Boardwalk OAC',
    platformNote:
      'Gulf South (Boardwalk OAC) posts ID1/ID2/ID3 only — TIMELY/EVENING are not published as CSV on this platform.',
    methodLine:
      'TSQ at Stratton Ridge (loc 24329, delivery) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d',
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
      'TSQ at EGTS–Loudoun (loc 40704, Cove Point LNG LP interconnect, delivery leg) · Dth ÷ 1.025 ÷ 1,000 = MMcf/d · Zeros = no cargo activity',
  },

  sabine_pass: {
    id: 'sabine_pass',
    display: 'Sabine Pass',
    source: 'cheniere',
    seriesPrefix: 'creole_trail',
    loc: 'CT200111',
    flow: 'd',
    locName: 'Creole Trail – SPLIQ delivery interconnect',
    nameplate: 4500,
    signal: 'oac-proxy',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'Cheniere LNG Connection',
    platformNote:
      'Sabine Pass (Cheniere LNG Connection) posts all five NAESB cycles. Flow is INFERRED from capacity consumption — see ⓘ.',
    methodLine:
      'Implied flow = Design Capacity − Operationally Available at CT200111 (Creole Trail, SPLIQ) · PROXY: Cheniere does not publish Scheduled Quantities',
  },

  corpus_christi: {
    id: 'corpus_christi',
    display: 'Corpus Christi',
    source: 'cheniere',
    seriesPrefix: 'corpus_christi',
    loc: 'CC100221',
    flow: 'd',
    locName: 'Corpus Christi – CCLIQ delivery interconnect (CC200221 class)',
    nameplate: 2400,
    signal: 'oac-proxy',
    cycles: ['timely', 'evening', 'id1', 'id2', 'id3'],
    platformLabel: 'Cheniere LNG Connection',
    platformNote:
      'Corpus Christi (Cheniere LNG Connection) posts all five NAESB cycles. Flow is INFERRED from capacity consumption — see ⓘ.',
    methodLine:
      'Implied flow = Design Capacity − Operationally Available at CC100221 (Corpus Christi, CCLIQ) · PROXY: Cheniere does not publish Scheduled Quantities',
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
 * Terminals whose numbers are inferred proxies and therefore EXCLUDED from
 * the fleet aggregate total (footnoted in the UI).
 *
 * @type {string[]}
 */
export const FLEET_PROXY_EXCLUSIONS = ['sabine_pass', 'corpus_christi'];

/**
 * Resolve the series-id matcher strings for one terminal.
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
