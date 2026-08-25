/**
 * Basin Egress registry — Haynesville take-away corridors on Gulf South.
 *
 * Derived from config/meters/classification.json (class: basin_egress).
 * Every meter here ships in the bundle via the publisher's relevance
 * allowlist; the headline uses HIGH-confidence meters only, the corridor
 * table includes MEDIUM ones flagged, and storage meters feed nothing yet
 * (they are listed for the next panel — kept out of this module's totals).
 *
 * Corridors group meters by the pipe/zone they evacuate into. Assignment
 * follows each classification entry's researched evidence (Rextag profiles,
 * PHMSA operator tables) — no guesses.
 */

export const BASIN_SOURCE = 'gulf_south';

/** Dth/d → MMcf/d conversion must match util/lng-metrics.dth_to_mmcf. */

/**
 * @typedef {Object} EgressMeter
 * @property {number} loc - Gulf South location id
 * @property {string} name - human-readable meter name from classification
 * @property {'high'|'medium'} confidence - classification confidence
 * @property {string} corridor - egress corridor key
 * @property {'D'|'R'|'?'} flow - flow direction as posted
 * @property {boolean} inHeadline - high-confidence only
 */

/**
 * Corridor groups, ordered for the stacked area chart (largest last so its
 * band sits at the top of the stack). Keys are stable ids, not display text.
 *
 * @type {Array<{key: string, label: string}>}
 */
export const CORRIDORS = [
  { key: 'transco', label: 'Transco (Station 85 + Petal)' },
  { key: 'midship_bennington', label: 'Bennington (Midship / Enable / MarkWest)' },
  { key: 'texas_gas_lonewa', label: 'Texas Gas (Lonewa)' },
  { key: 'carthage', label: 'Carthage hub gathers' },
  { key: 'gathering', label: 'Haynesville gathering / processing' },
  { key: 'other', label: 'Other egress points' },
];

/**
 * The 37 basin_egress meters. `inHeadline` is true only for confidence:
 * high entries (the classification file is authoritative).
 *
 * @type {Array<EgressMeter>}
 */
export const EGRESS_METERS = [
  // ── Transco corridor ──
  { loc: 22108, name: 'Rock Springs/Scott Mtn (To Transco 85)', confidence: 'high', corridor: 'transco', flow: 'D', inHeadline: true },
  { loc: 23373, name: 'Transco (Petal Pipeline)', confidence: 'high', corridor: 'transco', flow: '?', inHeadline: true },

  // ── Texas Gas ──
  { loc: 3362, name: 'Lonewa (To Texas Gas)', confidence: 'high', corridor: 'texas_gas_lonewa', flow: 'D', inHeadline: true },

  // ── Other HIGH-confidence (each its own named line in the table) ──
  { loc: 21805, name: 'Discovery Gas Transmission', confidence: 'high', corridor: 'other', flow: '?', inHeadline: true },
  { loc: 22110, name: 'Gulf Run Delhi', confidence: 'high', corridor: 'other', flow: '?', inHeadline: true },
  { loc: 22708, name: 'Bulldog Panola (BTA EGT Gathering)', confidence: 'high', corridor: 'gathering', flow: '?', inHeadline: true },
  { loc: 24469, name: 'Bland Lake-Kudu', confidence: 'high', corridor: 'gathering', flow: '?', inHeadline: true },
  { loc: 26016, name: 'BBT Trans-Union Claiborne Parish', confidence: 'high', corridor: 'other', flow: '?', inHeadline: true },

  // ── Bennington complex (all medium) ──
  { loc: 22329, name: 'Sherman (From Enterprise)', confidence: 'medium', corridor: 'other', flow: 'R', inHeadline: false },
  { loc: 22330, name: 'Bennington (From Enable OK)', confidence: 'medium', corridor: 'midship_bennington', flow: 'R', inHeadline: false },
  { loc: 22492, name: 'Bennington (From Mark West)', confidence: 'medium', corridor: 'midship_bennington', flow: 'R', inHeadline: false },
  { loc: 24421, name: 'Bennington (From Midship)', confidence: 'medium', corridor: 'midship_bennington', flow: 'R', inHeadline: false },

  // ── Carthage hub (all medium) ──
  { loc: 21921, name: 'Midcoast - Carthage (Expansion)', confidence: 'medium', corridor: 'carthage', flow: '?', inHeadline: false },
  { loc: 21922, name: 'Energy Transfer - Carthage (Expansion)', confidence: 'medium', corridor: 'carthage', flow: '?', inHeadline: false },
  { loc: 21923, name: 'Enterprise - Carthage (Expansion)', confidence: 'medium', corridor: 'carthage', flow: '?', inHeadline: false },
  { loc: 22171, name: 'BTA Plant Rec - Carthage (Expansion)', confidence: 'medium', corridor: 'carthage', flow: 'R', inHeadline: false },
  { loc: 22410, name: 'Plantation West Cp (KinderHawk)', confidence: 'medium', corridor: 'carthage', flow: '?', inHeadline: false },
  { loc: 22561, name: 'Plantation West II - Expansion', confidence: 'medium', corridor: 'carthage', flow: '?', inHeadline: false },
  { loc: 24245, name: 'MarkWest Carthage [Expansion]', confidence: 'medium', corridor: 'carthage', flow: '?', inHeadline: false },

  // ── Gathering / processing (all medium) ──
  { loc: 21416, name: 'Section 23 Cp - Aethon United', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 21532, name: 'Ibex Koran Cp', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 22382, name: 'Thornlake Aethon', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 22631, name: 'Magnolia CDP II - Expansion', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 22653, name: 'DeSoto Parish (ETC Field Services)', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 24362, name: 'Momentum Midstream (M5 DeSoto)', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 24424, name: 'Aethon Hwy 5 (Expansion)', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 24446, name: 'Gemini Panola County TX (Expansion)', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 24454, name: 'AMP II ETX Panola (Expansion)', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 24501, name: 'Sponte Cp Panola County', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },
  { loc: 26108, name: 'GEP Haynesville II, LLC', confidence: 'medium', corridor: 'gathering', flow: '?', inHeadline: false },

  // ── Other egress points (all medium) ──
  { loc: 22129, name: 'Tennessee Heidelburg (Expansion)', confidence: 'medium', corridor: 'other', flow: '?', inHeadline: false },
  { loc: 22636, name: 'Logansport Cp 1', confidence: 'medium', corridor: 'other', flow: '?', inHeadline: false },
  { loc: 22647, name: 'Hall Summit Cp - QEP (Expansion)', confidence: 'medium', corridor: 'other', flow: '?', inHeadline: false },
  { loc: 22662, name: 'Holly Field Cp - EXCO (Expansion)', confidence: 'medium', corridor: 'other', flow: '?', inHeadline: false },
  { loc: 22810, name: 'Wharton (From Enterprise Texas)', confidence: 'medium', corridor: 'other', flow: 'R', inHeadline: false },
  { loc: 24494, name: 'TriState Longview (From TriState)', confidence: 'medium', corridor: 'other', flow: 'R', inHeadline: false },
  { loc: 9332, name: 'West Monroe (From Enable)', confidence: 'medium', corridor: 'other', flow: 'R', inHeadline: false },
];

/**
 * All basin_egress loc ids as lowercase strings — exactly what the
 * publisher's relevance allowlist needs.
 *
 * @returns {Set<string>}
 */
export function basinEgressLocIds() {
  return new Set(EGRESS_METERS.map((m) => String(m.loc)));
}

/**
 * Storage meters (class: storage) — next panel's inputs, allowlisted now so
 * history accumulates.
 *
 * @type {Array<{loc: number, name: string}>}
 */
export const STORAGE_METERS = [
  { loc: 10401, name: 'Bistineau Injection' },
  { loc: 10402, name: 'Bistineau Withdrawal' },
  { loc: 22806, name: 'Bistineau Injection Expansion' },
  { loc: 22807, name: 'Bistineau Withdrawal Expansion' },
  { loc: 23351, name: 'Tres Palacios Storage' },
  { loc: 23352, name: 'Bay Gas Storage @ Axis' },
  { loc: 23353, name: 'Bay Gas Storage @ Whistler Junction' },
  { loc: 23356, name: 'Jefferson Island Storage' },
  { loc: 23357, name: 'Napoleonville Storage (Bridgeline)' },
  { loc: 23358, name: 'Enstor Katy Storage' },
  { loc: 23360, name: 'Arcadia Gas Storage' },
  { loc: 23361, name: 'Bobcat Storage' },
  { loc: 23362, name: 'Sesh (Petal Storage)' },
  { loc: 23369, name: 'Tennessee Gas (Petal Storage)' },
  { loc: 23374, name: 'Petal Gas Storage (Gulf South Leg)' },
  { loc: 23375, name: 'Gulf South Leg (Petal Storage)' },
  { loc: 23377, name: 'Bistineau Storage (Enable)' },
  { loc: 23378, name: 'Leaf River Storage' },
  { loc: 23380, name: 'BBT Mississippi (Petal Storage)' },
  { loc: 23601, name: 'Jackson Storage Injection' },
  { loc: 23602, name: 'Jackson Storage Withdrawal' },
  { loc: 24346, name: 'Tooke Well W/D' },
  { loc: 50201, name: 'Petal Storage Injection/Withdrawal' },
  { loc: 50202, name: 'Petal Pipeline Injection/Withdrawal' },
];

/**
 * Power burn meters (class: power_burn) — allowlisted for future panels.
 *
 * @type {Array<{loc: number, name: string}>}
 */
export const POWER_BURN_METERS = [
  { loc: 68, name: 'Willow Glen Power (To GSU)' },
  { loc: 73, name: 'Nine Mile Power Plt (To LP&L)' },
  { loc: 2425, name: 'Gulf Power Plt @ Pensacola - Plt Crist' },
  { loc: 2534, name: 'Jack Watson Power Plant @ Gulfport' },
  { loc: 2602, name: 'Benndale Power Plant' },
  { loc: 2605, name: 'Moselle Power Plant' },
  { loc: 9141, name: 'Little Gypsy Power (To LP&L)' },
  { loc: 9258, name: 'Sterlington Power (To LP&L)' },
  { loc: 9702, name: 'Knox Lee Power (SWEPCO)' },
  { loc: 10675, name: 'Pirkey Power Plant (To SWEPCO)' },
  { loc: 20529, name: 'Plant Daniels (Power Plant)' },
  { loc: 20862, name: 'Calcasieu Power Lake Charles' },
  { loc: 21134, name: 'Santa Rosa Cogen Plant' },
  { loc: 21175, name: 'AEP - Eastex Cogen' },
  { loc: 21289, name: 'Oxychem Cogen (St Charles Parish)' },
  { loc: 21349, name: 'Marshall Power Plant (Entergy)' },
  { loc: 21681, name: 'Hargis Hebert Power Plant' },
  { loc: 21703, name: 'KM to GSPL @ Marshall Power Plant - PIG' },
  { loc: 23074, name: 'Rayburn Energy Station' },
  { loc: 24287, name: 'Colorado Bend Plant (State Hwy 60)' },
  { loc: 24352, name: 'St Charles Power Station' },
  { loc: 24439, name: 'Lake Charles Power Station' },
  { loc: 24451, name: 'Montgomery County Power Plant' },
  { loc: 26124, name: 'Vicksburg Advanced Power Station' },
];
