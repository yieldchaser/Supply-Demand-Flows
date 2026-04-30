/**
 * Transatlantic Storage Divergence — interlude card between Section 1 and Section 2.
 *
 * Derived metric: US storage deviation from 5y avg (%) vs EU aggregate deviation (pp).
 * Spread = us_signal − eu_signal → drives gauge needle + regime classification.
 *
 * Data sources:
 *   bundle.sources.eia_storage — US weekly storage (Lower 48 or aggregate)
 *   bundle.sources.gie_agsi   — 10 EU countries daily % full
 */

import { getSeries, computeWeeklyEnvelope, isoWeek } from '../util/series.js';

/* ─────────────────────────────────────────────── */
/*  Constants                                      */
/* ─────────────────────────────────────────────── */

const EU_COUNTRIES = ['de', 'fr', 'it', 'nl', 'es', 'at', 'be', 'cz', 'hu', 'pl'];

// Gauge range in pp spread units
const GAUGE_MIN = -30;
const GAUGE_MAX = 30;

/* ─────────────────────────────────────────────── */
/*  Entry point                                    */
/* ─────────────────────────────────────────────── */

export function renderDivergencePanel(panelEl, bundle) {
  const usSource = bundle.sources?.eia_storage;
  const euSource = bundle.sources?.gie_agsi;

  if (!usSource?.data?.length || !euSource?.data?.length) {
    panelEl.innerHTML = '<div class="panel-error">Storage data unavailable for transatlantic comparison.</div>';
    return;
  }

  /* ── US: current vs 5y avg ───────────────────────────────────── */
  // Prefer the Lower-48 aggregate series (single series_id for total storage).
  // EIA storage schema: series_id like 'eia_storage_lower48_bcf' or similar.
  // Fallback: collect all rows and keep the numerically largest-magnitude series
  // (that tends to be the national aggregate).
  let usSeries = _buildUsSeries(usSource.data);

  const usEnvelope = computeWeeklyEnvelope(usSeries, 5);
  const usLatest   = usSeries.length ? usSeries[usSeries.length - 1] : null;
  const usWk       = usLatest ? isoWeek(usLatest.period) : null;
  const usEnv      = usWk ? usEnvelope.get(usWk) : null;

  // us_signal: % deviation from seasonal mean (positive = above avg = loose)
  const us_signal = (usLatest && usEnv && usEnv.mean > 0)
    ? ((usLatest.value - usEnv.mean) / usEnv.mean) * 100
    : null;

  /* ── EU: capacity-weighted aggregate current vs 5y avg ────────── */
  const { euAggSeries, euLatest } = _buildEuAggregate(euSource.data);

  const euEnvelope = computeWeeklyEnvelope(euAggSeries, 5);
  const euWk       = euLatest ? isoWeek(euLatest.period) : null;
  const euEnv      = euWk ? euEnvelope.get(euWk) : null;

  // eu_signal: pp deviation from seasonal mean (positive = above avg = loose)
  const eu_signal = (euLatest && euEnv) ? euLatest.value - euEnv.mean : null;

  /* ── Spread + gauge ───────────────────────────────────────────── */
  const spread      = (us_signal ?? 0) - (eu_signal ?? 0);
  const needlePct   = Math.max(0, Math.min(100, ((spread - GAUGE_MIN) / (GAUGE_MAX - GAUGE_MIN)) * 100));
  const regime      = classifyRegime(us_signal ?? 0, eu_signal ?? 0);

  /* ── Date label (prefer EU daily freshness) ───────────────────── */
  const displayDate = (euLatest ?? usLatest)?.period.toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
  }) ?? '—';

  /* ── Render ───────────────────────────────────────────────────── */
  panelEl.innerHTML = `
    <div class="divergence-card">
      <div class="divergence-card__header">
        <span class="divergence-card__title">Transatlantic Storage Divergence</span>
        <span class="divergence-card__date">${displayDate}</span>
      </div>

      <div class="divergence-card__body">
        <!-- US side -->
        <div class="divergence-side divergence-side--us">
          <div class="divergence-side__label">US</div>
          <div class="divergence-side__value num ${us_signal != null && us_signal >= 0 ? 'is-loose' : 'is-tight'}">
            ${us_signal != null
              ? (us_signal >= 0 ? '+' : '') + us_signal.toFixed(1) + '%'
              : 'n/a'}
          </div>
          <div class="divergence-side__sub">vs 5y avg</div>
        </div>

        <!-- Gauge -->
        <div class="divergence-gauge">
          <div class="divergence-gauge__bar">
            <div class="divergence-gauge__needle" style="left: ${needlePct.toFixed(2)}%"></div>
          </div>
          <div class="divergence-gauge__labels">
            <span class="divergence-gauge__edge divergence-gauge__edge--left">US loose</span>
            <span class="divergence-gauge__spread num">${spread.toFixed(1)} pp spread</span>
            <span class="divergence-gauge__edge divergence-gauge__edge--right">EU tight</span>
          </div>
        </div>

        <!-- EU side -->
        <div class="divergence-side divergence-side--eu">
          <div class="divergence-side__label">EU</div>
          <div class="divergence-side__value num ${eu_signal != null && eu_signal >= 0 ? 'is-loose' : 'is-tight'}">
            ${eu_signal != null
              ? (eu_signal >= 0 ? '+' : '') + eu_signal.toFixed(1) + ' pp'
              : 'n/a'}
          </div>
          <div class="divergence-side__sub">vs 5y avg</div>
        </div>
      </div>

      <!-- Regime interpretation -->
      <div class="divergence-card__interpretation divergence-card__interpretation--${regime.kind}">
        <span class="divergence-card__icon">${regime.icon}</span>
        <span class="divergence-card__regime">${regime.label}</span>
        <span class="divergence-card__detail">— ${regime.detail}</span>
      </div>
    </div>
  `;
}

/* ================================================================== */
/*  US series builder                                                  */
/* ================================================================== */

function _buildUsSeries(rows) {
  // Try to find a national/Lower-48 aggregate series by common series_id patterns.
  const PREFER_PATTERNS = ['lower_48', 'lower48', 'l48', 'united_states', 'total'];

  // Group by series_id and find the best candidate.
  const seriesMap = new Map();
  for (const r of rows) {
    if (!seriesMap.has(r.series_id)) seriesMap.set(r.series_id, []);
    seriesMap.get(r.series_id).push(r);
  }

  // Pick the series_id that matches a preferred pattern, or the one with most rows.
  let bestKey = null;
  for (const key of seriesMap.keys()) {
    const lower = key.toLowerCase();
    if (PREFER_PATTERNS.some((p) => lower.includes(p))) {
      bestKey = key;
      break;
    }
  }
  if (!bestKey) {
    // Fallback: take the series with the most data points
    let maxLen = 0;
    for (const [k, v] of seriesMap) {
      if (v.length > maxLen) { maxLen = v.length; bestKey = k; }
    }
  }

  const raw = bestKey ? seriesMap.get(bestKey) : [];
  return raw
    .map((r) => ({ ...r, period: new Date(r.period), value: Number(r.value) }))
    .sort((a, b) => a.period - b.period);
}

/* ================================================================== */
/*  EU aggregate builder (matches eu-storage.js approach exactly)     */
/* ================================================================== */

function _buildEuAggregate(rows) {
  const countrySeries = EU_COUNTRIES.map((cc) => {
    const fullPct   = getSeries(rows, `gie_storage_${cc}_full_pct`);
    const gasInStor = getSeries(rows, `gie_storage_${cc}_gas_in_storage`);
    const maxStorage = gasInStor.length
      ? Math.max(...gasInStor.map((r) => r.value))
      : 0;
    return { cc, fullPct, maxStorage };
  }).filter((c) => c.fullPct.length > 0 && c.maxStorage > 0);

  const allTimes = [
    ...new Set(countrySeries.flatMap((c) => c.fullPct.map((r) => r.period.getTime()))),
  ].sort((a, b) => a - b);

  const euAggSeries = allTimes.map((t) => {
    let weightedSum = 0;
    let weightSum   = 0;
    for (const c of countrySeries) {
      const match = c.fullPct.find((r) => r.period.getTime() === t);
      if (match) {
        weightedSum += match.value * c.maxStorage;
        weightSum   += c.maxStorage;
      }
    }
    return weightSum > 0 ? { period: new Date(t), value: weightedSum / weightSum } : null;
  }).filter(Boolean);

  const euLatest = euAggSeries.length ? euAggSeries[euAggSeries.length - 1] : null;
  return { euAggSeries, euLatest };
}

/* ================================================================== */
/*  Regime classifier                                                  */
/* ================================================================== */

function classifyRegime(us_signal, eu_signal) {
  const us_loose = us_signal > 3;   // US well above seasonal norm
  const us_tight = us_signal < -3;  // US well below seasonal norm
  const eu_loose = eu_signal > 5;   // EU well above seasonal norm
  const eu_tight = eu_signal < -5;  // EU well below seasonal norm

  if (us_loose && eu_tight) return {
    label: 'Bullish for US LNG exports',
    detail: 'US storage comfortable while European storage entered injection season anomalously low.',
    icon: '🚀',
    kind: 'bullish',
  };
  if (us_tight && eu_tight) return {
    label: 'Global gas stress',
    detail: 'Both regions entering season with depleted storage. Watch for price acceleration on either side of the Atlantic.',
    icon: '⚠️',
    kind: 'warning',
  };
  if (us_loose && eu_loose) return {
    label: 'Slack global market',
    detail: 'Storage comfortable on both sides. Spreads compressed; volatility likely subdued near-term.',
    icon: '😴',
    kind: 'neutral',
  };
  if (us_tight && eu_loose) return {
    label: 'Reverse flow pressure',
    detail: 'US storage tight while European storage healthy. Atypical configuration — watch for export curtailment risk.',
    icon: '🔄',
    kind: 'warning',
  };
  return {
    label: 'Neutral transatlantic balance',
    detail: 'Neither region is meaningfully out of seasonal norms. Watch the spread for early regime shifts.',
    icon: '⚖️',
    kind: 'neutral',
  };
}
