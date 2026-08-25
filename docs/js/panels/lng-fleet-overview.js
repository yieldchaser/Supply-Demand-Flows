/**
 * LNG Fleet Overview — responsive 9-card grid above the hero Feedgas panel.
 *
 * Each card: latest MMcf/d (mono tnum), utilization bar (color ladder shared
 * with the hero panel), 7-day sparkline (inline SVG), WoW delta. Cards are
 * clickable and drive the hero panel below.
 *
 * AGGREGATE SEMANTICS: the fleet total sums MEASURED scheduled quantities
 * only — oac-proxy terminals (Sabine Pass, Corpus Christi) are excluded and
 * footnoted, because their numbers are inferred from capacity consumption,
 * not published SQ.
 */

import * as d3 from 'd3';
import {
  LNG_TERMINALS,
  LNG_FLEET_ORDER,
  FLEET_PROXY_EXCLUSIONS,
  DEFAULT_TERMINAL_ID,
  terminalSeriesPrefixes,
} from '../util/lng-terminals.js';
import { dth_to_mmcf, get_utilization_level } from '../util/lng-metrics.js';

/** Cycle publication priority (later cycles supersede earlier ones). */
const CYCLE_PRIORITY = {
  timely: 1,
  evening: 2,
  latec: 3, // TETCO's legacy overnight correction re-post (final for its gas day)
  late: 4,
  id1: 5,
  id2: 6,
  id3: 7,
};

/**
 * Cycle priority for a token, falling back to numeric id{HH}00 buckets
 * (TETCO posts hourly intraday snapshots — higher hour = fresher).
 *
 * @param {string} cycle
 * @returns {number}
 */
export function cyclePriority(cycle) {
  if (CYCLE_PRIORITY[cycle] !== undefined) return CYCLE_PRIORITY[cycle];
  const m = /^id(\d{2})00$/.exec(cycle);
  if (m) return 100 + parseInt(m[1], 10);
  return 0;
}

/**
 * Build a date -> {cycle -> MMcf} map for a direct-SQ terminal.
 * Zeros are legitimate — never treated as missing.
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {LngTerminal} t
 * @returns {Object<string, Object<string, number>>}
 */
export function buildSqCycleMaps(rows, t) {
  const { sqPrefix } = terminalSeriesPrefixes(t);
  const byDate = {};
  rows.forEach((r) => {
    // Case-insensitive: some curated ids keep uppercase loc tokens
    // (e.g. creole_trail_sq_CT200111_id3).
    const sid = r.series_id.toLowerCase();
    if (!sid.startsWith(sqPrefix)) return;
    const cycle = sid.slice(sqPrefix.length).toLowerCase();
    if (!byDate[r.period]) byDate[r.period] = {};
    byDate[r.period][cycle] = dth_to_mmcf(Number(r.value));
  });
  return byDate;
}

/**
 * Build a daily series (one point per gas day, latest cycle wins) for a
 * direct-SQ terminal.
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {LngTerminal} t
 * @returns {Array<{dateStr: string, date: Date, value: number, cycle: string}>}
 */
export function buildDailySqSeries(rows, t) {
  return buildDailyFromCycles(buildSqCycleMaps(rows, t));
}

/**
 * Build a date -> {cycle -> implied MMcf} map for an oac-proxy terminal:
 *   implied_flow(cycle) = (design ?? opcap) − oac   [Dth -> MMcf]
 * Falls back to opcap when design is absent for that cycle; cycles missing
 * either side are skipped (rendered as "—" upstream).
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {LngTerminal} t
 * @returns {Object<string, Object<string, number>>}
 */
export function buildProxyImpliedByDate(rows, t) {
  const { kindPrefixes } = terminalSeriesPrefixes(t);
  /** @type {Object<string, Object<string, Object<string, number>>>} */
  const rawByDate = {};
  rows.forEach((r) => {
    // Case-insensitive: some curated ids keep uppercase loc tokens.
    const sid = r.series_id.toLowerCase();
    for (const kind of Object.keys(kindPrefixes)) {
      const p = kindPrefixes[kind];
      if (!sid.startsWith(p)) continue;
      const cycle = sid.slice(p.length).toLowerCase();
      if (!rawByDate[r.period]) rawByDate[r.period] = {};
      if (!rawByDate[r.period][cycle]) rawByDate[r.period][cycle] = {};
      rawByDate[r.period][cycle][kind] = Number(r.value);
      break;
    }
  });

  /** @type {Object<string, Object<string, number>>} */
  const impliedByDate = {};
  Object.keys(rawByDate).forEach((dateStr) => {
    Object.keys(rawByDate[dateStr]).forEach((cycle) => {
      const parts = rawByDate[dateStr][cycle];
      const capacity =
        parts.design !== undefined ? parts.design : parts.opcap;
      if (capacity === undefined || parts.oac === undefined) return;
      if (!impliedByDate[dateStr]) impliedByDate[dateStr] = {};
      impliedByDate[dateStr][cycle] = dth_to_mmcf(capacity - parts.oac);
    });
  });
  return impliedByDate;
}

/**
 * Build a daily series for an oac-proxy terminal.
 *
 * @param {Array<{series_id: string, period: string, value: number}>} rows
 * @param {LngTerminal} t
 * @returns {Array<{dateStr: string, date: Date, value: number, cycle: string}>}
 */
export function buildDailyProxySeries(rows, t) {
  return buildDailyFromCycles(buildProxyImpliedByDate(rows, t));
}

/**
 * Collapse a date -> {cycle -> MMcf} map into a chronologically sorted daily
 * series keeping the highest-priority cycle per day.
 *
 * @param {Object<string, Object<string, number>>} byDate
 * @returns {Array<{dateStr: string, date: Date, value: number, cycle: string}>}
 */
export function buildDailyFromCycles(byDate) {
  const out = [];
  Object.keys(byDate).forEach((dateStr) => {
    const cycles = byDate[dateStr];
    let best = null;
    let bestPrio = -1;
    Object.keys(cycles).forEach((cy) => {
      const prio = cyclePriority(cy);
      if (prio > bestPrio) {
        bestPrio = prio;
        best = cy;
      }
    });
    if (best !== null) {
      out.push({ dateStr, date: new Date(dateStr), value: cycles[best], cycle: best });
    }
  });
  out.sort((a, b) => a.date.getTime() - b.date.getTime());
  return out;
}

/**
 * Compute per-terminal summary metrics used by cards + the aggregate line.
 *
 * @param {Object} bundle
 * @param {LngTerminal} t
 * @returns {{ok: boolean, latest?: number, utilPct?: number, spark?: number[],
 *   wow?: {delta: number, pct: number}, days?: number}}
 */
export function terminalSummary(bundle, t) {
  // Multi-feed terminals: combine per-feed daily series (sum of feeds).
  if (Array.isArray(t.feeds) && t.feeds.length > 0) {
    const feedDailies = [];
    for (const feed of t.feeds) {
      const d = buildFeedDaily(bundle, feed);
      if (d.length) feedDailies.push(d);
    }
    if (feedDailies.length === 0) return { ok: false };

    // Merge by date: total = sum of available feeds that day (zeros are data).
    const byDate = {};
    feedDailies.forEach((daily) => {
      daily.forEach((d) => {
        if (!byDate[d.dateStr]) byDate[d.dateStr] = { date: d.date, total: 0 };
        byDate[d.dateStr].total += d.value;
      });
    });
    const merged = Object.entries(byDate)
      .map(([dateStr, v]) => ({ dateStr, date: v.date, value: v.total }))
      .sort((a, b) => a.date.getTime() - b.date.getTime());
    if (merged.length === 0) return { ok: false };
    // Anchor the headline to the latest day where EVERY feed reported — a
    // pipe that posts later in the gas day must not fake a fleet collapse.
    const nFeeds = feedDailies.length;
    const counts = {};
    feedDailies.forEach((daily) => {
      daily.forEach((d) => {
        counts[d.dateStr] = (counts[d.dateStr] || 0) + 1;
      });
    });
    let headlineSource = null;
    for (let i = merged.length - 1; i >= 0; i--) {
      if (counts[merged[i].dateStr] === nFeeds) {
        headlineSource = merged[i];
        break;
      }
    }
    return summarizeDaily(merged, t.nameplate, headlineSource);
  }

  const src = bundle.sources?.[t.source];
  if (!src || !src.data || !src.data.length) return { ok: false };
  const daily =
    t.signal === 'oac-proxy'
      ? buildDailyProxySeries(src.data, t)
      : buildDailySqSeries(src.data, t);
  if (daily.length === 0) return { ok: false };
  return summarizeDaily(daily, t.nameplate);
}

/**
 * Build the combined daily series for one feed entry of a multi-feed terminal.
 *
 * @param {Object} bundle
 * @param {{source: string, series: string}} feed
 * @returns {Array<{dateStr: string, date: Date, value: number}>}
 */
function buildFeedDaily(bundle, feed) {
  const src = bundle.sources?.[feed.source];
  if (!src || !src.data) return [];
  const prefix = `${feed.series.toLowerCase()}_`;
  /** @type {Object<string, Object<string, number>>} */
  const byDate = {};
  src.data.forEach((r) => {
    const sid = String(r.series_id).toLowerCase();
    if (!sid.startsWith(prefix)) return;
    const cycle = sid.slice(prefix.length);
    if (!byDate[r.period]) byDate[r.period] = {};
    byDate[r.period][cycle] = dth_to_mmcf(Number(r.value));
  });
  const out = [];
  Object.keys(byDate).forEach((dateStr) => {
    const cycles = byDate[dateStr];
    let best = null;
    let bestPrio = -1;
    Object.keys(cycles).forEach((cy) => {
      const prio = cyclePriority(cy);
      if (prio > bestPrio) {
        bestPrio = prio;
        best = cy;
      }
    });
    if (best !== null) {
      out.push({ dateStr, date: new Date(dateStr), value: cycles[best] });
    }
  });
  out.sort((a, b) => a.date.getTime() - b.date.getTime());
  return out;
}

/**
 * Shared summary math for a finished daily series.
 *
 * @param {Array<{dateStr: string, date: Date, value: number}>} daily
 * @param {number} nameplate
 * @returns {{ok: boolean, latest: number, utilPct: number, spark: number[],
 *   wow: {delta: number, pct: number}|null, days: number}}
 */
function summarizeDaily(daily, nameplate, headlineSource = null) {
  // headlineSource: optional pre-computed {dateStr, date, value} for the
  // LATEST card (multi-feed terminals anchor it to the latest fully-reported
  // day). Sparkline/WoW still run off the full merged series.
  const latestPoint = headlineSource || daily[daily.length - 1];
  const latest = latestPoint.value;
  const utilPct = (latest / nameplate) * 100;

  const sparkWindow = daily.slice(-8);
  const spark = sparkWindow.map((d) => d.value);

  let wow = null;
  const target = new Date(latestPoint.date);
  target.setDate(target.getDate() - 7);
  const targetStr = target.toISOString().slice(0, 10);
  const weekAgo = daily.find((d) => d.dateStr === targetStr);
  if (weekAgo) {
    wow = {
      delta: latest - weekAgo.value,
      pct: weekAgo.value !== 0 ? ((latest - weekAgo.value) / weekAgo.value) * 100 : 0,
    };
  }
  return { ok: true, latest, utilPct, spark, wow, days: daily.length };
}

/**
 * Render a tiny inline SVG sparkline (~80x24) with a current-value dot.
 *
 * @param {number[]} values
 * @param {string} colorClass
 * @returns {string} SVG markup
 */
export function sparklineSvg(values, colorClass) {
  const W = 80;
  const H = 24;
  const PAD = 2;
  if (!values || values.length < 2) {
    return `<svg class="fleet-sparkline" viewBox="0 0 ${W} ${H}" aria-hidden="true"></svg>`;
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min;
  const yOf = (v) =>
    span === 0 ? H / 2 : H - PAD - ((v - min) / span) * (H - PAD * 2);
  const xOf = (i) => PAD + (i / (values.length - 1)) * (W - PAD * 2);
  const pts = values.map((v, i) => `${xOf(i).toFixed(1)},${yOf(v).toFixed(1)}`).join(' ');
  const lx = xOf(values.length - 1);
  const ly = yOf(values[values.length - 1]);
  return `
    <svg class="fleet-sparkline" viewBox="0 0 ${W} ${H}" aria-hidden="true">
      <polyline fill="none" stroke-width="1.5" class="fleet-sparkline__path ${colorClass}"
        points="${pts}" />
      <circle cx="${lx.toFixed(1)}" cy="${ly.toFixed(1)}" r="2.2"
        class="fleet-sparkline__dot ${colorClass}" />
    </svg>`;
}

/**
 * Render the fleet overview panel.
 *
 * @param {HTMLElement} panelEl
 * @param {Object} bundle
 * @param {{activeTerminalId?: string, onSelect?: (id: string) => void}} [opts]
 */
export function renderLngFleetOverview(
  panelEl,
  bundle,
  opts = {}
) {
  const activeId = opts.activeTerminalId || DEFAULT_TERMINAL_ID;

  /** @type {{t: LngTerminal, s: ReturnType<typeof terminalSummary>}[]} */
  const summaries = [];
  LNG_FLEET_ORDER.forEach((id) => {
    const t = LNG_TERMINALS[id];
    if (!t) return;
    summaries.push({
      t,
      s: t.operational === false ? { ok: false } : terminalSummary(bundle, t),
    });
  });

  // Aggregate over MEASURED terminals only (excludes oac-proxy + offline).
  let totalLatest = 0;
  let totalNameplate = 0;
  let counted = 0;
  summaries.forEach(({ t, s }) => {
    if (t.operational === false) return;
    if (FLEET_PROXY_EXCLUSIONS.includes(t.id)) return;
    if (!s.ok) return;
    totalLatest += s.latest;
    totalNameplate += t.nameplate;
    counted += 1;
  });
  const fleetPct = totalNameplate > 0 ? (totalLatest / totalNameplate) * 100 : 0;

  panelEl.innerHTML = '';

  // ---- Header ----
  const header = document.createElement('div');
  header.className = 'fleet-header';
  header.innerHTML = `
    <h2 class="fleet-title">US LNG Export Fleet · Feedgas</h2>
    <p class="fleet-aggregate num">
      Total scheduled: <strong>${totalLatest.toLocaleString(undefined, { maximumFractionDigits: 0 })} MMcf/d</strong>
      across ${counted} terminals ·
      ${fleetPct.toFixed(1)}% of ${totalNameplate.toLocaleString()} MMcf/d measured-fleet nameplate
    </p>
  `;
  panelEl.appendChild(header);

  // ---- Card grid ----
  const grid = document.createElement('div');
  grid.className = 'fleet-grid';

  summaries.forEach(({ t, s }) => {
    const card = document.createElement('button');
    const isOffline = t.operational === false;
    const isProxy = FLEET_PROXY_EXCLUSIONS.includes(t.id);

    card.className =
      'fleet-card' +
      (t.id === activeId ? ' fleet-card--active' : '') +
      (isOffline ? ' fleet-card--offline' : '') +
      (isProxy ? ' fleet-card--proxy' : '');

    if (isOffline) {
      card.disabled = true;
      card.innerHTML = `
        <div class="fleet-card__name">${t.display}</div>
        <div class="fleet-card__status">${t.statusText || 'Not operational'}</div>
        ${sparklineSvg([], '')}
      `;
      grid.appendChild(card);
      return;
    }

    const level = get_utilization_level(s.ok ? s.utilPct : 0);
    const utilText = s.ok ? `${s.utilPct.toFixed(1)}%` : '—';
    const valueText = s.ok ? `${s.latest.toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '—';

    let wowHtml = '<span class="fleet-wow fleet-wow--none">—</span>';
    if (s.wow) {
      const dir = s.wow.delta > 0 ? 'up' : s.wow.delta < 0 ? 'down' : 'flat';
      const arrow = dir === 'up' ? '▲' : dir === 'down' ? '▼' : '▬';
      wowHtml = `<span class="fleet-wow fleet-wow--${dir}">${arrow} ${
        s.wow.delta >= 0 ? '+' : ''
      }${s.wow.delta.toFixed(0)}</span>`;
    }

    const proxyBadge = isProxy
      ? `<span class="fleet-proxy-badge" title="Inferred proxy: Design Capacity − OAC. Cheniere does not publish Scheduled Quantities.">ⓘ</span>`
      : '';
    const creepBadge = s.ok && s.utilPct >= 100
      ? `<span class="capacity-creep-tooltip" title="running above stated nameplate — capacity creep or measurement basis">ⓘ</span>`
      : '';

    card.title = `${t.display} · latest ${s.days ?? '?'} gas days`;
    card.innerHTML = `
      <div class="fleet-card__top">
        <span class="fleet-card__name">${t.display}${proxyBadge}</span>
        ${wowHtml}
      </div>
      <div class="fleet-card__value-row">
        <span class="fleet-card__value num">${valueText}</span>
        <span class="fleet-card__unit">MMcf/d</span>
      </div>
      <div class="fleet-card__util">
        <span class="fleet-card__util-pct num">${utilText}</span>${creepBadge}
        <div class="utilization-bar-bg fleet-card__bar">
          <div class="utilization-bar-fill ${level.colorClass}" style="width: ${
            s.ok ? Math.min(s.utilPct, 100) : 0
          }%"></div>
        </div>
      </div>
      ${sparklineSvg(s.spark || [], level.colorClass)}
    `;

    card.addEventListener('click', () => {
      if (typeof opts.onSelect === 'function') opts.onSelect(t.id);
    });
    grid.appendChild(card);
  });

  panelEl.appendChild(grid);

  // ---- Footnote ----
  const footnote = document.createElement('p');
  footnote.className = 'fleet-footnote';
  footnote.innerText =
    'Fleet total excludes Corpus Christi (capacity-proxy headline; its measured TGP Sinton meter shows a ~53% BEST-AVAILABLE swing until cycle pinning lands, so it ships as diagnostic only). Sabine Pass is now MEASURED via the KM NGPL interconnect — partial coverage (Transco Z3 feed not publicly posted), so its figure is conservative.';
  panelEl.appendChild(footnote);
}
