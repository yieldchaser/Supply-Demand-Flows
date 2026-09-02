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
  COMPARISON_FEED_EXCLUSIONS,
  DEFAULT_TERMINAL_ID,
} from '../util/lng-terminals.js';
import { dth_to_mmcf, get_utilization_level } from '../util/lng-metrics.js';

import {
  buildSqCycleMaps,
  buildDailySqSeries,
  buildProxyImpliedByDate,
  buildDailyProxySeries,
  buildDailyFromCycles,
  buildFeedDaily,
  summarizeDaily,
  terminalSummary,
  cyclePriority,
} from '../util/lng-fleet-data.js';

export {
  buildSqCycleMaps,
  buildDailySqSeries,
  buildProxyImpliedByDate,
  buildDailyProxySeries,
  buildDailyFromCycles,
  terminalSummary,
  cyclePriority,
};

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

  // Aggregate over FULLY-MEASURED terminals only (excludes partial-coverage
  // terminals + offline). Each excluded terminal is named in the caveats.
  let totalLatest = 0;
  let totalNameplate = 0;
  const countedIds = [];
  summaries.forEach(({ t, s }) => {
    if (t.operational === false) return;
    if (FLEET_PROXY_EXCLUSIONS.includes(t.id)) return;
    if (!s.ok) return;
    totalLatest += s.latest;
    totalNameplate += t.nameplate;
    countedIds.push(t.id);
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
    across ${countedIds.length} measured terminals ·
    ${fleetPct.toFixed(1)}% of ${totalNameplate.toLocaleString()} MMcf/d fleet nameplate
    (Sabine Pass + Golden Pass are measured-partial — CT200111-D covers ~31% of Sabine's nameplate; GP is ramping)
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
    // TASK 3 (2026-08-26): measured-partial terminals MUST wear a prominent
    // badge naming the invisible fraction — same visual weight as Freeport's
    // KMTP caveat. A partial number presented as a terminal total is the
    // Cove-Point-139% / Sabine-overstated class of error.
    const isPartial =
      Array.isArray(t.feeds) &&
      t.feeds.some((f) => f.kind === 'measured-partial') &&
      !t.feeds.some((f) => f.kind === 'measured');
    const partialBadge = isPartial
      ? `<span class="fleet-partial-badge" title="${t.coverageNote || 'Measured-partial: only a share of terminal feedgas is publicly visible.'}">partial ▲</span>`
      : '';
    const creepBadge = s.ok && s.utilPct >= 100
      ? `<span class="capacity-creep-tooltip" title="running above stated nameplate — capacity creep or measurement basis">ⓘ</span>`
      : '';

    card.title = `${t.display} · latest ${s.days ?? '?'} gas days`;
    card.innerHTML = `
      <div class="fleet-card__top">
        <span class="fleet-card__name">${t.display}${proxyBadge}${partialBadge}</span>
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

  // ---- Coverage caveats — PROMINENT, above the fine print ----
  // A measured-but-partial number presented as a terminal total is worse
  // than an honest proxy. Sabine's caveat renders at the same visual level
  // as Freeport's KMTP note on the hero panel: its own callout block.
  const caveats = document.createElement('div');
  caveats.className = 'fleet-caveats';
  const sabineCaveat = document.createElement('p');
  sabineCaveat.className = 'fleet-footnote fleet-footnote--caveat';
  sabineCaveat.innerHTML =
    '<strong>⚠ Sabine Pass is measured-partial.</strong> Cheniere’s Creole Trail EBB exposes a ' +
    'consolidated plant-delivery meter (CT200111-D ≈ 1,408 MMcf/d) plus the NGPL lateral — ' +
    'CTPL’s own view of feedgas it delivers into the plant, ≈ 31% of the 4,500 MMcf/d nameplate. ' +
    'NGI feeder-gas nominations put Sabine near 3.9 Bcf/d; the remaining ~2.5 Bcf/d is non-CTPL ' +
    'feedgas (other interconnects, intrastate) not posted publicly. Sabine stays in the fleet ' +
    'aggregate at its measured CTPL share, clearly labeled partial. (The old "five Gillis feeders ' +
    '= full terminal" framing was corrected 2026-08-26 — same error class as the Cove Point 139%.)';
  caveats.appendChild(sabineCaveat);
  const freeportCaveat = document.createElement('p');
  freeportCaveat.className = 'fleet-footnote';
  freeportCaveat.innerHTML =
    '<strong>Freeport</strong> measures interstate feedgas only (52.9% median coverage of 2,100 MMcf/d nameplate; 1,111.5 MMcf/d median over 100-day overlap 2026-05-25 to 2026-09-01; peak 30d sustained 1,538.0 MMcf/d = 73.2%). ' +
    'The remaining ~988 MMcf/d against nameplate reflects the unposted KMTP intrastate lateral (~400–450 MMcf/d capacity) plus terminal derates and unmeasured supplies.';
  caveats.appendChild(freeportCaveat);
  const cameronCaveat = document.createElement('p');
  cameronCaveat.className = 'fleet-footnote';
  cameronCaveat.innerHTML =
    '<strong>Cameron</strong> measures Cameron Interstate Pipeline (CIP loc 772300) only (72.9% median coverage of 2,000 MMcf/d nameplate; 1,458.6 MMcf/d median). ' +
    'CIP operates near capacity at ~96% of its 1.56 Bcf/d pipeline design capacity. The remaining ~27% (~500 MMcf/d) arrives via Columbia Gulf Transmission (TC Energy Cameron Extension, FERC CP15-514), which is not posted on GasNom.';
  caveats.appendChild(cameronCaveat);
  panelEl.appendChild(caveats);

  // ---- Fine print ----
  const footnote = document.createElement('p');
  footnote.className = 'fleet-footnote';
  footnote.innerText =
    'Fleet total (12,825.9 MMcf/d median across complete gas days) represents an interstate-visible floor, '
    + 'not a full physical census. Sabine Pass (~30%), Freeport (~53%), and Cameron (~73%) are measured-partial '
    + 'at their public interstate delivery meters; unmeasured pipeline laterals (Transco Z3, KMTP, Columbia Gulf) '
    + 'feed the remainder into the plants. Every terminal above is measured at its terminal or lateral interconnects. '
    + 'Corpus Christi was promoted to MEASURED 2026-08-25 — Cheniere publishes real Scheduled Quantities at CC200221. '
    + 'Its TGP Sinton meter (49861) ships as an independent cross-check only. '
    + 'Gillrina Road NUECES (TGP 47799) is Corpus-metro demand, not terminal-bound feedgas — excluded from all feedgas sums.';
  panelEl.appendChild(footnote);
}
