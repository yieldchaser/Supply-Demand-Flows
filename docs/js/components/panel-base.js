/**
 * Panel Base — shared panel rendering chrome.
 * Every real panel uses this to render header + vintage pill + body containers.
 */

import { classifyVintage } from '../util/format.js';
import { chartExportButton } from '../util/chart-snapshot.js';
import { redrawOnResize } from '../util/responsive-chart.js';

/**
 * Render the panel's outer chrome: header, vintage pill, container divs for chart + sidebar.
 * Caller fills in the `.panel-chart` and `.panel-sidebar` divs.
 *
 * @param {HTMLElement} panelEl
 * @param {{title: string, subtitle: string, sourceKey: string, latestPeriod: string,
 *   chartExport?: string, draw?: () => void}} opts  chartExport: snapshot id to
 *   offer as a download; draw: re-runs the panel's chart so it can follow its
 *   column's width (see util/responsive-chart)
 * @returns {{chartEl: HTMLElement, sidebarEl: HTMLElement}}
 */
export function renderPanelChrome(panelEl, { title, subtitle, sourceKey, latestPeriod, chartExport = null, draw = null }) {
  const vintage = classifyVintage(sourceKey, latestPeriod);
  const colorClass = {
    fresh: 'vintage-pill__dot--fresh',
    stale: 'vintage-pill__dot--stale',
    critical: 'vintage-pill__dot--critical',
  }[vintage.freshness];

  panelEl.innerHTML = `
    <header class="panel-header">
      <div class="panel-header__left">
        <h2 class="panel-header__title">${escapeHtml(title)}</h2>
        <p class="panel-header__subtitle">${escapeHtml(subtitle)}</p>
      </div>
      <div class="panel-header__right">
        ${chartExport ? chartExportButton(chartExport) : ''}
        <span class="vintage-pill" title="${escapeHtml(vintage.tooltip)}">
          <span class="vintage-pill__dot ${colorClass}"></span>
          <span class="vintage-pill__text">${escapeHtml(vintage.labelText)}</span>
        </span>
      </div>
    </header>
    <div class="panel-body">
      <div class="panel-chart"></div>
      <aside class="panel-sidebar"></aside>
    </div>
  `;
  const chartEl = /** @type {HTMLElement} */ (panelEl.querySelector('.panel-chart'));
  // Charts bake the measured container width into their viewBox, and that width
  // is a guess while the panel renders: boot draws before this frame's layout
  // settles, and a chapter the reader has not opened renders at zero width.
  // Redraw once the real width lands instead of shipping a stretched drawing.
  if (draw && chartEl) redrawOnResize(chartEl, draw);
  return {
    chartEl,
    sidebarEl: /** @type {HTMLElement} */ (panelEl.querySelector('.panel-sidebar')),
  };
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
