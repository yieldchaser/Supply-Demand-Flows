/**
 * Panel Base — shared panel rendering chrome.
 * Every real panel uses this to render header + vintage pill + body containers.
 */

import { classifyVintage } from '../util/format.js';

/**
 * Render the panel's outer chrome: header, vintage pill, container divs for chart + sidebar.
 * Caller fills in the `.panel-chart` and `.panel-sidebar` divs.
 *
 * @param {HTMLElement} panelEl
 * @param {{title: string, subtitle: string, sourceKey: string, latestPeriod: string}} opts
 * @returns {{chartEl: HTMLElement, sidebarEl: HTMLElement}}
 */
export function renderPanelChrome(panelEl, { title, subtitle, sourceKey, latestPeriod }) {
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
      <span class="vintage-pill" title="${escapeHtml(vintage.tooltip)}">
        <span class="vintage-pill__dot ${colorClass}"></span>
        <span class="vintage-pill__text">${escapeHtml(vintage.labelText)}</span>
      </span>
    </header>
    <div class="panel-body">
      <div class="panel-chart"></div>
      <aside class="panel-sidebar"></aside>
    </div>
  `;
  return {
    chartEl: panelEl.querySelector('.panel-chart'),
    sidebarEl: panelEl.querySelector('.panel-sidebar'),
  };
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
