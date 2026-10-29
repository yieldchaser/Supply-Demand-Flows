/**
 * Panel Placeholder — panel shell for a source whose data has not arrived yet.
 * Renders the header, vintage marker and a one-line statement of what the
 * panel shows once the source posts.
 */

import { classifyVintage } from '../util/format.js';


const PANEL_DESCRIPTIONS = {
  eia_storage: 'Weekly storage levels across 5 regions with 5-year historical band.',
  eia_supply: 'Monthly production, consumption, and implied storage flows.',
  baker_hughes_weekly: 'Weekly US rig count with week-over-week momentum.',
};

/**
 * @param {string} panelId — DOM id of the panel element
 * @param {object} opts
 * @param {string} opts.title
 * @param {string} opts.subtitle
 * @param {string} opts.source — bundle source key
 * @param {object} opts.bundle
 */
export function renderPanelPlaceholder(panelId, { title, subtitle, source, bundle }) {
  const panel = document.getElementById(panelId);
  if (!panel) return;

  const src = bundle.sources?.[source];
  if (!src || !src.latest_period) return;

  const vintage = classifyVintage(source, src.latest_period);
  const colorClass = {
    fresh: 'vintage-pill__dot--fresh',
    stale: 'vintage-pill__dot--stale',
    critical: 'vintage-pill__dot--critical',
  }[vintage.freshness];

  const desc = PANEL_DESCRIPTIONS[source] || '';

  panel.innerHTML = `
    <div class="panel-header">
      <div class="panel-header__left">
        <h2 class="panel-header__title">${title}</h2>
        <p class="panel-header__subtitle">${subtitle}</p>
      </div>
      <span class="vintage-pill" title="${vintage.tooltip}">
        <span class="vintage-pill__dot ${colorClass}"></span>
        <span class="vintage-pill__text">${vintage.labelText}</span>
      </span>
    </div>
    <div class="panel-placeholder">
      <p class="panel-placeholder__label">${desc}</p>
      <p class="panel-placeholder__desc">Awaiting first post from ${subtitle.split(' · ')[0]}.</p>
    </div>
  `;
}
