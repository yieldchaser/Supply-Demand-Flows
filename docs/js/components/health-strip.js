/**
 * Health Strip — 3 pills showing source freshness.
 *
 * Dot color logic is driven by classifyVintage() in format.js which uses
 * per-source publish-lag + cadence rules to compare against the EXPECTED
 * publish date, not the raw period start.
 */

import { classifyVintage } from '../util/format.js';

const SOURCE_CONFIG = {
  eia_storage: {
    label: 'EIA Storage',
    panelId: 'panel-storage',
    healthKey: 'eia_storage',
  },
  eia_supply: {
    label: 'EIA Supply',
    panelId: 'panel-balance',
    healthKey: 'eia_supply',
  },
  baker_hughes_weekly: {
    label: 'Baker Hughes',
    panelId: 'panel-rigs',
    healthKey: 'baker_hughes_rigs',
  },
  gie_agsi: {
    label: 'GIE AGSI+',
    panelId: 'panel-eu-storage',
    healthKey: 'gie_agsi',
  },
  eia_lng_exports: {
    label: 'EIA LNG',
    panelId: 'panel-lng-total',
    healthKey: 'eia_lng',
  },
};

/**
 * @param {object} bundle — parsed bundle from bundle-loader.js
 */
export function renderHealthStrip(bundle) {
  const strip = document.querySelector('.health-strip');
  if (!strip) return;

  strip.innerHTML = '';

  for (const [sourceKey, config] of Object.entries(SOURCE_CONFIG)) {
    const src = bundle.sources?.[sourceKey];
    if (!src || !src.latest_period) continue;

    const vintage = classifyVintage(sourceKey, src.latest_period);
    const colorClass = {
      fresh: 'vintage-pill__dot--fresh',
      stale: 'vintage-pill__dot--stale',
      critical: 'vintage-pill__dot--critical',
    }[vintage.freshness];

    const pill = document.createElement('a');
    pill.className = 'health-pill';
    pill.href = `#${config.panelId}`;
    pill.title = vintage.tooltip;
    pill.setAttribute('data-source', sourceKey);

    pill.innerHTML = `
      <span class="health-pill__dot ${colorClass}"></span>
      <span class="health-pill__label">${config.label}</span>
      <span class="health-pill__meta">${vintage.labelText}</span>
    `;

    strip.appendChild(pill);
  }
}
