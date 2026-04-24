/**
 * Health Strip — 3 pills showing source freshness.
 *
 * Dot color logic:
 *   eia_storage (weekly):       fresh <8d, stale 8-15d, critical >15d
 *   eia_supply (monthly):       fresh <35d, stale 35-45d, critical >45d
 *   baker_hughes_weekly:        fresh <8d, stale 8-15d, critical >15d
 */

import { formatRelativeTime, classifyVintage, getCadenceDays } from '../util/format.js';

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
};

/**
 * @param {object} bundle — parsed bundle from bundle-loader.js
 */
export function renderHealthStrip(bundle) {
  const strip = document.querySelector('.health-strip');
  if (!strip) return;

  strip.innerHTML = '';

  for (const [sourceKey, config] of Object.entries(SOURCE_CONFIG)) {
    const vintage = bundle.sourceVintage(sourceKey);
    const cadence = getCadenceDays(sourceKey);
    const ageDays = vintage?.ageDays ?? 999;
    const freshness = classifyVintage(ageDays, cadence);
    const relTime = vintage ? formatRelativeTime(vintage.latest) : 'unknown';

    const pill = document.createElement('a');
    pill.className = 'health-pill';
    pill.href = `#${config.panelId}`;
    pill.setAttribute('data-source', sourceKey);

    pill.innerHTML = `
      <span class="health-pill__dot vintage-pill__dot--${freshness}"></span>
      <span class="health-pill__label">${config.label}</span>
      <span class="health-pill__meta">${relTime}</span>
    `;

    strip.appendChild(pill);
  }
}
