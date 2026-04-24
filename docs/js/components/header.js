/**
 * Header component — flame logo, title, bundle timestamp.
 * Timestamp auto-updates every 60s via setInterval.
 */

import { formatRelativeTime } from '../util/format.js';

/**
 * @param {object} bundle — parsed bundle from bundle-loader.js
 */
export function renderHeader(bundle) {
  const header = document.querySelector('.site-header');
  if (!header) return;

  header.innerHTML = `
    <div class="header-lockup">
      <img src="assets/flame.svg" alt="" class="brand-flame" width="36" height="36">
      <div>
        <h1 class="header-title">Blue Tide</h1>
        <p class="header-subtitle">North America Natural Gas Observatory</p>
      </div>
    </div>
    <div class="header-meta">
      <span class="header-meta__refresh-dot"></span>
      <span class="header-meta__text" id="bundle-timestamp"></span>
    </div>
  `;

  const el = document.getElementById('bundle-timestamp');
  const update = () => {
    el.textContent = `Bundle refreshed ${formatRelativeTime(bundle.generatedAt)}`;
  };
  update();
  setInterval(update, 60_000);
}
