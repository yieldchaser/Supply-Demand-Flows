/**
 * Footer component — credits, data sources, bundle metadata.
 */

import { formatRelativeTime } from '../util/format.js';

/**
 * @param {object} bundle — parsed bundle from bundle-loader.js
 */
export function renderFooter(bundle) {
  const footer = document.querySelector('.site-footer');
  if (!footer) return;

  const hash = bundle.hash || '—';
  const generatedText = formatRelativeTime(bundle.generatedAt);

  footer.innerHTML = `
    <div class="footer-inner">
      <div class="footer-left">
        <span>Blue Tide</span>
        <span class="footer-left__sep">·</span>
        <span>Data: EIA, Baker Hughes</span>
        <span class="footer-left__sep">·</span>
        <a href="https://github.com/yieldchaser/Supply-Demand-Flows"
           target="_blank" rel="noopener noreferrer">GitHub</a>
      </div>
      <div class="footer-right">
        Bundle ${hash} · Generated ${generatedText}
      </div>
    </div>
  `;
}
