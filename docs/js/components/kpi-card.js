/**
 * KPI Card — sidebar metric card component.
 */

/**
 * Render a single KPI card as an HTML string.
 * @param {{label: string, value: string, delta?: {value: string, kind: 'bullish'|'bearish'|'neutral'}, helpText?: string}} props
 * @returns {string} HTML string
 */
export function kpiCardHtml({ label, value, delta, helpText }) {
  const deltaHtml = delta
    ? `<div class="kpi-card__delta kpi-card__delta--${delta.kind}">${escapeHtml(delta.value)}</div>`
    : '';
  const helpHtml = helpText
    ? `<div class="kpi-card__help">${escapeHtml(helpText)}</div>`
    : '';
  return `
    <div class="kpi-card">
      <div class="kpi-card__label">${escapeHtml(label)}</div>
      <div class="kpi-card__value num">${escapeHtml(value)}</div>
      ${deltaHtml}
      ${helpHtml}
    </div>
  `;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
