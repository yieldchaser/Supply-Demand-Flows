/**
 * Shared chart and formatting kit for the Europe chapter panels.
 *
 * The storage and supply panels draw the same kinds of things (line and band
 * charts with a crosshair tooltip, signed figures, tables) and must look and
 * read the same way; one implementation keeps them from drifting apart.
 * Styles live in css/components/eu-storage.css under `.eu-panel`.
 */

import * as d3 from 'd3';

export function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
}
export const isNum = (x) => Number.isFinite(x);
export function fx(x, d = 1) {
  return isNum(x) ? x.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d }) : '—';
}
export function sx(x, d = 1) {
  if (!isNum(x)) return '—';
  const s = fx(Math.abs(x), d);
  return x > 0 ? `+${s}` : x < 0 ? `−${s}` : s;
}
export const tone = (x) => (!isNum(x) ? 'muted' : x > 0 ? 'pos' : x < 0 ? 'neg' : '');

export function frame(host, { height, margin }) {
  const width = Math.max(host.getBoundingClientRect().width || 640, 280);
  const w = width - margin.left - margin.right;
  const h = height - margin.top - margin.bottom;
  const svg = d3.select(host).append('svg')
    .attr('viewBox', `0 0 ${width} ${height}`)
    .attr('width', width)
    .attr('height', height)
    .attr('class', 'eu-svg');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
  return { svg, g, w, h, width, margin };
}

export function yGrid(g, y, w, fmt, ticks = 5, zeroMajor = false) {
  for (const t of y.ticks(ticks)) {
    g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(t)).attr('y2', y(t))
      .attr('class', zeroMajor && t === 0 ? 'eu-grid eu-grid--zero' : 'eu-grid');
    g.append('text').attr('x', -8).attr('y', y(t)).attr('dy', '0.32em').attr('text-anchor', 'end')
      .attr('class', 'eu-axis').text(fmt(t));
  }
}

/** Absolute-positioned tooltip inside `host`; positions are host pixels. */
export function tooltip(host) {
  const el = document.createElement('div');
  el.className = 'eu-tt';
  el.setAttribute('role', 'status');
  el.style.opacity = '0';
  host.appendChild(el);
  return {
    show(html, px, py) {
      el.innerHTML = html;
      el.style.opacity = '1';
      const hw = host.clientWidth;
      const tw = el.offsetWidth;
      const left = px + 16 + tw > hw ? px - tw - 16 : px + 16;
      el.style.left = `${Math.max(0, left)}px`;
      el.style.top = `${Math.max(0, py)}px`;
    },
    hide() { el.style.opacity = '0'; },
  };
}

export function hitArea(g, w, h, onMove, onLeave) {
  g.append('rect').attr('class', 'eu-hit').attr('width', w).attr('height', h)
    .attr('fill', 'transparent')
    .on('pointermove', (ev) => onMove(d3.pointer(ev, g.node())))
    .on('pointerleave', onLeave);
}

export function blockHead(title, desc = '', right = '') {
  return `
    <header class="eu-block__head">
      <div>
        <h3 class="eu-block__title">${title}</h3>
        ${desc ? `<p class="eu-block__desc">${desc}</p>` : ''}
      </div>
      ${right}
    </header>`;
}

export function legend(items) {
  return `<div class="eu-legend">${items.map((it) => `<span class="eu-legend__item"><span class="eu-key eu-key--${it.kind}"${it.color ? ` style="--c:${it.color}"` : ''}></span>${it.label}</span>`).join('')}</div>`;
}

