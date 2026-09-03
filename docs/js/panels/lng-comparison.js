/**
 * Section 5b · Multi-Terminal Feedgas Comparison Panel.
 *
 * Compares two or more LNG export terminals on a single chart in either
 * absolute MMcf/d or percentage of nameplate capacity.
 *
 * Strictly prevents misleading comparisons by displaying coverage badges
 * and caveats prominently inline whenever partial-coverage terminals are selected.
 *
 * Vanilla JS + D3 — zero TypeScript in executable code.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { LNG_TERMINALS, LNG_FLEET_ORDER } from '../util/lng-terminals.js';
import { buildTerminalComparison } from '../util/terminal-comparison.js';
import { formatCsvWithProvenance } from '../util/export-data.js';

const TERMINAL_COLORS = {
  freeport: '#38bdf8',       // sky
  cove_point: '#34d399',     // emerald
  sabine_pass: '#fbbf24',    // amber
  cameron: '#a78bfa',        // violet
  corpus_christi: '#f472b6', // pink
  calcasieu: '#2dd4bf',      // teal
  plaquemines: '#fb923c',    // orange
  golden_pass: '#94a3b8',    // slate
};

/**
 * Render the Multi-Terminal Comparison Panel.
 *
 * @param {HTMLElement} panelEl
 * @param {Object} bundle
 */
export function renderLngComparisonPanel(panelEl, bundle) {
  panelEl.innerHTML = '';

  // 1. Initial State from URL params or default
  let selectedTerminals = ['freeport', 'cove_point'];
  let mode = 'mmcf'; // 'mmcf' or 'pct'

  if (typeof window !== 'undefined' && window.location) {
    const params = new URLSearchParams(window.location.search);
    const urlComp = params.get('compare');
    if (urlComp) {
      const parsed = urlComp.split(',').filter((id) => LNG_TERMINALS[id] && LNG_TERMINALS[id].operational !== false);
      if (parsed.length > 0) selectedTerminals = parsed;
    }
    const urlMode = params.get('comp_mode');
    if (urlMode === 'pct' || urlMode === 'mmcf') mode = urlMode;
  }

  // 2. Chrome
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Multi-Terminal Comparison · Normalized & Absolute',
    subtitle: 'Cross-terminal feedgas comparison with explicit coverage tiers and caveats',
    sourceKey: 'gulf_south',
    latestPeriod: bundle.sources?.gulf_south?.latest_period || '',
  });

  // 3. Control Bar (Mode toggle + Terminal checkboxes + Export)
  const controls = document.createElement('div');
  controls.className = 'comp-controls';
  controls.style.display = 'flex';
  controls.style.flexWrap = 'wrap';
  controls.style.gap = '12px';
  controls.style.alignItems = 'center';
  controls.style.marginBottom = '16px';

  // Mode Toggle
  const modeGroup = document.createElement('div');
  modeGroup.className = 'comp-mode-group';
  modeGroup.innerHTML = `
    <label style="font-size: 11px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted); margin-right: 6px;">Axis Metric:</label>
    <button class="chip ${mode === 'mmcf' ? 'chip--active' : ''}" data-mode="mmcf">Absolute Flow (MMcf/d)</button>
    <button class="chip ${mode === 'pct' ? 'chip--active' : ''}" data-mode="pct">% of Nameplate</button>
  `;
  modeGroup.querySelectorAll('button').forEach((btn) => {
    btn.onclick = () => {
      mode = btn.dataset.mode;
      updateUrlParams();
      renderComparisonView();
    };
  });
  controls.appendChild(modeGroup);

  // Terminal selector chips
  const termGroup = document.createElement('div');
  termGroup.className = 'comp-term-group';
  termGroup.style.display = 'flex';
  termGroup.style.flexWrap = 'wrap';
  termGroup.style.gap = '6px';

  LNG_FLEET_ORDER.forEach((id) => {
    const term = LNG_TERMINALS[id];
    if (!term || term.operational === false) return;

    const btn = document.createElement('button');
    const isChecked = selectedTerminals.includes(id);
    const badge = term.expectedCoveragePct < 90
      ? `<span style="font-size: 9px; opacity: 0.8; margin-left: 4px;">(${term.expectedCoveragePct}% cov)</span>`
      : '';
    btn.className = `chip ${isChecked ? 'chip--active' : ''}`;
    btn.innerHTML = `${isChecked ? '☑' : '☐'} ${term.display} ${badge}`;
    btn.onclick = () => {
      if (selectedTerminals.includes(id)) {
        if (selectedTerminals.length > 1) {
          selectedTerminals = selectedTerminals.filter((k) => k !== id);
        }
      } else {
        selectedTerminals.push(id);
      }
      updateUrlParams();
      renderComparisonView();
    };
    termGroup.appendChild(btn);
  });
  controls.appendChild(termGroup);

  // Export Button
  const exportBtn = document.createElement('button');
  exportBtn.className = 'chip chip--export';
  exportBtn.style.marginLeft = 'auto';
  exportBtn.innerHTML = '📥 Export Comparison CSV';
  exportBtn.onclick = () => {
    const comp = buildTerminalComparison(bundle, selectedTerminals);
    const rows = comp.dates.map((d) => {
      const row = { date: d };
      comp.terminals.forEach((t) => {
        const val = t.series[d];
        row[`${t.id}_mmcf_d`] = val ? val.mmcf : '';
        row[`${t.id}_util_pct`] = val ? val.pctNameplate : '';
      });
      return row;
    });
    const caveatText = comp.caveats.join(' | ');
    const csvStr = formatCsvWithProvenance('Multi-Terminal Comparison', rows, {
      coverageNote: caveatText || 'All displayed terminals',
    });
    const blob = new Blob([csvStr], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.setAttribute('download', 'lng_comparison.csv');
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };
  controls.appendChild(exportBtn);

  chartEl.parentNode.insertBefore(controls, chartEl);

  function updateUrlParams() {
    if (typeof window !== 'undefined' && window.history && window.history.replaceState) {
      const p = new URLSearchParams(window.location.search);
      p.set('compare', selectedTerminals.join(','));
      p.set('comp_mode', mode);
      window.history.replaceState(null, '', `?${p.toString()}`);
    }
  }

  function renderComparisonView() {
    // Re-render chips active state
    modeGroup.querySelectorAll('button').forEach((b) => {
      b.classList.toggle('chip--active', b.dataset.mode === mode);
    });
    termGroup.querySelectorAll('button').forEach((b, idx) => {
      const id = LNG_FLEET_ORDER.filter((k) => LNG_TERMINALS[k] && LNG_TERMINALS[k].operational !== false)[idx];
      const isChecked = selectedTerminals.includes(id);
      const term = LNG_TERMINALS[id];
      const badge = term.expectedCoveragePct < 90
        ? `<span style="font-size: 9px; opacity: 0.8; margin-left: 4px;">(${term.expectedCoveragePct}% cov)</span>`
        : '';
      b.className = `chip ${isChecked ? 'chip--active' : ''}`;
      b.innerHTML = `${isChecked ? '☑' : '☐'} ${term.display} ${badge}`;
    });

    const comp = buildTerminalComparison(bundle, selectedTerminals);
    drawComparisonChart(chartEl, comp, mode);
    renderComparisonSidebar(sidebarEl, comp, mode);
  }

  renderComparisonView();
}

/**
 * Draw comparison line chart.
 */
function drawComparisonChart(container, comp, mode) {
  container.innerHTML = '';
  if (!comp || comp.dates.length === 0 || comp.terminals.length === 0) {
    container.innerHTML = '<p class="downtime-events__empty">Select at least one terminal to compare.</p>';
    return;
  }

  const margin = { top: 20, right: 30, bottom: 40, left: 56 };
  const totalH = 340;
  const rect = container.getBoundingClientRect();
  const width = Math.max((rect.width || 600), 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const dates = comp.dates.map((d) => new Date(`${d}T00:00:00Z`));
  const x = d3.scaleTime().domain(d3.extent(dates)).range([0, width]);

  // Compute Y extent based on mode
  let maxY = 1;
  comp.terminals.forEach((t) => {
    comp.dates.forEach((d) => {
      const entry = t.series[d];
      if (entry) {
        const val = mode === 'pct' ? entry.pctNameplate : entry.mmcf;
        if (val > maxY) maxY = val;
      }
    });
  });
  if (mode === 'pct') maxY = Math.max(maxY, 100);

  const y = d3.scaleLinear().domain([0, maxY * 1.1]).range([height, 0]);

  // Gridlines
  [0.25, 0.5, 0.75, 1].forEach((frac) => {
    g.append('line').attr('x1', 0).attr('x2', width)
      .attr('y1', y(maxY * frac)).attr('y2', y(maxY * frac))
      .attr('stroke', 'rgba(255,255,255,0.05)');
  });

  // 100% Nameplate reference line for pct mode
  if (mode === 'pct') {
    g.append('line').attr('x1', 0).attr('x2', width)
      .attr('y1', y(100)).attr('y2', y(100))
      .attr('stroke', '#4b5563').attr('stroke-width', 1.5).attr('stroke-dasharray', '4,4');
    g.append('text').attr('x', width - 8).attr('y', y(100) - 6)
      .attr('text-anchor', 'end').attr('font-size', '10px').attr('font-family', 'var(--font-mono)')
      .style('fill', 'rgba(255,255,255,0.45)').text('100% Nameplate');
  }

  // Draw lines per terminal
  comp.terminals.forEach((t) => {
    const color = TERMINAL_COLORS[t.id] || '#38bdf8';
    const points = comp.dates.map((dateStr, idx) => {
      const pt = t.series[dateStr];
      const hasVal = pt !== undefined && pt !== null;
      return {
        date: dates[idx],
        val: hasVal ? (mode === 'pct' ? pt.pctNameplate : pt.mmcf) : null,
        defined: hasVal,
      };
    });

    const line = d3.line()
      .defined((d) => d.defined)
      .x((d) => x(d.date))
      .y((d) => y(d.val))
      .curve(d3.curveMonotoneX);

    g.append('path').datum(points).attr('d', line)
      .style('fill', 'none')
      .style('stroke', color)
      .style('stroke-width', 2)
      .style('stroke-dasharray', t.isPartial ? '4,2' : 'none');
  });

  // Y axis labels
  [0, 0.25, 0.5, 0.75, 1].forEach((frac) => {
    g.append('text').attr('x', -8).attr('y', y(maxY * frac) + 4).attr('text-anchor', 'end')
      .attr('font-size', 10).attr('font-family', 'var(--font-mono)')
      .style('fill', 'var(--chart-label)')
      .text(mode === 'pct' ? `${(maxY * frac).toFixed(0)}%` : (maxY * frac).toFixed(0));
  });

  // X axis labels
  const step = Math.max(1, Math.floor(dates.length / 6));
  for (let i = 0; i < dates.length; i += step) {
    g.append('text').attr('x', x(dates[i])).attr('y', height + 20).attr('text-anchor', 'middle')
      .attr('font-size', 10).attr('font-family', 'var(--font-sans)')
      .style('fill', 'var(--chart-label)')
      .text(dates[i].toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }));
  }
}

/**
 * Render sidebar with terminal legend, coverage notes, and comparison caveats.
 */
function renderComparisonSidebar(container, comp, mode) {
  container.innerHTML = '';

  // Legend (Option b: visible per-terminal covered span)
  const legendHtml = comp.terminals.map((t) => {
    const color = TERMINAL_COLORS[t.id] || '#38bdf8';
    const covBadge = t.isPartial
      ? `<span class="badge badge--warning" style="margin-left: 6px; font-size: 9px;">${t.coveragePct}% partial</span>`
      : `<span class="badge badge--normal" style="margin-left: 6px; font-size: 9px;">${t.coveragePct}%</span>`;
    const spanText = t.firstDate && t.lastDate
      ? `<div style="font-size: 10px; color: var(--text-muted); margin-left: 22px; margin-top: 2px;">Known: ${t.firstDate} → ${t.lastDate} (${t.dayCount.toLocaleString('en-US')} days)</div>`
      : '';
    return `
      <div style="margin-bottom: 10px; font-size: 12px;">
        <div style="display: flex; align-items: center;">
          <span style="display: inline-block; width: 14px; height: 3px; background: ${color}; ${t.isPartial ? 'border-top: 1px dashed ' + color : ''}; margin-right: 8px;"></span>
          <strong>${t.label}</strong>
          ${covBadge}
        </div>
        ${spanText}
      </div>
    `;
  }).join('');

  // Caveat block
  let caveatHtml = '';
  if (comp.caveats.length > 0) {
    caveatHtml = `
      <div style="margin-top: 16px; padding: 10px; background: rgba(251, 191, 36, 0.08); border-left: 3px solid #fbbf24; border-radius: 4px; font-size: 11px; line-height: 1.4; color: var(--text-muted);">
        <strong style="color: #fbbf24;">⚠ Coverage Caveats:</strong>
        <ul style="margin: 4px 0 0 16px; padding: 0;">
          ${comp.caveats.map((c) => `<li>${c}</li>`).join('')}
        </ul>
      </div>
    `;
  }

  container.innerHTML = `
    <div style="padding: 12px 0;">
      <h4 style="margin: 0 0 12px 0; font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted);">Selected Terminals</h4>
      ${legendHtml}
      ${caveatHtml}
    </div>
  `;
}
