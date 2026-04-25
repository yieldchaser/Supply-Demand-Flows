/**
 * Basin Momentum Deep — Section 2 of the Blue Tide dashboard.
 *
 * Why: the headline Basin Momentum panel shows 6 sparkline cards (total rigs).
 * This section answers the deeper analyst questions: which basins are gas- vs
 * oil-directed, how the gas-rig pie is shifting over 24 months, and which
 * basins are outside their 12-month range.
 *
 * What: four render functions, each targeting a pre-existing DOM element.
 *   renderBasinTable    — sortable rank table across all 15 basins
 *   renderBasinScatter  — rigs vs 4-week momentum scatter, bubble = 12mo avg
 *   renderBasinShare    — stacked area of gas-rig % for top-6 gas basins (24mo)
 *   renderBasinExtremes — slim strip: basins outside their 52-week range
 *
 * Data: uses bh_rollup_basin_{slug}, bh_rollup_basin_{slug}_gas/oil from
 * the baker_hughes_weekly source. Never touches granular rows.
 *
 * Failure modes:
 *   - Missing series → getSeries returns [] → metric returns null → filtered out
 *   - Empty data for a panel → graceful empty-state rendered, no throw
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { getSeries } from '../util/series.js';

/** All 15 known basins with display labels. */
const BASINS = [
  { slug: 'marcellus',       label: 'Marcellus' },
  { slug: 'haynesville',     label: 'Haynesville' },
  { slug: 'permian',         label: 'Permian' },
  { slug: 'eagle_ford',      label: 'Eagle Ford' },
  { slug: 'utica',           label: 'Utica' },
  { slug: 'dj_niobrara',     label: 'DJ-Niobrara' },
  { slug: 'bakken',          label: 'Bakken' },
  { slug: 'cana_woodford',   label: 'Cana Woodford' },
  { slug: 'arkoma_woodford', label: 'Arkoma Woodford' },
  { slug: 'ardmore_woodford',label: 'Ardmore Woodford' },
  { slug: 'barnett',         label: 'Barnett' },
  { slug: 'granite_wash',    label: 'Granite Wash' },
  { slug: 'fayetteville',    label: 'Fayetteville' },
  { slug: 'mississippian',   label: 'Mississippian' },
  { slug: 'williston',       label: 'Williston' },
];

/**
 * Build a metrics object for every basin that has data.
 * Returns null for basins with zero rows so callers can filter(Boolean).
 */
function computeBasinMetrics(bundle) {
  const data = bundle.sources.baker_hughes_weekly.data;

  return BASINS.map((b) => {
    const total = getSeries(data, `bh_rollup_basin_${b.slug}`);
    const gas   = getSeries(data, `bh_rollup_basin_${b.slug}_gas`);
    const oil   = getSeries(data, `bh_rollup_basin_${b.slug}_oil`);

    if (total.length === 0) return null;

    const latest = total[total.length - 1];

    /** Value n weeks ago, or null if not enough history. */
    const priorValue = (n) => (total.length > n ? total[total.length - 1 - n].value : null);
    const delta = (n) => {
      const p = priorValue(n);
      return p !== null ? latest.value - p : null;
    };

    // 52-week (≈12-month) range for extreme detection
    const last52 = total.slice(-52);
    const range12m = {
      min: Math.min(...last52.map((r) => r.value)),
      max: Math.max(...last52.map((r) => r.value)),
    };

    let extreme = null;
    // Only flag as extreme if the latest reading is strictly outside the 52w range
    // (the 52w window itself includes the latest point, so we compare to all prior
    //  points, not the window that includes the latest)
    const priorWindow = total.slice(-53, -1);
    if (priorWindow.length >= 4) {
      const priorMin = Math.min(...priorWindow.map((r) => r.value));
      const priorMax = Math.max(...priorWindow.map((r) => r.value));
      if (latest.value > priorMax) extreme = 'high';
      else if (latest.value < priorMin) extreme = 'low';
    }

    const gasAtLatest = gas.find(r => r.period.getTime() === latest.period.getTime());
    const gasLatest = gasAtLatest ? gasAtLatest.value : 0;
    const oilAtLatest = oil.find(r => r.period.getTime() === latest.period.getTime());
    const oilLatest = oilAtLatest ? oilAtLatest.value : 0;
    const gasShare  = latest.value > 0 ? Math.min(100, Math.max(0, (gasLatest / latest.value) * 100)) : 0;
    const avg12m    = last52.reduce((s, r) => s + r.value, 0) / last52.length;

    let flavor;
    if (gasShare >= 70) flavor = 'gas';
    else if (gasShare >= 30) flavor = 'mixed';
    else flavor = 'oil';

    return {
      ...b,
      latest: latest.value,
      gasLatest,
      oilLatest,
      gasShare,
      flavor,
      d1w:  delta(1),
      d4w:  delta(4),
      d12w: delta(12),
      d52w: delta(52),
      avg12m,
      range12m,
      extreme,
      _total: total,
      _gas:   gas,
      _oil:   oil,
    };
  }).filter(Boolean);
}

// ─────────────────────────────────────────────────────────
// 1. Sortable Basin Table
// ─────────────────────────────────────────────────────────

/**
 * Render the sortable basin rank table.
 * Clicking any column header re-sorts; active column highlighted in Blue Flame.
 */
export function renderBasinTable(panelEl, bundle) {
  const { chartEl } = renderPanelChrome(panelEl, {
    title:        'All Basins',
    subtitle:     'Sortable · click any column header',
    sourceKey:    'baker_hughes_weekly',
    latestPeriod: bundle.sources.baker_hughes_weekly.latest_period,
  });

  // Full-width: hide sidebar, expand chart area
  panelEl.querySelector('.panel-sidebar').style.display = 'none';
  chartEl.style.gridColumn = '1 / -1';
  chartEl.style.minHeight  = 'unset';

  const metrics = computeBasinMetrics(bundle);
  if (metrics.length === 0) {
    chartEl.innerHTML = '<div class="extremes-empty">No basin data available.</div>';
    return;
  }

  let sortKey = 'latest';
  let sortDir = -1; // -1 = descending

  function fmtDelta(v) {
    if (v == null) return `<span style="color:var(--text-tertiary)">—</span>`;
    const cls  = v > 0 ? 'delta-pos' : v < 0 ? 'delta-neg' : '';
    const sign = v > 0 ? '+' : '';
    return `<span class="${cls}">${sign}${v}</span>`;
  }

  function render() {
    const sorted = [...metrics].sort((a, b) => {
      const av = a[sortKey] ?? -Infinity;
      const bv = b[sortKey] ?? -Infinity;
      // Label column sorts alphabetically
      if (sortKey === 'label') return String(a.label).localeCompare(b.label) * sortDir;
      return (av - bv) * sortDir;
    });

    const rows = sorted.map((m) => `
      <tr>
        <td>${m.label}<span class="badge badge--${m.flavor}">${m.flavor.toUpperCase()}</span></td>
        <td>${m.latest}</td>
        <td>${m.gasShare.toFixed(0)}%</td>
        <td>${fmtDelta(m.d1w)}</td>
        <td>${fmtDelta(m.d4w)}</td>
        <td>${fmtDelta(m.d12w)}</td>
        <td>${fmtDelta(m.d52w)}</td>
        <td>${Math.round(m.avg12m)}</td>
      </tr>
    `).join('');

    function thClass(key) { return sortKey === key ? 'sorted' : ''; }

    chartEl.innerHTML = `
      <table class="basin-table">
        <thead><tr>
          <th data-key="label"   class="${thClass('label')}">Basin</th>
          <th data-key="latest"  class="${thClass('latest')}">Rigs</th>
          <th data-key="gasShare" class="${thClass('gasShare')}">Gas %</th>
          <th data-key="d1w"    class="${thClass('d1w')}">1w Δ</th>
          <th data-key="d4w"    class="${thClass('d4w')}">4w Δ</th>
          <th data-key="d12w"   class="${thClass('d12w')}">12w Δ</th>
          <th data-key="d52w"   class="${thClass('d52w')}">52w Δ</th>
          <th data-key="avg12m" class="${thClass('avg12m')}">12m avg</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
    `;

    chartEl.querySelectorAll('th[data-key]').forEach((th) => {
      th.addEventListener('click', () => {
        const key = th.dataset.key;
        if (sortKey === key) sortDir = -sortDir;
        else { sortKey = key; sortDir = -1; }
        render();
      });
    });
  }

  render();
}

// ─────────────────────────────────────────────────────────
// 2. Scatter: current rigs vs 4-week momentum
// ─────────────────────────────────────────────────────────

/**
 * Render a bubble scatter chart.
 * X = current rig count, Y = 4-week delta, bubble size = 12-month average.
 * Fill opacity encodes gas share; stroke = blue-flame for gas-majority basins.
 */
export function renderBasinScatter(panelEl, bundle) {
  const { chartEl } = renderPanelChrome(panelEl, {
    title:        'Rigs vs 4-Week Momentum',
    subtitle:     'Bubble size = 12-month avg · fill = gas share',
    sourceKey:    'baker_hughes_weekly',
    latestPeriod: bundle.sources.baker_hughes_weekly.latest_period,
  });
  panelEl.querySelector('.panel-sidebar').style.display = 'none';
  chartEl.style.gridColumn = '1 / -1';
  chartEl.style.minHeight  = 'unset';

  const metrics = computeBasinMetrics(bundle).filter((m) => m.d4w !== null);
  if (metrics.length === 0) {
    chartEl.innerHTML = '<div class="extremes-empty">Not enough history for momentum chart.</div>';
    return;
  }

  const CHART_HEIGHT = 380;
  const margin = { top: 32, right: 24, bottom: 44, left: 52 };

  // Use a small fixed-width for viewBox; scales will stretch via viewBox
  const VB_WIDTH = 780;
  const width  = VB_WIDTH - margin.left - margin.right;
  const height = CHART_HEIGHT - margin.top - margin.bottom;

  const svg = d3.select(chartEl).append('svg')
    .attr('viewBox', `0 0 ${VB_WIDTH} ${CHART_HEIGHT}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('width', '100%')
    .style('display', 'block');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleLinear()
    .domain([0, d3.max(metrics, (m) => m.latest) * 1.15])
    .range([0, width]);

  const yVals = metrics.map((m) => m.d4w);
  const yAbs  = Math.max(Math.abs(d3.min(yVals)), Math.abs(d3.max(yVals)), 1);
  const y = d3.scaleLinear().domain([-yAbs * 1.3, yAbs * 1.3]).range([height, 0]);

  const r = d3.scaleSqrt()
    .domain([0, d3.max(metrics, (m) => m.avg12m)])
    .range([5, 30]);

  // Faint gridlines
  g.selectAll('.grid-x').data(x.ticks(6)).enter().append('line')
    .attr('x1', (d) => x(d)).attr('x2', (d) => x(d))
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.04)');

  g.selectAll('.grid-y').data(y.ticks(5)).enter().append('line')
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('x1', 0).attr('x2', width)
    .attr('stroke', 'rgba(255,255,255,0.04)');

  // Zero-momentum dashed line
  g.append('line')
    .attr('y1', y(0)).attr('y2', y(0)).attr('x1', 0).attr('x2', width)
    .attr('stroke', 'rgba(255,255,255,0.18)')
    .attr('stroke-dasharray', '4 3');

  // Bubbles — paint in two passes: fill first, stroke on top
  g.selectAll('.bubble').data(metrics).enter().append('circle')
    .attr('cx', (m) => x(m.latest))
    .attr('cy', (m) => y(m.d4w))
    .attr('r',  (m) => r(m.avg12m))
    .style('fill',         (m) => `rgba(125,211,252,${0.10 + (m.gasShare / 100) * 0.45})`)
    .style('stroke',       (m) => m.gasShare > 50 ? '#7dd3fc' : 'rgba(156,163,175,0.4)')
    .attr('stroke-width', 1.5);

  // Simple label collision avoidance: if a label would overlap a previously-placed
  // label, push it down by 12px. Sort by latest desc so larger basins get priority.
  const labeled = [...metrics].sort((a, b) => b.latest - a.latest);
  const placed = [];
  labeled.forEach(m => {
    let yOffset = -r(m.avg12m) - 4;
    const cx = x(m.latest);
    let cy = y(m.d4w) + yOffset;
    // Check collision with previously placed labels (within 50px x and 14px y)
    let bumps = 0;
    while (placed.some(p => Math.abs(p.x - cx) < 60 && Math.abs(p.y - cy) < 14) && bumps < 6) {
      cy += 14;
      bumps++;
    }
    placed.push({ x: cx, y: cy, basin: m });
  });

  // Basin labels — nudged above bubble
  g.selectAll('.bubble-lbl').data(placed).enter().append('text')
    .attr('x', (d) => d.x)
    .attr('y', (d) => d.y)
    .attr('text-anchor', 'middle')
    .attr('font-size', 10)
    .attr('font-family', 'Inter, sans-serif')
    .style('fill', 'rgba(255,255,255,0.72)')
    .text((d) => d.basin.label);

  // X-axis tick labels
  g.selectAll('.xt').data(x.ticks(6)).enter().append('text')
    .attr('x', (d) => x(d)).attr('y', height + 18)
    .attr('text-anchor', 'middle')
    .attr('font-size', 10).attr('font-family', 'JetBrains Mono, monospace')
    .style('fill', 'rgba(255,255,255,0.5)').text((d) => d);

  // Y-axis tick labels
  g.selectAll('.yt').data(y.ticks(5)).enter().append('text')
    .attr('x', -8).attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('font-size', 10).attr('font-family', 'JetBrains Mono, monospace')
    .style('fill', 'rgba(255,255,255,0.5)')
    .text((d) => (d > 0 ? '+' : '') + d);

  // Axis labels
  g.append('text')
    .attr('x', width / 2).attr('y', height + 38)
    .attr('text-anchor', 'middle').attr('font-size', 10)
    .style('fill', 'rgba(255,255,255,0.45)')
    .text('Current rig count →');

  g.append('text')
    .attr('transform', 'rotate(-90)')
    .attr('x', -height / 2).attr('y', -40)
    .attr('text-anchor', 'middle').attr('font-size', 10)
    .style('fill', 'rgba(255,255,255,0.45)')
    .text('4-week Δ rigs');
}

// ─────────────────────────────────────────────────────────
// 3. Stacked area: gas-rig share evolution (top 6 gas basins, 24 months)
// ─────────────────────────────────────────────────────────

/**
 * Render stacked area showing each of the top-6 gas basins' share of the
 * total gas-rig pool over the past 24 months.
 */
export function renderBasinShare(panelEl, bundle) {
  const { chartEl } = renderPanelChrome(panelEl, {
    title:        'Gas-Rig Share by Basin',
    subtitle:     '24-month evolution · top 6 gas basins',
    sourceKey:    'baker_hughes_weekly',
    latestPeriod: bundle.sources.baker_hughes_weekly.latest_period,
  });
  panelEl.querySelector('.panel-sidebar').style.display = 'none';
  chartEl.style.gridColumn = '1 / -1';
  chartEl.style.minHeight  = 'unset';

  const data = bundle.sources.baker_hughes_weekly.data;

  // Resolve gas series for every basin and pick top-6 by latest gas rigs
  const withGas = BASINS.map((b) => {
    const gas = getSeries(data, `bh_rollup_basin_${b.slug}_gas`);
    return { ...b, gas, latest: gas.length ? gas[gas.length - 1].value : 0 };
  }).filter((b) => b.gas.length > 0).sort((a, b) => b.latest - a.latest).slice(0, 6);

  if (withGas.length === 0) {
    chartEl.innerHTML = '<div class="extremes-empty">No gas-rig data available.</div>';
    return;
  }

  // Build sorted, deduplicated period list filtered to last 24 months
  const cutoff = new Date();
  cutoff.setMonth(cutoff.getMonth() - 24);

  const allTimestamps = new Set(withGas.flatMap((b) => b.gas.map((r) => r.period.getTime())));
  const periods = [...allTimestamps].sort().map((t) => new Date(t)).filter((p) => p >= cutoff);

  if (periods.length < 2) {
    chartEl.innerHTML = '<div class="extremes-empty">Insufficient history for 24-month view.</div>';
    return;
  }

  // Build per-period share rows
  // Each row: { period, [slug]: sharePct, _total: totalGasRigs }
  const stacked = periods.map((p) => {
    const row = { period: p };
    let total = 0;
    withGas.forEach((b) => {
      const match = b.gas.find((r) => r.period.getTime() === p.getTime());
      const v = match ? match.value : 0;
      row[b.slug] = v;
      total += v;
    });
    row._total = total;
    // Convert counts to % shares
    withGas.forEach((b) => {
      row[b.slug] = total > 0 ? (row[b.slug] / total) * 100 : 0;
    });
    return row;
  });

  const CHART_HEIGHT = 380;
  const LEGEND_W = 110;
  const margin = { top: 32, right: LEGEND_W + 16, bottom: 44, left: 36 };
  const VB_WIDTH = 780;
  const width  = VB_WIDTH - margin.left - margin.right;
  const height = CHART_HEIGHT - margin.top - margin.bottom;

  const svg = d3.select(chartEl).append('svg')
    .attr('viewBox', `0 0 ${VB_WIDTH} ${CHART_HEIGHT}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('width', '100%')
    .style('display', 'block');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleTime().domain(d3.extent(stacked, (d) => d.period)).range([0, width]);
  const y = d3.scaleLinear().domain([0, 100]).range([height, 0]);

  const COLORS = ['#7dd3fc', '#38bdf8', '#22d3a4', '#0ea5e9', '#06b6d4', '#0891b2'];

  const stackGen = d3.stack()
    .keys(withGas.map((b) => b.slug))
    .order(d3.stackOrderDescending);
  const series = stackGen(stacked);

  const areaGen = d3.area()
    .x((d) => x(d.data.period))
    .y0((d) => y(d[0]))
    .y1((d) => y(d[1]))
    .curve(d3.curveMonotoneX);

  // Faint gridlines at 25% intervals
  g.selectAll('.grid-y').data([25, 50, 75]).enter().append('line')
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('x1', 0).attr('x2', width)
    .attr('stroke', 'rgba(255,255,255,0.05)');

  // Stacked area layers
  g.selectAll('.layer').data(series).enter().append('path')
    .attr('d', areaGen)
    .style('fill', (_, i) => COLORS[i % COLORS.length])
    .style('opacity', 0.82);

  // Y-axis labels
  g.selectAll('.yt').data([0, 25, 50, 75, 100]).enter().append('text')
    .attr('x', -6).attr('y', (d) => y(d) + 3)
    .attr('text-anchor', 'end')
    .attr('font-size', 10).attr('font-family', 'JetBrains Mono, monospace')
    .style('fill', 'rgba(255,255,255,0.5)')
    .text((d) => `${d}%`);

  // X-axis: monthly ticks, every 3 months
  const xTicks = x.ticks(d3.timeMonth.every(3));
  g.selectAll('.xt').data(xTicks).enter().append('text')
    .attr('x', (d) => x(d)).attr('y', height + 18)
    .attr('text-anchor', 'middle')
    .attr('font-size', 10).attr('font-family', 'Inter, sans-serif')
    .style('fill', 'rgba(255,255,255,0.5)')
    .text((d) => d3.timeFormat("%b '%y")(d));

  // Legend — positioned to the right of the chart area
  const legend = svg.append('g')
    .attr('transform', `translate(${margin.left + width + 16}, ${margin.top})`);

  withGas.forEach((b, i) => {
    const grp = legend.append('g').attr('transform', `translate(0, ${i * 22})`);
    grp.append('rect')
      .attr('width', 10).attr('height', 10).attr('rx', 2)
      .style('fill', COLORS[i % COLORS.length]);
    grp.append('text')
      .attr('x', 15).attr('y', 9)
      .attr('font-size', 10).attr('font-family', 'Inter, sans-serif')
      .style('fill', 'rgba(255,255,255,0.72)')
      .text(b.label);
  });
}

// ─────────────────────────────────────────────────────────
// 4. Extremes Strip
// ─────────────────────────────────────────────────────────

/**
 * Render the slim extremes strip.
 * Shows inline cards for every basin currently outside its prior-52-week range,
 * or a green "all clear" message when all basins are within range.
 *
 * Note: this panel does NOT use renderPanelChrome — it's a raw slim strip
 * where the panel element itself is the content container (panel--slim class).
 */
export function renderBasinExtremes(panelEl, bundle) {
  const metrics = computeBasinMetrics(bundle);
  const extremes = metrics.filter((m) => m.extreme);

  if (extremes.length === 0) {
    panelEl.innerHTML = `
      <div class="extremes-empty">
        <span style="color:var(--color-bullish-supply)">●</span>
        &nbsp;No basins in extreme territory · all ${metrics.length} basins within their 52-week range
      </div>
    `;
    return;
  }

  const cards = extremes.map((m) => {
    const dirLabel = m.extreme === 'high'
      ? `↑ above 52w max (${m.range12m.max})`
      : `↓ below 52w min (${m.range12m.min})`;
    return `
      <span class="extreme-card extreme-card--${m.extreme}">
        <strong>${m.label}</strong>
        <span style="font-family:var(--font-mono)">${m.latest}</span>
        <span style="color:var(--text-secondary);font-size:var(--fs-xs)">${dirLabel}</span>
      </span>
    `;
  }).join('');

  panelEl.innerHTML = `
    <div style="display:flex;flex-wrap:wrap;align-items:center;gap:var(--space-2)">
      ${cards}
    </div>
  `;
}
