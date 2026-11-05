/**
 * EU Storage Panel — GIE AGSI+ European Natural Gas Storage Observatory.
 *
 * Institutional architecture:
 *   1. Hero Aggregate Chart — Pan-European storage inventory (% full and TWh)
 *      overlaid on the 5-year historical envelope (min-max range and median norm).
 *   2. Interactive Country Grid — 10 selectable member state cards displaying:
 *      Country name, code, % full, working gas in TWh, deviation vs 5-year median
 *      in percentage points (pp) and TWh, current flow regime (Injection / Withdrawal / Neutral),
 *      and smooth non-zigzagging historical sparkline.
 *   3. Country Detail View:
 *      - Benchmark Strip: Prominently displaying "Where it should be right now"
 *        (5-year historical median for the exact day of the year) with exact deviation
 *        in pp and TWh, derived working-gas capacity, and rank among historical years.
 *      - Multi-Year Trajectory Chart: Overlaying 2021–2026 individual annual trajectories
 *        on the shaded 5-year historical band and median line with interactive hover scrub.
 *      - Flow Dynamics Chart: Daily injection and withdrawal with net flow bars and
 *        trailing 7-day moving average trend line.
 *      - 14-Day History Table: Date, % Full, TWh, Injection (GWh/d), Withdrawal (GWh/d),
 *        Net Flow (GWh/d).
 *
 * Data Source: bundle.sources.gie_agsi
 * Reporting: Daily GIE AGSI+ validated series.
 */

import * as d3 from 'd3';
import { renderPanelChrome, appendSection } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries, referenceDate } from '../util/series.js';
import { formatIsoDate } from '../util/format.js';
import {
  EU_COUNTRIES,
  buildCountryViews,
  buildAggregate,
  positionalNormal,
  rankAmongYears,
  monthlyEnvelope,
  rowsByYear,
  monthDayFraction,
  flowDynamics,
  countryStorageMetrics,
} from '../util/eu-storage-analysis.js';

const MONTH_LABEL = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const YEAR_COLORS = {
  2026: '#7dd3fc', // Blue Flame (Current)
  2025: '#38bdf8', // Sky Blue
  2024: '#c084fc', // Purple
  2023: '#34d399', // Mint
  2022: '#fbbf24', // Amber
  2021: '#94a3b8', // Slate
};

/* ─────────────────────────────────────────────── */
/*  Entry Point                                    */
/* ─────────────────────────────────────────────── */

export function renderEuStoragePanel(panelEl, bundle) {
  const source = bundle?.sources?.gie_agsi;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">GIE AGSI+ data unavailable.</div>';
    return;
  }

  const perCountry = {};
  for (const c of EU_COUNTRIES) {
    perCountry[c.cc] = {
      pct: getSeries(source.data, `gie_storage_${c.cc}_full_pct`),
      gas: getSeries(source.data, `gie_storage_${c.cc}_gas_in_storage`),
      injection: getSeries(source.data, `gie_storage_${c.cc}_injection`),
      withdrawal: getSeries(source.data, `gie_storage_${c.cc}_withdrawal`),
    };
  }

  const views = buildCountryViews(perCountry);
  if (views.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">No country series found in GIE data.</div>';
    return;
  }
  const aggregate = buildAggregate(views);
  const referenceYear = referenceYearOf(views) ?? 2026;

  // Selected country defaults to Germany ('de') or first available country
  const state = {
    selected: views.find((v) => v.cc === 'de')?.cc ?? views[0]?.cc ?? null,
    horizon: '5y',
    range: '1Y',
  };

  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'European Gas Storage',
    subtitle: `${views.length} countries · % full and TWh · 2021–${referenceYear} history`,
    sourceKey: 'gie_agsi',
    latestPeriod: source.latest_period,
    draw: () => drawRef.current?.(),
  });

  chartEl.innerHTML = `
    <div class="eu-controls-bar">
      <div class="eu-controls-group">
        <span class="eu-axis-text" style="margin-right: 6px; font-weight: 500;">Baseline Horizon:</span>
        <button type="button" class="eu-btn-pill active" data-horizon="5y">5Y Tactical (2021–2025)</button>
        <button type="button" class="eu-btn-pill" data-horizon="post2022">Post-2022 Structural (Mandate Era)</button>
      </div>
      <div class="eu-controls-group">
        <span class="eu-axis-text" style="margin-right: 6px; font-weight: 500;">Range:</span>
        <button type="button" class="eu-btn-pill" data-range="30D">30D</button>
        <button type="button" class="eu-btn-pill" data-range="90D">90D</button>
        <button type="button" class="eu-btn-pill active" data-range="1Y">1Y</button>
        <button type="button" class="eu-btn-pill" data-range="5Y">5Y</button>
        <button type="button" class="eu-btn-pill" data-range="MAX">MAX</button>
      </div>
    </div>
    <div class="eu-hero-container"></div>
    <div class="eu-grid-container"></div>
    <div class="eu-detail-container" aria-live="polite"></div>
    <div class="eu-method"></div>
  `;

  const controlsBar = chartEl.querySelector('.eu-controls-bar');
  controlsBar.addEventListener('click', (e) => {
    const horizonBtn = e.target.closest('[data-horizon]');
    if (horizonBtn) {
      const h = horizonBtn.dataset.horizon;
      if (state.horizon === h) return;
      state.horizon = h;
      controlsBar.querySelectorAll('[data-horizon]').forEach((b) => b.classList.toggle('active', b.dataset.horizon === h));
      drawRef.current?.();
      return;
    }
    const rangeBtn = e.target.closest('[data-range]');
    if (rangeBtn) {
      const r = rangeBtn.dataset.range;
      if (state.range === r) return;
      state.range = r;
      controlsBar.querySelectorAll('[data-range]').forEach((b) => b.classList.toggle('active', b.dataset.range === r));
      drawRef.current?.();
      return;
    }
  });

  const heroEl = chartEl.querySelector('.eu-hero-container');
  const gridEl = chartEl.querySelector('.eu-grid-container');
  const detailEl = chartEl.querySelector('.eu-detail-container');
  const methodEl = appendSection(panelEl, 'europe-method', chartEl.querySelector('.eu-method'));

  const drawDetail = (minYear) => {
    const selectedCc = state.selected ?? views[0]?.cc ?? null;
    const selectedView = views.find((v) => v.cc === selectedCc) ?? views[0];
    drawCountryDetail(detailEl, selectedView, referenceYear, aggregate, { minYear, range: state.range, horizon: state.horizon });
  };

  drawRef.current = () => {
    const minYear = state.horizon === 'post2022' ? 2022 : null;
    const aggregateNormal = (row) => positionalNormal(aggregate, row.period, { minYear });

    drawEuHeroChart(heroEl, aggregate, referenceYear, aggregateNormal, { minYear, range: state.range });
    drawCountryGrid(gridEl, views, referenceYear, {
      selected: state.selected,
      minYear,
      onSelect: (cc) => {
        state.selected = cc;
        gridEl.querySelectorAll('.eu-card').forEach((el) => {
          const isTarget = el.dataset.cc === cc;
          el.classList.toggle('is-selected', isTarget);
          el.setAttribute('aria-pressed', String(isTarget));
        });
        drawDetail(minYear);
      },
    });
    drawDetail(minYear);
  };

  drawRef.current();

  methodEl.innerHTML = renderMethodNote(views, aggregate);
  renderEuKpis(sidebarEl, aggregate, views, referenceYear, (row) => positionalNormal(aggregate, row.period));
}

/* ================================================================== */
/*  Methodology & Provenance Note                                     */
/* ================================================================== */

function renderMethodNote(views, aggregate) {
  const capacity = views.reduce((s, v) => s + (v.capacityTwh ?? 0), 0);
  const withCap = views.filter((v) => v.capacityTwh != null).length;
  const last = aggregate[aggregate.length - 1];
  const spread = views
    .map((v) => v.capacitySpreadPct)
    .filter((x) => Number.isFinite(x));
  const maxSpread = spread.length ? Math.max(...spread) : null;
  const maxYear = Math.max(0, ...aggregate.map((r) => r.period.getFullYear()));
  const nPriorYears = maxYear - 2021;

  return `
    <p class="data-provenance">
      Methodology: Working-gas capacity is derived per member state as
      <span class="num">gas in storage ÷ % full × 100</span> (median of daily reported ratios).
      ${withCap}/${views.length} member states resolve capacity, aggregating to
      <span class="num">${capacity.toFixed(1)} TWh</span> across the 10-country observatory
      (day-to-day ratio spread: ${maxSpread == null ? 'n/a' : `<span class="num">${maxSpread.toFixed(1)}%</span>`}).
      Pan-European aggregate equals Σ(stock TWh) ÷ Σ(capacity TWh), exactly matching the capacity-weighted volume.
      ${last ? `${last.nCountries}/${views.length} countries reporting on latest date.` : ''}
      Historical GIE AGSI+ records in this bundle span 2021 to ${maxYear}; all 5-year seasonal normal medians
      are constructed using the same calendar day across up to <span class="num">${nPriorYears}</span> prior years.
      Daily injection and withdrawal reflect GIE reported flows in GWh/d.
    </p>
  `;
}

/* ================================================================== */
/*  Hero Aggregate Chart                                              */
/* ================================================================== */

function drawEuHeroChart(container, aggregate, referenceYear, normalFor, opts = {}) {
  container.innerHTML = '';
  if (aggregate.length < 2) {
    container.innerHTML = '<div class="panel-error">Insufficient data for aggregate storage trajectory.</div>';
    return;
  }

  const { minYear = null, range = '1Y' } = opts;
  const isPost2022 = minYear === 2022;
  const horizonLabel = isPost2022 ? 'Post-2022 Structural' : '5-Year';

  const margin = { top: 20, right: 56, bottom: 32, left: 48 };
  const totalH = 340;
  const width = Math.max(container.getBoundingClientRect().width || 640, 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const isShort = range === '30D' || range === '90D';
  const isMax = range === 'MAX';
  const is5Y = range === '5Y';

  let legendHtml = '';
  if (is5Y) {
    const rawByYear = rowsByYear(aggregate, (r) => r.pct);
    const byYear = minYear ? rawByYear.filter((yr) => yr.year >= minYear) : rawByYear;
    legendHtml = `
      <div class="eu-hero-legend">
        ${byYear.map((yr) => `
          <span class="eu-legend-item">
            <span class="eu-legend-swatch" style="background: ${YEAR_COLORS[yr.year] || '#94a3b8'}"></span>
            ${yr.year}
          </span>
        `).join('')}
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--median"></span>${horizonLabel} Median</span>
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--band"></span>${horizonLabel} Range</span>
      </div>
    `;
  } else if (isShort) {
    legendHtml = `
      <div class="eu-hero-legend">
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--line"></span>${referenceYear} Trajectory (${range})</span>
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--median"></span>Day-Matched ${horizonLabel} Median</span>
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--band"></span>Day-Matched Min–Max Range</span>
      </div>
    `;
  } else if (isMax) {
    legendHtml = `
      <div class="eu-hero-legend">
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--line"></span>Pan-European Continuous History (2021–${referenceYear})</span>
      </div>
    `;
  } else {
    legendHtml = `
      <div class="eu-hero-legend">
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--line"></span>${referenceYear} Trajectory</span>
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--median"></span>${horizonLabel} Median Norm</span>
        <span class="eu-legend-item"><span class="eu-legend-swatch eu-legend-swatch--band"></span>${horizonLabel} Min–Max Range</span>
      </div>
    `;
  }

  const headerHtml = `
    <div class="eu-hero-header">
      <h3 class="eu-hero-title">Pan-European Storage Trajectory (% of Working Gas Capacity)</h3>
      ${legendHtml}
    </div>
  `;
  container.insertAdjacentHTML('beforeend', headerHtml);

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Handle Zoomed Short Ranges (30D / 90D)
  if (isShort) {
    const days = range === '30D' ? 30 : 90;
    const latestRow = aggregate[aggregate.length - 1];
    const cutoff = new Date(latestRow.period.getTime() - days * 86400000);
    const sliceRows = aggregate.filter((r) => r.period >= cutoff);
    if (sliceRows.length >= 2) {
      const slicePoints = sliceRows.map((r) => {
        const norm = normalFor ? normalFor(r) : { median: null, min: null, max: null, n: 0 };
        return {
          period: r.period,
          pct: r.pct,
          twh: r.twh,
          capacityTwh: r.capacityTwh,
          normMedian: norm?.median ?? null,
          normMin: norm?.min ?? null,
          normMax: norm?.max ?? null,
          normN: norm?.n ?? 0,
        };
      });

      const allY = slicePoints.flatMap((p) => [p.pct, p.normMin, p.normMax]).filter(Number.isFinite);
      const yExtent = allY.length ? d3.extent(allY) : [0, 100];
      const yPad = Math.max(3, (yExtent[1] - yExtent[0]) * 0.1);
      const yMin = Math.max(0, Math.floor((yExtent[0] - yPad) / 5) * 5);
      const yMax = Math.min(100, Math.ceil((yExtent[1] + yPad) / 5) * 5);
      const y = d3.scaleLinear().domain([yMin, yMax]).range([height, 0]);
      const x = d3.scaleTime().domain([cutoff, latestRow.period]).range([0, width]);

      // Y Gridlines
      for (const t of y.ticks(5)) {
        g.append('line').attr('x1', 0).attr('x2', width).attr('y1', y(t)).attr('y2', y(t)).attr('class', 'eu-grid');
        g.append('text').attr('x', -8).attr('y', y(t) + 4).attr('text-anchor', 'end')
          .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)')
          .attr('font-feature-settings', "'tnum'").text(`${t}%`);
      }

      // Min-Max envelope
      const validBand = slicePoints.filter((p) => Number.isFinite(p.normMin) && Number.isFinite(p.normMax));
      if (validBand.length > 1) {
        g.append('path').datum(validBand)
          .attr('d', d3.area().x((d) => x(d.period)).y0((d) => y(d.normMin)).y1((d) => y(d.normMax)).curve(d3.curveMonotoneX))
          .attr('class', 'eu-band eu-band--range');

        g.append('path').datum(validBand)
          .attr('d', d3.line().x((d) => x(d.period)).y((d) => y(d.normMedian)).curve(d3.curveMonotoneX))
          .attr('fill', 'none').attr('class', 'eu-band-median');
      }

      // Current Trajectory Line
      g.append('path').datum(slicePoints)
        .attr('d', d3.line().x((d) => x(d.period)).y((d) => y(d.pct)).curve(d3.curveMonotoneX))
        .attr('fill', 'none')
        .style('stroke', 'var(--blue-flame)')
        .attr('stroke-width', 2.5)
        .attr('stroke-linecap', 'round')
        .attr('stroke-linejoin', 'round');

      // Endpoint dot & label
      const last = slicePoints[slicePoints.length - 1];
      g.append('circle').attr('cx', x(last.period)).attr('cy', y(last.pct)).attr('r', 4.5).style('fill', 'var(--blue-flame)');
      g.append('text')
        .attr('x', Math.min(x(last.period) + 8, width + 4))
        .attr('y', y(last.pct) + 4)
        .attr('class', 'eu-axis-text')
        .attr('font-family', 'var(--font-mono)')
        .attr('font-feature-settings', "'tnum'")
        .style('fill', 'var(--blue-flame)')
        .style('font-weight', '600')
        .text(`${last.pct.toFixed(1)}%`);

      // X Axis Ticks (Dates)
      const tickStep = range === '30D' ? d3.timeWeek.every(1) : d3.timeWeek.every(2);
      const ticks = tickStep.range(cutoff, latestRow.period);
      for (const dt of ticks) {
        const tx = x(dt);
        g.append('line').attr('x1', tx).attr('x2', tx).attr('y1', height).attr('y2', height + 4).attr('stroke', 'rgba(255, 255, 255, 0.15)');
        g.append('text').attr('x', tx).attr('y', height + 18).attr('text-anchor', 'middle')
          .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').text(d3.timeFormat('%b %d')(dt));
      }

      setupEuHeroTimeHover(svg, g, x, y, width, height, margin, slicePoints, normalFor, { horizonLabel, isPost2022 });
      return;
    }
  }

  // Handle Continuous Multi-Year MAX Range
  if (isMax) {
    const earliest = aggregate[0].period;
    const latest = aggregate[aggregate.length - 1].period;
    const x = d3.scaleTime().domain([earliest, latest]).range([0, width]);
    const y = d3.scaleLinear().domain([0, 100]).range([height, 0]);

    // Gridlines
    for (const t of [0, 25, 50, 75, 100]) {
      g.append('line').attr('x1', 0).attr('x2', width).attr('y1', y(t)).attr('y2', y(t)).attr('class', t === 50 ? 'eu-grid eu-grid--major' : 'eu-grid');
      g.append('text').attr('x', -8).attr('y', y(t) + 4).attr('text-anchor', 'end')
        .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)')
        .attr('font-feature-settings', "'tnum'").text(`${t}%`);
    }

    // Continuous Line
    g.append('path').datum(aggregate)
      .attr('d', d3.line().x((d) => x(d.period)).y((d) => y(d.pct)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 2);

    // Latest dot & label
    const last = aggregate[aggregate.length - 1];
    g.append('circle').attr('cx', x(last.period)).attr('cy', y(last.pct)).attr('r', 4).style('fill', 'var(--blue-flame)');
    g.append('text')
      .attr('x', Math.min(x(last.period) + 8, width + 4))
      .attr('y', y(last.pct) + 4)
      .attr('class', 'eu-axis-text')
      .attr('font-family', 'var(--font-mono)')
      .attr('font-feature-settings', "'tnum'")
      .style('fill', 'var(--blue-flame)')
      .style('font-weight', '600')
      .text(`${last.pct.toFixed(1)}%`);

    // Yearly Ticks
    const yearTicks = d3.timeYear.range(d3.timeYear.floor(earliest), d3.timeYear.ceil(latest));
    for (const yt of yearTicks) {
      const tx = x(yt);
      if (tx >= 0 && tx <= width) {
        g.append('line').attr('x1', tx).attr('x2', tx).attr('y1', height).attr('y2', height + 4).attr('stroke', 'rgba(255, 255, 255, 0.2)');
        g.append('text').attr('x', tx).attr('y', height + 18).attr('text-anchor', 'middle')
          .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').text(yt.getFullYear());
      }
    }

    setupEuHeroTimeHover(svg, g, x, y, width, height, margin, aggregate, normalFor, { horizonLabel, isPost2022 });
    return;
  }

  // Handle 1Y & 5Y Seasonal Scale (0..1)
  const x = d3.scaleLinear().domain([0, 1]).range([0, width]);
  const band = monthlyEnvelope(aggregate, referenceYear, { minYear });
  const yMax = Math.max(100, ...band.map((b) => b.max ?? 0), ...aggregate.map((a) => a.pct));
  const y = d3.scaleLinear().domain([0, Math.ceil(yMax / 10) * 10]).range([height, 0]);

  // Horizontal Grid & Labels
  const yTicks = y.ticks(5);
  for (const t of yTicks) {
    g.append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', y(t)).attr('y2', y(t))
      .attr('class', t === 50 ? 'eu-grid eu-grid--major' : 'eu-grid');

    g.append('text')
      .attr('x', -8).attr('y', y(t) + 4)
      .attr('text-anchor', 'end')
      .attr('class', 'eu-axis-text')
      .attr('font-family', 'var(--font-mono)')
      .attr('font-feature-settings', "'tnum'")
      .text(`${t}%`);
  }

  // Historical 5-Year Min-Max Envelope & Median
  const bandPoints = band
    .map((b) => {
      const mid = b.month - 0.5;
      return b.median == null ? null : { x: mid / 12, ...b };
    })
    .filter(Boolean);

  if (bandPoints.length > 1) {
    const areaGen = d3.area()
      .x((d) => x(d.x))
      .y0((d) => y(d.min))
      .y1((d) => y(d.max))
      .curve(d3.curveMonotoneX);

    g.append('path')
      .datum(bandPoints)
      .attr('d', areaGen)
      .attr('class', 'eu-band eu-band--range');

    const medGen = d3.line()
      .x((d) => x(d.x))
      .y((d) => y(d.median))
      .curve(d3.curveMonotoneX);

    g.append('path')
      .datum(bandPoints)
      .attr('d', medGen)
      .attr('fill', 'none')
      .attr('class', 'eu-band-median');
  }

  // Multi-Year Overlay for 5Y Mode
  if (is5Y) {
    const rawByYear = rowsByYear(aggregate, (r) => r.pct);
    const byYear = minYear ? rawByYear.filter((yr) => yr.year >= minYear) : rawByYear;
    const yearLineGen = d3.line().x((d) => x(d.x)).y((d) => y(d.value)).curve(d3.curveMonotoneX);

    for (const yr of byYear) {
      if (yr.year === referenceYear || yr.points.length < 2) continue;
      const col = YEAR_COLORS[yr.year] || '#94a3b8';
      g.append('path').datum(yr.points)
        .attr('d', yearLineGen)
        .attr('fill', 'none')
        .style('stroke', col)
        .style('stroke-width', 1.35)
        .style('opacity', 0.8);
    }
  }

  // Reference Year Trajectory
  const refRows = aggregate.filter((r) => r.period.getFullYear() === referenceYear);
  const refPoints = refRows.map((r) => ({
    x: monthDayFraction(r.period),
    period: r.period,
    pct: r.pct,
    twh: r.twh,
    capacityTwh: r.capacityTwh,
  }));

  if (refPoints.length > 1) {
    const lineGen = d3.line()
      .x((d) => x(d.x))
      .y((d) => y(d.pct))
      .curve(d3.curveMonotoneX);

    g.append('path')
      .datum(refPoints)
      .attr('d', lineGen)
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 2.5)
      .attr('stroke-linecap', 'round')
      .attr('stroke-linejoin', 'round');

    const last = refPoints[refPoints.length - 1];
    g.append('circle')
      .attr('cx', x(last.x))
      .attr('cy', y(last.pct))
      .attr('r', 4)
      .style('fill', 'var(--blue-flame)');

    g.append('text')
      .attr('x', Math.min(x(last.x) + 8, width + 4))
      .attr('y', y(last.pct) + 4)
      .attr('class', 'eu-axis-text')
      .attr('font-family', 'var(--font-mono)')
      .attr('font-feature-settings', "'tnum'")
      .style('fill', 'var(--blue-flame)')
      .style('font-weight', '600')
      .text(`${last.pct.toFixed(1)}%`);
  }

  // Month Starts on X Axis
  for (let m = 0; m < 12; m++) {
    const mx = x(m / 12);
    g.append('line')
      .attr('x1', mx).attr('x2', mx)
      .attr('y1', height).attr('y2', height + 4)
      .attr('stroke', 'rgba(255, 255, 255, 0.15)');

    g.append('text')
      .attr('x', mx + (width / 12) / 2)
      .attr('y', height + 18)
      .attr('text-anchor', 'middle')
      .attr('class', 'eu-axis-text')
      .attr('font-family', 'var(--font-sans)')
      .text(MONTH_LABEL[m]);
  }

  setupEuHeroHover(svg, g, x, y, width, height, margin, refPoints, normalFor, { horizonLabel, isPost2022 });
}

function setupEuHeroHover(svg, g, x, y, width, height, margin, refPoints, normalFor, { horizonLabel = '5-Year', isPost2022 = false } = {}) {
  const host = svg.node().parentNode;
  const tooltip = d3.select(host).append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  const crosshair = g.append('line').attr('y1', 0).attr('y2', height).attr('class', 'eu-crosshair');
  const dot = g.append('circle').attr('r', 4.5).style('fill', 'var(--blue-flame)').style('opacity', 0);

  const hit = g.append('rect')
    .attr('class', 'eu-chart-hitbox')
    .attr('width', width)
    .attr('height', height)
    .attr('fill', 'none')
    .style('cursor', 'pointer')
    .style('pointer-events', 'all');

  hit.on('mousemove', (event) => {
    const [mx] = d3.pointer(event);
    const target = x.invert(mx);

    let best = null;
    for (const p of refPoints) {
      const lag = Math.abs(p.x - target);
      if (!best || lag < best.lag) best = { lag, p };
    }
    const maxLag = 8 / 365;
    if (!best || best.lag > maxLag) {
      tooltip.style('opacity', 0);
      crosshair.style('opacity', 0);
      dot.style('opacity', 0);
      return;
    }

    const row = best.p;
    const normal = normalFor ? normalFor(row) : null;
    const dev = normal?.median == null ? null : row.pct - normal.median;
    const devTwh = dev != null && row.capacityTwh ? (dev * row.capacityTwh) / 100 : null;

    crosshair.attr('x1', x(row.x)).attr('x2', x(row.x)).style('opacity', 1);
    dot.attr('cx', x(row.x)).attr('cy', y(row.pct)).style('opacity', 1);

    const hostRect = host.getBoundingClientRect();
    const svgRect = svg.node().getBoundingClientRect();
    const vb = svg.attr('viewBox').split(' ').map(Number);
    const k = svgRect.width / vb[2];
    const left = margin.left * k + x(row.x) * k + 14;
    const top = margin.top * k + y(row.pct) * k - 20;

    tooltip
      .style('opacity', 1)
      .style('left', `${Math.max(0, Math.min(left, hostRect.width - 240))}px`)
      .style('top', `${Math.max(0, top)}px`)
      .html(`
        <div class="tt-title">${escapeHtml(formatIsoDate(row.period))} · European Aggregate</div>
        <div class="tt-row"><span class="tt-label">Inventory Level</span><span class="num">${row.pct.toFixed(2)}%</span></div>
        <div class="tt-row"><span class="tt-label">Working Gas Stock</span><span class="num">${row.twh.toFixed(1)} TWh</span></div>
        <div class="tt-row"><span class="tt-label">Derived Capacity</span><span class="num">${row.capacityTwh.toFixed(1)} TWh</span></div>
        ${normal?.median == null ? '' : `
        <div class="tt-row"><span class="tt-label">${horizonLabel} Median Norm</span><span class="num">${normal.median.toFixed(2)}%</span></div>
        <div class="tt-row tt-row--accent">
          <span class="tt-label">vs ${isPost2022 ? 'Post-2022' : '5Y'} Median (n=${normal.n})</span>
          <span class="num ${dev >= 0 ? 'delta-pos' : 'delta-neg'}">
            ${dev >= 0 ? '+' : ''}${dev.toFixed(2)} pp${devTwh != null ? ` (${devTwh >= 0 ? '+' : ''}${devTwh.toFixed(1)} TWh)` : ''}
          </span>
        </div>`}
      `);
  }).on('mouseleave', () => {
    tooltip.style('opacity', 0);
    crosshair.style('opacity', 0);
    dot.style('opacity', 0);
  });
}

function setupEuHeroTimeHover(svg, g, x, y, width, height, margin, points, normalFor, { horizonLabel = '5-Year', isPost2022 = false } = {}) {
  const host = svg.node().parentNode;
  const tooltip = d3.select(host).append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  const crosshair = g.append('line').attr('y1', 0).attr('y2', height).attr('class', 'eu-crosshair');
  const dot = g.append('circle').attr('r', 4.5).style('fill', 'var(--blue-flame)').style('opacity', 0);

  const hit = g.append('rect')
    .attr('class', 'eu-chart-hitbox')
    .attr('width', width)
    .attr('height', height)
    .attr('fill', 'none')
    .style('cursor', 'pointer')
    .style('pointer-events', 'all');

  hit.on('mousemove', (event) => {
    const [mx] = d3.pointer(event);
    const targetDt = x.invert(mx);
    if (!targetDt) return;
    const targetMs = targetDt.getTime();

    let best = null;
    for (const p of points) {
      const lag = Math.abs(p.period.getTime() - targetMs);
      if (!best || lag < best.lag) best = { lag, p };
    }
    if (!best) {
      tooltip.style('opacity', 0);
      crosshair.style('opacity', 0);
      dot.style('opacity', 0);
      return;
    }

    const row = best.p;
    const normal = normalFor ? normalFor(row) : null;
    const medianVal = normal?.median ?? row.normMedian ?? null;
    const dev = medianVal != null ? row.pct - medianVal : null;
    const devTwh = dev != null && row.capacityTwh ? (dev * row.capacityTwh) / 100 : null;

    crosshair.attr('x1', x(row.period)).attr('x2', x(row.period)).style('opacity', 1);
    dot.attr('cx', x(row.period)).attr('cy', y(row.pct)).style('opacity', 1);

    const hostRect = host.getBoundingClientRect();
    const svgRect = svg.node().getBoundingClientRect();
    const vb = svg.attr('viewBox').split(' ').map(Number);
    const k = svgRect.width / vb[2];
    const left = margin.left * k + x(row.period) * k + 14;
    const top = margin.top * k + y(row.pct) * k - 20;

    tooltip
      .style('opacity', 1)
      .style('left', `${Math.max(0, Math.min(left, hostRect.width - 240))}px`)
      .style('top', `${Math.max(0, top)}px`)
      .html(`
        <div class="tt-title">${escapeHtml(formatIsoDate(row.period))} · European Aggregate</div>
        <div class="tt-row"><span class="tt-label">Inventory Level</span><span class="num">${row.pct.toFixed(2)}%</span></div>
        <div class="tt-row"><span class="tt-label">Working Gas Stock</span><span class="num">${row.twh != null ? row.twh.toFixed(1) : '—'} TWh</span></div>
        <div class="tt-row"><span class="tt-label">Derived Capacity</span><span class="num">${row.capacityTwh != null ? row.capacityTwh.toFixed(1) : '—'} TWh</span></div>
        ${medianVal == null ? '' : `
        <div class="tt-row"><span class="tt-label">${horizonLabel} Median</span><span class="num">${medianVal.toFixed(2)}%</span></div>
        <div class="tt-row tt-row--accent">
          <span class="tt-label">vs Baseline</span>
          <span class="num ${dev >= 0 ? 'delta-pos' : 'delta-neg'}">
            ${dev >= 0 ? '+' : ''}${dev.toFixed(2)} pp${devTwh != null ? ` (${devTwh >= 0 ? '+' : ''}${devTwh.toFixed(1)} TWh)` : ''}
          </span>
        </div>`}
      `);
  }).on('mouseleave', () => {
    tooltip.style('opacity', 0);
    crosshair.style('opacity', 0);
    dot.style('opacity', 0);
  });
}

/* ================================================================== */
/*  Country Grid (10 Member States)                                   */
/* ================================================================== */

function drawCountryGrid(container, views, referenceYear, { selected, onSelect, minYear = null }) {
  container.innerHTML = `
    <div class="eu-grid-header">
      <h3 class="eu-grid-header__title">EU Member State Inventories (10 Countries)</h3>
      <span class="eu-grid-header__hint">Click any country card to inspect multi-year trajectory & flow dynamics</span>
    </div>
    <div class="eu-country-grid" role="list"></div>
  `;
  const grid = container.querySelector('.eu-country-grid');

  for (const v of views) {
    if (!v.latest) continue;
    const pct = v.latest.pct;
    const levelClass = pct < 40 ? 'eu-card--low' : pct < 75 ? 'eu-card--mid' : 'eu-card--high';

    const metrics = countryStorageMetrics(v, referenceYear, { minYear });
    const deviationPp = metrics?.deviationPp;
    const deviationTwh = metrics?.deviationTwh;
    const normal = metrics?.normal ?? { n: 0, median: null };
    const flow7d = metrics?.flow7d;
    const regime = flow7d?.regime ?? 'neutral';
    const regimeLabel = regime === 'injection' ? 'Injection' : regime === 'withdrawal' ? 'Withdrawal' : 'Neutral';

    const isSel = selected === v.cc;
    const devSign = deviationPp != null && deviationPp >= 0 ? '+' : '';
    const devClass = deviationPp == null ? 'muted' : deviationPp >= 0 ? 'delta-pos' : 'delta-neg';
    const devTwhStr = deviationTwh != null ? ` (${deviationTwh >= 0 ? '+' : ''}${deviationTwh.toFixed(1)} TWh)` : '';
    const baselineLabel = minYear === 2022 ? 'post-2022' : '5Y';

    grid.insertAdjacentHTML('beforeend', `
      <button type="button" class="eu-card${isSel ? ' is-selected' : ''}"
              data-cc="${escapeHtml(v.cc)}" aria-pressed="${isSel}" role="listitem">
        <div class="eu-card__head">
          <div class="eu-card__identity">
            <span class="eu-card__label">${escapeHtml(v.label)}</span>
            <span class="eu-card__cc">${escapeHtml(v.cc.toUpperCase())}</span>
          </div>
          <span class="eu-regime-badge eu-regime-badge--${regime}" title="7-day net regime: ${regimeLabel}">
            <span class="eu-regime-dot"></span>
            ${regimeLabel}
          </span>
        </div>

        <div class="eu-card__metric">
          <span class="eu-card__value num">${pct.toFixed(1)}%</span>
          <span class="eu-card__sub num">${v.latest.twh == null ? '' : `${v.latest.twh.toFixed(1)} TWh`}</span>
        </div>

        <div class="eu-card__normal">
          <div class="eu-card__deviation-row">
            ${deviationPp == null
              ? '<span class="muted">No baseline</span>'
              : `<span class="num ${devClass}">${devSign}${deviationPp.toFixed(1)} pp${devTwhStr}</span>`}
          </div>
          <span class="eu-card__deviation-label">vs ${baselineLabel} median (n=${normal.n})</span>
        </div>

        <svg class="eu-card__spark" viewBox="0 0 200 46" preserveAspectRatio="none" aria-hidden="true"></svg>
      </button>
    `);

    drawCountryHistorySpark(grid.lastElementChild.querySelector('.eu-card__spark'), v, referenceYear, { minYear });
  }

  grid.addEventListener('click', (event) => {
    const card = event.target.closest('.eu-card');
    if (!card) return;
    onSelect(card.dataset.cc);
  });
}

/**
 * Clean, non-zigzagging sparkline:
 * - 5-year historical min-max shaded envelope
 * - 5-year median dashed line
 * - Prior years faint smooth curves
 * - Reference year prominent line with endpoint dot
 */
function drawCountryHistorySpark(svgEl, view, referenceYear, opts = {}) {
  const { minYear = null } = opts;
  const rows = view.rows.filter((r) => Number.isFinite(r.pct));
  if (rows.length < 2) return;

  const w = 200, h = 46, pad = 2;
  const x = d3.scaleLinear().domain([0, 1]).range([pad, w - pad]);
  const yMax = Math.max(100, ...rows.map((r) => r.pct));
  const y = d3.scaleLinear().domain([0, yMax]).range([h - 3, 3]);

  const band = monthlyEnvelope(rows.map((r) => ({ period: r.period, value: r.pct })), referenceYear, { minYear });
  const pts = band
    .filter((b) => b.min != null)
    .map((b) => ({ x: (b.month - 0.5) / 12, min: b.min, max: b.max, median: b.median }));

  const svg = d3.select(svgEl);
  svg.selectAll('*').remove();

  if (pts.length > 1) {
    svg.append('path').datum(pts)
      .attr('d', d3.area().x((d) => x(d.x)).y0((d) => y(d.min)).y1((d) => y(d.max)).curve(d3.curveMonotoneX))
      .attr('class', 'eu-band eu-band--range');

    svg.append('path').datum(pts)
      .attr('d', d3.line().x((d) => x(d.x)).y((d) => y(d.median)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .attr('class', 'eu-band-median');
  }

  const allYears = rowsByYear(rows, (r) => r.pct);

  // Prior years (2021–2025)
  for (const yr of allYears) {
    if ((minYear && yr.year < minYear) || yr.year === referenceYear || yr.points.length < 2) continue;
    svg.append('path').datum(yr.points)
      .attr('d', d3.line().x((d) => x(d.x)).y((d) => y(d.value)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .attr('class', 'eu-spark-prior');
  }

  // Reference year (2026)
  const ref = allYears.find((yr) => yr.year === referenceYear);
  if (ref && ref.points.length > 1) {
    svg.append('path').datum(ref.points)
      .attr('d', d3.line().x((d) => x(d.x)).y((d) => y(d.value)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 1.75);

    const last = ref.points[ref.points.length - 1];
    svg.append('circle')
      .attr('cx', x(last.x))
      .attr('cy', y(last.value))
      .attr('r', 2.5)
      .style('fill', 'var(--blue-flame)');
  }
}

/* ================================================================== */
/*  Country Detail View                                               */
/* ================================================================== */

function drawCountryDetail(container, view, referenceYear, aggregate, opts = {}) {
  container.innerHTML = '';
  if (!view) return;

  const { minYear = null, range = '1Y', horizon = '5y' } = opts;
  const isPost2022 = minYear === 2022;
  const horizonLabel = isPost2022 ? 'Post-2022' : '5-Year';
  const baselineSub = isPost2022 ? 'Post-2022 mandate era median for exact day of year' : '5-year median for exact day of year';

  const latest = view.latest;
  const pctRows = view.rows
    .map((r) => ({ period: r.period, value: r.pct }))
    .filter((r) => Number.isFinite(r.value));

  const normal = latest ? positionalNormal(pctRows, latest.period, { minYear }) : { median: null, n: 0, years: [] };
  const rank = latest && normal.n ? rankAmongYears(latest.pct, normal.values.map((v) => v.value)) : { rank: null, n: 0 };
  const byYear = rowsByYear(view.rows, (r) => r.pct);
  const priorYears = byYear.filter((y) => y.year < referenceYear).map((y) => y.year);

  const deviationPp = latest && normal.median != null ? latest.pct - normal.median : null;
  const deviationTwh = deviationPp != null && view.capacityTwh != null ? (deviationPp * view.capacityTwh) / 100 : null;

  const flow7d = flowDynamics(view.rows, { windowDays: 7 }) ?? {
    avgNetGwh: 0,
    rateTwhPerWeek: 0,
    regime: 'neutral',
  };

  const netFlow = (r) => {
    if (r.injectionGwh == null && r.withdrawalGwh == null) return null;
    return (r.injectionGwh ?? 0) - (r.withdrawalGwh ?? 0);
  };
  const lastNet = latest ? netFlow(latest) : null;

  const normalMedianPct = normal.median ?? 0;
  const normalMedianTwh = view.capacityTwh ? (normalMedianPct * view.capacityTwh) / 100 : 0;
  const devSign = deviationPp != null && deviationPp >= 0 ? '+' : '';
  const devClass = deviationPp == null ? 'muted' : deviationPp >= 0 ? 'eu-delta-pill--pos' : 'eu-delta-pill--neg';
  const flowRateClass = flow7d.avgNetGwh >= 0 ? 'delta-pos' : 'delta-neg';

  // 14-day history rows (newest first)
  const flowRows = view.rows.slice(-14).reverse();

  // Ordinal rank description
  const rankDescription = rank.rank === 1
    ? 'Lowest on record'
    : rank.rank === rank.n + 1
      ? 'Highest on record'
      : `${rank.rank}${ordinalSuffix(rank.rank)} lowest`;

  container.innerHTML = `
    <div class="eu-detail">
      <div class="eu-detail__head">
        <div class="eu-detail__title-group">
          <div class="eu-detail__meta-badges">
            <span class="eu-badge-cc">${escapeHtml(view.cc.toUpperCase())}</span>
            <span class="eu-regime-badge eu-regime-badge--${flow7d.regime}">
              <span class="eu-regime-dot"></span>
              ${flow7d.regime.toUpperCase()} FLOW REGIME
            </span>
          </div>
          <h3 class="eu-detail__title">${escapeHtml(view.label)} Natural Gas Storage</h3>
          <p class="eu-detail__sub">
            ${latest ? `Validated as of ${escapeHtml(formatIsoDate(latest.period))} · Working Gas Capacity: ${view.capacityTwh == null ? 'Pending' : `${view.capacityTwh.toFixed(1)} TWh`}` : 'No current observation'}
            · ${byYear.length} historical seasons on record
          </p>
        </div>
      </div>

      <div class="eu-benchmark-grid">
        <!-- 1. Current Inventory -->
        <div class="eu-benchmark-card">
          <span class="eu-benchmark-card__label">Current Storage Inventory</span>
          <div class="eu-benchmark-card__value-row">
            <span class="eu-benchmark-card__value num">${latest ? latest.pct.toFixed(1) : '—'}%</span>
            <span class="eu-benchmark-card__unit num">${latest && latest.twh != null ? `${latest.twh.toFixed(1)} TWh` : ''}</span>
          </div>
          <div class="eu-benchmark-card__meta">
            <span class="eu-benchmark-card__sub">
              ${latest && view.capacityTwh ? `${((latest.twh / view.capacityTwh) * 100).toFixed(1)}% of derived working gas` : 'Derived capacity basis'}
            </span>
          </div>
        </div>

        <!-- 2. Where It Should Be Right Now -->
        <div class="eu-benchmark-card eu-benchmark-card--highlight">
          <span class="eu-benchmark-card__label">Where It Should Be Right Now</span>
          <div class="eu-benchmark-card__value-row">
            <span class="eu-benchmark-card__value num">${normal.median != null ? normal.median.toFixed(1) : '—'}%</span>
            <span class="eu-benchmark-card__unit num">${normalMedianTwh > 0 ? `${normalMedianTwh.toFixed(1)} TWh` : ''}</span>
          </div>
          <div class="eu-benchmark-card__meta">
            ${deviationPp == null ? '<span class="muted">No baseline</span>' : `
              <span class="eu-delta-pill ${devClass}">
                ${devSign}${deviationPp.toFixed(1)} pp${deviationTwh != null ? ` (${devSign}${deviationTwh.toFixed(1)} TWh)` : ''}
              </span>
            `}
            <span class="eu-benchmark-card__sub">${baselineSub} (n=${normal.n})</span>
          </div>
        </div>

        <!-- 3. Working-Gas Capacity & Rank -->
        <div class="eu-benchmark-card">
          <span class="eu-benchmark-card__label">Derived Capacity & Rank</span>
          <div class="eu-benchmark-card__value-row">
            <span class="eu-benchmark-card__value num">${view.capacityTwh != null ? view.capacityTwh.toFixed(1) : '—'}</span>
            <span class="eu-benchmark-card__unit">TWh</span>
          </div>
          <div class="eu-benchmark-card__meta">
            <span class="eu-rank-pill">Rank ${rank.rank == null ? 'n/a' : `${rank.rank} of ${rank.n + 1}`}</span>
            <span class="eu-benchmark-card__sub">${rankDescription} (2021–${referenceYear})</span>
          </div>
        </div>

        <!-- 4. 7-Day Net Flow Pace -->
        <div class="eu-benchmark-card">
          <span class="eu-benchmark-card__label">7-Day Trailing Net Flow</span>
          <div class="eu-benchmark-card__value-row">
            <span class="eu-benchmark-card__value num ${flowRateClass}">
              ${flow7d.avgNetGwh >= 0 ? '+' : ''}${Math.round(flow7d.avgNetGwh).toLocaleString()}
            </span>
            <span class="eu-benchmark-card__unit">GWh/d</span>
          </div>
          <div class="eu-benchmark-card__meta">
            <span class="eu-delta-pill ${flowRateClass}">
              ${flow7d.rateTwhPerWeek >= 0 ? '+' : ''}${flow7d.rateTwhPerWeek?.toFixed(2)} TWh/wk
            </span>
            <span class="eu-benchmark-card__sub">DoD reported net: ${lastNet == null ? '—' : `${lastNet >= 0 ? '+' : ''}${Math.round(lastNet).toLocaleString()} GWh/d`}</span>
          </div>
        </div>
      </div>

      <div class="eu-detail__charts">
        <div class="eu-detail__chart-card" data-chart="history">
          <div class="eu-chart-head">
            <div>
              <h4 class="eu-chart-title">Multi-Year Trajectory vs ${horizonLabel} Seasonal Band (% Full)</h4>
              <p class="eu-chart-desc">Overlaying 2021–${referenceYear} annual curves on the ${horizonLabel.toLowerCase()} min–max envelope and median norm</p>
            </div>
          </div>
        </div>

        <div class="eu-detail__chart-card" data-chart="flow">
          <div class="eu-chart-head">
            <div>
              <h4 class="eu-chart-title">Flow Dynamics: Daily Net Flow & 7-Day Moving Average</h4>
              <p class="eu-chart-desc">Reported net flows (GWh/d) with 7-day trailing moving average trend line (${range} window)</p>
            </div>
          </div>
        </div>
      </div>

      <div class="eu-table-wrap">
        <table class="eu-detail__table">
          <caption>14-Day Storage & Flow History</caption>
          <thead>
            <tr>
              <th scope="col">Date</th>
              <th scope="col">% Full</th>
              <th scope="col">TWh</th>
              <th scope="col">Injection (GWh/d)</th>
              <th scope="col">Withdrawal (GWh/d)</th>
              <th scope="col">Net Flow (GWh/d)</th>
            </tr>
          </thead>
          <tbody>
            ${flowRows.map((r) => {
              const net = netFlow(r);
              const netClass = net == null ? 'muted' : net >= 0 ? 'eu-table-flow--pos' : 'eu-table-flow--neg';
              return `
                <tr>
                  <td>${escapeHtml(formatIsoDate(r.period))}</td>
                  <td class="num">${r.pct == null ? '—' : r.pct.toFixed(2)}%</td>
                  <td class="num">${r.twh == null ? '—' : r.twh.toFixed(1)}</td>
                  <td class="num">${r.injectionGwh == null ? '—' : Math.round(r.injectionGwh).toLocaleString()}</td>
                  <td class="num">${r.withdrawalGwh == null ? '—' : Math.round(r.withdrawalGwh).toLocaleString()}</td>
                  <td class="num ${netClass}">${net == null ? '—' : `${net >= 0 ? '+' : ''}${Math.round(net).toLocaleString()}`}</td>
                </tr>
              `;
            }).join('')}
          </tbody>
        </table>
      </div>

      <p class="data-provenance">
        Working-gas capacity for ${escapeHtml(view.label)} is derived as ${view.capacityTwh == null ? 'n/a' : `${view.capacityTwh.toFixed(1)} TWh`}
        (median across ${view.capacityN} daily reports; spread ${view.capacitySpreadPct == null ? 'n/a' : `${view.capacitySpreadPct.toFixed(1)}%`}).
        Injections and withdrawals represent verified daily physical flows reported directly by facility operators to GIE AGSI+.
      </p>
    </div>
  `;

  drawCountryHistoryChart(container.querySelector('[data-chart="history"]'), view, referenceYear, { minYear, range });
  drawCountryFlowChart(container.querySelector('[data-chart="flow"]'), flowRowsForRange(view.rows, range));
}

/* ================================================================== */
/*  Multi-Year Comparison Chart (2021–2026 Overlay + Hover Scrub)     */
/* ================================================================== */

function drawCountryHistoryChart(host, view, referenceYear, opts = {}) {
  const rect = host.getBoundingClientRect();
  const width = Math.max(rect.width || 480, 280);
  const totalH = 280;
  const margin = { top: 12, right: 32, bottom: 28, left: 42 };
  const w = width - margin.left - margin.right;
  const h = totalH - margin.top - margin.bottom;

  const { minYear = null, range = '1Y' } = opts;
  const isPost2022 = minYear === 2022;
  const normLabel = isPost2022 ? 'Post-2022' : '5Y';

  const rows = view.rows.filter((r) => Number.isFinite(r.pct));
  if (rows.length < 2) return;

  const isShort = range === '30D' || range === '90D';
  const isMax = range === 'MAX';

  // Handle Zoomed Short Ranges (30D / 90D)
  if (isShort) {
    const days = range === '30D' ? 30 : 90;
    const latestDate = rows[rows.length - 1].period;
    const cutoff = new Date(latestDate.getTime() - days * 86400000);
    const sliceRows = rows.filter((r) => r.period >= cutoff);
    if (sliceRows.length >= 2) {
      const slicePoints = sliceRows.map((r) => {
        const norm = positionalNormal(rows.map((row) => ({ period: row.period, value: row.pct })), r.period, { minYear });
        return {
          period: r.period,
          pct: r.pct,
          twh: r.twh,
          normMedian: norm?.median ?? null,
          normMin: norm?.min ?? null,
          normMax: norm?.max ?? null,
          normN: norm?.n ?? 0,
        };
      });

      const legendHtml = `
        <div class="eu-chart-legend">
          <span class="eu-legend-item">
            <span class="eu-legend-swatch eu-legend-swatch--line"></span>${referenceYear} Trajectory (${range})
          </span>
          <span class="eu-legend-item">
            <span class="eu-legend-swatch eu-legend-swatch--median"></span>${normLabel} Median
          </span>
          <span class="eu-legend-item">
            <span class="eu-legend-swatch eu-legend-swatch--band"></span>${normLabel} Range
          </span>
        </div>
      `;
      host.insertAdjacentHTML('beforeend', legendHtml);

      const svg = d3.select(host).append('svg')
        .attr('viewBox', `0 0 ${width} ${totalH}`)
        .attr('preserveAspectRatio', 'xMidYMid meet')
        .style('display', 'block');

      const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

      const allY = slicePoints.flatMap((p) => [p.pct, p.normMin, p.normMax]).filter(Number.isFinite);
      const yExtent = allY.length ? d3.extent(allY) : [0, 100];
      const yPad = Math.max(2, (yExtent[1] - yExtent[0]) * 0.1);
      const yMin = Math.max(0, Math.floor((yExtent[0] - yPad) / 5) * 5);
      const yMax = Math.min(100, Math.ceil((yExtent[1] + yPad) / 5) * 5);
      const y = d3.scaleLinear().domain([yMin, yMax]).range([h, 0]);
      const x = d3.scaleTime().domain([cutoff, latestDate]).range([0, w]);

      // Y Gridlines
      for (const t of y.ticks(4)) {
        g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(t)).attr('y2', y(t)).attr('class', 'eu-grid');
        g.append('text').attr('x', -6).attr('y', y(t) + 4).attr('text-anchor', 'end')
          .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').text(`${t}%`);
      }

      // Range Envelope
      const validBand = slicePoints.filter((p) => Number.isFinite(p.normMin) && Number.isFinite(p.normMax));
      if (validBand.length > 1) {
        g.append('path').datum(validBand)
          .attr('d', d3.area().x((d) => x(d.period)).y0((d) => y(d.normMin)).y1((d) => y(d.normMax)).curve(d3.curveMonotoneX))
          .attr('class', 'eu-band eu-band--range');

        g.append('path').datum(validBand)
          .attr('d', d3.line().x((d) => x(d.period)).y((d) => y(d.normMedian)).curve(d3.curveMonotoneX))
          .attr('fill', 'none').attr('class', 'eu-band-median');
      }

      // Trajectory
      g.append('path').datum(slicePoints)
        .attr('d', d3.line().x((d) => x(d.period)).y((d) => y(d.pct)).curve(d3.curveMonotoneX))
        .attr('fill', 'none')
        .style('stroke', 'var(--blue-flame)')
        .attr('stroke-width', 2.5);

      const last = slicePoints[slicePoints.length - 1];
      g.append('circle').attr('cx', x(last.period)).attr('cy', y(last.pct)).attr('r', 3.5).style('fill', 'var(--blue-flame)');

      // X Ticks
      const tickStep = range === '30D' ? d3.timeWeek.every(1) : d3.timeWeek.every(2);
      const ticks = tickStep.range(cutoff, latestDate);
      for (const dt of ticks) {
        const tx = x(dt);
        g.append('line').attr('x1', tx).attr('x2', tx).attr('y1', h).attr('y2', h + 4).attr('stroke', 'rgba(255, 255, 255, 0.15)');
        g.append('text').attr('x', tx).attr('y', h + 18).attr('text-anchor', 'middle')
          .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').text(d3.timeFormat('%b %d')(dt));
      }

      setupEuCountryTimeHover(host, svg, g, x, y, w, h, margin, slicePoints, view.label);
      return;
    }
  }

  // Handle Continuous Multi-Year MAX Range
  if (isMax) {
    const earliest = rows[0].period;
    const latest = rows[rows.length - 1].period;
    const legendHtml = `
      <div class="eu-chart-legend">
        <span class="eu-legend-item">
          <span class="eu-legend-swatch eu-legend-swatch--line"></span>Continuous History (2021–${referenceYear})
        </span>
      </div>
    `;
    host.insertAdjacentHTML('beforeend', legendHtml);

    const svg = d3.select(host).append('svg')
      .attr('viewBox', `0 0 ${width} ${totalH}`)
      .attr('preserveAspectRatio', 'xMidYMid meet')
      .style('display', 'block');

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
    const x = d3.scaleTime().domain([earliest, latest]).range([0, w]);
    const y = d3.scaleLinear().domain([0, 100]).range([h, 0]);

    for (const t of [0, 25, 50, 75, 100]) {
      g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(t)).attr('y2', y(t)).attr('class', 'eu-grid');
      g.append('text').attr('x', -6).attr('y', y(t) + 4).attr('text-anchor', 'end')
        .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').text(`${t}%`);
    }

    g.append('path').datum(rows)
      .attr('d', d3.line().x((d) => x(d.period)).y((d) => y(d.pct)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 2);

    const yearTicks = d3.timeYear.range(d3.timeYear.floor(earliest), d3.timeYear.ceil(latest));
    for (const yt of yearTicks) {
      const tx = x(yt);
      if (tx >= 0 && tx <= w) {
        g.append('line').attr('x1', tx).attr('x2', tx).attr('y1', h).attr('y2', h + 4).attr('stroke', 'rgba(255, 255, 255, 0.2)');
        g.append('text').attr('x', tx).attr('y', h + 18).attr('text-anchor', 'middle')
          .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').text(yt.getFullYear());
      }
    }

    setupEuCountryTimeHover(host, svg, g, x, y, w, h, margin, rows, view.label);
    return;
  }

  // Handle 1Y & 5Y Seasonal Scale (0..1)
  const rawByYear = rowsByYear(rows, (r) => r.pct);
  const byYear = minYear ? rawByYear.filter((yr) => yr.year >= minYear) : rawByYear;

  // Chart Legend HTML
  const legendHtml = `
    <div class="eu-chart-legend">
      ${byYear.map((yr) => `
        <span class="eu-legend-item" data-year="${yr.year}">
          <span class="eu-legend-swatch" style="background: ${YEAR_COLORS[yr.year] || '#94a3b8'}"></span>
          ${yr.year}
        </span>
      `).join('')}
      <span class="eu-legend-item">
        <span class="eu-legend-swatch eu-legend-swatch--median"></span>${normLabel} Median
      </span>
      <span class="eu-legend-item">
        <span class="eu-legend-swatch eu-legend-swatch--band"></span>${normLabel} Range
      </span>
    </div>
  `;
  host.insertAdjacentHTML('beforeend', legendHtml);

  const svg = d3.select(host).append('svg')
    .attr('viewBox', `0 0 ${width} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleLinear().domain([0, 1]).range([0, w]);
  const yMax = Math.max(100, ...rows.map((r) => r.pct));
  const y = d3.scaleLinear().domain([0, Math.ceil(yMax / 10) * 10]).range([h, 0]);

  // Horizontal Grid Lines
  for (const t of y.ticks(4)) {
    g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(t)).attr('y2', y(t)).attr('class', 'eu-grid');
    g.append('text').attr('x', -6).attr('y', y(t) + 4).attr('text-anchor', 'end')
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').text(`${t}%`);
  }

  // Month Labels
  for (let m = 0; m < 12; m += 2) {
    g.append('text').attr('x', x(m / 12)).attr('y', h + 18).attr('text-anchor', 'middle')
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-sans)').text(MONTH_LABEL[m]);
  }

  // Historical Envelope
  const band = monthlyEnvelope(rows.map((r) => ({ period: r.period, value: r.pct })), referenceYear, { minYear })
    .filter((b) => b.min != null)
    .map((b) => ({ x: (b.month - 0.5) / 12, min: b.min, max: b.max, median: b.median }));

  if (band.length > 1) {
    g.append('path').datum(band)
      .attr('d', d3.area().x((d) => x(d.x)).y0((d) => y(d.min)).y1((d) => y(d.max)).curve(d3.curveMonotoneX))
      .attr('class', 'eu-band eu-band--range');

    g.append('path').datum(band)
      .attr('d', d3.line().x((d) => x(d.x)).y((d) => y(d.median)).curve(d3.curveMonotoneX))
      .attr('fill', 'none').attr('class', 'eu-band-median');
  }

  // Multi-Year Trajectory Lines
  const lineGen = d3.line().x((d) => x(d.x)).y((d) => y(d.value)).curve(d3.curveMonotoneX);

  for (const yr of byYear) {
    if (yr.points.length < 2) continue;
    const isRef = yr.year === referenceYear;
    const color = YEAR_COLORS[yr.year] || '#94a3b8';

    g.append('path').datum(yr.points)
      .attr('d', lineGen)
      .attr('class', `eu-year-line eu-year-line--${yr.year}`)
      .style('stroke', color)
      .style('stroke-width', isRef ? 2.5 : 1.35)
      .style('opacity', isRef ? 1 : 0.8);

    if (isRef && yr.points.length) {
      const last = yr.points[yr.points.length - 1];
      g.append('circle').attr('cx', x(last.x)).attr('cy', y(last.value)).attr('r', 3.5)
        .style('fill', color);
    }
  }

  // Interactive Hover Scrubbing
  const tooltip = d3.select(host).append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  const crosshair = g.append('line').attr('y1', 0).attr('y2', h).attr('class', 'eu-crosshair');
  const dot = g.append('circle').attr('r', 4.5).style('fill', 'var(--blue-flame)').style('opacity', 0);

  const hit = g.append('rect')
    .attr('class', 'eu-chart-hitbox')
    .attr('width', w).attr('height', h)
    .attr('fill', 'none')
    .style('cursor', 'pointer')
    .style('pointer-events', 'all');

  hit.on('mousemove', (event) => {
    const [mx] = d3.pointer(event);
    const targetX = x.invert(mx);

    // Find nearest day from all rows
    let best = null;
    for (const r of rows) {
      const fx = monthDayFraction(r.period);
      const lag = Math.abs(fx - targetX);
      if (!best || lag < best.lag) best = { lag, r, fx };
    }

    if (!best || best.lag > 12 / 365) {
      tooltip.style('opacity', 0);
      crosshair.style('opacity', 0);
      dot.style('opacity', 0);
      return;
    }

    const hoveredDate = best.r.period;
    const norm = positionalNormal(rows.map((r) => ({ period: r.period, value: r.pct })), hoveredDate);

    // Current year observation
    const curYrObj = byYear.find((y) => y.year === referenceYear);
    let curPt = null;
    if (curYrObj) {
      for (const p of curYrObj.points) {
        const lag = Math.abs(p.x - targetX);
        if (!curPt || lag < curPt.lag) curPt = { lag, p };
      }
    }

    crosshair.attr('x1', x(best.fx)).attr('x2', x(best.fx)).style('opacity', 1);

    if (curPt && curPt.lag < 8 / 365) {
      dot.attr('cx', x(curPt.p.x)).attr('cy', y(curPt.p.value)).style('opacity', 1);
    } else {
      dot.style('opacity', 0);
    }

    const hostRect = host.getBoundingClientRect();
    const svgRect = svg.node().getBoundingClientRect();
    const vb = svg.attr('viewBox').split(' ').map(Number);
    const k = svgRect.width / vb[2];
    const left = margin.left * k + x(best.fx) * k + 12;
    const top = 16;

    const monthDay = `${hoveredDate.getDate()} ${MONTH_LABEL[hoveredDate.getMonth()]}`;

    let yearsListHtml = '';
    for (const yr of [...byYear].reverse()) {
      let nearest = null;
      for (const p of yr.points) {
        const lag = Math.abs(p.x - targetX);
        if (!nearest || lag < nearest.lag) nearest = { lag, p };
      }
      if (nearest && nearest.lag < 8 / 365) {
        const color = YEAR_COLORS[yr.year] || '#94a3b8';
        yearsListHtml += `
          <div class="tt-row">
            <span class="tt-label"><span class="tt-dot" style="background: ${color}"></span>${yr.year}</span>
            <span class="num">${nearest.p.value.toFixed(1)}%</span>
          </div>
        `;
      }
    }

    const dev = curPt && norm.median != null ? curPt.p.value - norm.median : null;

    tooltip
      .style('opacity', 1)
      .style('left', `${Math.max(0, Math.min(left, hostRect.width - 220))}px`)
      .style('top', `${top}px`)
      .html(`
        <div class="tt-title">${monthDay} · Multi-Year Comparison</div>
        ${norm.median != null ? `
          <div class="tt-row"><span class="tt-label">${normLabel} Median</span><span class="num">${norm.median.toFixed(1)}%</span></div>
          <div class="tt-row"><span class="tt-label">${normLabel} Range</span><span class="num">${norm.min.toFixed(1)}% – ${norm.max.toFixed(1)}%</span></div>
          ${dev != null ? `
            <div class="tt-row tt-row--accent">
              <span class="tt-label">vs ${normLabel} Median</span>
              <span class="num ${dev >= 0 ? 'delta-pos' : 'delta-neg'}">${dev >= 0 ? '+' : ''}${dev.toFixed(1)} pp</span>
            </div>
          ` : ''}
        ` : ''}
        <div style="margin-top: 6px; padding-top: 4px; border-top: 1px solid rgba(255,255,255,0.06);">
          ${yearsListHtml}
        </div>
      `);
  }).on('mouseleave', () => {
    tooltip.style('opacity', 0);
    crosshair.style('opacity', 0);
    dot.style('opacity', 0);
  });
}

function setupEuCountryTimeHover(host, svg, g, x, y, w, h, margin, points, label) {
  const tooltip = d3.select(host).append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  const crosshair = g.append('line').attr('y1', 0).attr('y2', h).attr('class', 'eu-crosshair');
  const dot = g.append('circle').attr('r', 4.5).style('fill', 'var(--blue-flame)').style('opacity', 0);

  const hit = g.append('rect')
    .attr('class', 'eu-chart-hitbox')
    .attr('width', w).attr('height', h)
    .attr('fill', 'none')
    .style('cursor', 'pointer')
    .style('pointer-events', 'all');

  hit.on('mousemove', (event) => {
    const [mx] = d3.pointer(event);
    const targetDt = x.invert(mx);
    if (!targetDt) return;
    const targetMs = targetDt.getTime();

    let best = null;
    for (const p of points) {
      const lag = Math.abs(p.period.getTime() - targetMs);
      if (!best || lag < best.lag) best = { lag, p };
    }
    if (!best) {
      tooltip.style('opacity', 0);
      crosshair.style('opacity', 0);
      dot.style('opacity', 0);
      return;
    }

    const row = best.p;
    const dev = row.normMedian != null ? row.pct - row.normMedian : null;

    crosshair.attr('x1', x(row.period)).attr('x2', x(row.period)).style('opacity', 1);
    dot.attr('cx', x(row.period)).attr('cy', y(row.pct)).style('opacity', 1);

    const hostRect = host.getBoundingClientRect();
    const svgRect = svg.node().getBoundingClientRect();
    const vb = svg.attr('viewBox').split(' ').map(Number);
    const k = svgRect.width / vb[2];
    const left = margin.left * k + x(row.period) * k + 12;
    const top = 16;

    tooltip
      .style('opacity', 1)
      .style('left', `${Math.max(0, Math.min(left, hostRect.width - 220))}px`)
      .style('top', `${top}px`)
      .html(`
        <div class="tt-title">${escapeHtml(formatIsoDate(row.period))} · ${escapeHtml(label)}</div>
        <div class="tt-row"><span class="tt-label">Inventory Level</span><span class="num">${row.pct.toFixed(2)}%</span></div>
        ${row.twh != null ? `<div class="tt-row"><span class="tt-label">Stock</span><span class="num">${row.twh.toFixed(1)} TWh</span></div>` : ''}
        ${row.normMedian != null ? `
          <div class="tt-row"><span class="tt-label">Baseline Median</span><span class="num">${row.normMedian.toFixed(1)}%</span></div>
          <div class="tt-row tt-row--accent">
            <span class="tt-label">vs Baseline</span>
            <span class="num ${dev >= 0 ? 'delta-pos' : 'delta-neg'}">${dev >= 0 ? '+' : ''}${dev.toFixed(1)} pp</span>
          </div>
        ` : ''}
      `);
  }).on('mouseleave', () => {
    tooltip.style('opacity', 0);
    crosshair.style('opacity', 0);
    dot.style('opacity', 0);
  });
}

/* ================================================================== */
/*  Flow Dynamics Chart (Net Bars + 7-Day Trailing Average Line)      */
/* ================================================================== */

function flowRowsForRange(rows, range) {
  if (!rows || !rows.length) return [];
  if (range === 'MAX') return rows;
  const daysMap = { '30D': 30, '90D': 90, '1Y': 365, '5Y': 1825 };
  const days = daysMap[range] ?? 365;
  const latestDate = rows[rows.length - 1].period;
  if (!latestDate) return rows.slice(-days);
  const cutoff = new Date(latestDate.getTime() - days * 86400000);
  const filtered = rows.filter((r) => r.period >= cutoff);
  return filtered.length >= 2 ? filtered : rows.slice(-2);
}

function drawCountryFlowChart(host, rowsAsc) {
  const rows = [...rowsAsc];
  if (rows.length < 2) return;

  const rect = host.getBoundingClientRect();
  const width = Math.max(rect.width || 480, 280);
  const totalH = 280;
  const margin = { top: 12, right: 20, bottom: 28, left: 56 };
  const w = width - margin.left - margin.right;
  const h = totalH - margin.top - margin.bottom;

  // Compute 7-day rolling average for each row
  const processed = rows.map((r, i) => {
    const net = (r.injectionGwh == null && r.withdrawalGwh == null)
      ? 0
      : (r.injectionGwh ?? 0) - (r.withdrawalGwh ?? 0);

    const windowSlice = rows.slice(Math.max(0, i - 6), i + 1);
    const valid = windowSlice.map((s) => (s.injectionGwh ?? 0) - (s.withdrawalGwh ?? 0));
    const avg7d = valid.length ? valid.reduce((acc, curr) => acc + curr, 0) / valid.length : net;

    return {
      period: r.period,
      injectionGwh: r.injectionGwh,
      withdrawalGwh: r.withdrawalGwh,
      netGwh: net,
      avg7dGwh: avg7d,
    };
  });

  const legendHtml = `
    <div class="eu-chart-legend">
      <span class="eu-legend-item">
        <span class="eu-legend-swatch" style="background: var(--color-bullish-supply, #22d3a4)"></span>Net Injection (GWh/d)
      </span>
      <span class="eu-legend-item">
        <span class="eu-legend-swatch" style="background: var(--color-bearish-supply, #ef4e5c)"></span>Net Withdrawal (GWh/d)
      </span>
      <span class="eu-legend-item">
        <span class="eu-legend-swatch" style="background: #fbbf24; height: 2px;"></span>7-Day Trailing Average
      </span>
    </div>
  `;
  host.insertAdjacentHTML('beforeend', legendHtml);

  const svg = d3.select(host).append('svg')
    .attr('viewBox', `0 0 ${width} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const maxVal = Math.max(
    10,
    ...processed.map((r) => Math.max(Math.abs(r.netGwh), Math.abs(r.avg7dGwh)))
  );
  const limit = Math.ceil(maxVal * 1.15);

  const y = d3.scaleLinear().domain([-limit, limit]).range([h, 0]);
  const x = d3.scaleBand()
    .domain(processed.map((r) => r.period.getTime()))
    .range([0, w])
    .paddingInner(0.25);

  // Horizontal Grid Lines & Ticks
  for (const t of y.ticks(4)) {
    g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(t)).attr('y2', y(t))
      .attr('class', t === 0 ? 'eu-grid eu-grid--major' : 'eu-grid');

    g.append('text').attr('x', -8).attr('y', y(t) + 4).attr('text-anchor', 'end')
      .attr('class', 'eu-axis-text')
      .attr('font-family', 'var(--font-mono)')
      .attr('font-feature-settings', "'tnum'")
      .text(Math.round(t).toLocaleString());
  }

  // Net Injection / Withdrawal Bars
  processed.forEach((r) => {
    const v = r.netGwh;
    const cx = x(r.period.getTime());
    const bw = x.bandwidth();

    if (v >= 0) {
      g.append('rect')
        .attr('x', cx)
        .attr('y', y(v))
        .attr('width', bw)
        .attr('height', Math.max(1, y(0) - y(v)))
        .attr('class', 'eu-flow-bar--injection')
        .attr('rx', 1.5);
    } else {
      g.append('rect')
        .attr('x', cx)
        .attr('y', y(0))
        .attr('width', bw)
        .attr('height', Math.max(1, y(v) - y(0)))
        .attr('class', 'eu-flow-bar--withdrawal')
        .attr('rx', 1.5);
    }
  });

  // 7-Day Trailing Average Trend Line
  const trendGen = d3.line()
    .x((d) => x(d.period.getTime()) + x.bandwidth() / 2)
    .y((d) => y(d.avg7dGwh))
    .curve(d3.curveMonotoneX);

  g.append('path')
    .datum(processed)
    .attr('d', trendGen)
    .attr('class', 'eu-flow-trend-line');

  // Date Labels (first, middle, last)
  const keyIndices = [0, Math.floor(processed.length / 2), processed.length - 1];
  for (const idx of keyIndices) {
    const r = processed[idx];
    const cx = x(r.period.getTime()) + x.bandwidth() / 2;
    g.append('text')
      .attr('x', cx)
      .attr('y', h + 18)
      .attr('text-anchor', idx === 0 ? 'start' : idx === processed.length - 1 ? 'end' : 'middle')
      .attr('class', 'eu-axis-text')
      .attr('font-family', 'var(--font-mono)')
      .text(formatIsoDate(r.period).slice(5));
  }

  // Interactive Hover Scrub
  const hoverRect = g.append('rect')
    .attr('class', 'eu-chart-hitbox')
    .attr('width', w)
    .attr('height', h)
    .attr('fill', 'none')
    .style('cursor', 'pointer')
    .style('pointer-events', 'all');

  const tip = d3.select(host).append('div').attr('class', 'chart-tooltip').style('opacity', 0);

  hoverRect.on('mousemove', (event) => {
    const [mx] = d3.pointer(event);
    let bestIdx = 0;
    let bestLag = Infinity;

    processed.forEach((r, i) => {
      const cx = x(r.period.getTime()) + x.bandwidth() / 2;
      const lag = Math.abs(cx - mx);
      if (lag < bestLag) {
        bestLag = lag;
        bestIdx = i;
      }
    });

    const r = processed[bestIdx];
    const hostRect = host.getBoundingClientRect();
    const left = Math.min(mx + margin.left + 12, hostRect.width - 200);

    tip.style('opacity', 1)
      .style('left', `${left}px`)
      .style('top', '16px')
      .html(`
        <div class="tt-title">${escapeHtml(formatIsoDate(r.period))} · Flow Dynamics</div>
        <div class="tt-row"><span class="tt-label">Reported Injection</span><span class="num">${r.injectionGwh == null ? '—' : Math.round(r.injectionGwh).toLocaleString()} GWh/d</span></div>
        <div class="tt-row"><span class="tt-label">Reported Withdrawal</span><span class="num">${r.withdrawalGwh == null ? '—' : Math.round(r.withdrawalGwh).toLocaleString()} GWh/d</span></div>
        <div class="tt-row tt-row--accent">
          <span class="tt-label">Net Daily Flow</span>
          <span class="num ${r.netGwh >= 0 ? 'delta-pos' : 'delta-neg'}">
            ${r.netGwh >= 0 ? '+' : ''}${Math.round(r.netGwh).toLocaleString()} GWh/d
          </span>
        </div>
        <div class="tt-row">
          <span class="tt-label">7-Day Trailing Avg</span>
          <span class="num" style="color: #fbbf24">
            ${r.avg7dGwh >= 0 ? '+' : ''}${Math.round(r.avg7dGwh).toLocaleString()} GWh/d
          </span>
        </div>
      `);
  }).on('mouseleave', () => tip.style('opacity', 0));
}

/* ================================================================== */
/*  Sidebar KPI Strip                                                 */
/* ================================================================== */

function renderEuKpis(sidebarEl, aggregate, views, referenceYear, normalFor) {
  if (aggregate.length < 2) return;

  const latest = aggregate[aggregate.length - 1];
  const priorDay = aggregate[aggregate.length - 2];
  const wowPp = latest.pct - priorDay.pct;
  const wowTwh = latest.twh - priorDay.twh;

  const normal = normalFor(latest);
  const dev = normal.median == null ? null : latest.pct - normal.median;
  const devTwh = dev != null && latest.capacityTwh ? (dev * latest.capacityTwh) / 100 : null;
  const rank = normal.n ? rankAmongYears(latest.pct, normal.values.map((v) => v.value)) : { rank: null, n: 0 };

  let lastInj = 0, lastWd = 0, reporting = 0;
  for (const v of views) {
    if (!v.latest) continue;
    if (v.latest.injectionGwh == null && v.latest.withdrawalGwh == null) continue;
    lastInj += v.latest.injectionGwh ?? 0;
    lastWd += v.latest.withdrawalGwh ?? 0;
    reporting += 1;
  }
  const lastNet = lastInj - lastWd;

  const withLatest = views.filter((v) => v.latest);
  const sorted = [...withLatest].sort((a, b) => a.latest.pct - b.latest.pct);
  const lowest = sorted[0];
  const highest = sorted[sorted.length - 1];

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'Panel Aggregate',
      value: `${latest.pct.toFixed(1)}%`,
      delta: {
        value: `${wowPp >= 0 ? '+' : ''}${wowPp.toFixed(2)} pp DoD`,
        kind: wowPp >= 0 ? 'bullish' : 'bearish',
      },
      helpText: `${latest.twh.toFixed(1)} of ${latest.capacityTwh.toFixed(1)} TWh · ${latest.nCountries} countries · ${formatIsoDate(latest.period)}`,
    }),
    kpiCardHtml({
      label: 'Daily Flow Net',
      value: `${lastNet >= 0 ? '+' : ''}${Math.round(lastNet).toLocaleString()} GWh/d`,
      delta: {
        value: `${wowTwh >= 0 ? '+' : ''}${wowTwh.toFixed(1)} TWh stock DoD`,
        kind: lastNet >= 0 ? 'bullish' : 'bearish',
      },
      helpText: `Injections ${Math.round(lastInj).toLocaleString()} vs withdrawals ${Math.round(lastWd).toLocaleString()} GWh/d (${reporting}/${views.length} countries reporting)`,
    }),
    kpiCardHtml({
      label: 'vs 5-Year Median',
      value: dev == null ? 'n/a' : `${dev >= 0 ? '+' : ''}${dev.toFixed(2)} pp`,
      delta: devTwh == null ? (rank.rank == null ? null : {
        value: `Rank ${rank.rank} of ${rank.n + 1} years`,
        kind: dev != null && dev >= 0 ? 'bullish' : 'bearish',
      }) : {
        value: `${devTwh >= 0 ? '+' : ''}${devTwh.toFixed(1)} TWh (Rank ${rank.rank} of ${rank.n + 1})`,
        kind: dev >= 0 ? 'bullish' : 'bearish',
      },
      helpText: normal.median == null
        ? 'No historical observation within calendar tolerance'
        : `5-year median ${normal.median.toFixed(2)}% across ${normal.years.join(', ')}`,
    }),
    kpiCardHtml({
      label: 'Regional Spread',
      value: lowest && highest ? `${(highest.latest.pct - lowest.latest.pct).toFixed(1)} pp` : 'n/a',
      delta: lowest && highest ? {
        value: `${lowest.cc.toUpperCase()} ${lowest.latest.pct.toFixed(1)}% to ${highest.cc.toUpperCase()} ${highest.latest.pct.toFixed(1)}%`,
        kind: 'neutral',
      } : null,
      helpText: lowest && highest ? `${lowest.label} vs ${highest.label} on ${formatIsoDate(latest.period)}` : '',
    }),
  ].join('');
}

/* ================================================================== */
/*  Utilities                                                         */
/* ================================================================== */

export function referenceYearOf(views) {
  const latest = referenceDate(...views.map((v) => v.rows));
  return latest === null ? null : latest.getFullYear();
}

function ordinalSuffix(i) {
  const j = i % 10, k = i % 100;
  if (j === 1 && k !== 11) return 'st';
  if (j === 2 && k !== 12) return 'nd';
  if (j === 3 && k !== 13) return 'rd';
  return 'th';
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
