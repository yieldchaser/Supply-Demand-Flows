/**
 * EU Storage Panel — GIE AGSI+ European Natural Gas Storage.
 *
 * Layout (single panel, three internal layers):
 *   1. Hero chart  — EU aggregate % full as Blue Flame line through 5-year envelope.
 *      EU aggregate is computed frontend-side as a storage-capacity-weighted mean of
 *      the 10-country % full values (the EU pre-aggregate series isn't in the bundle).
 *      The weight proxy is the all-time maximum gas_in_storage value for each country
 *      (working gas volume is not available in the schema).
 *   2. Country grid — 5×2 small-multiple cards; each has flag, latest %, 24-month
 *      sparkline. Border + value colour reflects level: red <30%, amber 30-60%, cyan ≥60%.
 *   3. KPI strip — EU aggregate, vs 5y avg, lowest country, highest country.
 *
 * Data: bundle.sources.gie_agsi.data — long-format rows:
 *   { series_id: 'gie_storage_de_full_pct', period: '2026-04-28', value: 25.1, region: 'DE', … }
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries } from '../util/series.js';
import { seasonalEnvelope, envelopeLookup, dayOnly } from '../util/season.js';
import { seasonWeekOf } from '../util/storage-analysis.js';

/* ─────────────────────────────────────────────── */
/*  Constants                                      */
/* ─────────────────────────────────────────────── */

const COUNTRIES = [
  { cc: 'de', label: 'Germany',     flag: '🇩🇪' },
  { cc: 'fr', label: 'France',      flag: '🇫🇷' },
  { cc: 'it', label: 'Italy',       flag: '🇮🇹' },
  { cc: 'nl', label: 'Netherlands', flag: '🇳🇱' },
  { cc: 'es', label: 'Spain',       flag: '🇪🇸' },
  { cc: 'at', label: 'Austria',     flag: '🇦🇹' },
  { cc: 'be', label: 'Belgium',     flag: '🇧🇪' },
  { cc: 'cz', label: 'Czechia',     flag: '🇨🇿' },
  { cc: 'hu', label: 'Hungary',     flag: '🇭🇺' },
  { cc: 'pl', label: 'Poland',      flag: '🇵🇱' },
];

/* ─────────────────────────────────────────────── */
/*  Entry point                                    */
/* ─────────────────────────────────────────────── */

export function renderEuStoragePanel(panelEl, bundle) {
  const source = bundle.sources?.gie_agsi;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">GIE AGSI+ data unavailable.</div>';
    return;
  }

  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'European Gas Storage',
    subtitle: '10 countries · % full · 5-year envelope',
    sourceKey: 'gie_agsi',
    latestPeriod: source.latest_period,
    draw: () => drawRef.current?.(),
  });

  /* ── Build per-country series ─────────────────────────────────── */
  const countryData = COUNTRIES.map((c) => {
    const pctSeries = getSeries(source.data, `gie_storage_${c.cc}_full_pct`);
    const gsSeries  = getSeries(source.data, `gie_storage_${c.cc}_gas_in_storage`);
    // Use all-time max gas_in_storage as a capacity proxy for weighting.
    const maxStorage = gsSeries.length
      ? Math.max(...gsSeries.map((r) => r.value))
      : 0;
    return {
      ...c,
      series: pctSeries,
      latest: pctSeries.length ? pctSeries[pctSeries.length - 1] : null,
      maxStorage,
    };
  }).filter((c) => c.series.length > 0);

  if (countryData.length === 0) {
    chartEl.innerHTML = '<div class="panel-error">No country series found in GIE data.</div>';
    return;
  }

  /* ── Compute EU aggregate (capacity-weighted mean of % full) ──── */
  // Collect all unique dates across all countries.
  const allTimes = [
    ...new Set(countryData.flatMap((c) => c.series.map((r) => r.period.getTime()))),
  ].sort((a, b) => a - b);

  // Value-by-date index per country: the aggregate used to run `find()` over
  // 2,000 daily rows for every country on every one of ~2,000 dates, and then
  // again on each resize redraw.
  const valueByDate = countryData.map((c) => new Map(c.series.map((r) => [r.period.getTime(), r.value])));

  const euAggregate = allTimes.map((t) => {
    let weightedSum = 0;
    let weightSum   = 0;
    countryData.forEach((c, i) => {
      if (c.maxStorage <= 0) return;
      const v = valueByDate[i].get(t);
      if (v != null && Number.isFinite(v)) {
        weightedSum += v * c.maxStorage;
        weightSum   += c.maxStorage;
      }
    });
    return weightSum > 0
      ? { period: new Date(t), value: weightedSum / weightSum }
      : null;
  }).filter(Boolean);

  /* ── Layout ───────────────────────────────────────────────────── */
  chartEl.innerHTML = `
    <div class="eu-hero-container"></div>
    <div class="eu-grid-container" style="margin-top: var(--space-6)"></div>
  `;

  // The hero and the country grid each measure their own container, so both
  // have to be rebuilt when the column resizes (appended, not re-injected: the
  // containers persist and their draws clear themselves).
  // Both the hero and the KPIs read the same seasonal normal, computed once.
  const normal = buildEuNormal(euAggregate);

  const drawAll = () => {
    drawEuHeroChart(chartEl.querySelector('.eu-hero-container'), euAggregate, normal);
    drawCountryGrid(chartEl.querySelector('.eu-grid-container'), countryData);
  };
  drawRef.current = drawAll;
  drawAll();
  renderEuKpis(sidebarEl, euAggregate, countryData, normal);
}

/* ================================================================== */
/*  Seasonal normal (week-of-season, not ISO week)                     */
/* ================================================================== */

/**
 * The 5-year envelope plus the week index used to read it, keyed by season week
 * so the chart, the hero hover and the KPI card all quote the same normal.
 *
 * The panel used to key the envelope and the current line by `isoWeek()`. The
 * ISO week of a Friday label jumps an extra week between years, so a tooltip
 * could compare a mid-April reading against a late-April normal and report a
 * deviation that flipped sign week to week.
 */
function buildEuNormal(euAggregate) {
  const latest = euAggregate[euAggregate.length - 1] ?? null;
  if (!latest) return { envelope: new Map(), weekKey: () => 0, latestWeek: 0, latestNormal: null };
  const envelope = seasonalEnvelope(euAggregate, { anchor: dayOnly(latest.period) });
  const weekKey = (r) => seasonWeekOf(euAggregate, r.period);
  const latestWeek = weekKey(latest);
  return {
    envelope,
    weekKey,
    latestWeek,
    latestNormal: envelopeLookup(envelope, latestWeek),
  };
}

/* ================================================================== */
/*  Hero Chart — EU aggregate % full with 5-year envelope             */
/* ================================================================== */

function drawEuHeroChart(container, euAggregate, normal) {
  container.innerHTML = '';
  if (euAggregate.length < 2) {
    container.innerHTML = '<div class="panel-error" style="font-size:12px">Insufficient data for EU aggregate chart.</div>';
    return;
  }

  const margin = { top: 24, right: 16, bottom: 36, left: 48 };
  const totalH = 360;
  const width  = Math.max((container.getBoundingClientRect().width || 600), 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  /* X: ISO week 1–52 */
  const x = d3.scaleLinear().domain([1, 52]).range([0, width]);

  /* Y: fixed 0–100% */
  const y = d3.scaleLinear().domain([0, 100]).range([height, 0]);

  /* Gridlines */
  [0, 25, 50, 75, 100].forEach((v) => {
    g.append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', y(v)).attr('y2', y(v))
      .attr('stroke', v === 50 ? 'rgba(255,255,255,0.07)' : 'rgba(255,255,255,0.04)');
  });

  /* 5-year envelope, indexed by week-of-season */
  const { envelope, weekKey } = normal;
  const envData = Array.from(envelope, ([wk, e]) => ({ wk, ...e })).sort((a, b) => a.wk - b.wk);

  if (envData.length > 1) {
    // Outer band (min–max)
    const areaOuter = d3.area()
      .x((d) => x(d.wk)).y0((d) => y(d.min)).y1((d) => y(d.max))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', areaOuter)
      .attr('fill', 'rgba(125,211,252,0.06)');

    // Inner band (p25–p75)
    const areaInner = d3.area()
      .x((d) => x(d.wk)).y0((d) => y(d.p25)).y1((d) => y(d.p75))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', areaInner)
      .attr('fill', 'rgba(125,211,252,0.12)');

    // Mean dashed line
    const meanLine = d3.line()
      .x((d) => x(d.wk)).y((d) => y(d.mean))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(envData).attr('d', meanLine)
      .attr('fill', 'none')
      .attr('stroke', 'rgba(125,211,252,0.4)')
      .attr('stroke-width', 1)
      .attr('stroke-dasharray', '2,4');
  }

  /* Current-year line — reference year from the data, not the viewer's clock */
  const currentYear = euAggregate.length
    ? Math.max(...euAggregate.map((r) => r.period.getFullYear()))
    : null;
  const currentRows = currentYear === null
    ? []
    : euAggregate.filter((r) => r.period.getFullYear() === currentYear);

  if (currentRows.length > 0) {
    const lineCurrent = d3.line()
      .x((d) => x(weekKey(d))).y((d) => y(d.value))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(currentRows).attr('d', lineCurrent)
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 2.5)
      .attr('stroke-linecap', 'round').attr('stroke-linejoin', 'round')
      .style('filter', 'drop-shadow(0 0 8px rgba(125,211,252,0.4))');

    // Latest-point dot
    const latest = currentRows[currentRows.length - 1];
    g.append('circle')
      .attr('cx', x(weekKey(latest))).attr('cy', y(latest.value))
      .attr('r', 4)
      .style('fill', 'var(--blue-flame)')
      .style('filter', 'drop-shadow(0 0 6px rgba(125,211,252,0.6))');
  }

  /* X-axis month labels */
  const monthTicks = [
    { wk: 1, label: 'Jan' }, { wk: 9,  label: 'Mar' },
    { wk: 18, label: 'May' }, { wk: 27, label: 'Jul' },
    { wk: 36, label: 'Sep' }, { wk: 44, label: 'Nov' },
  ];
  g.selectAll('.eu-x-tick').data(monthTicks).enter()
    .append('text')
    .attr('x', (d) => x(d.wk)).attr('y', height + 22)
    .attr('text-anchor', 'middle')
    .attr('font-size', 11).attr('font-family', 'var(--font-sans)')
    .style('fill', 'var(--chart-label)')
    .text((d) => d.label);

  /* Y-axis labels (0/25/50/75/100) */
  [0, 25, 50, 75, 100].forEach((v) => {
    g.append('text')
      .attr('x', -8).attr('y', y(v) + 4)
      .attr('text-anchor', 'end')
      .attr('font-size', 11).attr('font-family', 'var(--font-mono)')
      .attr('font-feature-settings', "'tnum'")
      .style('fill', 'var(--chart-label)')
      .text(`${v}%`);
  });

  setupEuHeroHover(svg, g, x, y, width, height, currentRows, envelope, margin, weekKey);
}

/* ─── Hero hover ────────────────────────────────────────────────── */

/**
 * The observation nearest the hovered week, within the week itself.
 *
 * AGSI reports daily but the axis is weekly, so `weekKey(r) === wk` only hits
 * if the series happens to have a point on the first day of that week. Matching
 * the nearest point (±3 weeks) keeps the tooltip on the curve instead of
 * snapping it between weeks.
 */
function weekAt(rows, wk, weekKey) {
  let best = null;
  let bestLag = Infinity;
  for (const r of rows) {
    const lag = Math.abs(weekKey(r) - wk);
    if (lag < bestLag) { best = r; bestLag = lag; }
  }
  return bestLag <= 0.5 ? best : null;
}

function setupEuHeroHover(svg, g, x, y, width, height, currentRows, envelope, margin, weekKey) {
  const tooltipDiv = d3.select(svg.node().parentNode).append('div')
    .attr('class', 'chart-tooltip').style('opacity', 0);

  const crosshair = g.append('line')
    .attr('y1', 0).attr('y2', height)
    .attr('stroke', 'rgba(255,255,255,0.2)').attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,2').style('opacity', 0);

  const hoverDot = g.append('circle').attr('r', 5)
    .style('fill', 'var(--blue-flame)')
    .attr('stroke', 'rgba(10,14,20,0.8)').attr('stroke-width', 2)
    .style('opacity', 0);

  const hitbox = g.append('rect')
    .attr('width', width).attr('height', height)
    .attr('fill', 'none').style('pointer-events', 'all');

  hitbox
    .on('mousemove', function (event) {
      const [mx] = d3.pointer(event);
      const wk  = Math.round(x.invert(mx));
      const row = weekAt(currentRows, wk, weekKey);
      const env = row ? envelopeLookup(envelope, weekKey(row)) : null;

      // Only weeks that actually exist get a crosshair. The hover used to hide
      // itself exactly when the envelope was missing, which left the tooltip
      // stuck on a stale week the moment the cursor passed the latest
      // observation (end of the year, or any gap in the series).
      if (!row || !env) {
        crosshair.style('opacity', 0); hoverDot.style('opacity', 0); tooltipDiv.style('opacity', 0);
        return;
      }

      const rowWk = weekKey(row);
      crosshair.attr('x1', x(rowWk)).attr('x2', x(rowWk)).style('opacity', 1);

      const containerRect = svg.node().parentNode.getBoundingClientRect();
      const svgRect = svg.node().getBoundingClientRect();
      const scaleX  = svgRect.width / (parseInt(svg.attr('viewBox').split(' ')[2]));
      const tooltipX = margin.left * scaleX + x(rowWk) * scaleX + 16;
      const tooltipY = margin.top  * scaleX + y(row.value) * scaleX - 20;

      tooltipDiv
        .style('opacity', 1)
        .style('left', `${Math.min(tooltipX, containerRect.width - 200)}px`)
        .style('top',  `${Math.max(tooltipY, 10)}px`);

      {
        hoverDot.attr('cx', x(rowWk)).attr('cy', y(row.value)).style('opacity', 1);
        const vsAvg    = row.value - env.mean;
        tooltipDiv.html(`
          <div class="tt-date">${row.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}</div>
          <div class="tt-row"><span class="tt-label">EU aggregate</span><span class="num">${row.value.toFixed(1)}%</span></div>
          <div class="tt-row"><span class="tt-label">5y avg (n=${env.count})</span><span class="num">${env.mean.toFixed(1)}%</span></div>
          <div class="tt-row tt-row--accent"><span class="tt-label">vs avg</span><span class="num">${vsAvg >= 0 ? '+' : ''}${vsAvg.toFixed(1)} pp</span></div>
        `);
      }
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0); hoverDot.style('opacity', 0); tooltipDiv.style('opacity', 0);
    });
}

/* ================================================================== */
/*  Country Small-Multiples Grid                                       */
/* ================================================================== */

function drawCountryGrid(container, countryData) {
  container.innerHTML = '<div class="eu-country-grid"></div>';
  const grid = container.querySelector('.eu-country-grid');

  for (const c of countryData) {
    if (!c.latest) continue;

    const pct = c.latest.value;
    let levelClass;
    if (pct < 30)       levelClass = 'eu-card--low';
    else if (pct < 60)  levelClass = 'eu-card--mid';
    else                levelClass = 'eu-card--high';

    // Last ~24 months of daily data (AGSI is daily; 365 days × 2 ≈ 730 rows).
    // Thin it to at most 200 points for sparkline performance.
    const last24m = c.series.slice(-730);
    const step = Math.max(1, Math.floor(last24m.length / 200));
    const sparkRows = last24m.filter((_, i) => i % step === 0);

    grid.insertAdjacentHTML('beforeend', `
      <div class="eu-card ${levelClass}">
        <div class="eu-card__head">
          <span class="eu-card__flag">${c.flag}</span>
          <span class="eu-card__label">${c.label}</span>
        </div>
        <div class="eu-card__metric">
          <span class="eu-card__value num">${pct.toFixed(1)}%</span>
          <span class="eu-card__sub">full</span>
        </div>
        <svg class="eu-card__spark" viewBox="0 0 200 60" preserveAspectRatio="none"></svg>
      </div>
    `);

    drawCountrySparkline(
      grid.lastElementChild.querySelector('.eu-card__spark'),
      sparkRows,
      levelClass,
    );
  }
}

function drawCountrySparkline(svgEl, rows, levelClass) {
  if (rows.length < 2) return;

  const w = 200, h = 60;
  const xScale = d3.scaleLinear().domain([0, rows.length - 1]).range([4, w - 4]);
  const yScale = d3.scaleLinear().domain([0, 100]).range([h - 4, 4]);

  const colorMap = {
    'eu-card--low': '#ef4e5c',
    'eu-card--mid': '#f59e0b',
    'eu-card--high': '#7dd3fc',
  };
  const lineColor = colorMap[levelClass] ?? '#7dd3fc';
  const fillColor = lineColor + '22'; // ~13% alpha

  const svg = d3.select(svgEl);

  // 50% reference dashed line
  svg.append('line')
    .attr('x1', 4).attr('x2', w - 4)
    .attr('y1', yScale(50)).attr('y2', yScale(50))
    .attr('stroke', 'rgba(255,255,255,0.1)')
    .attr('stroke-width', 1)
    .attr('stroke-dasharray', '2,3');

  const lineGen = d3.line()
    .x((_, i) => xScale(i)).y((d) => yScale(d.value))
    .curve(d3.curveMonotoneX);

  const areaGen = d3.area()
    .x((_, i) => xScale(i))
    .y0(h - 4).y1((d) => yScale(d.value))
    .curve(d3.curveMonotoneX);

  svg.append('path').datum(rows).attr('d', areaGen).style('fill', fillColor);
  svg.append('path').datum(rows).attr('d', lineGen)
    .attr('fill', 'none').style('stroke', lineColor).attr('stroke-width', 1.5);

  // Latest-point dot
  svg.append('circle')
    .attr('cx', xScale(rows.length - 1))
    .attr('cy', yScale(rows[rows.length - 1].value))
    .attr('r', 2.5)
    .style('fill', lineColor);
}

/* ================================================================== */
/*  KPI Strip                                                          */
/* ================================================================== */

function renderEuKpis(sidebarEl, euAggregate, countryData, normal) {
  if (euAggregate.length < 2 || countryData.length === 0) return;

  const latest  = euAggregate[euAggregate.length - 1];
  const prior   = euAggregate[euAggregate.length - 2];
  const wowDelta = latest.value - prior.value;

  // vs the 5-year mean for the same week-of-season as the latest reading.
  // AGSI is daily, so the envelope's count is a day count, not a year count —
  // print it as such rather than implying five annual observations.
  const env      = normal.latestNormal;
  const latestWk = normal.latestWeek;
  const vsAvg    = env ? latest.value - env.mean : null;

  // Lowest and highest country by current % full
  const withLatest = countryData.filter((c) => c.latest);
  const sorted     = [...withLatest].sort((a, b) => a.latest.value - b.latest.value);
  const lowest     = sorted[0];
  const highest    = sorted[sorted.length - 1];

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'EU Aggregate',
      value: `${latest.value.toFixed(1)}%`,
      delta: {
        value: `${wowDelta >= 0 ? '+' : ''}${wowDelta.toFixed(2)} pp WoW`,
        kind:  wowDelta >= 0 ? 'bullish' : 'bearish',
      },
      helpText: `As of ${latest.period.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}`,
    }),
    kpiCardHtml({
      label: 'vs 5y avg',
      value: vsAvg != null ? `${vsAvg >= 0 ? '+' : ''}${vsAvg.toFixed(1)} pp` : 'n/a',
      delta: vsAvg != null ? {
        value: vsAvg >= 0 ? 'Above seasonal avg' : 'Below seasonal avg',
        kind:  vsAvg >= 0 ? 'bullish' : 'bearish',
      } : null,
      helpText: env
        ? `5y mean of week ${latestWk} of season: ${env.mean.toFixed(1)}% (${env.count} daily obs.)`
        : 'No seasonal normal available for this week',
    }),
    kpiCardHtml({
      label: 'Lowest country',
      value: lowest ? `${lowest.latest.value.toFixed(1)}%` : 'n/a',
      delta: lowest ? {
        value: lowest.label,
        kind: 'bearish',
      } : null,
      helpText: lowest ? `${lowest.flag} ${lowest.label} — tightest storage` : '',
    }),
    kpiCardHtml({
      label: 'Highest country',
      value: highest ? `${highest.latest.value.toFixed(1)}%` : 'n/a',
      delta: highest ? {
        value: highest.label,
        kind: 'bullish',
      } : null,
      helpText: highest ? `${highest.flag} ${highest.label} — most comfortable` : '',
    }),
  ].join('');
}
