/**
 * EU Storage Panel — GIE AGSI+ European Natural Gas Storage.
 *
 * Layout (single panel, three internal layers):
 *   1. Hero chart  — panel aggregate % full, one line for the reference year
 *      through the calendar-month band of the prior years. The aggregate is
 *      Σ(stock TWh) ÷ Σ(derived capacity TWh), so the same numbers that produce
 *      the percentage also produce the TWh reading beside it.
 *   2. Country grid — ten selectable cards; each carries level, TWh, change vs
 *      its same-point-of-year median and the number of prior years that median
 *      rests on, plus a full-history sparkline.
 *   3. Country detail — opened by clicking a card (or below the grid): every
 *      year in the bundle as its own line against the prior-years median band,
 *      the positional normal with its sample, storage in TWh, and GIE's own
 *      daily injection / withdrawal / net figures (GWh/d).
 *
 * Data: bundle.sources.gie_agsi.data — long-format rows:
 *   { series_id: 'gie_storage_<cc>_<field>', period: '2026-04-28', value: 25.1 }
 *   fields: full_pct (%), gas_in_storage (TWh), injection (GWh/d), withdrawal (GWh/d)
 *
 * Nothing here is hard-coded that the data can derive: capacity is
 * `stock / pct × 100` per country, the reference year comes from the newest row,
 * and every normal prints how many prior years it used.
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
  monthOfDay,
} from '../util/eu-storage-analysis.js';

const MONTH_LABEL = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/* ─────────────────────────────────────────────── */
/*  Entry point                                    */
/* ─────────────────────────────────────────────── */

export function renderEuStoragePanel(panelEl, bundle) {
  const source = bundle.sources?.gie_agsi;
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

  // Reference year comes from the newest observation, never the wall clock: a
  // bundle opened in January still labels December's data with its own year.
  const referenceYear = referenceYearOf(views);

  const state = {
    selected: null,
  };

  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'European Gas Storage',
    subtitle: `${views.length} countries · % full and TWh · history by year`,
    sourceKey: 'gie_agsi',
    latestPeriod: source.latest_period,
    draw: () => drawRef.current?.(),
  });

  chartEl.innerHTML = `
    <div class="eu-hero-container"></div>
    <div class="eu-grid-container"></div>
    <div class="eu-detail-container" aria-live="polite"></div>
    <div class="eu-method"></div>
  `;
  const heroEl = chartEl.querySelector('.eu-hero-container');
  const gridEl = chartEl.querySelector('.eu-grid-container');
  const detailEl = chartEl.querySelector('.eu-detail-container');
  // The method note is long-form prose about how the figures were built, not a
  // chart: it is its own section under its own heading.
  const methodEl = appendSection(panelEl, 'europe-method', chartEl.querySelector('.eu-method'));

  const aggregateNormal = (row) => positionalNormal(aggregate, row.period);

  const drawDetail = () => {
    const cc = state.selected ?? views[0]?.cc ?? null;
    drawCountryDetail(detailEl, views.find((v) => v.cc === cc) ?? views[0], referenceYear, aggregate);
  };

  drawRef.current = () => {
    drawEuHeroChart(heroEl, aggregate, referenceYear, aggregateNormal);
    drawCountryGrid(gridEl, views, referenceYear, {
      selected: state.selected,
      onSelect: (cc) => {
        state.selected = state.selected === cc ? null : cc;
        gridEl.querySelectorAll('.eu-card').forEach((el) => {
          el.classList.toggle('is-selected', el.dataset.cc === state.selected);
          el.setAttribute('aria-pressed', String(el.dataset.cc === state.selected));
        });
        drawDetail();
      },
    });
    drawDetail();
  };
  drawRef.current();

  methodEl.innerHTML = renderMethodNote(views, aggregate);
  renderEuKpis(sidebarEl, aggregate, views, referenceYear, aggregateNormal);
}

/* ================================================================== */
/*  Method note (printed on the panel, not tooltips only)              */
/* ================================================================== */

function renderMethodNote(views, aggregate) {
  const capacity = views.reduce((s, v) => s + (v.capacityTwh ?? 0), 0);
  const withCap = views.filter((v) => v.capacityTwh != null).length;
  const last = aggregate[aggregate.length - 1];
  const spread = views
    .map((v) => v.capacitySpreadPct)
    .filter((x) => Number.isFinite(x));
  const maxSpread = spread.length ? Math.max(...spread) : null;
  return `
    <p class="data-provenance">
      Method. Working gas derived as <span class="num">stock ÷ % × 100</span> per country per day
      (median of daily ratios). ${withCap}/${views.length} countries resolve a capacity,
      total <span class="num">${capacity.toFixed(1)} TWh</span>;
      largest day-to-day spread of that ratio
      ${maxSpread == null ? 'n/a' : `<span class="num">${maxSpread.toFixed(2)}%</span>`}.
      Aggregate = Σ stock ÷ Σ capacity, so the % is exactly the volume-weighted mean.
      ${last ? `${last.nCountries}/${views.length} countries reporting on the last day.` : ''}
      AGSI history in this bundle begins 2021-01-01, so any "prior years" figure is at most
      <span class="num">${Math.max(0, ...aggregate.map((r) => r.period.getFullYear())) - 2021}</span>
      prior years — the count is printed wherever a median is used, never assumed.
      Flows are GIE's reported daily injection / withdrawal in GWh/d.
      This is a ${views.length}-country panel, not an EU-plus-UK aggregate.
    </p>
  `;
}

/* ================================================================== */
/*  Hero chart — aggregate % full vs calendar-month band               */
/* ================================================================== */

function drawEuHeroChart(container, aggregate, referenceYear, normalFor) {
  container.innerHTML = '';
  if (aggregate.length < 2) {
    container.innerHTML = '<div class="panel-error">Insufficient data for the aggregate chart.</div>';
    return;
  }

  const margin = { top: 20, right: 56, bottom: 32, left: 44 };
  const totalH = 340;
  const width = Math.max((container.getBoundingClientRect().width || 640), 300) - margin.left - margin.right;
  const height = totalH - margin.top - margin.bottom;

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Horizontal axis: calendar month-day position, identical in every year.
  const x = d3.scaleLinear().domain([0, 1]).range([0, width]);
  const band = monthlyEnvelope(aggregate, referenceYear);
  const yMax = Math.max(
    100,
    ...band.map((b) => b.max ?? 0),
    ...aggregate.map((a) => a.pct),
  );
  const y = d3.scaleLinear().domain([0, Math.ceil(yMax / 10) * 10]).range([height, 0]);

  const yTicks = y.ticks(5);
  for (const t of yTicks) {
    g.append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', y(t)).attr('y2', y(t))
      .attr('class', t === 50 ? 'eu-grid eu-grid--major' : 'eu-grid');
    g.append('text')
      .attr('x', -8).attr('y', y(t) + 4)
      .attr('text-anchor', 'end')
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').attr('font-feature-settings', "'tnum'")
      .text(`${t}%`);
  }

  // Prior-years band: min–max, then the median of those prior years.
  const bandPoints = band
    .map((b) => {
      const mid = b.month - 0.5;
      return b.median == null ? null : { x: mid / 12, ...b };
    })
    .filter(Boolean);
  if (bandPoints.length > 1) {
    const outer = d3.area()
      .x((d) => x(d.x)).y0((d) => y(d.min)).y1((d) => y(d.max))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(bandPoints).attr('d', outer).attr('class', 'eu-band eu-band--range');
    const med = d3.line()
      .x((d) => x(d.x)).y((d) => y(d.median))
      .curve(d3.curveMonotoneX);
    g.append('path').datum(bandPoints).attr('d', med)
      .attr('fill', 'none').attr('class', 'eu-band-median');
  }

  // Reference year line.
  const refRows = aggregate.filter((r) => r.period.getFullYear() === referenceYear);
  const refPoints = refRows.map((r) => ({
    x: dateToX(r.period),
    period: r.period,
    pct: r.pct,
    twh: r.twh,
    capacityTwh: r.capacityTwh,
  }));
  if (refPoints.length > 1) {
    const line = d3.line().x((d) => x(d.x)).y((d) => y(d.pct)).curve(d3.curveMonotoneX);
    g.append('path').datum(refPoints).attr('d', line)
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 2)
      .attr('stroke-linecap', 'round').attr('stroke-linejoin', 'round');
    const last = refPoints[refPoints.length - 1];
    g.append('circle').attr('cx', x(last.x)).attr('cy', y(last.pct)).attr('r', 3.5)
      .style('fill', 'var(--blue-flame)');
    g.append('text')
      .attr('x', Math.min(x(last.x) + 8, width + 4)).attr('y', y(last.pct) + 4)
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').attr('font-feature-settings', "'tnum'")
      .style('fill', 'var(--blue-flame)')
      .text(`${last.pct.toFixed(1)}%`);
  }

  // X axis: month starts.
  for (let m = 0; m < 12; m += 2) {
    g.append('text')
      .attr('x', x(m / 12)).attr('y', height + 20)
      .attr('text-anchor', 'middle')
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-sans)')
      .text(MONTH_LABEL[m]);
  }

  // Direct legend, no colour swatches: the band is described by its sample.
  const yearsUsed = [...new Set(band.flatMap((b) => b.years))].sort((a, b) => a - b);
  const legend = `${referenceYear} · solid line   |   ${yearsUsed.length
    ? `${yearsUsed[0]}–${yearsUsed[yearsUsed.length - 1]} median + range (n=${yearsUsed.length} prior years, by month)`
    : 'no prior-year band in bundle'}`;
  container.insertAdjacentHTML('beforeend', `<div class="eu-hero-legend">${escapeHtml(legend)}</div>`);

  setupEuHeroHover(svg, g, x, y, width, height, margin, refPoints, bandPoints, normalFor);
}

/** Position of a date on the 0..1 calendar axis. */
function dateToX(date) {
  return (monthOfDay(date) - 1 + (date.getDate() - 0.5) / new Date(date.getFullYear(), date.getMonth() + 1, 0).getDate()) / 12;
}

function setupEuHeroHover(svg, g, x, y, width, height, margin, refPoints, bandPoints, normalFor) {
  const host = svg.node().parentNode;
  const tooltip = d3.select(host).append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  const crosshair = g.append('line').attr('y1', 0).attr('y2', height).attr('class', 'eu-crosshair');
  const dot = g.append('circle').attr('r', 4).style('fill', 'var(--blue-flame)').style('opacity', 0);

  const hit = g.append('rect')
    .attr('width', width).attr('height', height)
    .attr('fill', 'none').style('pointer-events', 'all');

  hit.on('mousemove', (event) => {
    const [mx] = d3.pointer(event);
    const target = x.invert(mx);
    // Nearest observation in *date* terms, not index terms: a gap in the
    // series must leave the tooltip empty rather than snapping across months.
    let best = null;
    for (const p of refPoints) {
      const lag = Math.abs(p.x - target);
      if (!best || lag < best.lag) best = { lag, p };
    }
    const maxLag = 6 / 365;
    if (!best || best.lag > maxLag) {
      tooltip.style('opacity', 0); crosshair.style('opacity', 0); dot.style('opacity', 0);
      return;
    }
    const row = best.p;
    const normal = normalFor(row);
    const dev = normal.median == null ? null : row.pct - normal.median;

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
      .style('left', `${Math.max(0, Math.min(left, hostRect.width - 210))}px`)
      .style('top', `${Math.max(0, top)}px`)
      .html(`
        <div class="tt-date">${escapeHtml(formatIsoDate(row.period))} · ${row.pct.toFixed(2)}%</div>
        <div class="tt-row"><span class="tt-label">Stock</span><span class="num">${row.twh.toFixed(1)} TWh</span></div>
        <div class="tt-row"><span class="tt-label">Capacity (derived)</span><span class="num">${row.capacityTwh.toFixed(1)} TWh</span></div>
        ${normal.median == null ? '' : `
        <div class="tt-row"><span class="tt-label">Prior-years median</span><span class="num">${normal.median.toFixed(2)}%</span></div>
        <div class="tt-row tt-row--accent"><span class="tt-label">vs median (n=${normal.n})</span><span class="num">${dev >= 0 ? '+' : ''}${dev.toFixed(2)} pp</span></div>`}
      `);
  }).on('mouseleave', () => {
    tooltip.style('opacity', 0); crosshair.style('opacity', 0); dot.style('opacity', 0);
  });
}

/* ================================================================== */
/*  Country grid                                                      */
/* ================================================================== */

function drawCountryGrid(container, views, referenceYear, { selected, onSelect }) {
  container.innerHTML = '<div class="eu-country-grid" role="list"></div>';
  const grid = container.querySelector('.eu-country-grid');

  for (const v of views) {
    if (!v.latest) continue;
    const pct = v.latest.pct;
    const levelClass = pct < 30 ? 'eu-card--low' : pct < 60 ? 'eu-card--mid' : 'eu-card--high';
    const normal = positionalNormal(v.rows.map((r) => ({ period: r.period, value: r.pct })), v.latest.period);
    const dev = normal.median == null ? null : pct - normal.median;
    const isSel = selected === v.cc;

    grid.insertAdjacentHTML('beforeend', `
      <button type="button" class="eu-card ${levelClass}${isSel ? ' is-selected' : ''}"
              data-cc="${escapeHtml(v.cc)}" aria-pressed="${isSel}" role="listitem">
        <span class="eu-card__head">
          <span class="eu-card__label">${escapeHtml(v.label)}</span>
          <span class="eu-card__cc">${escapeHtml(v.cc.toUpperCase())}</span>
        </span>
        <span class="eu-card__metric">
          <span class="eu-card__value num">${pct.toFixed(1)}%</span>
          <span class="eu-card__sub">${v.latest.twh == null ? '' : `${v.latest.twh.toFixed(1)} TWh`}</span>
        </span>
        <span class="eu-card__normal">
          ${dev == null
            ? '<span class="muted">no prior-year sample</span>'
            : `<span class="num ${dev >= 0 ? 'delta-pos' : 'delta-neg'}">${dev >= 0 ? '+' : ''}${dev.toFixed(1)} pp</span>
               <span class="muted">vs median n=${normal.n}</span>`}
        </span>
        <svg class="eu-card__spark" viewBox="0 0 200 48" preserveAspectRatio="none" aria-hidden="true"></svg>
      </button>
    `);

    drawCountryHistorySpark(grid.lastElementChild.querySelector('.eu-card__spark'), v, referenceYear);
  }

  grid.addEventListener('click', (event) => {
    const card = event.target.closest('.eu-card');
    if (!card) return;
    onSelect(card.dataset.cc);
  });
}

/**
 * Sparkline: the country's whole history by calendar position, with the prior
 * years in a faint band and the reference year on top. It is deliberately not a
 * trailing window — the point of the card is "where does this year sit against
 * everything we have", and a trailing window cannot show that.
 */
function drawCountryHistorySpark(svgEl, view, referenceYear) {
  const rows = view.rows.filter((r) => Number.isFinite(r.pct));
  if (rows.length < 2) return;
  const w = 200, h = 48, pad = 2;
  const x = d3.scaleLinear().domain([0, 1]).range([pad, w - pad]);
  const yMax = Math.max(100, ...rows.map((r) => r.pct));
  const y = d3.scaleLinear().domain([0, yMax]).range([h - 3, 3]);

  const band = monthlyEnvelope(rows.map((r) => ({ period: r.period, value: r.pct })), referenceYear);
  const pts = band
    .filter((b) => b.min != null)
    .map((b) => ({ x: (b.month - 0.5) / 12, min: b.min, max: b.max, median: b.median }));

  const svg = d3.select(svgEl);
  if (pts.length > 1) {
    svg.append('path').datum(pts)
      .attr('d', d3.area().x((d) => x(d.x)).y0((d) => y(d.min)).y1((d) => y(d.max)).curve(d3.curveMonotoneX))
      .attr('class', 'eu-band eu-band--range');
  }
  const ref = rowsByYear(rows, (r) => r.pct).find((yr) => yr.year === referenceYear);
  if (ref && ref.points.length > 1) {
    svg.append('path').datum(ref.points)
      .attr('d', d3.line().x((d) => x(d.x)).y((d) => y(d.value)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .style('stroke', 'var(--blue-flame)')
      .attr('stroke-width', 1.5);
  }
  // Prior years, thin, so the card shows the spread as well as this year.
  for (const yr of rowsByYear(rows, (r) => r.pct)) {
    if (yr.year === referenceYear || yr.points.length < 2) continue;
    svg.append('path').datum(yr.points)
      .attr('d', d3.line().x((d) => x(d.x)).y((d) => y(d.value)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .attr('class', 'eu-spark-prior');
  }
}

/* ================================================================== */
/*  Country detail                                                    */
/* ================================================================== */

function drawCountryDetail(container, view, referenceYear, aggregate) {
  container.innerHTML = '';
  if (!view) return;

  const pctRows = view.rows.map((r) => ({ period: r.period, value: r.pct })).filter((r) => Number.isFinite(r.value));
  const latest = view.latest;
  const normal = latest ? positionalNormal(pctRows, latest.period) : { median: null, n: 0, years: [] };
  const rank = latest && normal.n ? rankAmongYears(latest.pct, normal.values.map((v) => v.value)) : { rank: null, n: 0 };
  const byYear = rowsByYear(view.rows, (r) => r.pct);
  const priorYears = byYear.filter((y) => y.year < referenceYear).map((y) => y.year);

  const dev = latest && normal.median != null ? latest.pct - normal.median : null;

  const flowRows = view.rows.slice(-14).reverse();
  const netFlow = (r) => {
    if (r.injectionGwh == null && r.withdrawalGwh == null) return null;
    return (r.injectionGwh ?? 0) - (r.withdrawalGwh ?? 0);
  };
  const lastNet = latest ? netFlow(latest) : null;

  container.innerHTML = `
    <div class="eu-detail">
      <div class="eu-detail__head">
        <div>
          <h3 class="eu-detail__title">${escapeHtml(view.label)}</h3>
          <p class="eu-detail__sub">
            ${latest ? `${escapeHtml(formatIsoDate(latest.period))} · ${latest.pct.toFixed(2)}% of
            ${view.capacityTwh == null ? 'capacity' : `${view.capacityTwh.toFixed(1)} TWh`}` : 'no current reading'}
            · ${byYear.length} years in bundle
          </p>
        </div>
        <dl class="eu-detail__stats">
          <div><dt>vs prior-years median</dt><dd class="num ${dev == null ? 'muted' : dev >= 0 ? 'delta-pos' : 'delta-neg'}">
            ${dev == null ? 'n/a' : `${dev >= 0 ? '+' : ''}${dev.toFixed(2)} pp`}</dd></div>
          <div><dt>median sample</dt><dd class="num">n=${normal.n}${normal.years.length ? ` (${normal.years.join(', ')})` : ''}</dd></div>
          <div><dt>rank among prior years</dt><dd class="num">${rank.rank == null ? 'n/a' : `${rank.rank} of ${rank.n + 1}`}</dd></div>
          <div><dt>reported net flow</dt><dd class="num ${lastNet == null ? 'muted' : lastNet >= 0 ? 'delta-pos' : 'delta-neg'}">
            ${lastNet == null ? 'n/a' : `${lastNet >= 0 ? '+' : ''}${Math.round(lastNet).toLocaleString()} GWh/d`}</dd></div>
        </dl>
      </div>
      <div class="eu-detail__charts">
        <div class="eu-detail__chart" data-chart="history">
          <p class="eu-detail__cap">% full, one line per year · prior-years median and range by month (n=${priorYears.length})</p>
        </div>
        <div class="eu-detail__chart" data-chart="flow">
          <p class="eu-detail__cap">Reported daily flow, GWh/d · last ${flowRows.length} days · injection above zero, withdrawal below</p>
        </div>
      </div>
      <table class="panel-table eu-detail__table">
        <caption>Last ${flowRows.length} days · level and reported flow</caption>
        <thead>
          <tr><th scope="col">Day</th><th scope="col">% full</th><th scope="col">TWh</th><th scope="col">Injection GWh/d</th><th scope="col">Withdrawal GWh/d</th><th scope="col">Net GWh/d</th></tr>
        </thead>
        <tbody>
          ${flowRows.map((r) => {
            const net = netFlow(r);
            return `<tr>
              <td class="lbl">${escapeHtml(formatIsoDate(r.period))}</td>
              <td>${r.pct == null ? '—' : r.pct.toFixed(2)}</td>
              <td>${r.twh == null ? '—' : r.twh.toFixed(1)}</td>
              <td>${r.injectionGwh == null ? '—' : Math.round(r.injectionGwh).toLocaleString()}</td>
              <td>${r.withdrawalGwh == null ? '—' : Math.round(r.withdrawalGwh).toLocaleString()}</td>
              <td class="${net == null ? 'muted' : net >= 0 ? 'pos' : 'neg'}">${net == null ? '—' : `${net >= 0 ? '+' : ''}${Math.round(net).toLocaleString()}`}</td>
            </tr>`;
          }).join('')}
        </tbody>
      </table>
      <p class="data-provenance">
        Capacity derived from this country's own stock ÷ % readings (median over ${view.capacityN} days;
        spread ${view.capacitySpreadPct == null ? 'n/a' : `${view.capacitySpreadPct.toFixed(2)}%`}).
        Injection and withdrawal are GIE's reported fields, not differences of the stock series.
        Days of cover is not shown: this bundle carries no EU gas demand series to divide by.
        ${priorYears.length < 3 ? `<span class="data-provenance__flag">Only ${priorYears.length} prior year(s) in the bundle — a median over so few years is not a normal.</span>` : ''}
      </p>
    </div>
  `;

  drawCountryHistoryChart(container.querySelector('[data-chart="history"]'), view, referenceYear);
  drawCountryFlowChart(container.querySelector('[data-chart="flow"]'), flowRows);
}

function drawCountryHistoryChart(host, view, referenceYear) {
  const rect = host.getBoundingClientRect();
  const width = Math.max(rect.width || 420, 260);
  const totalH = 260;
  const margin = { top: 12, right: 44, bottom: 26, left: 40 };
  const w = width - margin.left - margin.right;
  const h = totalH - margin.top - margin.bottom;

  const rows = view.rows.filter((r) => Number.isFinite(r.pct));
  if (rows.length < 2) return;

  const svg = d3.select(host).append('svg')
    .attr('viewBox', `0 0 ${width} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleLinear().domain([0, 1]).range([0, w]);
  const yMax = Math.max(100, ...rows.map((r) => r.pct));
  const y = d3.scaleLinear().domain([0, yMax]).range([h, 0]);

  for (const t of y.ticks(4)) {
    g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(t)).attr('y2', y(t)).attr('class', 'eu-grid');
    g.append('text').attr('x', -6).attr('y', y(t) + 4).attr('text-anchor', 'end')
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').text(`${t}%`);
  }
  for (let m = 0; m < 12; m += 2) {
    g.append('text').attr('x', x(m / 12)).attr('y', h + 18).attr('text-anchor', 'middle')
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-sans)').text(MONTH_LABEL[m]);
  }

  const band = monthlyEnvelope(rows.map((r) => ({ period: r.period, value: r.pct })), referenceYear)
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

  const byYear = rowsByYear(rows, (r) => r.pct);
  for (const yr of byYear) {
    if (yr.points.length < 2) continue;
    const isRef = yr.year === referenceYear;
    g.append('path').datum(yr.points)
      .attr('d', d3.line().x((d) => x(d.x)).y((d) => y(d.value)).curve(d3.curveMonotoneX))
      .attr('fill', 'none')
      .attr('class', isRef ? 'eu-year eu-year--current' : 'eu-year');
  }

  // Label the current year at its last point; label prior years at their right
  // ends when they fit without overlap (right margin holds them).
  const ref = byYear.find((yr) => yr.year === referenceYear);
  if (ref && ref.points.length) {
    const last = ref.points[ref.points.length - 1];
    g.append('text').attr('x', Math.min(x(last.x) + 6, w + 4)).attr('y', y(last.value) + 4)
      .attr('class', 'eu-year-label eu-year-label--current').text(ref.year);
  }
  const priors = byYear.filter((yr) => yr.year !== referenceYear && yr.points.length);
  const rightmost = priors.map((yr) => ({ yr, p: yr.points[yr.points.length - 1] }))
    .sort((a, b) => a.p.value - b.p.value);
  let lastLy = -Infinity;
  for (const { yr, p } of rightmost) {
    const ly = y(p.value) + 4;
    if (ly - lastLy < 11) continue;
    lastLy = ly;
    g.append('text').attr('x', Math.min(x(p.x) + 6, w + 4)).attr('y', ly)
      .attr('class', 'eu-year-label').text(yr.year);
  }
}

function drawCountryFlowChart(host, rowsAsc) {
  const rows = [...rowsAsc].reverse();
  if (rows.length < 2) return;
  const rect = host.getBoundingClientRect();
  const width = Math.max(rect.width || 420, 260);
  const totalH = 260;
  const margin = { top: 12, right: 12, bottom: 26, left: 52 };
  const w = width - margin.left - margin.right;
  const h = totalH - margin.top - margin.bottom;

  const net = rows.map((r) => (r.injectionGwh == null && r.withdrawalGwh == null
    ? null
    : (r.injectionGwh ?? 0) - (r.withdrawalGwh ?? 0)));
  const vals = net.filter((v) => v != null);
  const limit = Math.max(1, ...vals.map((v) => Math.abs(v)));
  const y = d3.scaleLinear().domain([-limit, limit]).range([h, 0]);
  const x = d3.scaleBand().domain(rows.map((r) => r.period.getTime())).range([0, w]).paddingInner(0.2);

  const svg = d3.select(host).append('svg')
    .attr('viewBox', `0 0 ${width} ${totalH}`)
    .attr('preserveAspectRatio', 'xMidYMid meet');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  for (const t of y.ticks(4)) {
    g.append('line').attr('x1', 0).attr('x2', w).attr('y1', y(t)).attr('y2', y(t))
      .attr('class', t === 0 ? 'eu-grid eu-grid--major' : 'eu-grid');
    g.append('text').attr('x', -6).attr('y', y(t) + 4).attr('text-anchor', 'end')
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-mono)').attr('font-feature-settings', "'tnum'")
      .text(Math.round(t).toLocaleString());
  }

  rows.forEach((r, i) => {
    const v = net[i];
    const cx = x(r.period.getTime()) + x.bandwidth() / 2;
    if (v == null) return;
    g.append('line').attr('x1', cx).attr('x2', cx)
      .attr('y1', y(0)).attr('y2', y(v))
      .attr('class', v >= 0 ? 'eu-flow eu-flow--in' : 'eu-flow eu-flow--out')
      .attr('stroke-width', Math.max(1, x.bandwidth()));
  });

  // first and last day labels only — the table below carries every value
  for (const idx of [0, rows.length - 1]) {
    g.append('text')
      .attr('x', x(rows[idx].period.getTime()) + x.bandwidth() / 2).attr('y', h + 18)
      .attr('text-anchor', idx === 0 ? 'start' : 'end')
      .attr('class', 'eu-axis-text').attr('font-family', 'var(--font-sans)')
      .text(formatIsoDate(rows[idx].period).slice(5));
  }

  const hover = g.append('rect').attr('width', w).attr('height', h).attr('fill', 'none').style('pointer-events', 'all');
  const tip = d3.select(host).append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  hover.on('mousemove', (event) => {
    const [mx] = d3.pointer(event);
    let bestIdx = 0;
    let bestLag = Infinity;
    rows.forEach((r, i) => {
      const lag = Math.abs(x(r.period.getTime()) + x.bandwidth() / 2 - mx);
      if (lag < bestLag) { bestLag = lag; bestIdx = i; }
    });
    const r = rows[bestIdx];
    const v = net[bestIdx];
    tip.style('opacity', 1)
      .style('left', `${Math.min(mx + margin.left + 12, width - 180)}px`)
      .style('top', '20px')
      .html(`
        <div class="tt-date">${escapeHtml(formatIsoDate(r.period))}</div>
        <div class="tt-row"><span class="tt-label">Injection</span><span class="num">${r.injectionGwh == null ? '—' : Math.round(r.injectionGwh).toLocaleString()} GWh/d</span></div>
        <div class="tt-row"><span class="tt-label">Withdrawal</span><span class="num">${r.withdrawalGwh == null ? '—' : Math.round(r.withdrawalGwh).toLocaleString()} GWh/d</span></div>
        <div class="tt-row tt-row--accent"><span class="tt-label">Net</span><span class="num">${v == null ? '—' : `${v >= 0 ? '+' : ''}${Math.round(v).toLocaleString()} GWh/d`}</span></div>
      `);
  }).on('mouseleave', () => tip.style('opacity', 0));
}

/* ================================================================== */
/*  KPI strip                                                          */
/* ================================================================== */

function renderEuKpis(sidebarEl, aggregate, views, referenceYear, normalFor) {
  if (aggregate.length < 2) return;

  const latest = aggregate[aggregate.length - 1];
  const priorDay = aggregate[aggregate.length - 2];
  const wowPp = latest.pct - priorDay.pct;
  const wowTwh = latest.twh - priorDay.twh;

  const normal = normalFor(latest);
  const dev = normal.median == null ? null : latest.pct - normal.median;
  const rank = normal.n ? rankAmongYears(latest.pct, normal.values.map((v) => v.value)) : { rank: null, n: 0 };

  // Net reported flow across every country on the last day, GWh/d → the sign
  // of the season. This is the aggregate the cards only show per country.
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
      label: 'Panel aggregate',
      value: `${latest.pct.toFixed(1)}%`,
      delta: {
        value: `${wowPp >= 0 ? '+' : ''}${wowPp.toFixed(2)} pp DoD`,
        kind: wowPp >= 0 ? 'bullish' : 'bearish',
      },
      helpText: `${latest.twh.toFixed(1)} of ${latest.capacityTwh.toFixed(1)} TWh · ${latest.nCountries} countries · ${formatIsoDate(latest.period)}`,
    }),
    kpiCardHtml({
      label: 'Stock change',
      value: `${wowTwh >= 0 ? '+' : ''}${wowTwh.toFixed(1)} TWh`,
      delta: {
        value: `${lastNet >= 0 ? '+' : ''}${Math.round(lastNet).toLocaleString()} GWh/d reported net`,
        kind: lastNet >= 0 ? 'bullish' : 'bearish',
      },
      helpText: `Reported injection ${Math.round(lastInj).toLocaleString()} − withdrawal ${Math.round(lastWd).toLocaleString()} GWh/d (${reporting}/${views.length} countries reporting)`,
    }),
    kpiCardHtml({
      label: 'vs prior-years median',
      value: dev == null ? 'n/a' : `${dev >= 0 ? '+' : ''}${dev.toFixed(2)} pp`,
      delta: rank.rank == null ? null : {
        value: `${rank.rank} of ${rank.n + 1} years for this date`,
        kind: dev != null && dev >= 0 ? 'bullish' : 'bearish',
      },
      helpText: normal.median == null
        ? 'No prior-year observation within 10 days of this date'
        : `Median of ${normal.years.join(', ')} = ${normal.median.toFixed(2)}% (n=${normal.n} years)`,
    }),
    kpiCardHtml({
      label: 'Spread',
      value: lowest && highest ? `${(highest.latest.pct - lowest.latest.pct).toFixed(1)} pp` : 'n/a',
      delta: lowest && highest ? {
        value: `${lowest.cc.toUpperCase()} ${lowest.latest.pct.toFixed(1)}% → ${highest.cc.toUpperCase()} ${highest.latest.pct.toFixed(1)}%`,
        kind: 'neutral',
      } : null,
      helpText: lowest && highest ? `${lowest.label} vs ${highest.label} on ${formatIsoDate(latest.period)}` : '',
    }),
  ].join('');
}

/**
 * The reference year, exposed for the measurement script so it can assert that
 * the panel and the script agree on which year is "current".
 *
 * Derived from the newest observation in the joined country rows, not from
 * `Date.now()`: a bundle read in January must still call December its own year.
 * Returns null when there is no dated row at all.
 */
export function referenceYearOf(views) {
  const latest = referenceDate(...views.map((v) => v.rows));
  return latest === null ? null : latest.getFullYear();
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}
