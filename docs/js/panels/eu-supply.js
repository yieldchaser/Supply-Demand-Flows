/**
 * Where Europe's gas comes from — EU imports by supply route (ENTSOG) and the
 * transatlantic LNG link (EIA exports by destination vs EU LNG send-out).
 *
 * Blocks
 *   KPIs        imports (7-day mean) with YoY, LNG share, Russian-origin share
 *               now vs 2021, US share of EU LNG (12 months)
 *   Mix         stacked 7-day mean by route (1Y / 3Y / since 2021)
 *   Routes      table: now, a year ago, change, share now vs 2021
 *   LNG link    monthly EU LNG send-out with the US-sourced part
 *   Terminals   LNG send-out by country, last 30 days vs a year ago
 *
 * Arithmetic: util/eu-supply-analysis.js. Imports exclude EU domestic
 * production, so shares are of measured imports, not of consumption.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries } from '../util/series.js';
import { registerChartSnapshot } from '../util/chart-snapshot.js';
import { escapeHtml, isNum, fx, sx, tone, frame, yGrid, tooltip, hitArea, blockHead, legend } from '../util/eu-chart-kit.js';
import {
  ROUTES,
  TWH_PER_BCF,
  UKRAINE_TRANSIT_END,
  buildRouteIndex,
  supplyMix,
  rollingRouteSeries,
  monthlyEnergy,
  usLngToEu,
  transatlanticLng,
} from '../util/eu-supply-analysis.js';

const CHART_ID = 'eu-supply-mix';
const fmtDay = d3.timeFormat('%-d %b %Y');
const LNG_COUNTRIES = { be: 'Belgium', de: 'Germany', es: 'Spain', fi: 'Finland', fr: 'France', gr: 'Greece', hr: 'Croatia', it: 'Italy', lt: 'Lithuania', nl: 'Netherlands', pl: 'Poland', pt: 'Portugal' };

/** Pure model, exported for the chapter download and tests. */
export function buildEuSupplyModel(bundle) {
  const en = bundle?.sources?.entsog?.data;
  if (!en?.length) return null;
  const index = buildRouteIndex((id) => getSeries(en, `entsog_route_${id}`));
  if (!index.days.length) return null;
  const asOf = index.days[index.days.length - 1];
  const mix = supplyMix(index, asOf);
  const lngRows = bundle.sources.eia_lng_exports?.data ?? [];
  const lngIds = [...new Set(lngRows.map((r) => r.series_id))];
  const us = usLngToEu((id) => getSeries(lngRows, id), lngIds);
  const euLng = monthlyEnergy(getSeries(en, 'entsog_route_lng'));
  const link = transatlanticLng(us, euLng);
  const byCountry = Object.keys(LNG_COUNTRIES).map((cc) => {
    const rows = getSeries(en, `entsog_lng_${cc}`);
    const win = (end, n) => {
      const lo = new Date(end.getFullYear(), end.getMonth(), end.getDate() - n + 1);
      const xs = rows.filter((r) => r.period >= lo && r.period <= end).map((r) => r.value);
      return xs.length >= n * 0.8 ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
    };
    const ya = new Date(asOf.getFullYear() - 1, asOf.getMonth(), asOf.getDate());
    return { cc, label: LNG_COUNTRIES[cc], now: win(asOf, 30), prior: win(ya, 30) };
  }).filter((r) => isNum(r.now) || isNum(r.prior));
  return { index, asOf, mix, link, byCountry };
}

export function euSupplyExport(model) {
  if (!model?.mix) return null;
  const r = (x, d = 1) => (isNum(x) ? Number(x.toFixed(d)) : null);
  return {
    basis: 'ENTSOG physical flows at EU entry points (EU receiving side), 7-day mean, GWh/d. Imports exclude EU production. '
      + `US LNG to EU-27 from EIA exports by destination, converted at ${TWH_PER_BCF.toFixed(4)} TWh/Bcf.`,
    as_of: model.asOf.toISOString().slice(0, 10),
    imports_gwh_d: r(model.mix.total, 0),
    russian_origin_share_pct: r(model.mix.russianShare),
    russian_origin_share_2021_pct: r(model.mix.russianShare2021),
    routes: model.mix.rows.map((x) => ({ id: x.id, gwh_d: r(x.gwh, 0), share_pct: r(x.share), year_ago_gwh_d: r(x.priorGwh, 0), share_2021_pct: r(x.share2021) })),
    us_lng_to_eu: model.link.slice(-24).map((x) => ({ month: x.month, us_twh: r(x.usTwh), eu_lng_sendout_twh: r(x.euTwh), share_pct: r(x.share), share_12m_pct: r(x.share12) })),
  };
}

export function renderEuSupplyPanel(panelEl, bundle) {
  const model = buildEuSupplyModel(bundle);
  if (!model) {
    panelEl.innerHTML = '<div class="panel-error">ENTSOG flow data unavailable.</div>';
    return;
  }
  const state = { span: '1y' };
  const series = rollingRouteSeries(model.index);
  registerSnapshot(series, state);

  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: "Where Europe's Gas Comes From",
    subtitle: `ENTSOG entry flows · ${ROUTES.length} routes · daily since 2021 · US LNG link from EIA`,
    sourceKey: 'entsog',
    latestPeriod: bundle.sources.entsog.latest_period,
    chartExport: CHART_ID,
    draw: () => drawRef.current?.(),
  });
  chartEl.classList.add('eu-panel');
  chartEl.innerHTML = `
    <p class="eu-reading" data-role="reading"></p>
    <section class="eu-block" data-role="mix"></section>
    <section class="eu-block" data-role="routes"></section>
    <div class="eu-pair">
      <section class="eu-block" data-role="link"></section>
      <section class="eu-block" data-role="terminals"></section>
    </div>
    <details class="eu-block eu-daily">
      <summary>Method</summary>
      <div class="data-provenance eu-method__body">
        <p><b>Imports.</b> ENTSOG Transparency "Physical Flow" at the EU-side entry of every import point (non-EU operators do not publish). Where several EU operators report the same meter, identical series are counted once; a route-day appears only when every point that normally reports has done so. Values are energy (GWh/d, gross calorific value).</p>
        <p><b>Routes.</b> Norway: pipelines landing in DE, NL, BE, FR and Baltic Pipe. LNG: regasification send-out at EU terminals. Russia: Nord Stream, TurkStream (Strandzha 2), the Baltic/Finnish borders. The Ukraine border carried Russian transit until the contract expired on ${UKRAINE_TRANSIT_END.toISOString().slice(0, 10)}; flows across it since are not counted as Russian. UK interconnectors are a border, not an origin (re-exported Norwegian, UK and LNG gas).</p>
        <p><b>Not included.</b> EU domestic production (Netherlands, Romania, Italy, Denmark, Germany), so shares are of imports, not of supply. Transit that leaves and re-enters the EU (Serbia to Hungary) is excluded to avoid counting TurkStream twice.</p>
        <p><b>US LNG link.</b> EIA monthly LNG exports to EU-27 destinations (Bcf, converted at ${TWH_PER_BCF.toFixed(4)} TWh/Bcf from ~1,037 Btu/cf) against EU LNG send-out the same month. Cargoes in transit and terminal tank changes make single months noisy; the 12-month ratio is the steadier read.</p>
      </div>
    </details>`;

  const $ = (role) => chartEl.querySelector(`[data-role="${role}"]`);
  renderKpis(sidebarEl, model);
  $('reading').innerHTML = reading(model);

  drawRef.current = () => {
    drawMix($('mix'), series, state, (span) => { state.span = span; drawRef.current(); });
    drawRoutes($('routes'), model);
    drawLink($('link'), model.link);
    drawTerminals($('terminals'), model);
  };
  drawRef.current();
}

function registerSnapshot(series, state) {
  registerChartSnapshot(CHART_ID, 'eu-imports-by-route', series.slice(-800).map((r) => ({
    date: r.period,
    ...Object.fromEntries(ROUTES.map((x) => [`${x.id}_gwh_d_7d`, Number(r[x.id].toFixed(1))])),
    total_gwh_d_7d: Number(r.total.toFixed(1)),
  })), { window: '7-day mean', span: state.span });
}

function renderKpis(sidebarEl, model) {
  const { mix, link } = model;
  const lng = mix.rows.find((r) => r.id === 'lng');
  const lastLink = [...link].reverse().find((r) => isNum(r.share12));
  const yoyPct = isNum(mix.priorTotal) && mix.priorTotal > 0 ? ((mix.total - mix.priorTotal) / mix.priorTotal) * 100 : null;
  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'EU imports · 7-day mean',
      value: `${fx(mix.total, 0)} GWh/d`,
      delta: yoyPct == null ? null : { value: `${sx(yoyPct)}% vs a year ago`, kind: yoyPct >= 0 ? 'bullish' : 'bearish' },
      helpText: `${fx(mix.total * 7 / 1000, 1)} TWh/week · to ${fmtDay(model.asOf)}`,
    }),
    kpiCardHtml({
      label: 'LNG share of imports',
      value: `${fx(lng.share)}%`,
      delta: { value: `${fx(lng.share2021)}% in 2021`, kind: 'neutral' },
      helpText: `${fx(lng.gwh, 0)} GWh/d send-out`,
    }),
    kpiCardHtml({
      label: 'Russian-origin share',
      value: `${fx(mix.russianShare)}%`,
      delta: { value: `${fx(mix.russianShare2021)}% in 2021`, kind: 'neutral' },
      helpText: 'TurkStream, Nord Stream, Baltic borders; Ukraine transit until 2025',
    }),
    kpiCardHtml({
      label: 'US share of EU LNG · 12 months',
      value: lastLink ? `${fx(lastLink.share12, 0)}%` : 'n/a',
      delta: lastLink ? { value: `${fx(lastLink.share, 0)}% in ${lastLink.month}`, kind: 'neutral' } : null,
      helpText: 'EIA exports to EU-27 ÷ ENTSOG LNG send-out',
    }),
  ].join('');
}

function reading(model) {
  const { mix } = model;
  const top = [...mix.rows].sort((a, b) => b.gwh - a.gwh);
  const movers = mix.rows.filter((r) => isNum(r.yoyGwh)).sort((a, b) => Math.abs(b.yoyGwh) - Math.abs(a.yoyGwh)).slice(0, 2);
  const parts = [`Over the 7 days to ${fmtDay(model.asOf)} the EU imported <b>${fx(mix.total, 0)} GWh/d</b>`];
  if (isNum(mix.priorTotal)) parts[0] += `, <b class="${tone(mix.total - mix.priorTotal)}">${sx(mix.total - mix.priorTotal, 0)} GWh/d</b> on the same week last year`;
  parts[0] += `. ${escapeHtml(top[0].label)} supplied ${fx(top[0].share, 0)}% and ${escapeHtml(top[1].label)} ${fx(top[1].share, 0)}%.`;
  if (movers.length) parts.push(`Largest changes on a year ago: ${movers.map((m) => `${escapeHtml(m.label)} ${sx(m.yoyGwh, 0)} GWh/d`).join(', ')}.`);
  parts.push(`Russian-origin gas is ${fx(mix.russianShare)}% of imports, against ${fx(mix.russianShare2021)}% in 2021.`);
  return parts.join(' ');
}

function drawMix(host, series, state, onSpan) {
  const spans = { '1y': 365, '3y': 365 * 3, all: Infinity };
  const last = series[series.length - 1].period;
  const from = Number.isFinite(spans[state.span]) ? new Date(last.getTime() - spans[state.span] * 86400000) : series[0].period;
  const rows = series.filter((r) => r.period >= from);
  const btn = (k, l) => `<button type="button" data-span="${k}" aria-pressed="${state.span === k}">${l}</button>`;
  host.innerHTML = blockHead('Imports by route', '7-day mean of daily entry flows, GWh/d. EU production is not an import and is not shown.',
    `<div class="eu-seg eu-seg--sm" role="group" aria-label="Span">${btn('1y', '1Y')}${btn('3y', '3Y')}${btn('all', 'Since 2021')}</div>`);
  host.querySelector('.eu-seg').addEventListener('click', (e) => {
    const b = e.target.closest('[data-span]');
    if (b && b.dataset.span !== state.span) onSpan(b.dataset.span);
  });
  const chartHost = document.createElement('div');
  chartHost.className = 'eu-chart';
  host.appendChild(chartHost);
  chartHost.insertAdjacentHTML('beforeend', legend(ROUTES.map((r) => ({ kind: 'bar-pos', color: r.color, label: r.label }))));
  chartHost.querySelectorAll('.eu-key--bar-pos').forEach((el, i) => { el.style.background = ROUTES[i].color; });

  const height = chartHost.getBoundingClientRect().width < 560 ? 280 : 340;
  const { g, w, h } = frame(chartHost, { height, margin: { top: 10, right: 16, bottom: 28, left: 56 } });
  const x = d3.scaleTime().domain(d3.extent(rows, (d) => d.period)).range([0, w]);
  const y = d3.scaleLinear().domain([0, d3.max(rows, (d) => d.total) * 1.05]).nice().range([h, 0]);
  yGrid(g, y, w, (t) => d3.format(',')(t));
  for (const t of x.ticks(w < 420 ? 4 : 7)) {
    g.append('text').attr('x', x(t)).attr('y', h + 18).attr('text-anchor', 'middle').attr('class', 'eu-axis').text(d3.timeFormat(state.span === '1y' ? '%b %y' : '%Y')(t));
  }
  const stack = d3.stack().keys(ROUTES.map((r) => r.id))(rows);
  const area = d3.area().x((d) => x(d.data.period)).y0((d) => y(d[0])).y1((d) => y(d[1]));
  stack.forEach((layer, i) => {
    g.append('path').datum(layer).attr('d', area).attr('fill', ROUTES[i].color).attr('fill-opacity', 0.78);
  });
  const tt = tooltip(chartHost);
  const cross = g.append('line').attr('class', 'eu-cross').attr('y1', 0).attr('y2', h).style('opacity', 0);
  const bis = d3.bisector((d) => d.period).center;
  hitArea(g, w, h, ([mx]) => {
    const d = rows[bis(rows, x.invert(mx))];
    if (!d) return;
    cross.attr('x1', x(d.period)).attr('x2', x(d.period)).style('opacity', 1);
    const items = ROUTES.filter((r) => d[r.id] > 0.5).sort((a, b) => d[b.id] - d[a.id]);
    tt.show(`
      <div class="eu-tt__title">${fmtDay(d.period)} · 7-day mean</div>
      ${items.map((r) => `<div class="eu-tt__row"><span><i class="eu-tt__sw" style="background:${r.color}"></i>${escapeHtml(r.label)}</span><span class="num">${fx(d[r.id], 0)} · ${fx((d[r.id] / d.total) * 100, 0)}%</span></div>`).join('')}
      <div class="eu-tt__row eu-tt__row--accent"><span>Total imports</span><span class="num">${fx(d.total, 0)} GWh/d</span></div>
    `, x(d.period) + 56, 10);
  }, () => { tt.hide(); cross.style('opacity', 0); });
}

function drawRoutes(host, model) {
  const rows = [...model.mix.rows].sort((a, b) => b.gwh - a.gwh);
  host.innerHTML = `
    ${blockHead('Routes now, a year ago and in 2021', `7-day means to ${fmtDay(model.asOf)}. "2021 share" is the full-year 2021 average, before the Russian supply cut.`)}
    <div class="eu-table-scroll">
      <table class="eu-table">
        <thead><tr><th scope="col">Route</th><th scope="col" class="num">GWh/d</th><th scope="col" class="num">Year ago</th><th scope="col" class="num">Change</th><th scope="col" class="num">Share</th><th scope="col" class="num">2021 share</th><th scope="col">Share now vs 2021</th></tr></thead>
        <tbody>
          ${rows.map((r) => `
            <tr>
              <th scope="row"><i class="eu-tt__sw" style="background:${r.color};width:10px;height:10px;margin-right:8px;display:inline-block"></i>${escapeHtml(r.label)}</th>
              <td class="num">${fx(r.gwh, 0)}</td>
              <td class="num">${fx(r.priorGwh, 0)}</td>
              <td class="num ${tone(r.yoyGwh)}">${sx(r.yoyGwh, 0)}</td>
              <td class="num">${fx(r.share)}%</td>
              <td class="num">${fx(r.share2021)}%</td>
              <td><span class="eu-bullet" aria-hidden="true" style="width:110px"><span class="eu-bullet__bar" style="width:${Math.min(100, (r.share ?? 0) * 2)}%;background:${r.color}"></span><span class="eu-bullet__tick" style="left:${Math.min(100, (r.share2021 ?? 0) * 2)}%"></span></span></td>
            </tr>`).join('')}
          <tr class="is-ref"><th scope="row">All routes</th><td class="num">${fx(model.mix.total, 0)}</td><td class="num">${fx(model.mix.priorTotal, 0)}</td><td class="num ${tone(model.mix.total - (model.mix.priorTotal ?? model.mix.total))}">${sx(model.mix.total - (model.mix.priorTotal ?? NaN), 0)}</td><td class="num">100%</td><td class="num">100%</td><td class="muted">bar: share now (0–50%) · tick: 2021</td></tr>
        </tbody>
      </table>
    </div>`;
}

function drawLink(host, link) {
  const rows = link.slice(-36);
  host.innerHTML = blockHead('US LNG in Europe', 'Monthly EU LNG send-out (TWh) and the part matched by US LNG exports to EU-27 destinations; line: US share over the trailing 12 months.');
  if (rows.length < 3) return;
  const chartHost = document.createElement('div');
  chartHost.className = 'eu-chart';
  host.appendChild(chartHost);
  chartHost.insertAdjacentHTML('beforeend', legend([
    { kind: 'band', label: 'EU LNG send-out' },
    { kind: 'line', color: '#fbbf24', label: 'US LNG to EU-27' },
    { kind: 'median', label: 'US share, 12 months (right)' },
  ]));
  chartHost.querySelector('.eu-key--line').style.height = '9px';
  const { g, w, h } = frame(chartHost, { height: 260, margin: { top: 10, right: 44, bottom: 28, left: 48 } });
  const x = d3.scaleBand().domain(rows.map((r) => r.month)).range([0, w]).paddingInner(0.2);
  const y = d3.scaleLinear().domain([0, d3.max(rows, (r) => Math.max(r.euTwh, r.usTwh)) * 1.08]).nice().range([h, 0]);
  const y2 = d3.scaleLinear().domain([0, 100]).range([h, 0]);
  yGrid(g, y, w, (t) => `${t}`);
  for (const t of [0, 50, 100]) g.append('text').attr('x', w + 6).attr('y', y2(t)).attr('dy', '0.32em').attr('class', 'eu-axis').text(`${t}%`);
  g.selectAll('rect.eu').data(rows).join('rect').attr('x', (r) => x(r.month)).attr('width', x.bandwidth())
    .attr('y', (r) => y(r.euTwh)).attr('height', (r) => h - y(r.euTwh)).attr('fill', 'rgba(148,163,184,0.28)');
  g.selectAll('rect.us').data(rows).join('rect').attr('x', (r) => x(r.month) + x.bandwidth() * 0.2).attr('width', x.bandwidth() * 0.6)
    .attr('y', (r) => y(r.usTwh)).attr('height', (r) => h - y(r.usTwh)).attr('fill', '#fbbf24').attr('fill-opacity', 0.85);
  const withShare = rows.filter((r) => isNum(r.share12));
  g.append('path').datum(withShare).attr('class', 'eu-median')
    .attr('d', d3.line().x((r) => x(r.month) + x.bandwidth() / 2).y((r) => y2(r.share12)));
  const every = Math.ceil(rows.length / (w < 420 ? 3 : 6));
  rows.forEach((r, i) => {
    if (i % every !== 0) return;
    g.append('text').attr('x', x(r.month) + x.bandwidth() / 2).attr('y', h + 18).attr('text-anchor', 'middle').attr('class', 'eu-axis').text(r.month);
  });
  const tt = tooltip(chartHost);
  hitArea(g, w, h, ([mx]) => {
    const i = Math.max(0, Math.min(rows.length - 1, Math.floor(mx / x.step())));
    const r = rows[i];
    tt.show(`
      <div class="eu-tt__title">${r.month}</div>
      <div class="eu-tt__row"><span>EU LNG send-out</span><span class="num">${fx(r.euTwh)} TWh</span></div>
      <div class="eu-tt__row"><span>US LNG to EU-27</span><span class="num">${fx(r.usTwh)} TWh (${fx(r.usBcf, 0)} Bcf)</span></div>
      <div class="eu-tt__row eu-tt__row--accent"><span>US share, month / 12m</span><span class="num">${fx(r.share, 0)}% / ${r.share12 == null ? '—' : `${fx(r.share12, 0)}%`}</span></div>
    `, x(r.month) + 48, 10);
  }, () => tt.hide());
}

function drawTerminals(host, model) {
  const rows = [...model.byCountry].sort((a, b) => (b.now ?? 0) - (a.now ?? 0));
  host.innerHTML = blockHead('LNG send-out by country', 'Mean of the last 30 days vs the same 30 days a year ago, GWh/d.');
  const chartHost = document.createElement('div');
  chartHost.className = 'eu-chart';
  host.appendChild(chartHost);
  chartHost.insertAdjacentHTML('beforeend', legend([{ kind: 'line', color: '#fbbf24', label: 'Last 30 days' }, { kind: 'median', label: 'Year ago' }]));
  const rowH = 22;
  const { g, w } = frame(chartHost, { height: rows.length * rowH + 28, margin: { top: 4, right: 110, bottom: 24, left: 86 } });
  const x = d3.scaleLinear().domain([0, d3.max(rows, (r) => Math.max(r.now ?? 0, r.prior ?? 0)) * 1.05]).nice().range([0, w]);
  for (const t of x.ticks(w < 320 ? 2 : 4)) {
    g.append('line').attr('x1', x(t)).attr('x2', x(t)).attr('y1', 0).attr('y2', rows.length * rowH).attr('class', 'eu-grid');
    g.append('text').attr('x', x(t)).attr('y', rows.length * rowH + 16).attr('text-anchor', 'middle').attr('class', 'eu-axis').text(d3.format(',')(t));
  }
  rows.forEach((r, i) => {
    const yy = i * rowH;
    g.append('text').attr('x', -8).attr('y', yy + rowH / 2).attr('dy', '0.32em').attr('text-anchor', 'end').attr('class', 'eu-attr__label').text(r.label);
    if (isNum(r.now)) g.append('rect').attr('x', 0).attr('y', yy + 5).attr('height', rowH - 10).attr('width', x(r.now)).attr('fill', '#fbbf24').attr('fill-opacity', 0.8);
    if (isNum(r.prior)) g.append('line').attr('x1', x(r.prior)).attr('x2', x(r.prior)).attr('y1', yy + 2).attr('y2', yy + rowH - 2).attr('stroke', 'var(--text-bright)').attr('stroke-width', 2);
    g.append('text').attr('x', w + 8).attr('y', yy + rowH / 2).attr('dy', '0.32em').attr('class', 'eu-attr__value')
      .text(`${fx(r.now, 0)}${isNum(r.prior) && isNum(r.now) ? ` (${sx(r.now - r.prior, 0)})` : ''}`);
  });
}
