/**
 * Balance Panel — US Natural Gas Supply & Demand.
 *
 * Data schema (bundle.sources.eia_supply.data), all US48, unit MMCF per month:
 *   supply_fpd  Dry production
 *   supply_ovi  Input supplemental fuels
 *   supply_saw  Underground storage withdrawals
 *   supply_sai  Underground storage injections
 *   supply_vc0  Total consumption
 *   supply_vg4  Balancing item (EIA's own reconciliation line — may publish a
 *               month later than the others)
 * Periods are `YYYY-MM` strings; values are converted to Bcf and Bcf/d here.
 *
 * The identity EIA publishes (Natural Gas Monthly, Table 1, "supply and
 * disposition") is
 *
 *   consumption = dry production + supplemental gaseous fuels + net imports
 *                 + net storage withdrawals + balancing item
 *
 * Net storage withdrawals (withdrawals − injections) is a *supply* leg: gas
 * taken out of storage is delivered this month, gas injected is not consumed.
 * The balancing item is EIA's reconciliation line, also on the supply side.
 *
 * This API route carries every leg except net imports (`natural-gas/move/expc`
 * · `impc`). Solving the identity for it gives
 *
 *   implied net exports = production + supplemental + net withdrawals
 *                         + balancing item − consumption
 *
 * which the panel prints as an inference and checks against the LNG export
 * total. A previous version put storage and the balancing item on the wrong
 * side (`− net withdrawals`, `− balancing item`): its "imbalance" swung between
 * −55 and +50 Bcf/d with the storage season, implied the US *imported* 33 Bcf/d
 * in summer, and the panel then doubted EIA's sign convention. With EIA's signs
 * the same data give 11–21 Bcf/d of net exports, rising with LNG capacity.
 * See `tests/balance_identity.test.mjs`.
 */

import * as d3 from 'd3';
import { renderPanelChrome, appendSection } from '../components/panel-base.js';
import { kpiCardHtml } from '../components/kpi-card.js';
import { getSeries, referenceDate } from '../util/series.js';
import { formatPeriodKey, parsePeriod } from '../util/period.js';
import { LNG_TERMINALS } from '../util/lng-terminals.js';

/** `YYYY-MM` key of a period, or null when it cannot be read. */
function monthKey(period) {
  const key = formatPeriodKey(parsePeriod(period));
  return key ? key.slice(0, 7) : null;
}

/** Days per month for a period date — the divisor for monthly → daily. */
export function daysInMonth(period) {
  return new Date(period.getFullYear(), period.getMonth() + 1, 0).getDate();
}

/**
 * The legs of the identity this bundle can supply, in EIA's published order.
 *
 * Sign convention: `+` is supply, `−` is disposition. Net storage withdrawals
 * and the balancing item are supply legs, so both enter with `+` and their own
 * published sign (a net-injection month has negative `swd`).
 */
const KNOWN_LEGS = [
  { key: 'fpd', sign: 1 },
  { key: 'ovi', sign: 1 },
  { key: 'swd', sign: 1 },
  { key: 'vg4', sign: 1 },
  { key: 'vc0', sign: -1 },
];

/**
 * The one Table 1 leg this bundle does not carry: net imports.
 *
 * Net storage withdrawals is scraped (`supply_saw` − `supply_sai`) and is in
 * the identity above; listing it here as missing described the target state,
 * not this panel.
 */
const MISSING_LEGS = [
  {
    key: 'netImports',
    label: 'Net imports',
    route: 'natural-gas/move/expc · move/impc',
    shownBy: 'the LNG panel carries the export leg only, not pipeline trade',
  },
];

/**
 * One month's balance in Bcf (monthly totals) and Bcf/d (per-day rates).
 *
 * `imbalanceBcf` is supply on the legs this route carries minus consumption,
 * i.e. what net imports would have to be, with the opposite sign, to close the
 * identity: positive = the US exported that much more than it imported.
 *
 * `null` means a leg is unpublished, not that the month balances.
 */
export function monthBalance(row, days) {
  const { fpd, ovi, saw, sai, vc0, vg4 } = row;
  const daysN = days ?? 30;
  const perDay = (v) => (v == null ? null : v / daysN);

  // Net storage withdrawals is the identity's single storage leg. Substituting
  // gross withdrawals and gross injections as two legs double-counts nothing
  // but it does charge the monthly gross throughput instead of the net change.
  const swd = saw != null && sai != null ? saw - sai : null;

  const known = { fpd, ovi, swd, vc0, vg4 };
  const complete = KNOWN_LEGS.every(({ key }) => known[key] != null);
  const imbalanceBcf = complete
    ? KNOWN_LEGS.reduce((sum, { key, sign }) => sum + sign * known[key], 0)
    : null;
  // The chart's two sides: everything available to the domestic market before
  // trade (production, supplemental, net withdrawals, balancing item) against
  // consumption. The gap between them is the implied net export.
  const suppliedBcf = complete ? fpd + ovi + swd + vg4 : null;
  const drainedBcf = vc0 != null ? vc0 : null;

  return {
    netBcf: imbalanceBcf,
    residualBcf: imbalanceBcf,
    imbalanceBcf,
    // The identity solved for the one leg this route lacks. An inference from
    // the published legs, not a measurement, and labelled so where printed.
    impliedNetImportsBcf: imbalanceBcf == null ? null : -imbalanceBcf,
    impliedNetExportsBcf: imbalanceBcf,
    storageChangeBcf: swd,
    // The figure this panel used to print, kept so the correction to the
    // identity is visible rather than an unremarked change of definition.
    naiveFlowBcf: fpd != null && vc0 != null ? fpd - vc0 : null,
    fpd, ovi, saw, sai, swd, vc0, vg4, days: daysN,
    suppliedBcf,
    drainedBcf,
    netBcfd: perDay(imbalanceBcf),
    imbalanceBcfd: perDay(imbalanceBcf),
    fpdBcfd: perDay(fpd),
    vc0Bcfd: perDay(vc0),
    sawBcfd: perDay(saw),
    saiBcfd: perDay(sai),
    swdBcfd: perDay(swd),
    oviBcfd: perDay(ovi),
    vg4Bcfd: perDay(vg4),
  };
}

/**
 * Build the monthly balance history, oldest first.
 *
 * Months are keyed on the parsed calendar month so a mixed label format cannot
 * split one month into two rows, and a month is dropped when either production
 * or consumption is missing — a half-populated month would otherwise draw a
 * bridge leg of zero and read as a collapse in demand.
 */
export function buildBalanceModel(rows) {
  const byId = (id) => {
    const map = new Map();
    for (const r of getSeries(rows || [], id)) {
      const key = monthKey(r.period);
      const v = Number(r.value);
      if (key && Number.isFinite(v)) map.set(key, v / 1000);
    }
    return map;
  };

  const s = {
    fpd: byId('supply_fpd'),
    ovi: byId('supply_ovi'),
    saw: byId('supply_saw'),
    sai: byId('supply_sai'),
    vc0: byId('supply_vc0'),
    vg4: byId('supply_vg4'),
  };

  const months = [...new Set([...s.fpd.keys(), ...s.vc0.keys()])].sort();
  const series = [];
  for (const key of months) {
    const period = parsePeriod(key);
    if (!period || !s.fpd.has(key) || !s.vc0.has(key)) continue;
    series.push({
      period,
      ...monthBalance({
        fpd: s.fpd.get(key) ?? null,
        ovi: s.ovi.get(key) ?? null,
        saw: s.saw.get(key) ?? null,
        sai: s.sai.get(key) ?? null,
        vc0: s.vc0.get(key) ?? null,
        vg4: s.vg4.get(key) ?? null,
      }, daysInMonth(period)),
    });
  }

  const withResidual = series.filter((m) => m.imbalanceBcf != null);
  return {
    series,
    latest: series[series.length - 1] ?? null,
    closingMonths: withResidual.length,
    maxAbsResidualBcf: withResidual.length
      ? d3.max(withResidual, (m) => Math.abs(m.imbalanceBcf))
      : null,
    medianAbsResidualBcf: withResidual.length
      ? d3.median(withResidual, (m) => Math.abs(m.imbalanceBcf))
      : null,
    minImpliedBcfd: withResidual.length ? Math.min(...withResidual.map((m) => m.imbalanceBcfd)) : null,
    maxImpliedBcfd: withResidual.length ? Math.max(...withResidual.map((m) => m.imbalanceBcfd)) : null,
  };
}

/**
 * LNG exports for a month, in Bcf, from the `eia_lng_exports` rows.
 *
 * `lng_export_total` periods are `YYYY-MM-01` while the supply route writes
 * `YYYY-MM`, so the join is on the calendar month, not the raw string.
 */
export function buildLngByMonth(rows) {
  const map = new Map();
  for (const r of getSeries(rows || [], 'lng_export_total')) {
    const key = monthKey(r.period);
    const v = Number(r.value);
    if (key && Number.isFinite(v)) map.set(key, v);
  }
  return map;
}

/**
 * Daily feedgas from the certified LNG terminal registry, in Bcf for a month.
 *
 * The registry's feedgas meters keep reporting for days the monthly EIA bundle
 * has not caught up with, and they are the pipeline-side view of the same
 * molecules the LNG panel charts. They are a cross-check on net imports, never
 * a substitute: they cover the measured `sq` terminals and omit pipeline exports to Canada and
 * Mexico and pipeline imports, so they are labelled as a floor.
 *
 * Only `sq` terminals are summed. An `oac-proxy` terminal (Cheniere) does not
 * publish a scheduled quantity and is an inferred capacity residual, and a
 * `kind` of `comparison` re-measures gas another meter already counts.
 *
 * @param {object} sources  `bundle.sources`
 * @returns {Map<string, {bcf:number, days:number, terminals:number}>} by `YYYY-MM`
 */
// Cycle tokens are platform-specific and not always `id1`/`id2`/`id3`: Enbridge
// stamps intraday re-posts with the hour (`tetco_sq_79999_d_id0100` … `_id2300`),
// and `_late` / `_latec` / `_best` are re-posts of the same day, not new gas.
const CYCLE_SUFFIX = /_(timely|evening|id\d+|best|late[a-z]*)$/i;

/**
 * True when a series id is the feed's stem, or that stem plus a cycle token.
 *
 * @param {string} seriesId
 * @param {string} stem
 */
export function isCycleRow(seriesId, stem) {
  if (!seriesId || !stem) return false;
  if (seriesId === stem) return true;
  if (!seriesId.startsWith(stem)) return false;
  return CYCLE_SUFFIX.test(seriesId.slice(stem.length));
}

export function buildFeedgasByMonth(sources) {
  const byMonth = new Map();
  for (const t of Object.values(LNG_TERMINALS || {})) {
    if (t.operational === false || t.signal !== 'sq') continue;
    // Single-source terminals carry a `seriesPrefix` — a *prefix*, spanning one
    // pipeline's whole family of meters (Gulf South's loc 24329 posts only as
    // `gulf_south_sq_24329_d_id{1,2,3}`, so an exact series would match nothing).
    const feeds = (t.feeds?.length ? t.feeds : [{ source: t.source, seriesPrefix: t.seriesPrefix }])
      .filter((f) => (f.kind ?? 'measured') === 'measured' && (f.series || f.seriesPrefix));
    for (const feed of feeds) {
      const src = sources?.[feed.source];
      if (!src?.data) continue;
      // `feed.series` is the *stem* of a series id: every cycle posts as
      // `<stem>_<cycle>` (`_id1`, `_timely`, `_evening`, `_best`, …). Match the
      // stem with or without a cycle suffix rather than a bare prefix, or a feed
      // named `cpl_sq_10001_d` would also swallow `cpl_sq_10001_d_id2` and any
      // neighbouring series whose id merely starts the same way.
      const rows = src.data.filter((r) => (feed.series
        ? isCycleRow(r.series_id, feed.series)
        : String(r.series_id).startsWith(feed.seriesPrefix)));
      // One daily value per day per meter; a meter that posts several cycles is
      // reduced to its latest cycle rather than summed into a multiple of itself.
      const perDay = new Map();
      for (const r of rows) {
        const day = formatPeriodKey(parsePeriod(r.period));
        const v = Number(r.value);
        if (!day || !Number.isFinite(v)) continue;
        perDay.set(day, v);
      }
      for (const [day, mmcfd] of perDay) {
        if (!(mmcfd > 0)) continue;
        const key = day.slice(0, 7);
        const acc = byMonth.get(key) ?? { bcf: 0, days: 0, terminals: new Set() };
        acc.bcf += mmcfd / 1000; // MMcf/d × one day = MMcf → Bcf
        acc.days += 1;
        acc.terminals.add(t.id);
        byMonth.set(key, acc);
      }
    }
  }
  const out = new Map();
  for (const [key, acc] of byMonth) {
    out.set(key, { bcf: acc.bcf, days: acc.days, terminals: acc.terminals.size });
  }
  return out;
}

/* ─────────────────────────────────────────────── */
/*  Panel                                          */
/* ─────────────────────────────────────────────── */

export function renderBalancePanel(panelEl, bundle) {
  const source = bundle.sources.eia_supply;
  if (!source || !source.data || source.data.length === 0) {
    panelEl.innerHTML = '<div class="panel-error">EIA Supply data unavailable.</div>';
    return;
  }

  const model = buildBalanceModel(source.data);
  const lngByMonth = buildLngByMonth(bundle.sources.eia_lng_exports?.data);
  const feedgasByMonth = buildFeedgasByMonth(bundle.sources);
  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Supply & Demand Balance',
    subtitle: 'EIA · monthly US48 Table 1 identity · net trade solved from the other five legs',
    sourceKey: 'eia_supply',
    latestPeriod: source.latest_period,
    draw: () => drawRef.current?.(),
  });

  drawRef.current = () => drawBalanceChart(chartEl, model);
  drawRef.current();
  renderBalanceKpis(sidebarEl, model, lngByMonth);
  appendBridge(panelEl, model, lngByMonth, feedgasByMonth);
}

/* ─────────────────────────────────────────────── */
/*  Bridge                                         */
/* ─────────────────────────────────────────────── */

/**
 * The latest complete month, leg by leg, in Bcf/d with the Bcf total beside it.
 *
 * Bcf/d because that is the unit the legs are quoted in and it makes months of
 * different lengths comparable; the Bcf total sits next to it so the arithmetic
 * can be checked without a mental conversion.
 */
function appendBridge(panelEl, model, lngByMonth, feedgasByMonth = new Map()) {
  const panelBody = panelEl.querySelector('.panel-body');
  const latest = model.latest;
  if (!panelBody || !latest) return;

  const sidebar = panelBody.querySelector('.panel-sidebar');
  const wrap = document.createElement('div');
  wrap.className = 'panel-subchart';

  const head = (hero, badge, subtitle) => `
    <div class="panel-subchart__header">
      <div class="panel-subchart__title-group">
        <span class="panel-subchart__title">Balance bridge · ${monthLabel(latest.period)}</span>
        <span class="panel-subchart__hero num">${hero}</span>
        <span class="panel-subchart__badge ${badge}"></span>
      </div>
      <span class="panel-subchart__subtitle">${subtitle}</span>
    </div>`;

  if (latest.imbalanceBcf == null) {
    const missing = KNOWN_LEGS.filter(({ key }) => latest[key] == null).map(({ key }) => LEG_LABELS[key]);
    wrap.innerHTML = `
      ${head('—', '', `${latest.days}-day month`)}
      <div class="panel-subchart__body panel-subchart__body--prose">
        <span class="panel-table__note">${monthLabel(latest.period)} is missing
        ${missing.join(', ') || 'a leg'}, so the identity is not printed for it rather than
        assumed to close. The chart ends at the newest month where production and
        consumption are both published.</span>
      </div>`;
    appendSection(panelBody, 'overview-bridge', wrap, sidebar || null);
    return;
  }

  // Via monthKeyOf, not a local padStart key: buildLngByMonth reduces to the
  // same calendar month, and the two must not drift apart.
  const monthKeyStr = monthKeyOf(latest.period);
  const lngBcf = lngByMonth?.get(monthKeyStr) ?? null;

  const legRows = KNOWN_LEGS.map(({ key, sign }) => {
    const v = latest[key];
    const signed = v == null ? null : sign * v;
    const signedD = signed == null ? null : signed / latest.days;
    const label = key === 'swd'
      ? `${LEG_LABELS.swd}<span class="bridge-table__hint">withdrawals − injections · ${formatNumber(latest.saw, 0)} − ${formatNumber(latest.sai, 0)}</span>`
      : LEG_LABELS[key];
    return `
      <tr>
        <td class="lbl">${label}</td>
        <td class="num ${signClass(signedD)}">${signedD == null ? 'not published' : signedBcfd(signedD)}</td>
        <td class="num muted">${signed == null ? '—' : signedBcfd(signed, 0)}</td>
      </tr>`;
  }).join('');

  const missingRows = MISSING_LEGS.map((leg) => `
      <tr class="bridge-table__missing">
        <td class="lbl">${leg.label}<span class="bridge-table__hint">not in this route · ${leg.shownBy}</span></td>
        <td class="num muted" colspan="2">not scraped</td>
      </tr>`).join('');

  const residualD = latest.imbalanceBcf;
  const residualPerDay = residualD / latest.days;
  const lngPerDay = lngBcf == null ? null : lngBcf / latest.days;
  const pipelinePerDay = lngPerDay == null ? null : residualPerDay - lngPerDay;
  const plausible = residualPerDay > 0 && residualPerDay < (latest.vc0Bcfd ?? Infinity) * 0.5;

  const impliedNote = `<br>Solving the identity for the missing leg gives <strong>${signedBcfd(residualPerDay, 1)} Bcf/d of
       net exports</strong> (${signedBcfd(residualD, 0)} Bcf). That is an inference from the published legs, not a
       measurement: EIA's own trade series is not in this bundle.${lngPerDay == null ? '' : ` LNG exports were
       ${formatNumber(lngPerDay, 1)} Bcf/d that month, which leaves ${signedBcfd(pipelinePerDay, 1)} Bcf/d for net
       pipeline trade (exports to Mexico less imports from Canada) plus the measurement noise of five legs.`}`;
  const impliedCaveat = plausible
    ? ''
    : `<br><strong>Check:</strong> the implied figure is outside the range US net trade can take
       (positive and well under half of consumption); one of the published legs may be provisional.`;

  const badge = `<span class="panel-subchart__badge--bullish">Implied net exports ${signedBcfd(residualPerDay, 1)} Bcf/d</span>`;

  // Sum of the printed rows must be the printed imbalance: the bridge is
  // checked against its own arithmetic by the test suite.
  const legSumBcf = KNOWN_LEGS.reduce((sum, { key, sign }) => sum + sign * (latest[key] ?? 0), 0);

  // Feedgas is the newest data the bundle holds that touches this identity, so
  // it is shown beside the monthly legs even though it cannot close them.
  const feedgasMonths = [...feedgasByMonth.keys()].sort();
  const feedgasKey = feedgasMonths[feedgasMonths.length - 1];
  const feedgas = feedgasKey ? feedgasByMonth.get(feedgasKey) : null;
  const feedgasNote = !feedgas
    ? ''
    : `<br>The terminal meters are the most recent view: ${formatNumber(feedgas.bcf, 0)} Bcf of feedgas
       across ${feedgas.terminals} measured terminals in ${monthLabel(parsePeriod(feedgasKey))}
       (${formatNumber(feedgas.bcf / latest.days, 1)} Bcf/d monthly equivalent). It runs ahead of EIA's
       monthly series and is a floor on LNG exports, not on net trade: pipeline trade with Canada and
       Mexico is not in it.`;

  wrap.innerHTML = `
    ${head(
      `${signedBcfd(residualPerDay)} Bcf/d`,
      '',
      `${latest.days}-day month · Table 1 legs; net trade solved from the rest`,
    )}
    <div class="panel-subchart__body panel-subchart__body--auto">
      <table class="bridge-table">
        <thead><tr><th>Leg (Table 1 order)</th><th>Bcf/d</th><th>Bcf</th></tr></thead>
        <tbody>
          ${legRows}
          ${missingRows}
          <tr class="bridge-table__residual">
            <td class="lbl">Implied net exports = supply legs − consumption</td>
            <td class="num ${signClass(residualPerDay)}">${signedBcfd(residualPerDay, 2)}</td>
            <td class="num muted">${signedBcfd(residualD, 1)}</td>
          </tr>
        </tbody>
      </table>
      <span class="panel-table__note">
        Identity (EIA Natural Gas Monthly, Table 1): consumption = dry production + supplemental fuels
        + net imports + net storage withdrawals + balancing item. This route carries every leg except
        <strong>net imports</strong>; the rows above sum to ${signedBcfd(legSumBcf, 1)} Bcf, which is
        net imports with the opposite sign.
        Across ${model.closingMonths} months the implied net exports run
        ${formatNumber(model.minImpliedBcfd, 1)} to ${formatNumber(model.maxImpliedBcfd, 1)} Bcf/d.
        ${impliedNote}
        ${impliedCaveat}
        ${feedgasNote}
        See <code>tests/balance_identity.test.mjs</code>.
      </span>
    </div>`;

  if (badge) {
    wrap.querySelector('.panel-subchart__header .panel-subchart__title-group')
      .insertAdjacentHTML('beforeend', badge);
  }
  appendSection(panelBody, 'overview-bridge', wrap, sidebar || null);
}

const LEG_LABELS = {
  fpd: 'Dry production',
  ovi: 'Supplemental fuels',
  swd: 'Net storage withdrawals',
  saw: 'Withdrawals',
  sai: 'Injections',
  vc0: 'Consumption',
  vg4: 'Balancing item',
};

/* ─────────────────────────────────────────────── */
/*  Chart                                          */
/* ─────────────────────────────────────────────── */

const CHART_HEIGHT = 360;

function drawBalanceChart(container, model) {
  container.innerHTML = '';
  const width = Math.max(container.getBoundingClientRect().width, 300) - 56 - 16;
  const height = CHART_HEIGHT - 28 - 36;

  // Three years back from the newest observation, not from the wall clock, so
  // a bundle that runs a week stale keeps its history instead of sliding empty.
  const anchor = referenceDate(model.series.map((m) => ({ period: m.period })));
  let rows = model.series;
  if (anchor) {
    const cutoff = new Date(anchor);
    cutoff.setFullYear(cutoff.getFullYear() - 3);
    rows = rows.filter((m) => m.period >= cutoff);
  }
  rows = rows.filter((m) => m.suppliedBcf != null && m.drainedBcf != null);

  if (rows.length === 0) {
    container.innerHTML =
      '<div class="panel-error">No month in the last 3 years has the full set of balance legs.</div>';
    return;
  }

  const margin = { top: 28, right: 16, bottom: 36, left: 56 };
  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${CHART_HEIGHT}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block')
    .style('width', '100%');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleTime().domain(d3.extent(rows, (m) => m.period)).range([0, width]);
  const yMax = d3.max(rows, (m) => Math.max(m.suppliedBcf, m.drainedBcf)) * 1.06;
  const y = d3.scaleLinear().domain([0, yMax]).range([height, 0]);
  const yTicks = y.ticks(6);

  g.append('g')
    .selectAll('line')
    .data(yTicks)
    .join('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'var(--chart-grid)');

  // The band between the two sides of the identity is the implied net export:
  // supply available to the domestic market minus what it consumed.
  const area = d3.area()
    .x((m) => x(m.period))
    .y0((m) => y(m.drainedBcf))
    .y1((m) => y(m.suppliedBcf))
    .curve(d3.curveMonotoneX);
  // `--chart-band` is not a defined token, so this fill used to render black.
  g.append('path').datum(rows).attr('d', area).attr('fill', 'var(--color-accent-bg)');

  const line = (key, color) => g.append('path')
    .datum(rows)
    .attr('d', d3.line().x((m) => x(m.period)).y((m) => y(m[key])).curve(d3.curveMonotoneX))
    .attr('fill', 'none')
    .attr('stroke', color)
    .attr('stroke-width', 2)
    .attr('stroke-linejoin', 'round');

  line('suppliedBcf', 'var(--blue-flame)');
  line('drainedBcf', 'var(--color-warning)');

  g.append('g')
    .selectAll('text')
    .data(yTicks)
    .join('text')
    .attr('x', -8)
    .attr('y', (d) => y(d) + 4)
    .attr('text-anchor', 'end')
    .attr('fill', 'var(--chart-label)')
    .attr('font-size', 10)
    .attr('font-family', 'var(--font-mono)')
    .text((d) => {
      if (d === 0) return '0';
      if (d >= 1000) {
        const k = d / 1000;
        return Number.isInteger(k) ? `${k}k` : `${k.toFixed(1)}k`;
      }
      return `${Math.round(d)}`;
    });

  g.append('g')
    .attr('transform', `translate(0,${height})`)
    .call(d3.axisBottom(x).ticks(d3.timeYear.every(1)).tickFormat(d3.timeFormat('%Y')))
    .call((sel) => {
      sel.selectAll('line, path').attr('stroke', 'var(--chart-axis)');
      sel.selectAll('text')
        .attr('fill', 'var(--chart-label)')
        .attr('font-size', 10)
        .attr('font-family', 'var(--font-mono)');
    });

  // Legend: the two lines are the two sides of the identity, and the fill
  // between them is the imbalance — labelled here because an unlabelled band
  // reads as decoration.
  const legend = svg.append('g').attr('transform', `translate(${margin.left}, 10)`);
  let xOff = 0;
  for (const item of [
    { label: 'Supply incl. net storage withdrawals', color: 'var(--blue-flame)' },
    { label: 'Consumption', color: 'var(--color-warning)' },
    { label: 'Gap = implied net exports', color: 'var(--color-accent-border)' },
  ]) {
    const grp = legend.append('g').attr('transform', `translate(${xOff}, 0)`);
    grp.append('line')
      .attr('class', 'chart-legend-swatch')
      .attr('x1', 0).attr('x2', 14).attr('y1', 5).attr('y2', 5)
      .attr('stroke', item.color)
      .attr('stroke-width', 2);
    const text = grp.append('text')
      .attr('class', 'chart-legend-text')
      .attr('x', 20).attr('y', 9)
      .text(item.label);
    xOff += text.node().getComputedTextLength() + 46;
  }

  setupBalanceHover(svg, g, x, y, width, height, rows, margin);
}

function setupBalanceHover(svg, g, x, y, width, height, rows, margin) {
  const host = d3.select(svg.node().parentNode);
  const tooltip = host.append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  const crosshair = g.append('line')
    .attr('class', 'chart-crosshair')
    .attr('y1', 0).attr('y2', height)
    .style('opacity', 0);

  g.append('rect')
    .attr('width', width)
    .attr('height', height)
    .attr('fill', 'transparent')
    .on('mousemove', (event) => {
      const [mx] = d3.pointer(event);
      const at = x.invert(mx);
      // Nearest month by date, so a gap cannot snap across a year.
      const m = rows.reduce((best, r) =>
        Math.abs(r.period - at) < Math.abs(best.period - at) ? r : best);
      const xPos = x(m.period);
      crosshair.attr('x1', xPos).attr('x2', xPos).style('opacity', 1);

      const svgRect = svg.node().getBoundingClientRect();
      const scale = svgRect.width / (width + margin.left + margin.right);
      const left = Math.min(
        margin.left * scale + xPos * scale + 16,
        host.node().getBoundingClientRect().width - 220,
      );

      const row = (label, v) => (v == null ? ''
        : `<div class="tt-row"><span class="tt-label">${label}</span><span class="num">${signedBcfd(v)} Bcf/d</span></div>`);

      tooltip
        .style('opacity', 1)
        .style('left', `${left}px`)
        .style('top', '36px')
        .html(`
          <div class="tt-date">${monthLabel(m.period)} · ${m.days} days</div>
          ${row('Supply incl. storage', m.suppliedBcf / m.days)}
          ${row('Consumption', m.drainedBcf / m.days)}
          ${row('Dry production', m.fpdBcfd)}
          ${row('Net storage withdrawals', m.swdBcfd)}
          <div class="tt-row tt-row--accent">
            <span class="tt-label">Implied net exports</span>
            <span class="num">${signedBcfd(m.imbalanceBcfd)} Bcf/d</span>
          </div>
          <div class="tt-row">
            <span class="tt-label">Balancing item</span>
            <span class="num">${m.vg4Bcfd == null ? 'not published' : `${signedBcfd(m.vg4Bcfd)} Bcf/d`}</span>
          </div>`);
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0);
      tooltip.style('opacity', 0);
    });
}

/* ─────────────────────────────────────────────── */
/*  KPIs                                           */
/* ─────────────────────────────────────────────── */

function renderBalanceKpis(sidebarEl, model, lngByMonth) {
  const latest = model.latest;
  if (!latest) return;
  const prevYear = model.series.find(
    (m) => m.period.getFullYear() === latest.period.getFullYear() - 1
      && m.period.getMonth() === latest.period.getMonth(),
  );

  const yoyPct = (key) => {
    if (!prevYear || prevYear[key] == null || latest[key] == null || prevYear[key] === 0) return null;
    return ((latest[key] - prevYear[key]) / prevYear[key]) * 100;
  };
  const yoyLabel = (pct) => (pct == null
    ? null
    : { value: `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}% YoY`, kind: 'neutral' });

  sidebarEl.innerHTML = [
    kpiCardHtml({
      label: 'Supply',
      value: `${formatNumber(latest.fpdBcfd)} Bcf/d`,
      delta: yoyLabel(yoyPct('fpdBcfd')),
      helpText: `Dry production · ${formatNumber(latest.fpd, 0)} Bcf in ${monthLabel(latest.period)}`,
    }),
    kpiCardHtml({
      label: 'Demand',
      value: `${formatNumber(latest.vc0Bcfd)} Bcf/d`,
      delta: yoyLabel(yoyPct('vc0Bcfd')),
      helpText: `Total consumption · ${formatNumber(latest.vc0, 0)} Bcf in ${monthLabel(latest.period)}`,
    }),
    kpiCardHtml({
      label: 'Net to storage',
      value: latest.swd == null ? '—' : `${signedBcfd(latest.swdBcfd)} Bcf/d`,
      helpText: latest.swd == null
        ? 'Withdrawals or injections not published for this month'
        : `Withdrawals − injections · ${signedBcfd(latest.storageChangeBcf, 0)} Bcf
         (${latest.storageChangeBcf >= 0 ? 'net withdrawal' : 'net injection'}) in ${monthLabel(latest.period)}`,
    }),
    kpiCardHtml({
      label: 'Implied net exports',
      value: latest.imbalanceBcfd == null ? '—' : `${signedBcfd(latest.imbalanceBcfd)} Bcf/d`,
      helpText: latest.imbalanceBcfd == null
        ? 'A leg is missing for this month, so net trade cannot be solved'
        : `Table 1 solved for net trade${lngByMonth?.has(monthKeyOf(latest.period))
          ? ` · LNG exports ${formatNumber(lngByMonth.get(monthKeyOf(latest.period)) / latest.days, 1)} Bcf/d`
          : ''} · inferred, not measured`,
    }),
  ].join('');
}

/** `YYYY-MM` for a month date — the key both supply and LNG rows reduce to. */
export function monthKeyOf(period) {
  const key = formatPeriodKey(period);
  return key ? key.slice(0, 7) : null;
}

/* ─────────────────────────────────────────────── */
/*  Formatting                                     */
/* ─────────────────────────────────────────────── */

function monthLabel(date) {
  return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
}

function formatNumber(v, digits = 1) {
  if (v == null || !Number.isFinite(v)) return '—';
  return Math.abs(v).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function signedBcfd(v, digits = 1) {
  if (v == null || !Number.isFinite(v)) return '—';
  const mag = Math.abs(v).toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
  return `${v > 0 ? '+' : v < 0 ? '−' : ''}${mag}`;
}

function signClass(v) {
  if (v == null || !Number.isFinite(v)) return 'muted';
  return v > 0 ? 'delta-pos' : v < 0 ? 'delta-neg' : 'muted';
}
