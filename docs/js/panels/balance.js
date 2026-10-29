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
* The identity EIA publishes (Natural Gas Monthly, Table 1) is
 *
 *   dry production + supplemental gaseous fuels + net imports
 *       − net storage withdrawals = consumption + balancing item
 *
 * `supply_vg4` is EIA's own reconciliation line: "the difference between the
 * sum of the components of natural gas supply and the sum of the components of
 * natural gas disposition". It is *not* an import term and *not* demand — the
 * two legs it is commonly mistaken for have opposite signs, and net imports in
 * fact runs roughly eleven times larger than it.
 *
 * This API route carries four of the six legs. There is no net-imports leg
 * (`natural-gas/move/expc` · `impc`), so the identity cannot be closed here and
 * the panel prints what it can balance, states the imbalance, and names the two
 * missing legs instead of folding them into a residual labelled "unexplained".
 * See `tests/balance_identity.test.mjs`.
 */

import * as d3 from 'd3';
import { renderPanelChrome } from '../components/panel-base.js';
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
 * Sign convention: `+` adds to supply, `−` adds to disposition. Net storage
 * withdrawals sit on the **supply** side of Table 1 — gas leaving storage is
 * delivered this month — so its term is `− swd`: a withdrawal month reduces the
 * imbalance exactly as a production month raises it, and a net injection
 * (negative `swd`) adds to it.
 */
const KNOWN_LEGS = [
  { key: 'fpd', sign: 1 },
  { key: 'ovi', sign: 1 },
  { key: 'swd', sign: -1 },
  { key: 'vc0', sign: -1 },
  { key: 'vg4', sign: -1 },
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
 * The identity is *not* closed here: `imbalanceBcf` is EIA's published function
 * of the terms this bundle has, and the residual it leaves is attributed — in
 * the panel — to the legs that are missing, never to a fiction that this route
 * is a closed system.
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
  // Known supply is production + supplemental fuels; the storage term is the
  // third supply leg and belongs in the imbalance, not in this line, or a
  // withdrawal month and a rejection month draw at the same height.
  const suppliedBcf = fpd != null && ovi != null ? fpd + ovi : null;
  const drainedBcf = vc0 != null ? vc0 : null;

  return {
    netBcf: imbalanceBcf,
    residualBcf: imbalanceBcf,
    imbalanceBcf,
    // What the missing leg is worth, by subtraction: the identity solved for
    // net imports. It is an inference from one published month, not a
    // measurement, and it is labelled that way wherever it is printed.
    impliedNetImportsBcf: imbalanceBcf == null || vg4 == null ? null : imbalanceBcf - vg4,
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
    subtitle: 'EIA · monthly US48 Table 1 identity — four of six legs in this route, imbalance named',
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
      <div class="panel-subchart__body panel-subchart__body--auto">
        <span class="panel-table__note">${monthLabel(latest.period)} is missing
        ${missing.join(', ') || 'a leg'}, so the identity is not printed for it rather than
        assumed to close. The chart ends at the newest month where production and
        consumption are both published.</span>
      </div>`;
    panelBody.insertBefore(wrap, sidebar);
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
  const impliedNetImports = latest.impliedNetImportsBcf;
  const impliedNote = impliedNetImports == null
    ? ''
    : `<br>What that missing leg is worth, by subtraction: solving the identity for net imports gives
       ${signedBcfd(impliedNetImports, 0)} Bcf (${signedBcfd(impliedNetImports / latest.days, 1)} Bcf/d).
       That is an inference from the published legs, not a measurement — EIA's real net imports are not
       in this bundle — but it would have to be physically implausible for the imbalance to mean anything
       else.`;

  // Implying the missing import leg is only defensible when it lands somewhere
  // real net imports can land; a seasonal swing of this size is not one of them.
  const impliedShare = latest.vc0 == null || impliedNetImports == null
    ? null
    : impliedNetImports / latest.vc0;
  const impliedSuspicious = impliedShare != null && (impliedShare < -0.15 || impliedShare > 0.15);
  const impliedCaveat = impliedSuspicious
    ? `<br><strong>The implied leg is ${Math.round(impliedShare * 100)}% of total consumption</strong>,
       which is larger than US net imports can be in any month — most of the annual range is storage.
       Read the complement instead: if EIA's Table 1 counts storage as one net line, this series draws net
       <em>withdrawal</em> in every injection season, so the storage legs here may carry a disposition sign
       that the panel has not yet verified against the published table. Treat the signed imbalance as
       unreliable until that is checked; the magnitudes are unaffected.`
    : '';

  const badge = `<span class="panel-subchart__badge--bearish">Imbalance ${signedBcfd(residualPerDay, 1)} Bcf/d</span>`;

  // Sum of the printed rows must be the printed imbalance: the bridge is
  // checked against its own arithmetic by the test suite.
  const legSumBcf = KNOWN_LEGS.reduce((sum, { key, sign }) => sum + sign * (latest[key] ?? 0), 0);

  const naiveNote = latest.naiveFlowBcf != null && latest.storageChangeBcf != null    ? `<br>EIA's balancing item is a reconciliation line, not demand and not an import term. The
       panel's former "production − consumption" figure reads ${signedBcfd(latest.naiveFlowBcf, 0)} Bcf
       for this month, ${signedBcfd(latest.naiveFlowBcf - latest.storageChangeBcf, 0)} Bcf away from the
       ${signedBcfd(latest.storageChangeBcf, 0)} Bcf net storage change and
       ${signedBcfd(latest.vg4, 0)} Bcf away from EIA's balancing item.`
    : '';

  const lngNote = lngBcf == null
    ? ''
    : `<br>LNG exports alone were ${formatNumber(lngBcf, 1)} Bcf that month
       (${formatNumber(lngBcf / latest.days, 1)} Bcf/d) — one component of net imports, and most of why
       the imbalance is positive.`;

  // Feedgas is the newest data the bundle holds that touches this identity, so
  // it is shown beside the monthly legs even though it cannot close them.
  const feedgasMonths = [...feedgasByMonth.keys()].sort();
  const feedgasKey = feedgasMonths[feedgasMonths.length - 1];
  const feedgas = feedgasKey ? feedgasByMonth.get(feedgasKey) : null;
  const feedgasNote = !feedgas
    ? ''
    : `<br>The terminal meters are the most recent view: ${formatNumber(feedgas.bcf, 0)} Bcf of feedgas
       across ${feedgas.terminals} terminals in ${monthLabel(parsePeriod(feedgasKey))}
       (${formatNumber(feedgas.bcf / latest.days, 1)} Bcf/d monthly equivalent). That is a floor on US net
       imports — pipeline trade with Canada and Mexico is not in it — and it is ${lngBcf == null ? 'not'
       : `roughly ${formatNumber(feedgas.bcf / lngBcf, 2)}×`} the EIA LNG export total for the same month.`;

  wrap.innerHTML = `
    ${head(
      `${signedBcfd(residualPerDay)} Bcf/d`,
      '',
      `${latest.days}-day month · imbalance on the four legs this route carries`,
    )}
    <div class="panel-subchart__body panel-subchart__body--auto">
      <table class="bridge-table">
        <thead><tr><th>Leg (Table 1 order)</th><th>Bcf/d</th><th>Bcf</th></tr></thead>
        <tbody>
          ${legRows}
          ${missingRows}
          <tr class="bridge-table__residual">
            <td class="lbl">Imbalance = known supply − known disposition</td>
            <td class="num ${signClass(residualPerDay)}">${signedBcfd(residualPerDay, 2)}</td>
            <td class="num muted">${signedBcfd(residualD, 1)}</td>
          </tr>
        </tbody>
      </table>
      <span class="panel-table__note">
        Identity: dry production + supplemental fuels + net imports − net storage withdrawals =
        consumption + balancing item. This route supplies the first two, the storage change and the
        two right-hand terms; <strong>net imports is not scraped</strong>, so the four available legs
        do not close and the imbalance above is what is left after the two missing rows
        (${signedBcfd(legSumBcf, 1)} Bcf of legs, printed as
        ${signedBcfd(residualD, 1)} Bcf).
        Across ${model.closingMonths} months with a published balancing item the imbalance is
        ${formatNumber(model.medianAbsResidualBcf, 0)} Bcf/month at the median and
        ${formatNumber(model.maxAbsResidualBcf, 0)} Bcf at its largest.
        ${impliedNote}
        ${impliedCaveat}
        ${lngNote}
        ${feedgasNote}
        ${naiveNote}
        See <code>tests/balance_identity.test.mjs</code>.
      </span>
    </div>`;

  if (badge) {
    wrap.querySelector('.panel-subchart__header .panel-subchart__title-group')
      .insertAdjacentHTML('beforeend', badge);
  }
  panelBody.insertBefore(wrap, sidebar);
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

  // The band between the two sides of the identity is the imbalance, not a
  // storage flow: the legs it omits (net imports) fall between the two lines.
  // Labelling it as storage flow was the panel's original error.
  const area = d3.area()
    .x((m) => x(m.period))
    .y0((m) => y(m.drainedBcf))
    .y1((m) => y(m.suppliedBcf))
    .curve(d3.curveMonotoneX);
  g.append('path').datum(rows).attr('d', area).attr('fill', 'var(--chart-band)');

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
    .text((d) => (d >= 1000 ? `${(d / 1000).toFixed(0)}k` : `${Math.round(d)}`));

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
    { label: 'Known supply', color: 'var(--blue-flame)' },
    { label: 'Known disposition', color: 'var(--color-warning)' },
    { label: 'Band = imbalance', color: 'var(--blue-flame-deep)' },
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
          ${row('Known supply', m.suppliedBcf / m.days)}
          ${row('Known disposition', m.drainedBcf / m.days)}
          ${row('Dry production', m.fpdBcfd)}
          ${row('Consumption', m.vc0Bcfd)}
          ${row('Net storage withdrawals', m.swdBcfd)}
          <div class="tt-row tt-row--accent">
            <span class="tt-label">Imbalance (net imports missing)</span>
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
      helpText: `Dry production · ${formatNumber(latest.fpdBcf, 0)} Bcf in ${monthLabel(latest.period)}`,
    }),
    kpiCardHtml({
      label: 'Demand',
      value: `${formatNumber(latest.vc0Bcfd)} Bcf/d`,
      delta: yoyLabel(yoyPct('vc0Bcfd')),
      helpText: `Total consumption · ${formatNumber(latest.vc0Bcf, 0)} Bcf in ${monthLabel(latest.period)}`,
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
      label: 'Imbalance',
      value: latest.imbalanceBcfd == null ? '—' : `${signedBcfd(latest.imbalanceBcfd)} Bcf/d`,
      helpText: latest.imbalanceBcfd == null
        ? 'A leg is missing for this month, so the imbalance is not measurable'
        : `Supply − disposition on the four legs in this route. Net imports is not scraped
           and is the missing term${lngByMonth?.has(monthKeyOf(latest.period)) ? ' (LNG exports alone are in the LNG panel)' : ''}.`,
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
