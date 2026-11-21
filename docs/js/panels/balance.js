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
 * The supply route carries every leg except net imports, which `eia_flows`
 * measures (`natural-gas/move/expc` · `impc`). The bridge prints the measured
 * leg and the identity's statistical difference; it also keeps the solved
 * figure as a cross-check, and falls back to it for a month with no trade row.
 * Solving the identity for net trade gives
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
import { redrawOnResize } from '../util/responsive-chart.js';
import { registerChartSnapshot } from '../util/chart-snapshot.js';

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
 * The one Table 1 leg the supply route does not carry: net imports. It is
 * measured by `eia_flows` (EIA `move/impc` · `move/expc`); when a month has no
 * trade row the bridge prints the leg as missing and solves for it instead.
 */
const MISSING_LEGS = [
  {
    key: 'netImports',
    label: 'Net imports',
    route: 'natural-gas/move/expc · move/impc',
    shownBy: 'EIA trade not yet published for this month',
  },
];

/**
 * `eia_flows` series → field, all MMcf per month in the source and Bcf here.
 *
 * Partner series (`_canada`, `_mexico`) are EIA's partner totals, which carry
 * the small trucked LNG/CNG volumes too; they are within 0.3 Bcf of the
 * pipeline figures in every month since 2019 and are labelled as pipeline
 * trade because that is what they are to within rounding.
 */
const FLOW_FIELDS = {
  trade_import_total: 'imp',
  trade_import_pipeline: 'impPipe',
  trade_import_lng: 'impLng',
  trade_import_canada: 'impCan',
  trade_import_mexico: 'impMex',
  trade_export_total: 'exp',
  trade_export_pipeline: 'expPipe',
  trade_export_lng: 'expLng',
  trade_export_canada: 'expCan',
  trade_export_mexico: 'expMex',
  cons_residential: 'res',
  cons_commercial: 'com',
  cons_industrial: 'ind',
  cons_electric_power: 'pwr',
  cons_lease_plant: 'lease',
  cons_pipeline: 'pipe',
  cons_vehicle: 'veh',
  cons_total: 'cons',
};

/**
 * EIA measured trade and sector consumption by `YYYY-MM`, in Bcf.
 *
 * `netImports` is imports − exports and is set only when both totals exist,
 * so a half-published month never reads as a swing in trade.
 *
 * @param {Array<{series_id:string, period:string, value:number}>} rows  `eia_flows` rows
 * @returns {Map<string, object>}
 */
export function buildFlowsByMonth(rows) {
  const out = new Map();
  for (const r of rows || []) {
    const field = FLOW_FIELDS[r.series_id];
    const key = field ? monthKey(r.period) : null;
    const v = Number(r.value);
    if (!key || !Number.isFinite(v)) continue;
    if (!out.has(key)) out.set(key, {});
    out.get(key)[field] = v / 1000;
  }
  for (const m of out.values()) {
    m.netImports = m.imp != null && m.exp != null ? m.imp - m.exp : null;
  }
  return out;
}

/**
 * The identity closed with measured trade, for one balance month.
 *
 * `statDiffBcf` = supply legs + measured net imports − consumption. EIA's
 * balancing item is defined to absorb that difference, so it should be near
 * zero; a large value means a leg here is defined differently from Table 1.
 *
 * @returns {{netImportsBcf:number, netExportsBcf:number, statDiffBcf:number}|null}
 */
export function measuredClosure(month, flows) {
  if (!month || month.imbalanceBcf == null || flows?.netImports == null) return null;
  return {
    netImportsBcf: flows.netImports,
    netExportsBcf: -flows.netImports,
    statDiffBcf: month.imbalanceBcf + flows.netImports,
  };
}

/**
 * Where the month's gas went, in Bcf/d: consumption by sector plus exports by
 * route, which together are total disposition. Imports from Canada and
 * production are carried alongside for the tooltip, not stacked.
 *
 * `other` groups lease & plant fuel, pipeline & distribution use and vehicle
 * fuel — gas the system consumes to run itself.
 *
 * @param {Map<string, object>} flowsByMonth  from buildFlowsByMonth
 * @param {Array<object>} balanceSeries       buildBalanceModel(...).series
 */
export function buildDisposition(flowsByMonth, balanceSeries = []) {
  const prodByKey = new Map(balanceSeries.map((m) => [monthKeyOf(m.period), m]));
  const out = [];
  for (const [key, f] of [...flowsByMonth.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const need = ['res', 'com', 'ind', 'pwr', 'lease', 'pipe', 'veh', 'expLng', 'expMex', 'expCan'];
    if (need.some((k) => f[k] == null)) continue;
    const period = parsePeriod(key);
    if (!period) continue;
    const days = daysInMonth(period);
    const d = (v) => (v == null ? null : v / days);
    const parts = {
      resCom: d(f.res + f.com),
      ind: d(f.ind),
      pwr: d(f.pwr),
      other: d(f.lease + f.pipe + f.veh),
      lng: d(f.expLng),
      mex: d(f.expMex),
      can: d(f.expCan),
    };
    const bal = prodByKey.get(key);
    out.push({
      key,
      period,
      days,
      parts,
      res: d(f.res),
      com: d(f.com),
      consumption: d(f.res + f.com + f.ind + f.pwr + f.lease + f.pipe + f.veh),
      exports: d(f.expLng + f.expMex + f.expCan),
      total: Object.values(parts).reduce((s, v) => s + v, 0),
      impCan: d(f.impCan),
      impLng: d(f.impLng),
      netExports: f.netImports == null ? null : d(-f.netImports),
      production: bal ? bal.fpdBcfd : null,
    });
  }
  return out;
}

/**
 * Same calendar month in earlier years: median, min and max of a value, and
 * how many years that rests on. The comparison a seasonal series needs —
 * January electric-power burn against other Januaries, not against July.
 */
export function sameMonthStats(rows, latest, accessor) {
  const prior = rows
    .filter((r) => r.period.getMonth() === latest.period.getMonth()
      && r.period.getFullYear() < latest.period.getFullYear())
    .map(accessor)
    .filter((v) => v != null && Number.isFinite(v))
    .sort((a, b) => a - b);
  if (!prior.length) return null;
  const mid = prior.length >> 1;
  const median = prior.length % 2 ? prior[mid] : (prior[mid - 1] + prior[mid]) / 2;
  return { median, min: prior[0], max: prior[prior.length - 1], n: prior.length };
}

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
        const acc = byMonth.get(key) ?? { bcf: 0, days: 0, terminals: new Set(), dates: new Set() };
        acc.bcf += mmcfd / 1000; // MMcf/d × one day = MMcf → Bcf
        acc.days += 1;
        acc.terminals.add(t.id);
        acc.dates.add(day);
        byMonth.set(key, acc);
      }
    }
  }
  const out = new Map();
  for (const [key, acc] of byMonth) {
    // `days` is terminal-days; `calendarDays` the distinct dates with any meter,
    // the divisor for a rate in a month still being reported.
    out.set(key, { bcf: acc.bcf, days: acc.days, terminals: acc.terminals.size, calendarDays: acc.dates.size });
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
  const flowsByMonth = buildFlowsByMonth(bundle.sources.eia_flows?.data);
  const measured = flowsByMonth.size > 0;
  const drawRef = { current: null };
  const { chartEl, sidebarEl } = renderPanelChrome(panelEl, {
    title: 'Supply & Demand Balance',
    subtitle: measured
      ? 'EIA · monthly US Table 1 identity · net trade measured (EIA imports & exports)'
      : 'EIA · monthly US Table 1 identity · net trade solved from the other five legs',
    sourceKey: 'eia_supply',
    latestPeriod: source.latest_period,
    draw: () => drawRef.current?.(),
  });

  drawRef.current = () => drawBalanceChart(chartEl, model, flowsByMonth);
  drawRef.current();
  renderBalanceKpis(sidebarEl, model, lngByMonth, flowsByMonth);
  appendBridge(panelEl, model, lngByMonth, feedgasByMonth, flowsByMonth);
  if (measured) appendDisposition(panelEl, model, flowsByMonth, bundle.sources.eia_flows);
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
function appendBridge(panelEl, model, lngByMonth, feedgasByMonth = new Map(), flowsByMonth = new Map()) {
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
  const flows = flowsByMonth.get(monthKeyStr) ?? null;
  const closure = measuredClosure(latest, flows);
  // EIA's LNG export total from the trade route first: it is the same series
  // the identity uses. The per-destination file is the fallback.
  const lngBcf = flows?.expLng ?? lngByMonth?.get(monthKeyStr) ?? null;
  const perDay = (v) => (v == null ? null : v / latest.days);

  const legRows = KNOWN_LEGS.map(({ key, sign }) => {
    const v = latest[key];
    const signed = v == null ? null : sign * v;
    const signedD = signed == null ? null : signed / latest.days;
    const label = key === 'swd'
      ? `${LEG_LABELS.swd}<span class="bridge-table__hint">withdrawals − injections · ${formatNumber(latest.saw, 0)} − ${formatNumber(latest.sai, 0)} Bcf</span>`
      : key === 'vc0' ? `${LEG_LABELS.vc0}<span class="bridge-table__hint">disposition · enters with a minus sign</span>`
        : LEG_LABELS[key];
    return `
      <tr>
        <td class="lbl">${label}</td>
        <td class="num ${signClass(signedD)}">${signedD == null ? 'not published' : signedBcfd(signedD)}</td>
        <td class="num muted">${signed == null ? '—' : signedBcfd(signed, 0)}</td>
      </tr>`;
  }).join('');

  const tradeRows = closure
    ? `
      <tr>
        <td class="lbl">Net imports (measured)<span class="bridge-table__hint">imports ${formatNumber(flows.imp, 0)} − exports ${formatNumber(flows.exp, 0)} Bcf · EIA move/impc · expc</span></td>
        <td class="num ${signClass(closure.netImportsBcf)}">${signedBcfd(perDay(closure.netImportsBcf))}</td>
        <td class="num muted">${signedBcfd(closure.netImportsBcf, 0)}</td>
      </tr>`
    : MISSING_LEGS.map((leg) => `
      <tr class="bridge-table__missing">
        <td class="lbl">${leg.label}<span class="bridge-table__hint">${leg.shownBy}</span></td>
        <td class="num muted" colspan="2">not published</td>
      </tr>`).join('');

  const residualD = latest.imbalanceBcf;
  const residualPerDay = residualD / latest.days;
  const lngPerDay = perDay(lngBcf);
  const plausible = residualPerDay > 0 && residualPerDay < (latest.vc0Bcfd ?? Infinity) * 0.5;

  // Sum of the printed rows must be the printed result: the bridge is checked
  // against its own arithmetic by the test suite.
  const legSumBcf = KNOWN_LEGS.reduce((sum, { key, sign }) => sum + sign * (latest[key] ?? 0), 0);

  // How well the identity closes with measured trade, across every month that
  // has it — the evidence for (or against) reading the legs at face value.
  const closures = model.series
    .map((m) => ({ m, c: measuredClosure(m, flowsByMonth.get(monthKeyOf(m.period))) }))
    .filter(({ c }) => c != null);
  const recent = closures.slice(-12);
  const maxRecentDiff = recent.length ? Math.max(...recent.map(({ m, c }) => Math.abs(c.statDiffBcf / m.days))) : null;
  const maxAllDiff = closures.length ? Math.max(...closures.map(({ m, c }) => Math.abs(c.statDiffBcf / m.days))) : null;
  // Seasonal shape of the earlier residual: winter vs summer mean, and the
  // average annual sum over complete calendar years.
  const seasonal = (() => {
    const old = closures.filter(({ m, c }) => Math.abs(c.statDiffBcf / m.days) > 0.1);
    if (maxAllDiff == null || maxAllDiff <= 0.1 || old.length < 12) return null;
    const mean = (xs) => xs.reduce((t, v) => t + v, 0) / xs.length;
    const isWinter = (m) => [10, 11, 0, 1, 2].includes(m.period.getMonth());
    const w = old.filter(({ m }) => isWinter(m)).map(({ m, c }) => c.statDiffBcf / m.days);
    const su = old.filter(({ m }) => !isWinter(m)).map(({ m, c }) => c.statDiffBcf / m.days);
    const oldYears = new Set(old.map(({ m }) => m.period.getFullYear()));
    const byYear = d3.rollup(closures.filter(({ m }) => oldYears.has(m.period.getFullYear())), (v) => (v.length === 12 ? v.reduce((t, { c }) => t + c.statDiffBcf, 0) : null),
      ({ m }) => m.period.getFullYear());
    const years = [...byYear.values()].filter((v) => v != null);
    if (!w.length || !su.length || !years.length) return null;
    return { winter: mean(w), summer: mean(su), annual: mean(years) };
  })();

  const netExportsD = closure ? perDay(closure.netExportsBcf) : residualPerDay;
  const pipeNetD = closure && flows.expMex != null && flows.expCan != null && flows.impCan != null
    ? perDay(flows.expMex + flows.expCan - flows.impCan - (flows.impMex ?? 0)) : null;

  const resultRows = closure
    ? `
      <tr class="bridge-table__residual">
        <td class="lbl">Statistical difference = all rows above</td>
        <td class="num ${signClass(perDay(closure.statDiffBcf))}">${signedBcfd(perDay(closure.statDiffBcf), 2)}</td>
        <td class="num muted">${signedBcfd(closure.statDiffBcf, 1)}</td>
      </tr>
      <tr>
        <td class="lbl">Cross-check: net exports solved from the five legs<span class="bridge-table__hint">supply legs − consumption · what the panel showed before trade was measured</span></td>
        <td class="num muted">${signedBcfd(residualPerDay, 2)}</td>
        <td class="num muted">${signedBcfd(residualD, 1)}</td>
      </tr>`
    : `
      <tr class="bridge-table__residual">
        <td class="lbl">Implied net exports = supply legs − consumption</td>
        <td class="num ${signClass(residualPerDay)}">${signedBcfd(residualPerDay, 2)}</td>
        <td class="num muted">${signedBcfd(residualD, 1)}</td>
      </tr>`;

  const tradeNote = closure
    ? `<br>Measured net exports were <strong>${signedBcfd(netExportsD, 1)} Bcf/d</strong>:
       LNG ${formatNumber(lngPerDay, 1)} Bcf/d${pipeNetD == null ? '' : ` and net pipeline trade
       ${signedBcfd(pipeNetD, 1)} Bcf/d (exports to Mexico ${formatNumber(perDay(flows.expMex), 1)} and Canada
       ${formatNumber(perDay(flows.expCan), 1)}, less imports from Canada ${formatNumber(perDay(flows.impCan), 1)})`}.
       With them the identity closes to ${signedBcfd(perDay(closure.statDiffBcf), 2)} Bcf/d this month and to
       ${maxRecentDiff < 0.005 ? 'within rounding (under 0.01 Bcf/d)' : `within ${formatNumber(maxRecentDiff, 2)} Bcf/d`} in each of the last ${recent.length} months with trade data.
       ${seasonal ? `Earlier months leave up to ${formatNumber(maxAllDiff, 2)} Bcf/d, averaging
       ${signedBcfd(seasonal.winter, 2)} Bcf/d in Nov–Mar and ${signedBcfd(seasonal.summer, 2)} in Apr–Oct but netting to
       ${signedBcfd(seasonal.annual, 1)} Bcf a year on average. A difference that reverses with the heating season and nets out
       over the year is the shape of a storage cycle: EIA's storage route here carries underground storage only, so
       the likely missing piece is LNG peak-shaving storage, which drains in winter and refills in summer. That is an
       inference from the pattern; this bundle does not measure it.` : ''}`
    : `<br>Solving the identity for the missing leg gives <strong>${signedBcfd(residualPerDay, 1)} Bcf/d of
       net exports</strong> (${signedBcfd(residualD, 0)} Bcf). That is an inference from the published legs, not a
       measurement: EIA's trade series has not reached this month yet.${lngPerDay == null ? '' : ` LNG exports were
       ${formatNumber(lngPerDay, 1)} Bcf/d that month, which leaves ${signedBcfd(residualPerDay - lngPerDay, 1)} Bcf/d for net
       pipeline trade plus the measurement noise of five legs.`}`;
  const impliedCaveat = closure || plausible
    ? ''
    : `<br><strong>Check:</strong> the implied figure is outside the range US net trade can take
       (positive and well under half of consumption); one of the published legs may be provisional.`;

  const badge = `<span class="panel-subchart__badge--bullish">${closure ? 'Net exports' : 'Implied net exports'} ${signedBcfd(netExportsD, 1)} Bcf/d</span>`;

  // Feedgas is the newest data the bundle holds that touches this identity, so
  // it is shown beside the monthly legs even though it cannot close them.
  const feedgasMonths = [...feedgasByMonth.keys()].sort();
  const feedgasKey = feedgasMonths[feedgasMonths.length - 1];
  const feedgas = feedgasKey ? feedgasByMonth.get(feedgasKey) : null;
  const feedgasNote = !feedgas
    ? ''
    : `<br>The terminal meters are the most recent view: ${formatNumber(feedgas.bcf, 0)} Bcf of feedgas
       across ${feedgas.terminals} measured terminals in ${monthLabel(parsePeriod(feedgasKey))}
       (${formatNumber(feedgas.bcf / feedgas.calendarDays, 1)} Bcf/d over the ${feedgas.calendarDays}
       ${feedgas.calendarDays === daysInMonth(parsePeriod(feedgasKey)) ? 'days of the month' : `of ${daysInMonth(parsePeriod(feedgasKey))} days reported so far`}).
       It runs ahead of EIA's monthly series and is a floor on LNG exports, not on net trade: pipeline
       trade with Canada and Mexico is not in it.`;

  wrap.innerHTML = `
    ${head(
      `${signedBcfd(netExportsD)} Bcf/d`,
      '',
      closure
        ? `${latest.days}-day month · Table 1 legs with measured trade`
        : `${latest.days}-day month · Table 1 legs; net trade solved from the rest`,
    )}
    <div class="panel-subchart__body panel-subchart__body--auto">
      <table class="bridge-table">
        <thead><tr><th>Leg (Table 1 order)</th><th>Bcf/d</th><th>Bcf</th></tr></thead>
        <tbody>
          ${legRows}
          ${tradeRows}
          ${resultRows}
        </tbody>
      </table>
      <span class="panel-table__note">
        Identity (EIA Natural Gas Monthly, Table 1): consumption = dry production + supplemental fuels
        + net imports + net storage withdrawals + balancing item. The five supply-route legs sum to
        ${signedBcfd(legSumBcf, 1)} Bcf, which is net imports with the opposite sign.
        Across ${model.closingMonths} months the solved net exports run
        ${formatNumber(model.minImpliedBcfd, 1)} to ${formatNumber(model.maxImpliedBcfd, 1)} Bcf/d.
        ${tradeNote}
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

function drawBalanceChart(container, model, flowsByMonth = new Map()) {
  container.innerHTML = '';
  const margin = { top: 28, right: 16, bottom: 36, left: 56 };
  const width = Math.max(container.getBoundingClientRect().width, 300) - margin.left - margin.right;
  const height = CHART_HEIGHT - margin.top - margin.bottom;

  // Three years back from the newest observation, not from the wall clock, so
  // a bundle that runs a week stale keeps its history instead of sliding empty.
  const anchor = referenceDate(model.series.map((m) => ({ period: m.period })));
  let src = model.series;
  if (anchor) {
    const cutoff = new Date(anchor);
    cutoff.setFullYear(cutoff.getFullYear() - 3);
    src = src.filter((m) => m.period >= cutoff);
  }
  // Per-day rates, not monthly totals: a February total is ~10% below a
  // January one from the calendar alone, which read as a demand drop.
  const rows = src
    .filter((m) => m.suppliedBcf != null && m.drainedBcf != null)
    .map((m) => {
      const c = measuredClosure(m, flowsByMonth.get(monthKeyOf(m.period)));
      const f = flowsByMonth.get(monthKeyOf(m.period));
      return {
        ...m,
        supplied: m.suppliedBcf / m.days,
        drained: m.drainedBcf / m.days,
        withTrade: c ? (m.drainedBcf + c.netExportsBcf) / m.days : null,
        netExportsD: c ? c.netExportsBcf / m.days : null,
        statDiffD: c ? c.statDiffBcf / m.days : null,
        lngD: f?.expLng != null ? f.expLng / m.days : null,
      };
    });

  if (rows.length === 0) {
    container.innerHTML =
      '<div class="panel-error">No month in the last 3 years has the full set of balance legs.</div>';
    return;
  }

  const svg = d3.select(container)
    .append('svg')
    .attr('viewBox', `0 0 ${width + margin.left + margin.right} ${CHART_HEIGHT}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block')
    .style('width', '100%');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleTime().domain(d3.extent(rows, (m) => m.period)).range([0, width]);
  // Not anchored at zero: the reading is the gap between two lines near
  // 80–140 Bcf/d, which a zero baseline would squeeze into a sliver.
  const lo = d3.min(rows, (m) => Math.min(m.supplied, m.drained, m.withTrade ?? Infinity));
  const hi = d3.max(rows, (m) => Math.max(m.supplied, m.drained, m.withTrade ?? -Infinity));
  const pad = (hi - lo) * 0.06;
  const y = d3.scaleLinear().domain([lo - pad, hi + pad]).nice().range([height, 0]);
  const yTicks = y.ticks(6);

  g.append('g')
    .selectAll('line')
    .data(yTicks)
    .join('line')
    .attr('x1', 0).attr('x2', width)
    .attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'var(--chart-grid)');

  // The band between the two sides of the identity is net exports: supply
  // available to the domestic market minus what it consumed.
  const area = d3.area()
    .x((m) => x(m.period))
    .y0((m) => y(m.drained))
    .y1((m) => y(m.supplied))
    .curve(d3.curveMonotoneX);
  // `--chart-band` is not a defined token, so this fill used to render black.
  g.append('path').datum(rows).attr('d', area).attr('fill', 'var(--color-accent-bg)');

  const line = (key, color, dash = null) => g.append('path')
    .datum(rows.filter((m) => m[key] != null))
    .attr('d', d3.line().x((m) => x(m.period)).y((m) => y(m[key])).curve(d3.curveMonotoneX))
    .attr('fill', 'none')
    .attr('stroke', color)
    .attr('stroke-width', 2)
    .attr('stroke-dasharray', dash)
    .attr('stroke-linejoin', 'round');

  line('supplied', 'var(--blue-flame)');
  line('drained', 'var(--color-warning)');
  const hasTrade = rows.some((m) => m.withTrade != null);
  if (hasTrade) line('withTrade', 'var(--text-secondary)', '4 3');

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
    .text((d) => d3.format(',')(d));
  g.append('text')
    .attr('transform', `translate(${-44},${height / 2}) rotate(-90)`)
    .attr('text-anchor', 'middle')
    .attr('fill', 'var(--chart-label)')
    .attr('font-size', 10)
    .text('Bcf/d');

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
  // between them is net exports — labelled because an unlabelled band reads
  // as decoration. The dashed line is the measured check on that band.
  const legend = svg.append('g').attr('transform', `translate(${margin.left}, 10)`);
  let xOff = 0;
  const items = [
    { label: 'Supply incl. net storage withdrawals', color: 'var(--blue-flame)' },
    { label: 'Consumption', color: 'var(--color-warning)' },
    { label: hasTrade ? 'Gap = net exports' : 'Gap = implied net exports', color: 'var(--color-accent-border)' },
  ];
  if (hasTrade) items.push({ label: 'Consumption + measured net exports', color: 'var(--text-secondary)', dash: '4 3' });
  let yOff = 0;
  for (const item of items) {
    const grp = legend.append('g');
    grp.append('line')
      .attr('class', 'chart-legend-swatch')
      .attr('x1', 0).attr('x2', 14).attr('y1', 5).attr('y2', 5)
      .attr('stroke', item.color)
      .attr('stroke-dasharray', item.dash ?? null)
      .attr('stroke-width', 2);
    const text = grp.append('text')
      .attr('class', 'chart-legend-text')
      .attr('x', 20).attr('y', 9)
      .text(item.label);
    const w = text.node().getComputedTextLength() + 34;
    // Wrap at the plot width so a phone does not clip the last items.
    if (xOff > 0 && xOff + w > width) { xOff = 0; yOff += 14; }
    grp.attr('transform', `translate(${xOff}, ${yOff})`);
    xOff += w;
  }
  if (yOff > 0) {
    g.attr('transform', `translate(${margin.left},${margin.top + yOff})`);
    svg.attr('viewBox', `0 0 ${width + margin.left + margin.right} ${CHART_HEIGHT + yOff}`);
  }

  setupBalanceHover(svg, g, x, width, height, rows, margin);
}

function setupBalanceHover(svg, g, x, width, height, rows, margin) {
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
        host.node().getBoundingClientRect().width - 240,
      );

      const row = (label, v, cls = '') => (v == null ? ''
        : `<div class="tt-row ${cls}"><span class="tt-label">${label}</span><span class="num">${signedBcfd(v)} Bcf/d</span></div>`);
      // Levels print unsigned; a "+" belongs to flows and changes.
      const lvl = (label, v) => (v == null ? ''
        : `<div class="tt-row"><span class="tt-label">${label}</span><span class="num">${formatNumber(v)} Bcf/d</span></div>`);

      tooltip
        .style('opacity', 1)
        .style('left', `${Math.max(0, left)}px`)
        .style('top', '36px')
        .html(`
          <div class="tt-date">${monthLabel(m.period)} · ${m.days} days</div>
          ${lvl('Supply incl. storage', m.supplied)}
          ${lvl('Consumption', m.drained)}
          ${lvl('Dry production', m.fpdBcfd)}
          ${row('Net storage withdrawals', m.swdBcfd)}
          ${row('Balancing item', m.vg4Bcfd)}
          ${m.netExportsD == null
            ? row('Implied net exports', m.imbalanceBcfd, 'tt-row--accent')
            : `${row('Net exports (measured)', m.netExportsD, 'tt-row--accent')}
               ${row('of which LNG', m.lngD)}
               ${row('Implied from the five legs', m.imbalanceBcfd)}
               ${row('Statistical difference', m.statDiffD)}`}`);
    })
    .on('mouseleave', () => {
      crosshair.style('opacity', 0);
      tooltip.style('opacity', 0);
    });
}

/* ─────────────────────────────────────────────── */
/*  Disposition: sector demand and measured trade  */
/* ─────────────────────────────────────────────── */

/** Stack order, bottom → top: domestic use (warm), then exports (cool). */
const DISPOSITION_PARTS = [
  { key: 'resCom', label: 'Residential & commercial', color: '#fbbf24' },
  { key: 'ind', label: 'Industrial', color: '#f97316' },
  { key: 'pwr', label: 'Electric power', color: '#e879f9' },
  { key: 'other', label: 'Lease, plant, pipeline & vehicle fuel', color: '#9ca3af' },
  { key: 'lng', label: 'LNG exports', color: '#38bdf8' },
  { key: 'mex', label: 'Pipeline exports to Mexico', color: '#22d3a4' },
  { key: 'can', label: 'Pipeline exports to Canada', color: '#818cf8' },
];

const DISPOSITION_MONTHS = 48;

function appendDisposition(panelEl, model, flowsByMonth, source) {
  const panelBody = panelEl.querySelector('.panel-body');
  const rows = buildDisposition(flowsByMonth, model.series);
  if (!panelBody || rows.length === 0) return;
  const latest = rows[rows.length - 1];
  const prev = rows.find((r) => r.period.getFullYear() === latest.period.getFullYear() - 1
    && r.period.getMonth() === latest.period.getMonth());

  const wrap = document.createElement('div');
  wrap.className = 'panel-subchart';
  const tableRows = [
    { label: 'Residential', get: (r) => r.res },
    { label: 'Commercial', get: (r) => r.com },
    { label: 'Industrial', get: (r) => r.parts.ind },
    { label: 'Electric power', get: (r) => r.parts.pwr },
    { label: 'Lease, plant, pipeline & vehicle fuel', get: (r) => r.parts.other },
    { label: 'Consumption', get: (r) => r.consumption, strong: true },
    { label: 'LNG exports', get: (r) => r.parts.lng },
    { label: 'Pipeline exports to Mexico', get: (r) => r.parts.mex },
    { label: 'Pipeline exports to Canada', get: (r) => r.parts.can },
    { label: 'Imports from Canada', get: (r) => r.impCan, hint: 'supply side · not in the stack' },
    { label: 'Net exports', get: (r) => r.netExports, strong: true, hint: 'all exports − all imports' },
  ];
  const body = tableRows.map(({ label, get, strong, hint }) => {
    const v = get(latest);
    const p = prev ? get(prev) : null;
    const st = sameMonthStats(rows, latest, get);
    const vsMed = st && v != null ? v - st.median : null;
    return `
      <tr${strong ? ' class="bridge-table__residual"' : ''}>
        <td class="lbl">${label}${hint ? `<span class="bridge-table__hint">${hint}</span>` : ''}</td>
        <td class="num">${formatNumber(v, 1)}</td>
        <td class="num ${signClass(p == null || v == null ? null : v - p)}">${p == null || v == null ? '—' : signedBcfd(v - p, 1)}</td>
        <td class="num ${signClass(vsMed)}">${vsMed == null ? '—' : signedBcfd(vsMed, 1)}</td>
        <td class="num muted">${st ? `${formatNumber(st.min, 1)}–${formatNumber(st.max, 1)}` : '—'}</td>
      </tr>`;
  }).join('');
  const st0 = sameMonthStats(rows, latest, (r) => r.consumption);
  const firstYear = rows[0].period.getFullYear();
  const share = (k) => (latest.parts[k] / latest.total) * 100;

  wrap.innerHTML = `
    <div class="panel-subchart__header">
      <div class="panel-subchart__title-group">
        <span class="panel-subchart__title">Where the gas goes · ${monthLabel(latest.period)}</span>
        <span class="panel-subchart__hero num">${formatNumber(latest.total, 1)} Bcf/d</span>
      </div>
      <span class="panel-subchart__subtitle">Consumption by sector + exports by route · EIA monthly, measured · Bcf/d</span>
    </div>
    <div class="panel-subchart__body panel-subchart__body--auto">
      <div class="balance-disposition__chart"></div>
      <table class="bridge-table">
        <thead><tr>
          <th>${monthLabel(latest.period)}</th><th>Bcf/d</th><th>vs ${prev ? prev.period.getFullYear() : 'yr ago'}</th>
          <th>vs median${st0 ? ` (${st0.n} yr)` : ''}</th><th>Range</th>
        </tr></thead>
        <tbody>${body}</tbody>
      </table>
      <span class="panel-table__note">
        Total disposition is consumption plus exports, ${formatNumber(latest.total, 1)} Bcf/d in
        ${monthLabel(latest.period)}: electric power ${formatNumber(share('pwr'), 0)}%, LNG exports
        ${formatNumber(share('lng'), 0)}%, industrial ${formatNumber(share('ind'), 0)}%. "vs median" and "Range"
        compare with the same calendar month in each earlier year since ${firstYear}, because every
        component here is seasonal and a month is only comparable with the same month. The sectors sum
        to EIA's total consumption, the same leg the balance above uses. Dry production is the dashed line; the gap between it and the
        top of the stack is what imports (${formatNumber(latest.impCan, 1)} Bcf/d from Canada this month),
        net storage withdrawals, supplemental fuels and the balancing item supplied.
      </span>
    </div>`;
  appendSection(panelBody, 'overview-disposition', wrap, panelBody.querySelector('.panel-sidebar') || null);

  const chartEl = wrap.querySelector('.balance-disposition__chart');
  const recent = rows.slice(-DISPOSITION_MONTHS);
  const draw = () => drawDisposition(chartEl, recent);
  const w = draw();
  redrawOnResize(chartEl, draw, { width: w });
  registerChartSnapshot('balance-disposition', 'us-gas-disposition', recent.map((r) => ({
    month: r.key,
    ...Object.fromEntries(DISPOSITION_PARTS.map((p) => [p.label, Number(r.parts[p.key].toFixed(3))])),
    'Dry production': r.production == null ? null : Number(r.production.toFixed(3)),
  })), { unit: 'Bcf/d', source: 'EIA natural-gas/cons/sum, move/expc', latest: source?.latest_period ?? null });
}

function drawDisposition(container, rows) {
  container.innerHTML = '';
  const H = 300;
  const margin = { top: 34, right: 12, bottom: 28, left: 44 };
  const outerW = Math.max(container.getBoundingClientRect().width, 320);
  const width = outerW - margin.left - margin.right;
  const height = H - margin.top - margin.bottom;
  const svg = d3.select(container).append('svg')
    .attr('viewBox', `0 0 ${outerW} ${H}`)
    .attr('preserveAspectRatio', 'xMidYMid meet')
    .style('display', 'block').style('width', '100%');
  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  const x = d3.scaleBand().domain(rows.map((r) => r.key)).range([0, width]).padding(0.18);
  const yMax = d3.max(rows, (r) => Math.max(r.total, r.production ?? 0));
  const y = d3.scaleLinear().domain([0, yMax * 1.04]).nice().range([height, 0]);

  g.append('g').selectAll('line').data(y.ticks(5)).join('line')
    .attr('x1', 0).attr('x2', width).attr('y1', (d) => y(d)).attr('y2', (d) => y(d))
    .attr('stroke', 'var(--chart-grid)');
  g.append('g').selectAll('text').data(y.ticks(5)).join('text')
    .attr('x', -6).attr('y', (d) => y(d) + 4).attr('text-anchor', 'end')
    .attr('fill', 'var(--chart-label)').attr('font-size', 10).attr('font-family', 'var(--font-mono)')
    .text((d) => d);
  g.append('text').attr('x', -6).attr('y', -10).attr('text-anchor', 'end')
    .attr('fill', 'var(--chart-label)').attr('font-size', 10).text('Bcf/d');

  for (const r of rows) {
    let acc = 0;
    for (const p of DISPOSITION_PARTS) {
      const v = r.parts[p.key];
      g.append('rect')
        .attr('x', x(r.key)).attr('width', x.bandwidth())
        .attr('y', y(acc + v)).attr('height', Math.max(0, y(acc) - y(acc + v)))
        .attr('fill', p.color).attr('opacity', 0.85);
      acc += v;
    }
  }
  const prodRows = rows.filter((r) => r.production != null);
  g.append('path').datum(prodRows)
    .attr('d', d3.line().x((r) => x(r.key) + x.bandwidth() / 2).y((r) => y(r.production)))
    .attr('fill', 'none').attr('stroke', 'var(--text-bright)').attr('stroke-width', 1.5)
    .attr('stroke-dasharray', '4 3');

  // January ticks only, so labels never collide at phone width.
  const ticks = rows.filter((r) => r.period.getMonth() === 0);
  g.append('g').selectAll('text').data(ticks).join('text')
    .attr('x', (r) => x(r.key) + x.bandwidth() / 2).attr('y', height + 16).attr('text-anchor', 'middle')
    .attr('fill', 'var(--chart-label)').attr('font-size', 10).attr('font-family', 'var(--font-mono)')
    .text((r) => r.period.getFullYear());

  // Legend wraps onto a second row when the column is narrow.
  const legend = svg.append('g').attr('transform', `translate(${margin.left}, 8)`);
  let lx = 0;
  let ly = 0;
  for (const p of [...DISPOSITION_PARTS, { label: 'Dry production', line: true }]) {
    const grp = legend.append('g');
    if (p.line) {
      grp.append('line').attr('x1', 0).attr('x2', 12).attr('y1', 5).attr('y2', 5)
        .attr('stroke', 'var(--text-bright)').attr('stroke-dasharray', '4 3').attr('stroke-width', 1.5);
    } else {
      grp.append('rect').attr('width', 10).attr('height', 10).attr('fill', p.color);
    }
    const t = grp.append('text').attr('class', 'chart-legend-text').attr('x', 15).attr('y', 9).text(p.label);
    const w = t.node().getComputedTextLength() + 30;
    if (lx > 0 && lx + w > width) { lx = 0; ly += 13; }
    grp.attr('transform', `translate(${lx},${ly})`);
    lx += w;
  }
  if (ly > 0) {
    // Make room for the wrapped legend: move the plot down and grow the
    // drawing, rather than letting the legend rows sit on the bars.
    g.attr('transform', `translate(${margin.left},${margin.top + ly})`);
    svg.attr('viewBox', `0 0 ${outerW} ${H + ly}`);
  }

  const host = d3.select(container).style('position', 'relative');
  const tooltip = host.append('div').attr('class', 'chart-tooltip').style('opacity', 0);
  g.append('rect').attr('width', width).attr('height', height).attr('fill', 'transparent')
    .on('mousemove', (event) => {
      const [mx] = d3.pointer(event);
      const i = Math.max(0, Math.min(rows.length - 1, Math.floor(mx / x.step())));
      const r = rows[i];
      const line = (label, v, color) => `<div class="tt-row"><span class="tt-label">${color
        ? `<span style="display:inline-block;width:8px;height:8px;background:${color};margin-right:6px"></span>` : ''}${label}</span><span class="num">${formatNumber(v, 1)}</span></div>`;
      const box = container.getBoundingClientRect();
      const px = ((margin.left + x(r.key)) / outerW) * box.width;
      tooltip.style('opacity', 1)
        .style('left', `${Math.max(0, Math.min(px + 14, box.width - 250))}px`).style('top', '30px')
        .html(`<div class="tt-date">${monthLabel(r.period)} · Bcf/d</div>
          ${[...DISPOSITION_PARTS].reverse().map((p) => line(p.label, r.parts[p.key], p.color)).join('')}
          <div class="tt-row tt-row--accent"><span class="tt-label">Total disposition</span><span class="num">${formatNumber(r.total, 1)}</span></div>
          ${r.production == null ? '' : line('Dry production', r.production)}
          ${r.impCan == null ? '' : line('Imports from Canada', r.impCan)}
          ${r.netExports == null ? '' : line('Net exports', r.netExports)}`);
    })
    .on('mouseleave', () => tooltip.style('opacity', 0));
  return outerW;
}

/* ─────────────────────────────────────────────── */
/*  KPIs                                           */
/* ─────────────────────────────────────────────── */

function renderBalanceKpis(sidebarEl, model, lngByMonth, flowsByMonth = new Map()) {
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
    (() => {
      const f = flowsByMonth.get(monthKeyOf(latest.period));
      const c = measuredClosure(latest, f);
      if (c) {
        const prevF = prevYear ? flowsByMonth.get(monthKeyOf(prevYear.period)) : null;
        const prevNet = prevF?.netImports == null ? null : -prevF.netImports / prevYear.days;
        const net = c.netExportsBcf / latest.days;
        return kpiCardHtml({
          label: 'Net exports',
          value: `${signedBcfd(net)} Bcf/d`,
          delta: prevNet == null ? null : { value: `${signedBcfd(net - prevNet)} Bcf/d YoY`, kind: 'neutral' },
          helpText: `Measured (EIA imports − exports) · LNG ${formatNumber(f.expLng / latest.days, 1)} Bcf/d ·
            identity closes to ${signedBcfd(c.statDiffBcf / latest.days, 2)} Bcf/d`,
        });
      }
      return kpiCardHtml({
        label: 'Implied net exports',
        value: latest.imbalanceBcfd == null ? '—' : `${signedBcfd(latest.imbalanceBcfd)} Bcf/d`,
        helpText: latest.imbalanceBcfd == null
          ? 'A leg is missing for this month, so net trade cannot be solved'
          : `Table 1 solved for net trade${lngByMonth?.has(monthKeyOf(latest.period))
            ? ` · LNG exports ${formatNumber(lngByMonth.get(monthKeyOf(latest.period)) / latest.days, 1)} Bcf/d`
            : ''} · inferred, not measured`,
      });
    })(),
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
  // A value that rounds to zero prints unsigned: "−0.00" reads as a deficit.
  if (Number(Math.abs(v).toFixed(digits)) === 0) return (0).toFixed(digits);
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
