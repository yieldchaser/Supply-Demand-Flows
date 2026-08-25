// Extract buildEgressSeries + helpers from basin-egress.js and run them
// against the real bundle shard to verify share-in-use + OAC level data.
const fs = require('fs');
const path = require('path');

const REPO = 'C:/Users/Dell/Github/Supply-Demand-Flows';
let src = fs.readFileSync(path.join(REPO, 'docs/js/panels/basin-egress.js'), 'utf8');
src = src.replace(/import[\s\S]*?from '[^']*';/g, '');
src = src.replace(/^export /gm, '');

const bundle = JSON.parse(
  fs.readFileSync(path.join(REPO, 'docs/data/src.gulf_south.f7fa603f.json'), 'utf8')
);

// Pull the real registry values (BASIN_SOURCE, CORRIDORS, EGRESS_METERS).
const registrySrc = fs.readFileSync(path.join(REPO, 'docs/js/util/basin-egress.js'), 'utf8');
const BASIN_SOURCE = /BASIN_SOURCE = '([^']+)'/.exec(registrySrc)[1];
// eslint-disable-next-line no-eval
(0, eval)(registrySrc.replace(/^export /gm, ''));
const CORRIDORS = globalThis.CORRIDORS || [];
const EGRESS_METERS = globalThis.EGRESS_METERS || [];

// Evaluate the module body in this context with d3/document stubs.
const d3 = { select: () => ({ append: () => ({ attr() { return this; }, style() { return this; }, text() { return this; }, datum() { return this; }, enter() { return this; } }) }), line: () => {}, area: () => {}, scaleLinear: () => ({ domain() { return this; }, range() { return this; } }), max: () => 0, curveMonotoneX: {} };
eval(src.replace(/\bdocument\b/g, '({createElement:()=>({style:{},setAttribute(){}})})'));

const { daily, latest } = buildEgressSeries(bundle.data);
console.log('days:', daily.length);
const last = daily[daily.length - 1];
console.log('latest dateStr:', last.dateStr);
console.log('byCorridorSqShared:', last.byCorridorSqShared);
console.log('byCorridorOac:', last.byCorridorOac);
console.log('oacLevelByCorridor:', last.oacLevelByCorridor);
