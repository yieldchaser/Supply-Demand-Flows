// Extract buildDailyByKind from basin-egress.js and test against real shard.
const fs = require('fs');
const path = require('path');
const REPO = 'C:/Users/Dell/Github/Supply-Demand-Flows';

const src = fs.readFileSync(path.join(REPO, 'docs/js/panels/basin-egress.js'), 'utf8');
// Grab from the function start to its closing "return out;\n}".
const start = src.indexOf('function buildDailyByKind');
const endMarker = 'return out;\n}';
const end = src.indexOf(endMarker, start);
if (start < 0 || end < 0) { console.log('MARKERS NOT FOUND', start, end); process.exit(1); }
const fnSrc = src.slice(start, end + endMarker.length);
const BASIN_SOURCE = 'gulf_south';
const CYCLE_RANK = { id1: 1, id2: 2, id3: 3 };
eval(fnSrc);

const bundle = JSON.parse(
  fs.readFileSync(path.join(REPO, 'docs/data/src.gulf_south.f7fa603f.json'), 'utf8')
);
const oac = buildDailyByKind(bundle.data, 3362, 'oac');
const keys = Object.keys(oac).sort();
console.log('lonewa oac days:', keys.length, '| latest:', keys[keys.length - 1], '=', oac[keys[keys.length - 1]]);
