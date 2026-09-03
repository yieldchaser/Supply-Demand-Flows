/**
 * Real client load benchmark for Blue Tide Observatory.
 *
 * Measures uncompressed bytes, gzipped over-the-wire bytes, JSON.parse time,
 * and memory footprint for:
 *   1. Core boot path (manifest + index + 3 core shards: eia_storage, eia_supply, baker_hughes_weekly)
 *   2. Full dashboard path (all 12 source shards + index + manifest)
 *
 * Runs via built-in node with zero dependencies.
 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import { performance } from 'node:perf_hooks';

const DATA_DIR = path.resolve('docs/data');
const manifestPath = path.join(DATA_DIR, 'manifest.json');

if (!fs.existsSync(manifestPath)) {
  console.error(`Error: ${manifestPath} does not exist.`);
  process.exit(1);
}

const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
const indexPath = path.join(DATA_DIR, manifest.index_url);
const index = JSON.parse(fs.readFileSync(indexPath, 'utf8'));

function measureFiles(fileList) {
  let rawBytes = 0;
  let gzipBytes = 0;
  const texts = [];

  for (const f of fileList) {
    const p = path.join(DATA_DIR, f);
    if (!fs.existsSync(p)) continue;
    const buf = fs.readFileSync(p);
    rawBytes += buf.length;
    gzipBytes += zlib.gzipSync(buf).length;
    texts.push(buf.toString('utf8'));
  }

  // Measure parse time across 5 iterations
  const times = [];
  let parsedObj = null;
  const memBefore = process.memoryUsage().heapUsed;

  for (let i = 0; i < 5; i++) {
    const t0 = performance.now();
    for (const txt of texts) {
      parsedObj = JSON.parse(txt);
    }
    const t1 = performance.now();
    times.push(t1 - t0);
  }

  const memAfter = process.memoryUsage().heapUsed;
  const medianTime = times.sort((a, b) => a - b)[Math.floor(times.length / 2)];

  return {
    fileCount: fileList.length,
    rawBytes,
    rawMB: (rawBytes / (1024 * 1024)).toFixed(2),
    gzipBytes,
    gzipKB: (gzipBytes / 1024).toFixed(1),
    parseTimeMs: medianTime.toFixed(2),
    heapUsedMB: Math.max(0, (memAfter - memBefore) / (1024 * 1024)).toFixed(2),
  };
}

// 1. Core Boot Path
const coreFiles = [
  'manifest.json',
  manifest.index_url,
  ...index.core.map((key) => index.sources[key].file),
];

// 2. Full Shards Path
const allFiles = [
  'manifest.json',
  manifest.index_url,
  ...Object.keys(index.sources).map((key) => index.sources[key].file),
];

const coreMetrics = measureFiles(coreFiles);
const fullMetrics = measureFiles(allFiles);

console.log('='.repeat(75));
console.log('BLUE TIDE LOAD BENCHMARK (scripts/measure_load.mjs)');
console.log('='.repeat(75));
console.log(`Manifest hash: ${manifest.hash} (generated: ${manifest.generated_at})\n`);

console.log('Metric                     | Core Boot (3 sources) | Full (All 12 sources) | Delta');
console.log('-'.repeat(75));
console.log(
  `Requests to first paint   | ${coreMetrics.fileCount} requests            | ${fullMetrics.fileCount} requests           | -${(
    ((fullMetrics.fileCount - coreMetrics.fileCount) / fullMetrics.fileCount) *
    100
  ).toFixed(1)}%`
);
console.log(
  `Transfer size (gzipped)    | ${coreMetrics.gzipKB} KB               | ${fullMetrics.gzipKB} KB              | -${(
    ((fullMetrics.gzipBytes - coreMetrics.gzipBytes) / fullMetrics.gzipBytes) *
    100
  ).toFixed(1)}%`
);
console.log(
  `Uncompressed payload       | ${coreMetrics.rawMB} MB                 | ${fullMetrics.rawMB} MB                | -${(
    ((fullMetrics.rawBytes - coreMetrics.rawBytes) / fullMetrics.rawBytes) *
    100
  ).toFixed(1)}%`
);
console.log(
  `JSON.parse CPU time        | ${coreMetrics.parseTimeMs} ms                | ${fullMetrics.parseTimeMs} ms               | -${(
    ((fullMetrics.parseTimeMs - coreMetrics.parseTimeMs) / fullMetrics.parseTimeMs) *
    100
  ).toFixed(1)}%`
);
console.log('-'.repeat(75));
console.log('Execution: COMPLETE');
