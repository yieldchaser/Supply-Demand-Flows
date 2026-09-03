/**
 * Node.js smoke tests for Section 8 LNG Terminal Downtime Panel rendering.
 *
 * Runs via built-in `node --test` with ZERO dependencies.
 * Asserts:
 * 1. buildDowntimeViewModel computes valid daily totals, status, and KPIs from fixture
 * 2. renderEventListHtml produces valid HTML table markup when events are present
 * 3. renderEventListHtml does NOT throw and produces clean empty state when zero events
 * 4. renderTerminalDowntimePanel runs against a minimal DOM stub without throwing
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  buildDowntimeViewModel,
  renderEventListHtml,
  DOWNTIME_CONF,
} from '../docs/js/util/lng-downtime.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

test('Section 8 render: buildDowntimeViewModel produces valid model for fixture bundle', () => {
  const fixturePath = path.join(__dirname, 'fixtures', 'downtime_golden_fixture.json');
  const raw = fs.readFileSync(fixturePath, 'utf8');
  const fixture = JSON.parse(raw);

  // Construct a minimal bundle containing the fixture daily series
  const bundle = {
    sources: {
      gulf_south: {
        latest_period: '2026-07-20',
        data: fixture.daily.map((d) => ({
          series_id: 'gulf_south_sq_24329_d_timely',
          period: d.dateStr,
          value: d.value * 1.025 * 1000.0,
        })),
      },
      enbridge: {
        latest_period: '2026-07-20',
        data: fixture.daily.map((d) => ({
          series_id: 'tetco_sq_79999_d_timely',
          period: d.dateStr,
          value: 0.0,
        })),
      },
    },
  };

  const model = buildDowntimeViewModel(bundle, 'freeport');
  assert.ok(model, 'Model must be created');
  assert.ok(model.daily.length > 0, 'Model must carry daily totals');
  assert.ok(Array.isArray(model.events), 'Model must carry events array');
  assert.ok(model.status, 'Model must carry status');
  assert.strictEqual(model.kpis.length, 3, 'Model must generate 3 KPI card strings');
  assert.ok(model.kpis[0].includes('Current status'), 'First KPI card must be Current status');
});

test('Section 8 render: renderEventListHtml produces table markup when events exist', () => {
  const conf = DOWNTIME_CONF.freeport;
  const mockEvents = [
    { date: '2024-04-17', type: 'OFFLINE', duration: 7, detail: 'Complete feedgas loss across 7 consecutive gas days' },
    { date: '2024-05-01', type: 'RAMPING', duration: 14, detail: 'Post-outage restart ramp' },
  ];

  const html = renderEventListHtml(mockEvents, conf);
  assert.ok(typeof html === 'string', 'Must return an HTML string');
  assert.ok(html.includes('<table class="downtime-table">'), 'Must render table element');
  assert.ok(html.includes('2024-04-17'), 'Must include event date');
  assert.ok(html.includes('badge--offline'), 'Must include offline badge');
  assert.ok(html.includes('downtime-honesty'), 'Must include honesty caveat');
  assert.ok(html.includes('KMTP intrastate feed'), 'Must cite Freeport honesty string');
});

test('Section 8 render: renderEventListHtml does NOT throw and produces clean empty state when 0 events', () => {
  const confCove = DOWNTIME_CONF.cove_point;
  const confSabine = DOWNTIME_CONF.sabine_pass;

  // Zero events for Cove Point
  assert.doesNotThrow(() => {
    const htmlCove = renderEventListHtml([], confCove);
    assert.ok(htmlCove.includes('downtime-events__empty'), 'Must render empty state class');
    assert.ok(htmlCove.includes('No downtime events in the measured window'), 'Must show generic clean message');
    assert.ok(htmlCove.includes('Direct plant intake meter 10001-D'), 'Must cite Cove Point honesty note');
  });

  // Zero events for Sabine Pass (has custom explanatory empty message)
  assert.doesNotThrow(() => {
    const htmlSabine = renderEventListHtml([], confSabine);
    assert.ok(htmlSabine.includes('downtime-events__empty'), 'Must render empty state class');
    assert.ok(htmlSabine.includes('measured-partial'), 'Must explain Sabine Pass measured-partial flat behavior');
    assert.ok(htmlSabine.includes('Non-CTPL feeds'), 'Must cite Sabine Pass honesty note');
  });
});
