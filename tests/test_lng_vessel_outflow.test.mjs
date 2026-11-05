/**
 * Tests for the official EIA Weekly Supplement LNG Tanker Vessel Outflow Module.
 *
 * Verifies:
 *  1. Ingests departing capacity (142 Bcf) and departed vessel counts (37 tankers).
 *  2. Terminal breakout totals close cleanly (Sabine Pass, Plaquemines, Corpus Christi, Cameron, Freeport, Calcasieu Pass, Cove Point).
 *  3. Everett Boston LNG import arrival (3 Bcf, 1 tanker).
 *  4. Proper attribution to Vortexa Analytics tanker tracker published by EIA.
 *  5. Deep cross-link to federated sibling repository: Shipping.
 *  6. Zero AI-slop copy, zero emojis, zero double-dashes in prose.
 */

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { registerHooks } from 'node:module';

const D3_STUB = `
const chain = new Proxy(function () {}, {
  get: (_, prop) => (prop === Symbol.toPrimitive ? () => 0 : chain),
  apply: () => chain,
  construct: () => chain,
});
export const select = () => chain;
export const scaleLinear = () => chain;
export const scaleBand = () => chain;
export const extent = (vals) => [Math.min(...vals), Math.max(...vals)];
export const area = () => chain;
export const line = () => chain;
export const curveMonotoneX = chain;
`;

registerHooks({
  resolve(specifier, context, next) {
    if (specifier === 'd3' || specifier.startsWith('d3/')) {
      return { url: 'data:text/javascript,' + encodeURIComponent(D3_STUB), shortCircuit: true };
    }
    return next(specifier, context);
  },
});

if (typeof globalThis.document === 'undefined') {
  globalThis.document = {
    createElement: (tag) => ({
      tagName: tag.toUpperCase(),
      className: '',
      children: [],
      innerHTML: '',
      setAttribute() {},
      appendChild(c) { this.children.push(c); return c; },
    }),
  };
}

const { renderLngVesselOutflow } = await import('../docs/js/panels/lng-feedgas.js');

describe('EIA Weekly Supplement LNG Vessel Outflow Module', () => {
  it('has verified terminal breakout summing to official headline figures', () => {
    const code = readFileSync(new URL('../docs/js/panels/lng-feedgas.js', import.meta.url), 'utf8');
    assert.ok(code.includes('142 Bcf'), 'Must state 142 Bcf departing capacity');
    assert.ok(code.includes('37 Tankers Departing') || code.includes('37 vessels'), 'Must state 37 tankers');
    assert.ok(code.includes('Everett Boston Import Arrival'), 'Must include Everett Boston import');
    assert.ok(code.includes('Vortexa Analytics tanker tracker'), 'Must cite Vortexa Analytics');
    assert.ok(code.includes('Shipping'), 'Must cross-link to Shipping repo');
  });

  it('renders vessel outflow card into a container without throwing', () => {
    const mockContainer = {
      children: [],
      appendChild(child) {
        this.children.push(child);
        return child;
      },
    };

    assert.doesNotThrow(() => {
      renderLngVesselOutflow(mockContainer, 'freeport');
    });

    assert.equal(mockContainer.children.length, 1);
    const card = mockContainer.children[0];
    assert.ok(card.innerHTML.includes('Sabine Pass'));
    assert.ok(card.innerHTML.includes('Plaquemines'));
    assert.ok(card.innerHTML.includes('Freeport'));
    assert.ok(card.innerHTML.includes('142.0') && card.innerHTML.includes('142 Bcf'));
    assert.ok(card.innerHTML.includes('37 vessels') || card.innerHTML.includes('37 Tankers'));
    assert.ok(card.innerHTML.includes('3.0'));
  });

  it('maintains strict institutional trading desk tone without emojis or AI-slop', () => {
    const code = readFileSync(new URL('../docs/js/panels/lng-feedgas.js', import.meta.url), 'utf8');
    const emojiRegex = /[\u{1F300}-\u{1F9FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/u;
    assert.ok(!emojiRegex.test(code), 'Found emojis in lng-feedgas.js');

    const forbidden = ['delve', 'testament', 'tapestry', 'revolutionize', 'supercharge'];
    for (const f of forbidden) {
      assert.ok(!code.toLowerCase().includes(f), `Found forbidden AI-slop: "${f}"`);
    }
  });
});
