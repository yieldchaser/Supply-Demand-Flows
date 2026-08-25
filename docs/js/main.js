import { loadBundle, ensureSource } from './data/bundle-loader.js';
import { renderHeader } from './components/header.js';
import { renderHealthStrip } from './components/health-strip.js';
import { renderFooter } from './components/footer.js';
import { renderStoragePanel } from './panels/storage.js';
import { renderBalancePanel } from './panels/balance.js';
import { renderRigsPanel } from './panels/rigs.js';
import { renderBasinsPanel } from './panels/basins.js';
import {
  renderBasinTable,
  renderBasinScatter,
  renderBasinShare,
  renderBasinExtremes,
} from './panels/basin-deep.js';
import { renderEuStoragePanel } from './panels/eu-storage.js';
import { renderDivergencePanel } from './panels/divergence.js';
import { renderLngTotalPanel } from './panels/lng-total.js';
import { renderLngSharesPanel } from './panels/lng-shares.js';
import { renderLngFeedgasPanel } from './panels/lng-feedgas.js';
import { renderLngFleetOverview } from './panels/lng-fleet-overview.js';
import { renderBasinEgressPanel } from './panels/basin-egress.js';

/**
 * Render a single panel inside its own try/catch so that a failure
 * in one panel cannot propagate to main().catch and replace the whole page.
 *
 * @param {string} name - human-readable panel name (used in error log + fallback UI)
 * @param {Function} renderFn - zero-argument async-capable function that does the render
 */
async function safeRender(name, renderFn) {
  try {
    await renderFn();
  } catch (err) {
    console.error(`[${name}] panel render failed:`, err);
    // Write an inline error state so the user sees which panel failed,
    // while all other panels continue to function normally.
    const panelEl = document.getElementById(`panel-${name}`);
    if (panelEl) {
      panelEl.innerHTML = `
        <div class="panel-error">
          <p>This panel couldn\u2019t load. Other panels are unaffected.</p>
          <details><summary>Technical details</summary>
            <code>${String(err.message ?? err)}</code>
          </details>
        </div>
      `;
    }
  }
}

/**
 * Render the panels of one data section after lazily loading its sources.
 *
 * @param {any} bundle
 * @param {string|string[]} sourceKeys - sources this section needs
 * @param {string} sectionName - for logging / error surfaces
 * @param {Function} renderFn - sync render closure over the bundle
 */
async function lazySection(bundle, sourceKeys, sectionName, renderFn) {
  const keys = Array.isArray(sourceKeys) ? sourceKeys : [sourceKeys];
  try {
    await Promise.all(keys.map((k) => ensureSource(bundle, k)));
  } catch (err) {
    console.error(`[${sectionName}] source load failed:`, err);
  }
  await safeRender(sectionName, renderFn);
}

/** Sources needed by each dashboard section (bundle.sources keys). */
const SECTIONS = {
  storage: ['eia_storage', 'eia_supply'],
  balance: ['eia_supply'],
  rigs: ['baker_hughes_weekly'],
  basins: ['baker_hughes_weekly'],
  'basin-table': ['baker_hughes_weekly'],
  'basin-scatter': ['baker_hughes_weekly'],
  'basin-share': ['baker_hughes_weekly'],
  'basin-extremes': ['baker_hughes_weekly'],
  divergence: ['eia_storage', 'gie_agsi'],
  'eu-storage': ['gie_agsi'],
  'lng-total': ['eia_lng_exports'],
  'lng-shares': ['eia_lng_exports'],
  lng: [
    'quorum',
    'gulf_south',
    'enbridge',
    'gasnom',
    'bhe',
    'cheniere',
    'kinder_morgan',
  ],
  'basin-egress': ['gulf_south'],
};

async function main() {
  // Boot parses CORE sources only (~1 MB combined): header + health strip +
  // the first paint sections. Everything else loads per-section below.
  const bundle = await loadBundle();

  renderHeader(bundle);
  renderHealthStrip(bundle);

  // EIA panels — live charts
  await lazySection(bundle, SECTIONS.storage, 'storage', () =>
    renderStoragePanel(document.getElementById('panel-storage'), bundle));
  await lazySection(bundle, SECTIONS.balance, 'balance', () =>
    renderBalancePanel(document.getElementById('panel-balance'), bundle));

  // Baker Hughes panels — live charts
  await lazySection(bundle, SECTIONS.rigs, 'rigs', () =>
    renderRigsPanel(document.getElementById('panel-rigs'), bundle));
  await lazySection(bundle, SECTIONS.basins, 'basins', () =>
    renderBasinsPanel(document.getElementById('panel-basins'), bundle));

  // Interlude: Transatlantic Storage Divergence (cross-source derived metric)
  await lazySection(bundle, SECTIONS.divergence, 'divergence', () =>
    renderDivergencePanel(document.getElementById('panel-divergence'), bundle));

  // Section 2: Basin Momentum Deep
  await lazySection(bundle, SECTIONS['basin-table'], 'basin-table', () =>
    renderBasinTable(document.getElementById('panel-basin-table'), bundle));
  await lazySection(bundle, SECTIONS['basin-scatter'], 'basin-scatter', () =>
    renderBasinScatter(document.getElementById('panel-basin-scatter'), bundle));
  await lazySection(bundle, SECTIONS['basin-share'], 'basin-share', () =>
    renderBasinShare(document.getElementById('panel-basin-share'), bundle));
  await lazySection(bundle, SECTIONS['basin-extremes'], 'basin-extremes', () =>
    renderBasinExtremes(document.getElementById('panel-basin-extremes'), bundle));

  // Section 3: European Storage Context
  await lazySection(bundle, SECTIONS['eu-storage'], 'eu-storage', () =>
    renderEuStoragePanel(document.getElementById('panel-eu-storage'), bundle));

  // Section 4: US LNG Exports Tracker
  await lazySection(bundle, SECTIONS['lng-total'], 'lng-total', () =>
    renderLngTotalPanel(document.getElementById('panel-lng-total'), bundle));
  await lazySection(bundle, SECTIONS['lng-shares'], 'lng-shares', () =>
    renderLngSharesPanel(document.getElementById('panel-lng-shares'), bundle));

  // Section 5: LNG Feedgas Observatory — fleet grid ABOVE the hero panel.
  // Both share selection state: clicking a fleet card re-renders the hero
  // with that terminal, highlights the card, and scrolls to the hero.
  let activeTerminalId;
  const fleetEl = document.getElementById('panel-lng-fleet');
  const heroEl = document.getElementById('panel-lng-feedgas');

  function renderLngSection() {
    const handleSelect = (id) => {
      activeTerminalId = id;
      renderLngSection();
      heroEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
    };
    renderLngFleetOverview(fleetEl, bundle, {
      activeTerminalId,
      onSelect: handleSelect,
    });
    renderLngFeedgasPanel(heroEl, bundle, activeTerminalId, {
      onSelect: handleSelect,
    });
  }
  await lazySection(bundle, SECTIONS.lng, 'lng-fleet', () => renderLngSection());

  // Section 6: Basin Egress — the supply-side counterpart to LNG feedgas.
  await lazySection(bundle, SECTIONS['basin-egress'], 'basin-egress', () =>
    renderBasinEgressPanel(document.getElementById('panel-basin-egress'), bundle)
  );

  renderFooter(bundle);
}

main().catch((err) => {
  // This catch now only fires for boot-critical failures:
  // manifest/index/core-source load failure or JSON parse errors.
  // Individual section errors are caught by safeRender above.
  console.error('Blue Tide boot failure:', err);
  document.body.innerHTML = `
    <div class="boot-error">
      <h1>Blue Tide couldn't load the observatory.</h1>
      <pre>${String(err)}</pre>
      <p>If this persists, check that docs/data/manifest.json is reachable.</p>
    </div>`;
});
