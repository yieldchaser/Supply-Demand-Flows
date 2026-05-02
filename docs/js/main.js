import { loadBundle } from './data/bundle-loader.js';
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

async function main() {
  const bundle = await loadBundle();

  renderHeader(bundle);
  renderHealthStrip(bundle);

  // EIA panels — live charts
  await safeRender('storage',     () => renderStoragePanel(document.getElementById('panel-storage'), bundle));
  await safeRender('balance',     () => renderBalancePanel(document.getElementById('panel-balance'), bundle));

  // Baker Hughes panels — live charts
  await safeRender('rigs',        () => renderRigsPanel(document.getElementById('panel-rigs'), bundle));
  await safeRender('basins',      () => renderBasinsPanel(document.getElementById('panel-basins'), bundle));

  // Interlude: Transatlantic Storage Divergence (cross-source derived metric)
  await safeRender('divergence',  () => renderDivergencePanel(document.getElementById('panel-divergence'), bundle));

  // Section 2: Basin Momentum Deep
  await safeRender('basin-table',    () => renderBasinTable(document.getElementById('panel-basin-table'), bundle));
  await safeRender('basin-scatter',  () => renderBasinScatter(document.getElementById('panel-basin-scatter'), bundle));
  await safeRender('basin-share',    () => renderBasinShare(document.getElementById('panel-basin-share'), bundle));
  await safeRender('basin-extremes', () => renderBasinExtremes(document.getElementById('panel-basin-extremes'), bundle));

  // Section 3: European Storage Context
  await safeRender('eu-storage',  () => renderEuStoragePanel(document.getElementById('panel-eu-storage'), bundle));

  // Section 4: US LNG Exports Tracker
  await safeRender('lng-total',   () => renderLngTotalPanel(document.getElementById('panel-lng-total'), bundle));
  await safeRender('lng-shares',  () => renderLngSharesPanel(document.getElementById('panel-lng-shares'), bundle));

  renderFooter(bundle);
}

main().catch((err) => {
  // This catch now only fires for boot-critical failures:
  // bundle load failure, manifest 404, or JSON parse errors.
  // Individual panel errors are caught by safeRender above.
  console.error('Blue Tide boot failure:', err);
  document.body.innerHTML = `
    <div class="boot-error">
      <h1>Blue Tide couldn't load the observatory.</h1>
      <pre>${String(err)}</pre>
      <p>If this persists, check that docs/data/manifest.json is reachable.</p>
    </div>`;
});
