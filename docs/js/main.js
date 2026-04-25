import { loadBundle } from './data/bundle-loader.js';
import { renderHeader } from './components/header.js';
import { renderHealthStrip } from './components/health-strip.js';
import { renderFooter } from './components/footer.js';
import { renderStoragePanel } from './panels/storage.js';
import { renderBalancePanel } from './panels/balance.js';
import { renderRigsPanel } from './panels/rigs.js';
import { renderBasinsPanel } from './panels/basins.js';

async function main() {
  const bundle = await loadBundle();

  renderHeader(bundle);
  renderHealthStrip(bundle);

  // EIA panels — live charts
  renderStoragePanel(document.getElementById('panel-storage'), bundle);
  renderBalancePanel(document.getElementById('panel-balance'), bundle);

  // Baker Hughes panels — live charts
  renderRigsPanel(document.getElementById('panel-rigs'), bundle);
  renderBasinsPanel(document.getElementById('panel-basins'), bundle);

  renderFooter(bundle);
}

main().catch((err) => {
  console.error('Blue Tide boot failure:', err);
  document.body.innerHTML = `
    <div class="boot-error">
      <h1>Blue Tide couldn't load the observatory.</h1>
      <pre>${String(err)}</pre>
      <p>If this persists, check that docs/data/manifest.json is reachable.</p>
    </div>`;
});
