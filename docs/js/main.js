import { loadBundle } from './data/bundle-loader.js';
import { renderHeader } from './components/header.js';
import { renderHealthStrip } from './components/health-strip.js';
import { renderPanelPlaceholder } from './components/panel-placeholder.js';
import { renderFooter } from './components/footer.js';

async function main() {
  const bundle = await loadBundle();

  renderHeader(bundle);
  renderHealthStrip(bundle);

  renderPanelPlaceholder('panel-storage', {
    title: 'Weekly Storage',
    subtitle: 'EIA — 5 regions, 5-year band',
    source: 'eia_storage',
    bundle,
  });

  renderPanelPlaceholder('panel-balance', {
    title: 'Supply & Demand Balance',
    subtitle: 'EIA — monthly production, consumption, storage flows',
    source: 'eia_supply',
    bundle,
  });

  renderPanelPlaceholder('panel-rigs', {
    title: 'US Rig Count Monitor',
    subtitle: 'Baker Hughes — weekly',
    source: 'baker_hughes_weekly',
    bundle,
  });

  renderPanelPlaceholder('panel-basins', {
    title: 'Basin Momentum',
    subtitle: 'Baker Hughes — by basin, 4-week delta',
    source: 'baker_hughes_weekly',
    bundle,
  });

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
