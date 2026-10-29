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
import { renderLngFeedSubstitutionPanel } from './panels/lng-feed-substitution.js';
import { renderTerminalDowntimePanel } from './panels/lng-terminal-downtime.js';
import { renderBasinEgressPanel } from './panels/basin-egress.js';
import { renderLngComparisonPanel } from './panels/lng-comparison.js';

import { applyRangeCaveat, computeSeriesDateRange } from './util/range-state.js';
import { initChartExports } from './util/chart-snapshot.js';
import { foldMethodNotes, initChapterSections, updateChapterSections } from './components/chapter-sections.js';

/** Section key → the DOM id of the panel that section renders into. */
const SECTION_PANEL_ID = {
  lng: 'panel-lng-fleet',
  'lng-substitution': 'panel-lng-feed-substitution',
};

/**
 * The panel element a section name renders into.
 *
 * Most keys are the panel id without its prefix, but five are not: the fleet
 * section draws `panel-lng-fleet`, substitution `panel-lng-feed-substitution`,
 * egress `panel-basin-egress` (while its key is 'basin-egress'), and the two
 * storage panels are keyed 'storage' and 'eu-storage'. Falling back through the
 * chapter export's own list keeps a renamed panel from silently losing its
 * caveat and its folded method note.
 *
 * @param {string} name section key
 * @returns {HTMLElement|null}
 */
function sectionPanelEl(name) {
  const ids = [
    SECTION_PANEL_ID[name],
    `panel-${name}`,
    name,
    // 'basin-egress' renders panel-basin-egress, which the key already covers;
    // 'lng' and 'lng-substitution' are covered above.
  ].filter(Boolean);
  for (const id of ids) {
    const el = document.getElementById(id);
    if (el) return el;
  }
  return null;
}

/**
 * Derive the LNG range-caveat's history facts from the actual data in the bundle,
 * rather than from hardcoded literals that go stale as the dataset grows.
 *
 * 'gulf_south' is the shortest-history source shared by the LNG sections
 * (fleet, feedgas, substitution, downtime) and is therefore the binding
 * constraint on how far back a range selection can honestly go.
 *
 * @param {any} bundle
 * @returns {{earliestStr: string, latestStr: string, dayCount: number, sourceLabel: string}|null}
 */
function getLngCaveatSeriesInfo(bundle) {
  const rows = bundle?.sources?.gulf_south?.data;
  const range = computeSeriesDateRange(rows, 'period');
  if (!range) return null;
  return { ...range, sourceLabel: 'LNG feedgas' };
}

/**
 * Render a single panel inside its own try/catch so that a failure
 * in one panel cannot propagate to main().catch and replace the whole page.
 *
 * @param {string} name - human-readable panel name (used in error log + fallback UI)
 * @param {Function} renderFn - zero-argument async-capable function that does the render
 * @param {any} [bundle] - dashboard data bundle, used to derive range-caveat facts for LNG panels
 */
export async function safeRender(name, renderFn, bundle = null) {
  try {
    await renderFn();

    // Prompt Y §04 / Y3: Range exceeding history surfaces honest caveat rather than silent clipping
    if (typeof window !== 'undefined' && window.location) {
      // Not every section key equals its panel id ('lng' renders panel-lng-fleet,
      // 'lng-substitution' renders panel-lng-feed-substitution, 'basin-egress'
      // renders panel-basin-egress). Deriving the element from the name alone
      // left those panels silently caveat-free.
      const panelEl = document.getElementById(SECTION_PANEL_ID[name] || `panel-${name}`) || document.getElementById(name);
      const seriesInfo = bundle ? getLngCaveatSeriesInfo(bundle) : null;
      applyRangeCaveat(panelEl, name, window.location.search, seriesInfo);
    }

    // Panels print a method / provenance note under their charts. Fold the long
    // ones behind a disclosure once the panel has drawn, so the numbers lead and
    // the audit trail is one click away instead of a wall of mono type.
    if (typeof document !== 'undefined') {
      const caveatEl = sectionPanelEl(name);
      if (caveatEl) foldMethodNotes(caveatEl);
      // Blocks a panel tagged for another section are moved now, with their
      // charts already drawn — moving a node re-fires its redraw observers, so
      // lifting a half-rendered block would leave an empty frame behind.
      updateChapterSections(document);
    }
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
  await safeRender(sectionName, renderFn, bundle);
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
  'lng-substitution': ['gulf_south', 'enbridge', 'bhe', 'cheniere', 'kinder_morgan'],
  'lng-downtime': ['gulf_south', 'enbridge', 'bhe', 'cheniere', 'kinder_morgan'],
  'basin-egress': ['gulf_south'],
};

async function main() {
  // Split each chapter into anchored sub-sections before any panel renders, so
  // every deferred observer watches an element that is in its final place.
  try {
    initChapterSections(document);
  } catch (err) {
    // Enhancement only: a failure here leaves the flat panel stack intact.
    console.error('[sections] chapter sections unavailable, panels stacked flat:', err);
  }

  // One delegated listener turns every panel's [data-chart-export] button into a
  // download; panels register their plotted points as they draw.
  initChartExports(document);

  // Boot parses CORE sources only (~1 MB combined): header + health strip +
  // the first paint sections. Everything else loads per-section below.
  const bundle = await loadBundle();

  // Hand the live bundle to the page-level chapter export before any awaited
  // panel work, so a download is never built from a second, possibly newer
  // fetch than the numbers on screen.
  if (typeof window !== 'undefined' && window.dispatchEvent) {
    window.dispatchEvent(new CustomEvent('bluetide:bundle', { detail: bundle }));
  }

  renderHeader(bundle);
  renderHealthStrip(bundle);

  // EIA & Baker Hughes core panels — render immediately using pre-fetched core sources
  safeRender('storage', () => renderStoragePanel(document.getElementById('panel-storage'), bundle), bundle);
  safeRender('balance', () => renderBalancePanel(document.getElementById('panel-balance'), bundle), bundle);
  safeRender('rigs', () => renderRigsPanel(document.getElementById('panel-rigs'), bundle), bundle);
  safeRender('basins', () => renderBasinsPanel(document.getElementById('panel-basins'), bundle), bundle);

  /**
   * Defer below-the-fold sections until scrolled near viewport or browser is idle.
   * Prevents 10+ MB of non-core shards from blocking initial render.
   *
   * Chapters (js/components/chapter-nav.js) changed what "below the fold" means.
   * A panel inside a hidden chapter is never intersecting, but the idle timer
   * would still fire and pull its source shard — so opening the LNG chapter also
   * fetched GIE AGSI, eia_lng_exports and every other shard, and a reader who
   * only wants feedgas paid for the whole observatory. The idle path now requires
   * the panel to be in the visible chapter; the scroll path is unchanged, and
   * when chapter navigation is absent (no JS, or the module failed to load)
   * everything is 'visible' and the legacy single-scroll behaviour is identical.
   */
  function deferSection(panelId, sourceKeys, sectionName, renderFn) {
    const el = document.getElementById(panelId);
    let triggered = false;

    const execute = () => {
      if (triggered) return;
      triggered = true;
      lazySection(bundle, sourceKeys, sectionName, renderFn);
    };

    const isInvisible = () => {
      const chapter = el?.closest?.('[data-chapter]');
      return Boolean(chapter && chapter.hidden);
    };

    const executeIfVisible = () => {
      if (isInvisible()) return;
      execute();
    };

    // Selection made after boot: a chapter hidden at idle time gets its panels
    // when the reader actually opens it.
    const onChapterChange = () => {
      if (!isInvisible()) execute();
    };
    if (typeof window !== 'undefined' && window.addEventListener) {
      window.addEventListener('bluetide:chapter', onChapterChange);
    }

    if (el && typeof IntersectionObserver !== 'undefined') {
      const obs = new IntersectionObserver((entries) => {
        if (entries.some((e) => e.isIntersecting)) {
          obs.disconnect();
          execute();
        }
      }, { rootMargin: '400px' });
      obs.observe(el);

      // Idle fallback so background panels eventually load without interaction.
      // Gated on visibility so tab switching does not prefetch other chapters.
      if (typeof requestIdleCallback === 'function') {
        requestIdleCallback(executeIfVisible, { timeout: 3500 });
      } else {
        setTimeout(executeIfVisible, 2000);
      }
    } else {
      setTimeout(executeIfVisible, 50);
    }
  }

  // Interlude: Transatlantic Storage Divergence (cross-source derived metric)
  deferSection('panel-divergence', SECTIONS.divergence, 'divergence', () =>
    renderDivergencePanel(document.getElementById('panel-divergence'), bundle));

  // Section 2: Basin Momentum Deep
  deferSection('panel-basin-table', SECTIONS['basin-table'], 'basin-table', () =>
    renderBasinTable(document.getElementById('panel-basin-table'), bundle));
  deferSection('panel-basin-scatter', SECTIONS['basin-scatter'], 'basin-scatter', () =>
    renderBasinScatter(document.getElementById('panel-basin-scatter'), bundle));
  deferSection('panel-basin-share', SECTIONS['basin-share'], 'basin-share', () =>
    renderBasinShare(document.getElementById('panel-basin-share'), bundle));
  deferSection('panel-basin-extremes', SECTIONS['basin-extremes'], 'basin-extremes', () =>
    renderBasinExtremes(document.getElementById('panel-basin-extremes'), bundle));

  // Section 3: European Storage Context
  deferSection('panel-eu-storage', SECTIONS['eu-storage'], 'eu-storage', () =>
    renderEuStoragePanel(document.getElementById('panel-eu-storage'), bundle));

  // Section 4: US LNG Exports Tracker
  deferSection('panel-lng-total', SECTIONS['lng-total'], 'lng-total', () =>
    renderLngTotalPanel(document.getElementById('panel-lng-total'), bundle));
  deferSection('panel-lng-shares', SECTIONS['lng-shares'], 'lng-shares', () =>
    renderLngSharesPanel(document.getElementById('panel-lng-shares'), bundle));

  // Section 5: LNG Feedgas Observatory
  let activeTerminalId;
  // Whether the LNG section has ever been drawn. It is drawn by the deferred
  // observer when the chapter first becomes visible, and must not be drawn a
  // second time by the chapter-change listener below (two fleet renders in one
  // frame is wasted work, and a double scrollIntoView fights the reader).
  let lngDrawn = false;
  const fleetEl = document.getElementById('panel-lng-fleet');
  const heroEl = document.getElementById('panel-lng-feedgas');

  /**
   * Rebuild the whole fleet grid + hero chart for the current selection.
   *
   * @param {{scroll?: boolean}} [opts] scroll true only for a reader-initiated
   *   terminal pick; a chapter change must not yank the viewport.
   */
  function renderLngSection({ scroll = false } = {}) {
    const handleSelect = (id) => {
      activeTerminalId = id;
      renderLngSection({ scroll: true });
    };
    renderLngFleetOverview(fleetEl, bundle, {
      activeTerminalId,
      onSelect: handleSelect,
    });
    renderLngFeedgasPanel(heroEl, bundle, activeTerminalId, {
      onSelect: handleSelect,
    });
    lngDrawn = true;
    if (scroll && heroEl) heroEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  // chapter-nav.js announces a genuine chapter change (`bluetide:chapter-changed`)
  // separately from the boot-time `bluetide:chapter` dispatch, because the fleet
  // aggregate and the divergence interlude compute from sources the boot already
  // parsed: they render once and would otherwise keep showing boot-era numbers
  // for the rest of the session. Nothing was listening for that event, so the
  // list of such panels in chapter-nav.js was inert.
  const chapterRefresh = {
    'lng-fleet': () => {
      if (lngDrawn) renderLngSection();
    },
    divergence: () => renderDivergencePanel(document.getElementById('panel-divergence'), bundle),
  };
  if (typeof window !== 'undefined' && window.addEventListener) {
    window.addEventListener('bluetide:chapter-changed', () => {
      for (const [panelId, refresh] of Object.entries(chapterRefresh)) {
        const el = document.getElementById(`panel-${panelId}`);
        // An empty panel has not rendered yet; its deferred observer will do it.
        // One DOM test covers both 'lng-fleet' and 'lng-feedgas' because the
        // fleet grid and the hero chart are rendered by the same function.
        if (!el || !el.childElementCount) continue;
        try {
          refresh();
        } catch (err) {
          console.error(`[${panelId}] chapter refresh failed:`, err);
        }
      }
    });
  }
  deferSection('panel-lng-fleet', SECTIONS.lng, 'lng-fleet', () => renderLngSection());
  deferSection('panel-lng-comparison', SECTIONS.lng, 'lng-comparison', () =>
    renderLngComparisonPanel(document.getElementById('panel-lng-comparison'), bundle)
  );

  // Section 6: Basin Egress
  deferSection('panel-basin-egress', SECTIONS['basin-egress'], 'basin-egress', () =>
    renderBasinEgressPanel(document.getElementById('panel-basin-egress'), bundle)
  );

  // Section 7: Feed Substitution
  deferSection('panel-lng-feed-substitution', SECTIONS['lng-substitution'], 'lng-substitution', () =>
    renderLngFeedSubstitutionPanel(document.getElementById('panel-lng-feed-substitution'), bundle)
  );

  // Section 8: Terminal Downtime
  deferSection('panel-lng-downtime', SECTIONS['lng-downtime'], 'lng-downtime', () =>
    renderTerminalDowntimePanel(document.getElementById('panel-lng-downtime'), bundle)
  );

  renderFooter(bundle);

  // Fire the observer once for the chapter that is visible at boot, so a reader
  // who lands on #chapter=lng does not wait for a scroll event.
  if (typeof window !== 'undefined' && window.dispatchEvent) {
    window.dispatchEvent(new Event('bluetide:chapter'));
  }
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
