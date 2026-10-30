/**
 * Chapter sections — the sub-panel layer inside a chapter.
 *
 * Chapters fixed the top level (six domains instead of one 18-panel scroll), but
 * each chapter still ran its panels together: Overview alone was four panels plus
 * an interlude, and LNG stacked a fleet grid, a hero chart, a comparison, a
 * routing panel and a footnote into one unbroken column. This module gives a
 * chapter real sections.
 *
 * How it works
 *   1. A panel's top-level blocks are lifted into anchored sections. Each block
 *      says where it belongs with `data-chapter-section="<id>"`:
 *        - a block without the attribute is the panel's own reading, grouped by
 *          the panel's entry in SECTION_ORDER;
 *        - a block with one is a separate reading (a companion chart, a
 *          breakdown, a footnote) and becomes its own section.
 *      The attribute is written by the panel at append time (see
 *      `components/panel-base.js` → `appendSection`), so a block is claimed by
 *      name and the DOM order of the panel never decides where it lands. A
 *      renamed or forgotten tag leaves the block inside its panel — visible,
 *      just not lifted — instead of silently reassigning it to a neighbour.
 *   2. The blocks move, they are not recreated, so ids, observers and rendered
 *      charts survive.
 *   3. A jump list of the chapter's sections is inserted at the top of the
 *      chapter, so a chapter with five readings is navigable without scrolling.
 *   4. Everything is idempotent: `initChapterSections` wraps the chapter, and
 *      `updateChapterSections` (re-run by main.js after a panel renders or a
 *      terminal is picked) only re-lifts blocks that appeared or changed.
 *
 * Progressive enhancement, same rule as chapter-nav.js: without JS every panel
 * keeps its authored order and renders as before, and a module failure leaves
 * `main` untouched. Vanilla JS only (project rule: zero TypeScript syntax).
 */

/**
 * @typedef {Object} SectionDef
 * @property {string} id       stable anchor id suffix
 * @property {string} label    jump-list text
 * @property {string} [title]  heading text; defaults to label
 * @property {string} [blurb]  one line under the heading
 * @property {string[]} [panels] panel ids this section owns; when omitted the
 *   section is only ever filled by blocks the panels tag with its id
 * @property {boolean} [labelHidden] the panel prints the heading itself; show
 *   the section title to assistive tech and the jump list only
 * @property {boolean} [pair] the section draws a two-column row (a panel and
 *   its companion), stacked below 1100px
   */

/**
 * Section layout per chapter, in the order the jump list reads.
 *
 * Ids are stable: they are the `?section=` deep-link values and the scroll
 * targets from elsewhere on the page (the feedgas chart jumps to reliability).
 * A section whose content is tagged by a panel needs no `panels` entry; the
 * panel does not need to be listed in the chapter either.
 *
 * @type {Readonly<Record<string, ReadonlyArray<SectionDef>>>}
 */
export const SECTION_ORDER = Object.freeze({
  overview: [
    {
      id: 'overview-storage',
      label: 'Storage',
      title: 'Working Gas in Storage',
      blurb: 'Lower 48 and regional stocks against the calendar-matched norm',
      panels: ['panel-storage'],
    },
    {
      id: 'overview-cover',
      label: 'Days of cover',
      title: 'Days of Cover',
      blurb: 'Working gas divided by current consumption · weekly',
    },
    {
      id: 'overview-regions',
      label: 'Regions & season',
      title: 'Regional Decomposition & Season to Date',
      blurb: 'Which regions built or drew last week, and the season so far',
      // Filled only by the two blocks storage.js tags for it (season, then
      // regions). It must not own panel-storage, which the section above does.
    },
    {
      id: 'overview-balance',
      label: 'Supply / demand',
      title: 'Supply & Demand Balance',
      blurb: 'EIA supply against the storage-implied change · the residual is the feedgas floor',
      panels: ['panel-balance'],
    },
    {
      id: 'overview-bridge',
      label: 'Balance bridge',
      title: 'Balance Bridge',
      blurb: 'The latest complete month, leg by leg, with the imbalance named',
    },
    {
      id: 'overview-activity',
      label: 'Rig activity',
      title: 'Rig Activity',
      blurb: 'Horizontal gas and oil rigs · the leading supply signal',
      panels: ['panel-rigs', 'panel-basins'],
    },
    {
      id: 'overview-divergence',
      label: 'Transatlantic',
      title: 'Transatlantic Storage Divergence',
      blurb: 'US vs European storage tightness on one scale',
      panels: ['panel-divergence'],
    },
  ],

  basins: [
    {
      id: 'basin-momentum',
      label: 'Momentum table',
      title: 'Basin Momentum',
      blurb: 'Every basin ranked · sortable',
      panels: ['panel-basin-table'],
    },
    {
      id: 'basin-comparison',
      label: 'Scatter & shares',
      title: 'Gas vs Oil Positioning',
      blurb: 'Where each basin sits, and how the rig count is shared',
      panels: ['panel-basin-scatter', 'panel-basin-share'],
      pair: true,
    },
    {
      id: 'basin-extremes',
      label: 'Extremes',
      title: 'Extremes',
      blurb: 'The sharpest movers on either side',
      panels: ['panel-basin-extremes'],
    },
  ],

  lng: [
    {
      id: 'lng-fleet',
      label: 'Fleet',
      title: 'Terminal Fleet',
      blurb: 'Every monitored terminal at a glance · pick one to drive the charts below',
      panels: ['panel-lng-fleet'],
    },
    {
      id: 'lng-hero',
      label: 'Feedgas',
      title: 'Feedgas Profile',
      blurb: 'Selected terminal against its nameplate and its own history',
      panels: ['panel-lng-feedgas'],
    },
    {
      id: 'lng-hero-revisions',
      label: 'Cycle revisions',
      title: 'Intraday Cycle Revisions',
      blurb: 'How the selected terminal has revised its latest gas day, cycle by cycle',
    },
    {
      id: 'lng-hero-note',
      label: 'Meter & updates',
      title: 'Meter & Last Updated',
      blurb: 'What the meter measures, what it does not see, and which gas day is loaded',
    },
    {
      id: 'lng-comparison',
      label: 'Comparison',
      title: 'Terminal Comparison',
      blurb: 'Terminals side by side on the same scale',
      panels: ['panel-lng-comparison'],
    },
  ],

  flows: [
    {
      id: 'flows-egress',
      label: 'Haynesville egress',
      title: 'Basin Egress · Haynesville Take-Away',
      blurb: 'Scheduled flows out of the Haynesville via Gulf South · the supply-side read',
      panels: ['panel-basin-egress'],
    },
    {
      id: 'flows-substitution',
      label: 'Feed substitution',
      title: 'Feed Substitution at Multi-Feed Terminals',
      blurb: 'Routing vs demand · a share flip at constant total is re-routing, not lost volume',
      panels: ['panel-lng-feed-substitution'],
    },
  ],

  reliability: [
    {
      id: 'reliability-downtime',
      label: 'Downtime',
      title: 'Terminal Downtime / Turnaround Indicator',
      blurb: 'Operational outages, cargo-driven idle and commissioning ramps · false-positive guarded',
      panels: ['panel-lng-downtime'],
    },
  ],

  macro: [
    {
      id: 'exports-total',
      label: 'Exports',
      title: 'US LNG Exports · EIA Monthly',
      blurb: 'Total monthly volume over 36 months',
      panels: ['panel-lng-total'],
    },
    {
      id: 'exports-shares',
      label: 'Destinations',
      title: 'Destination Shares',
      blurb: 'Regional allocation of US LNG exports · % of total',
      panels: ['panel-lng-shares'],
    },
    {
      id: 'europe-storage',
      label: 'European storage',
      title: 'European Storage Context · GIE AGSI+',
      blurb: '10 countries · daily updates · multi-year history',
      panels: ['panel-eu-storage'],
    },
    {
      id: 'europe-method',
      label: 'Method note',
      title: 'How the European Figures Are Built',
      blurb: 'Resolved capacities, aggregate weighting and the limits of the sample',
      panels: ['panel-eu-storage'],
    },
  ],

});

/**
 * Attribute a panel puts on a block (and a section definition declares on the
 * chapter) to say which section the block belongs to.
 */
export const SECTION_ATTR = 'data-chapter-section';

/** CSS class of the emptied wrapper a panel leaves behind once its blocks move. */
const CONSUMED_CLASS = 'panel--consumed';

/**
 * Heading for a panel that is not declared in SECTION_ORDER: the panel's own
 * `<h2>`, or its id if the panel has not rendered its chrome yet.
 *
 * @param {HTMLElement} panelEl
 * @returns {string}
 */
function fallbackLabel(panelEl) {
  const h2 = panelEl.querySelector('.panel-header__title, h2');
  const text = h2 && h2.textContent ? h2.textContent.trim() : '';
  return text || panelEl.id;
}

/**
 * Section id for a chapter/section pair. Kept in one place so the URL helper,
 * the anchor href and the scroll lookup cannot drift apart.
 *
 * @param {string} chapterId
 * @param {string} sectionId
 * @returns {string}
 */
export function sectionDomId(chapterId, sectionId) {
  return `sec-${chapterId}-${sectionId.replace(/^(overview|basin|flows|reliability|macro|lng)-/, '')}`;
}

/**
 * URL for a section selection, preserving the chapter and every other param.
 *
 * @param {string} pathname
 * @param {string} search
 * @param {string} chapterId
 * @param {string|null} sectionId  null clears the section
 * @returns {string} relative URL
 */
export function sectionUrl(pathname, search, chapterId, sectionId) {
  const params = new URLSearchParams(String(search || ''));
  params.set('chapter', chapterId);
  if (sectionId) params.set('section', sectionId);
  else params.delete('section');
  return `${pathname}?${params.toString()}`;
}

/**
 * Resolve `?section=` against a chapter. Unknown, empty or foreign values fall
 * back to null (no section selected) rather than throwing.
 *
 * @param {string} chapterId
 * @param {string} search
 * @returns {string|null}
 */
export function resolveSection(chapterId, search) {
  const value = new URLSearchParams(String(search || '')).get('section');
  if (!value) return null;
  const defs = SECTION_ORDER[chapterId] || [];
  return defs.some((d) => d.id === value) ? value : null;
}

/**
 * Build the section shell: heading (with its anchor), optional blurb, body.
 *
 * @param {string} chapterId
 * @param {SectionDef} def
 * @returns {HTMLElement}
 */
function buildSectionEl(chapterId, def) {
  const el = document.createElement('section');
  el.className = 'chapter-section';
  el.id = sectionDomId(chapterId, def.id);
  el.dataset.sectionId = def.id;

  if (def.pair) el.dataset.pair = '1';

  const title = def.title || def.label || def.id;
  const head = document.createElement('header');
  head.className = 'chapter-section__head';
  if (def.labelHidden) head.className += ' chapter-section__head--sr';
  head.innerHTML =
    `<h3 class="chapter-section__title">${escapeHtml(title)}` +
    `<a class="chapter-section__anchor" href="#${el.id}" aria-label="Link to ${escapeHtml(title)}">#</a></h3>` +
    (def.blurb ? `<p class="chapter-section__blurb">${escapeHtml(def.blurb)}</p>` : '');
  head.querySelector('.chapter-section__anchor').addEventListener('click', (ev) => {
    if (ev.metaKey || ev.ctrlKey || ev.shiftKey || ev.altKey) return;
    ev.preventDefault();
    el.dispatchEvent(new CustomEvent('bluetide:section', { bubbles: true, detail: { sectionId: def.id } }));
    scrollToSection(el.closest('[data-chapter]').getAttribute('data-chapter'), def.id, el.ownerDocument.defaultView);
  });
  el.appendChild(head);

  const body = document.createElement('div');
  body.className = def.pair ? 'chapter-section__body deep-grid' : 'chapter-section__body';
  el.appendChild(body);
  return el;
}

/**
 * Jump list for a chapter's sections. Hidden while a chapter has only one.
 *
 * @param {string} chapterId
 * @param {ReadonlyArray<SectionDef>} defs
 * @param {{heading?: string}} [opts]
 * @returns {HTMLElement|null}
 */
function buildSectionNav(chapterId, defs, { heading = 'In this chapter' } = {}) {
  if (defs.length < 2) return null;
  const nav = document.createElement('nav');
  nav.className = 'chapter-sections';
  nav.dataset.chapter = chapterId;
  nav.setAttribute('aria-label', heading);
  nav.hidden = true;

  const label = document.createElement('span');
  label.className = 'chapter-sections__label';
  label.textContent = heading;
  nav.appendChild(label);

  defs.forEach((d) => {
    const a = document.createElement('a');
    a.className = 'chapter-sections__link';
    a.href = `#${sectionDomId(chapterId, d.id)}`;
    a.dataset.sectionId = d.id;
    a.textContent = d.label || d.title || d.id;
    a.hidden = d.labelHidden === true;
    a.addEventListener('click', (ev) => {
      // Left-click only: a modified click still opens/copies the anchor.
      if (ev.metaKey || ev.ctrlKey || ev.shiftKey || ev.altKey) return;
      // preventDefault is not enough on its own: it stops navigation but the
      // browser still treats the href as an in-page jump and lands the element
      // under the sticky tab bar. Taking the scroll here (and keeping the URL
      // via replaceState) means one owner of the scroll position.
      ev.preventDefault();
      scrollToSection(chapterId, d.id, window);
    });
    nav.appendChild(a);
  });
  return nav;
}

/**
 * Scroll to a section inside the active chapter, under the sticky tab bar, and
 * record the choice in the URL so it is shareable.
 *
 * @param {string} chapterId
 * @param {string} sectionId
 * @param {Window} win
 * @returns {void}
 */
export function scrollToSection(chapterId, sectionId, win) {
  const el = win.document.getElementById(sectionDomId(chapterId, sectionId));
  if (el) {
    const bar = win.document.getElementById('chapter-nav');
    const barH = bar ? bar.getBoundingClientRect().height : 0;
    const top = el.getBoundingClientRect().top + win.scrollY - barH - 12;
    win.scrollTo({ top: Math.max(0, top), behavior: 'smooth' });
  }
  const url = sectionUrl(win.location.pathname, win.location.search, chapterId, sectionId);
  win.history.replaceState({ ...(win.history.state || {}), section: sectionId }, '', url);
  win.document.querySelectorAll('.chapter-sections').forEach((nav) => {
    if (nav.dataset.chapter !== chapterId) return;
    nav.querySelectorAll('.chapter-sections__link').forEach((a) => {
      a.classList.toggle('is-active', a.dataset.sectionId === sectionId);
    });
  });
}

/* ================================================================== */
/*  Which section a panel's top-level block belongs to                 */
/* ================================================================== */

/**
 * Section a block names for itself, or null when it is the panel's own content.
 *
 * @param {HTMLElement} el
 * @returns {string|null}
 */
function declaredSection(el) {
  const value = el.getAttribute && el.getAttribute(SECTION_ATTR);
  return value ? value : null;
}

/**
 * What is left of a panel now that some of its blocks live in their own
 * sections: an emptied wrapper still takes its flex gap, and a panel that has
 * given every block away would print an empty frame.
 *
 * @param {HTMLElement} panelEl
 * @returns {number} sections the panel's blocks were lifted into
 */
function settlePanel(panelEl) {
  const lifted = panelEl.querySelectorAll(':scope > .chapter-lifted, :scope > * > .chapter-lifted');
  const wrappers = new Set();
  lifted.forEach((el) => {
    if (el.parentElement && el.parentElement !== panelEl) wrappers.add(el.parentElement);
  });
  wrappers.forEach((w) => {
    if (!w.childElementCount) w.remove();
  });

  let count = 0;
  panelEl.querySelectorAll(`[${SECTION_ATTR}]`).forEach((el) => {
    if (el.classList.contains('chapter-lifted')) count += 1;
  });
  const gaveEverything = count > 0 && panelEl.querySelectorAll(
    ':scope > header, :scope > :not([hidden]):not(:empty)',
  ).length === 0;
  panelEl.classList.toggle(CONSUMED_CLASS, gaveEverything);
  if (gaveEverything) panelEl.hidden = true;
  else panelEl.hidden = false;
  return count;
}

/* ================================================================== */
/*  Init: wrap every chapter in its sections                           */
/* ================================================================== */

/**
 * Split every chapter into its sections and insert the jump lists.
 *
 * Idempotent: a chapter already split (marked with `data-sections="1"`) is
 * skipped, so a re-run cannot nest wrappers around wrappers.
 *
 * @param {Document} doc
 * @returns {number} chapters that were split
 */
export function initChapterSections(doc) {
  const chapters = Array.from(doc.querySelectorAll('main.chapter-root > [data-chapter]'));
  /** @type {Map<HTMLElement, Map<string, string>>} */
  const fallbacksByChapter = new Map();
  let split = 0;

  chapters.forEach((chapterEl) => {
    if (chapterEl.dataset.sections === '1') return;
    const chapterId = chapterEl.getAttribute('data-chapter');
    const declared = SECTION_ORDER[chapterId] || [];

    /** @type {Map<string, HTMLElement>} section id → element, built lazily in `declared` order */
    const sectionEls = new Map();

    // A panel whose content a declared section already owns is not a section of
    // its own any more. Everything else the chapter authored stays visible as
    // its own section — a panel that only tags its blocks names the sections
    // that hold them, which is checked after the first hydration, below.
    const ownedPanels = new Set(declared.flatMap((d) => d.panels || []));
    const authoredPanels = Array.from(chapterEl.children).filter(
      (child) => child instanceof HTMLElement && child.id && child.id.startsWith('panel-'),
    );

    // 1. One section element per declared section, in declared order.
    declared.forEach((def) => {
      sectionEls.set(def.id, buildSectionEl(chapterId, def));
    });

    // 2. A panel that no declared section owns is its own single-panel section,
    //    in authored order, so a new panel never vanishes from the chapter.
    /** @type {Array<{key: string, el: HTMLElement}>} */
    const order = declared.map((def) => ({ key: def.id, el: sectionEls.get(def.id) }));

    /** @type {Map<string, string>} panel id → the id of its own fallback section */
    const fallbackSections = new Map();

    authoredPanels.forEach((panelEl) => {
      if (ownedPanels.has(panelEl.id)) return;
      const id = panelEl.id.replace(/^panel-/, '');
      // The panel prints its own header, so the section heading is a label for
      // the accessibility tree and the jump list only.
      const el = buildSectionEl(chapterId, { id, label: '', labelHidden: true });
      el.setAttribute('data-single-panel', '1');
      el.querySelector('.chapter-section__body').appendChild(panelEl);
      fallbackSections.set(panelEl.id, id);
      order.push({ key: id, el });
    });

    // 3. Declared sections own their panels: move the panel in, and let the
    //    section fill with the panel's untagged blocks once it renders. A
    //    section that owns nothing is dropped again: it would only ever print
    //    an empty heading.
    const emptied = new Set();
    declared.forEach((def) => {
      const target = sectionEls.get(def.id);
      const body = target.querySelector('.chapter-section__body');
      (def.panels || []).forEach((id) => {
        const panelEl = doc.getElementById(id);
        if (!panelEl || !chapterEl.contains(panelEl)) return;
        if (panelEl.parentElement && panelEl.parentElement.dataset.sectionId) return;
        body.appendChild(panelEl);
      });
      if (!body.childElementCount) emptied.add(def.id);
    });
    emptied.forEach((id) => {
      sectionEls.get(id).remove();
      sectionEls.delete(id);
    });

    if (!order.length) return;

    // 4. Jump list + sections, in order, on the chapter's own surface.
    const fragment = doc.createDocumentFragment();
    // The jump list follows the sections on the page, not the declarations.
    const nav = buildSectionNav(chapterId, declared.filter((def) => sectionEls.has(def.id)));
    if (nav) fragment.appendChild(nav);
    order.forEach(({ el }) => fragment.appendChild(el));

    chapterEl.replaceChildren(fragment);
    chapterEl.dataset.sections = '1';
    // Panel id → its fallback section id, for the pass below. Kept off the DOM
    // so nothing serialises it.
    fallbacksByChapter.set(chapterEl, fallbackSections);
    split += 1;
  });

  hydrateChapters(doc);

  // A panel that gave every block to declared sections is not a reading by
  // itself: its fallback section would print an empty frame. Those panels are
  // settled by the pass above, so the check has to come after it.
  chapters.forEach((chapterEl) => {
    const fallbacks = fallbacksByChapter.get(chapterEl);
    if (!fallbacks) return;
    fallbacks.forEach((sectionId, panelId) => {
      const panelEl = doc.getElementById(panelId);
      if (!panelEl || !panelEl.classList.contains(CONSUMED_CLASS)) return;
      const sectionEl = chapterEl.querySelector(
        `.chapter-section[data-section-id="${cssEscape(sectionId)}"]`,
      );
      if (sectionEl) sectionEl.remove();
      const nav = chapterEl.querySelector('.chapter-sections');
      const link = nav && nav.querySelector(`.chapter-sections__link[data-section-id="${cssEscape(sectionId)}"]`);
      if (link) link.remove();
    });
  });

  // Public deep link: ?chapter=lng&section=lng-hero marks that entry before any
  // scrolling, and main.js lands the viewport on it.
  const active = doc.querySelector('main.chapter-root > [data-chapter]:not([hidden])');
  if (active) {
    const chapterId = active.getAttribute('data-chapter');
    const sectionId = resolveSection(chapterId, doc.defaultView?.location?.search || '');
    if (sectionId) {
      doc.querySelectorAll('.chapter-sections').forEach((nav) => {
        nav.querySelectorAll('.chapter-sections__link').forEach((a) => {
          a.classList.toggle('is-active', a.dataset.sectionId === sectionId);
        });
      });
    }
  }

  return split;
}

/* ================================================================== */
/*  Update: lift the blocks that rendered since the last pass          */
/* ================================================================== */

/**
 * Re-lift a chapter's blocks. Cheap enough to call after every panel render:
 * it only looks at panels that are not consumed yet and only moves blocks that
 * are still inside them.
 *
 * Safe to call before `initChapterSections` (it does nothing then) and safe to
 * call twice.
 *
 * @param {Document} doc
 * @returns {number} blocks that moved
 */
export function updateChapterSections(doc) {
  return hydrateChapters(doc);
}

/**
 * For every split chapter, move each panel's blocks into the section they name.
 *
 * @param {Document} doc
 * @returns {number} blocks that moved
 */
function hydrateChapters(doc) {
  if (globalThis.__sectionsDebug) globalThis.__sectionsDebug.push('hydrate');
  // The hosts below are found by selector on a DOM that panels and observers
  // mutate constantly, so a throw is contained per panel and coerced to the
  // null check the loop already makes: one unplaceable block must not stop the
  // rest of the chapter (or the render that triggered it) from settling.
  let moved = 0;
  // Deliberately not `[data-sections="1"]`: attribute selectors match the
  // serialised attribute, and `dataset.sections = '1'` serialises to
  // `data-sections="1"`, which that selector does read — but the chapter root
  // is only ever found by the same walk main.js and init use.
  Array.from(doc.querySelectorAll('main.chapter-root > [data-chapter]')).forEach((chapterEl) => {
    if (chapterEl.dataset.sections !== '1') return;
    const chapterId = chapterEl.getAttribute('data-chapter');

    Array.from(chapterEl.querySelectorAll('.panel')).forEach((panelEl) => {
        // Blocks a panel appended directly to itself, and blocks it appended to
        // its own chart/sidebar columns, are lifted the same way.
      const hosts = [
        panelEl,
        ...safeQuery(panelEl, ':scope > .panel-body, :scope > * > .panel-body'),
        // A panel body that is itself a wrapper (the LNG terminals) carries its
        // own chrome: its .panel-body lives one level deeper.
        ...safeQuery(panelEl, ':scope > .panel-body .panel-body'),
      ];
      // The panel's own section, and the body it lives in. Every host is inside
      // *some* section — the panels themselves are — so "is this block in a
      // section?" is true of unplaced blocks too and would stop every lift.
      // What marks a block as already placed is that it sits in the body of a
      // section that is not this panel's own (a declared section it was fed
      // into on an earlier pass); the identity check below is what separates
      // the two, since both bodies share `.chapter-section__body`.
      const ownSection = panelEl.closest('.chapter-section');
      const ownBody = ownSection
        ? ownSection.querySelector(':scope > .chapter-section__body')
        : null;
      hosts.forEach((host) => {
        if (!host) return;
        Array.from(host.children).forEach((child) => {
          if (!(child instanceof HTMLElement)) return;
          if (child.classList.contains('panel-chart') || child.classList.contains('panel-sidebar')) return;
          if (child.id && child.id.startsWith('panel-')) return;
          const placedIn = child.parentElement && child.parentElement.closest('.chapter-section');
          if (placedIn && placedIn !== ownSection && ownBody && !ownBody.contains(child)) return;
          const sectionId = declaredSection(child);
          if (!sectionId) return;
          const target = chapterEl.querySelector(
            `.chapter-section[data-section-id="${cssEscape(sectionId)}"] > .chapter-section__body`,
          );
          if (!target) {
            // A tag with no declared section in this chapter is a wiring bug,
            // not a reason to hide the block: leave it where it is.
            console.warn(`[sections] ${panelEl.id} tagged a block as "${sectionId}" but ${chapterId} declares no such section`);
            return;
          }
          target.appendChild(child);
          // `chapter-lifted` is what settlePanel counts; `panel-lifted` records
          // that the block travels on with its panel when the chapter is
          // re-split, so a lift is never mistaken for authored panel content.
          child.classList.add('chapter-lifted', 'panel-lifted');
          moved += 1;
          // An emptied wrapper would otherwise keep its flex gap. A column that
          // wraps the chart and sidebar is never removed: those are the panel's
          // frame, not a block's wrapper.
          const parent = host;
          if (
            parent !== panelEl &&
            !parent.childElementCount &&
            !parent.classList.contains('panel-chart') &&
            !parent.classList.contains('panel-sidebar') &&
            // An emptied .panel-body is the panel's own chrome — the tab layer
            // looks it up to render into — not a block's wrapper. Removing it
            // leaves a panel that can never draw again.
            !parent.classList.contains('panel-body')
          ) parent.remove();
        });
      });

      settlePanel(panelEl);
    });

    // A section with nothing in it (its blocks have not rendered, or the panel
    // failed) is not a reading yet: drop it from the page and the jump list.
    // A section that draws its panels as a pair keeps its two-column row.
    const sectionStates = syncSectionState(chapterEl);
    sectionStates.forEach(({ el, filled }) => {
      const body = el.querySelector('.chapter-section__body');
      if (body && el.dataset.pair === '1') {
        body.classList.toggle('deep-grid', filled > 1);
      }
    });
  });
  return moved;
}

/**
 * Hide empty sections and their jump-list entries, show the ones that filled,
 * and print the jump list only once two sections are real.
 *
 * @param {HTMLElement} chapterEl
 * @returns {Array<{el: HTMLElement, filled: number}>} each section and how many
 *   blocks it holds
 */
function syncSectionState(chapterEl) {
  const sections = Array.from(chapterEl.querySelectorAll('.chapter-section'));
  const states = [];
  let shown = 0;
  sections.forEach((el) => {
    const filled = el.querySelectorAll('.chapter-section__body > *').length;
    el.hidden = filled === 0;
    if (filled) shown += 1;
    states.push({ el, filled });
  });

  const nav = chapterEl.querySelector('.chapter-sections');
  if (!nav) return states;
  nav.querySelectorAll('.chapter-sections__link').forEach((a) => {
    const target = chapterEl.querySelector(
      `.chapter-section[data-section-id="${cssEscape(a.dataset.sectionId)}"]`,
    );
    a.hidden = !target || target.hidden;
  });
  const visibleLinks = Array.from(nav.querySelectorAll('.chapter-sections__link')).filter((a) => !a.hidden);
  nav.hidden = shown < 2 || visibleLinks.length < 2;
  return states;
}

/**
 * `CSS.escape` without needing the DOM API (older browsers, and the test
 * harness): section ids are `<slug>-<slug>`, which the attribute-string escape
 * below already handles.
 *
 * @param {string} value
 * @returns {string}
 */
/**
 * `querySelectorAll` that yields an empty list instead of throwing (a selector
 * engine that rejects `:scope`, an element detached mid-pass).
 *
 * @param {Element} el
 * @param {string} selector
 * @returns {Element[]}
 */
function safeQuery(el, selector) {
  try {
    return Array.from(el.querySelectorAll(selector));
  } catch {
    return [];
  }
}

function cssEscape(value) {
  return String(value).replace(/["\\]/g, '\\$&');
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}

/* ================================================================== */
/*  Method notes: fold the long ones behind a disclosure               */
/* ================================================================== */

/**
 * Fold long prose that is printed on a panel today — the method / provenance
 * note — behind a disclosure, so the numbers lead and the audit trail is one
 * click away instead of a wall of mono type under every chart.
 *
 * Only `.data-provenance` blocks are touched: they are, by construction, the
 * "how this was computed" notes. Every other element is left alone.
 *
 * @param {ParentNode} root
 * @returns {number} notes folded
 */
export function foldMethodNotes(root) {
  let folded = 0;
  root.querySelectorAll('.data-provenance').forEach((note) => {
    if (note.closest('.panel-method')) return;
    // A short note (a single sentence) reads fine inline; folding it costs a
    // click and saves nothing. The wall-of-text problem starts around 200 chars.
    const text = note.textContent || '';
    if (text.trim().length < 200) return;

    const details = root.ownerDocument.createElement('details');
    details.className = 'panel-method';
    const summary = root.ownerDocument.createElement('summary');
    summary.className = 'panel-method__summary';
    summary.textContent = 'Method & provenance';
    details.appendChild(summary);

    note.parentNode.insertBefore(details, note);
    details.appendChild(note);
    folded += 1;
  });
  return folded;
}
