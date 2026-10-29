/**
 * Chapter navigation for the observatory.
 *
 * The dashboard grew to 18 panels across eight domains in one continuous
 * scroll. That made the page excellent to read top-to-bottom and poor to
 * navigate: every panel competes for the same scroll, nothing is linkable, and
 * a reader who cares about LNG pays the load cost of the basin deep-dive.
 *
 * This module turns each domain <section> with `data-chapter` into a tab.
 *
 * Design constraints, in priority order:
 *   1. Progressive enhancement — every chapter stays visible if JS is absent or
 *      this module fails to load. Navigation is added, never required.
 *   2. Deep links — `#chapter=lng` selects a chapter, so a real finding can be
 *      sent to someone. Back/forward works.
 *   3. Idle panels of hidden chapters must not render. main.js defers panels
 *      until they scroll near the viewport, and hiding a chapter removes it from
 *      the viewport, so a hidden chapter's panels simply never fire. Selecting a
 *      chapter makes main.js's existing observer fire on its own.
 *
 * Vanilla JS only (project rule: zero TypeScript syntax in docs/js).
 */

/**
 * @typedef {Object} Chapter
 * @property {string} id       URL value and data-chapter attribute
 * @property {string} label    tab label
 * @property {string} blurb    one-line description under the label
 * @property {string[]} sources  bundle source keys this chapter reads; used by
 *                              the chapter export so a download is labelled with
 *                              its actual provenance
 */

/** @type {ReadonlyArray<Chapter>} */
export const CHAPTERS = Object.freeze([
  { id: 'overview', label: 'Overview', blurb: 'Storage, supply/demand balance, rigs', sources: ['eia_storage', 'eia_supply', 'baker_hughes_weekly', 'gie_agsi'] },
  { id: 'basins', label: 'Basins', blurb: 'Basin momentum, shares, extremes', sources: ['baker_hughes_weekly'] },
  { id: 'lng', label: 'LNG Feedgas', blurb: 'Fleet, hero terminal, comparison', sources: ['quorum', 'gulf_south', 'enbridge', 'gasnom', 'bhe', 'cheniere', 'kinder_morgan'] },
  { id: 'flows', label: 'Flows & Routing', blurb: 'Egress and feed substitution', sources: ['gulf_south', 'enbridge', 'bhe', 'cheniere', 'kinder_morgan'] },
  { id: 'reliability', label: 'Reliability', blurb: 'Downtime, outages, turnarounds', sources: ['gulf_south', 'enbridge', 'bhe', 'cheniere', 'kinder_morgan'] },
  { id: 'macro', label: 'Macro & Europe', blurb: 'EIA exports, GIE storage, divergence', sources: ['eia_lng_exports', 'gie_agsi'] },
]);

const STORAGE_KEY = 'bluetide:chapter';

/**
 * URL for a chapter selection.
 *
 * Uses the `?chapter=` query param rather than a hash fragment. main.js's deep
 * links (`?terminal=`, range presets) already live in the query string, and the
 * terminal chips rewrite `?${params}` — which *drops* whatever is in the hash. A
 * hash-held chapter therefore survived until the reader clicked a terminal and
 * then silently bounced them back to Overview on reload. The query param is read
 * by the same code path and is preserved by those rewrites.
 *
 * @param {string} pathname
 * @param {string} search
 * @param {string} id
 * @returns {string} relative URL
 */
export function chapterUrl(pathname, search, id) {
  const params = new URLSearchParams(String(search || ''));
  params.set('chapter', id);
  return `${pathname}?${params.toString()}`;
}

/**
 * A panel that only ever renders once would quietly show numbers from boot for
 * the rest of the session. The chapter itself changes, so its view model has to
 * be rebuilt from the current bundle — this is the list of those panels.
 *
 * @type {ReadonlyArray<string>}
 */
export const CHAPTER_REFRESH_PANELS = Object.freeze(['lng-fleet', 'lng-feedgas', 'divergence']);

/**
 * Read the chapter id from a location search/hash, falling back to the store.
 *
 * @param {string} search  location.search
 * @param {string} hash    location.hash
 * @param {string|null} stored  prior selection from localStorage
 * @returns {string} a valid chapter id
 */
export function resolveChapter(search, hash, stored) {
  const valid = new Set(CHAPTERS.map((c) => c.id));
  const fromHash = new URLSearchParams(String(hash || '').replace(/^#/, '')).get('chapter');
  const fromSearch = new URLSearchParams(String(search || '')).get('chapter');
  for (const candidate of [fromHash, fromSearch, stored]) {
    if (candidate && valid.has(candidate)) return candidate;
  }
  return CHAPTERS[0].id;
}

/**
 * Hide every chapter except `id`. Returns the sections that were shown.
 *
 * @param {Document|HTMLElement} root
 * @param {string} id
 * @returns {HTMLElement[]}
 */
export function applyChapter(root, id) {
  const sections = Array.from(root.querySelectorAll('[data-chapter]'));
  const shown = [];
  sections.forEach((el) => {
    const match = el.getAttribute('data-chapter') === id;
    el.hidden = !match;
    if (match) {
      shown.push(el);
      // The tabs declare role="tab", which obliges the content to declare
      // role="tabpanel" and be pointed at by their aria-controls. Without this
      // a screen reader announces "tab, selected" and then nothing — the
      // panels they selected are not associated with them at all.
      if (!el.id) el.id = `chapter-panel-${id}`;
      el.setAttribute('role', 'tabpanel');
      const tab = root.querySelector(`[data-chapter-id="${id}"]`);
      if (tab && el.id) tab.setAttribute('aria-controls', el.id);
      // Focus lands on the panel after a tab click so the reader's next Tab
      // walks the content they just revealed, not the rest of the tab bar.
      if (el.tabIndex < 0) el.tabIndex = -1;
    } else if (el.hasAttribute('role')) {
      el.removeAttribute('role');
    }
  });
  return shown;
}

/**
 * Build the tab bar. Returns the container element.
 *
 * @param {string} activeId
 * @param {(id: string) => void} onSelect
 * @returns {HTMLElement}
 */
export function buildChapterBar(activeId, onSelect) {
  const bar = document.createElement('nav');
  bar.className = 'chapter-bar';
  bar.setAttribute('aria-label', 'Observatory chapters');

  const track = document.createElement('div');
  track.className = 'chapter-bar__track';
  track.setAttribute('role', 'tablist');

  CHAPTERS.forEach((c) => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'chapter-tab';
    btn.dataset.chapterId = c.id;
    btn.setAttribute('role', 'tab');
    btn.setAttribute('aria-selected', String(c.id === activeId));
    btn.setAttribute('tabindex', c.id === activeId ? '0' : '-1');
    btn.innerHTML = `<span class="chapter-tab__label">${c.label}</span>
      <span class="chapter-tab__blurb">${c.blurb}</span>`;
    btn.setAttribute('aria-controls', `chapter-panel-${c.id}`);
    if (c.id === activeId) btn.classList.add('is-active');
    btn.addEventListener('click', () => onSelect(c.id));
    btn.addEventListener('keydown', (ev) => {
      if (ev.key !== 'ArrowRight' && ev.key !== 'ArrowLeft') return;
      ev.preventDefault();
      const idx = CHAPTERS.findIndex((x) => x.id === c.id);
      const next = CHAPTERS[(idx + (ev.key === 'ArrowRight' ? 1 : CHAPTERS.length - 1)) % CHAPTERS.length];
      onSelect(next.id);
      const el = bar.querySelector(`[data-chapter-id="${next.id}"]`);
      if (el) /** @type {HTMLElement} */ (el).focus();
    });
    track.appendChild(btn);
  });

  bar.appendChild(track);
  return bar;
}

/**
 * The chapter's own download button.
 *
 * Sits at the end of the tab track. It is built by the caller (index.html wires
 * the bundle-dependent handler), so this only provides the styled slot; a page
 * without an exporter simply never appends one.
 *
 * @param {string} activeLabel
 * @returns {HTMLButtonElement}
 */
export function buildExportButton(activeLabel) {
  const btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'chapter-export';
  btn.setAttribute('aria-label', `Download the data behind the ${activeLabel} chapter`);
  btn.title = 'Download the bundle rows behind this chapter (JSON, with provenance)';
  btn.innerHTML = '<span aria-hidden="true">↓</span> <span class="chapter-export__label">Download ' +
    `${String(activeLabel).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')} data</span>`;
  return btn;
}

/**
 * Update the label of an existing export button after a chapter change.
 *
 * @param {HTMLElement|null} btn
 * @param {string} activeLabel
 * @returns {void}
 */
export function syncExportButton(btn, activeLabel) {
  if (!btn) return;
  btn.setAttribute('aria-label', `Download the data behind the ${activeLabel} chapter`);
  // The visible words too, not just the accessible name: the button stays on
  // screen across every chapter, and a tab that says "LNG Feedgas" beside a
  // button offering "Download Overview data" reads as a broken link.
  const text = `Download ${activeLabel} data`;
  const label = btn.querySelector?.('.chapter-export__label');
  if (label) label.textContent = text;
  // A button whose markup was replaced (or never had the span) still has to
  // relabel. Rebuilt as text nodes, so a label can never inject markup.
  else btn.textContent = `↓ ${text}`;
}

/**
 * Wire chapter navigation into the live page.
 *
 * @param {Document} doc
 * @param {Window} win
 * @returns {{select: (id: string) => void, current: () => string}}
 */
export function initChapters(doc, win) {
  const root = doc.querySelector('main.chapter-root') || doc.body;
  const host = doc.getElementById('chapter-nav');

  let stored = null;
  try {
    stored = win.localStorage.getItem(STORAGE_KEY);
  } catch {
    /* storage can be blocked; chapter selection still works, just not remembered */
  }

  let current = resolveChapter(win.location.search, win.location.hash, stored);

  const select = (id, { push = true } = {}) => {
    const changed = id !== current;
    current = id;
    const shown = applyChapter(root, id);
    const bar = doc.getElementById('chapter-nav');
    if (bar) {
      bar.querySelectorAll('[data-chapter-id]').forEach((el) => {
        const on = el.getAttribute('data-chapter-id') === id;
        el.classList.toggle('is-active', on);
        el.setAttribute('aria-selected', String(on));
        el.setAttribute('tabindex', on ? '0' : '-1');
      });
    }
    try {
      win.localStorage.setItem(STORAGE_KEY, id);
    } catch {
      /* ignore */
    }
    if (push) {
      const url = chapterUrl(win.location.pathname, win.location.search, id);
      win.history.pushState({ chapter: id }, '', url);
    }
    // Panels of a chapter that was hidden are deferred by main.js; tell it the
    // visible chapter changed so this one's panels load now (and only this one's).
    win.dispatchEvent(new Event('bluetide:chapter'));
    // Re-render requests for already-have-their-data views (the fleet aggregate
    // and the divergence interlude are computed from sources the boot already
    // parsed, so switching between chapters reshows them from stale numbers
    // until a reload). Announced separately so main.js can refresh only on a
    // genuine change, never on the boot-time dispatch above.
    if (changed) win.dispatchEvent(new Event('bluetide:chapter-changed'));
    // Bring the chosen chapter into view; scroll-spy style anchors land on the
    // tab bar rather than mid-page, so the reader knows where the page moved to.
    if (shown.length) {
      const top = shown[0].getBoundingClientRect().top + win.scrollY;
      const barH = bar ? bar.getBoundingClientRect().height : 0;
      win.scrollTo({ top: Math.max(0, top - barH - 8), behavior: 'instant' in win ? 'instant' : 'auto' });
    }
  };

  if (host) {
    host.replaceChildren(buildChapterBar(current, (id) => select(id)));
    host.hidden = false;
  }

  applyChapter(root, current);

  win.addEventListener('popstate', () => {
    select(resolveChapter(win.location.search, win.location.hash, null), { push: false });
  });

  // Shift+←/→ steps through chapters from anywhere except a text field or the
  // tab bar itself (which already handles plain ←/→ for its own focus ring).
  doc.addEventListener('keydown', (ev) => {
    if (!ev.shiftKey || (ev.key !== 'ArrowRight' && ev.key !== 'ArrowLeft')) return;
    if (ev.altKey || ev.ctrlKey || ev.metaKey) return;
    const target = /** @type {HTMLElement|null} */ (ev.target);
    if (target && target.closest && target.closest('input, textarea, select, [role="tablist"]')) {
      return;
    }
    ev.preventDefault();
    const idx = CHAPTERS.findIndex((c) => c.id === current);
    const step = ev.key === 'ArrowRight' ? 1 : CHAPTERS.length - 1;
    const next = CHAPTERS[(idx + step) % CHAPTERS.length];
    select(next.id);
    const el = doc.querySelector(`[data-chapter-id="${next.id}"]`);
    if (el) /** @type {HTMLElement} */ (el).focus();
  });

  return { select: (id) => select(id), current: () => current };
}
