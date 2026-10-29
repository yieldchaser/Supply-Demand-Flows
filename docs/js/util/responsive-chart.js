/**
 * Responsive chart redraw.
 *
 * Panels draw their SVG by measuring the container's width and baking it into
 * the viewBox. That width is not final when a panel renders:
 *
 *   - boot renders the core panels synchronously, inside the same frame that
 *     replaces the panel's innerHTML, before the browser has laid the new
 *     markup out, and
 *   - panels in a chapter the reader has not opened render while that chapter
 *     is `hidden`, so every ancestor measures 0 instead of the real column.
 *
 * A viewBox authored from a stale width is then stretched by CSS to the settled
 * column: axis text, ticks and strokes scale by the same ratio, and a fixed
 * pixel height baked into the drawing turns into a letterboxed band. The fix is
 * to redraw once the measured width actually changes, rather than to cap or
 * clip the result.
 *
 * The helper is idempotent per container: calling it again re-aims the draw
 * it should re-run instead of stacking a second observer.
 *
 * Vanilla JS only (project rule: zero TypeScript syntax in docs/js).
 */

/** Container -> {draw, lastWidth, frame, observer} */
const watchers = new WeakMap();

/**
 * Minimum gap between redraws driven by a resize burst (a window drag, a
 * sidebar transition). One frame is not enough: each redraw rebuilds a whole
 * SVG, so a drag would otherwise rebuild every panel ~60 times a second.
 */
const REDRAW_INTERVAL_MS = 120;

/**
 * Like redrawOnResize, for panels whose chart column also holds tabs or a
 * footnote that their draw must not wipe. Register this once the chrome and
 * tabs are in place: the observer fires on the next layout pass, and a draw
 * that rewrites the column would otherwise also delete the tabs.
 *
 * A mark-and-sweep pass is deferred by two frames so the initial layout of the
 * panel settles first; after that only real width changes redraw.
 *
 * @param {HTMLElement} container
 * @param {() => void} draw
 * @returns {void}
 */
export function watchColumn(container, draw) {
  if (!container || typeof draw !== 'function') return;
  const settle = () => {
    // Re-register with the draw that is current by then (tab switches replace it).
    redrawOnResize(container, draw, { width: container.getBoundingClientRect().width });
  };
  if (typeof requestAnimationFrame === 'undefined') return settle();
  requestAnimationFrame(() => requestAnimationFrame(settle));
}

/**
 * Redraw `container`'s chart whenever its width changes.
 *
 * `opts.width` is the width the completed draw was actually made at, so the
 * watcher never mistakes an already-stale drawing for a current one. Pass the
 * measured value the draw used (its viewBox width), not a re-measure — at boot
 * the container may still be 0 while the chart was drawn from a fallback.
 *
 * @param {HTMLElement} container   element the SVG is drawn into
 * @param {() => void} draw         re-runs the whole draw at the current width
 * @param {{width?: number}} [opts] width the completed draw was made at
 * @returns {void}
 */
export function redrawOnResize(container, draw, opts = {}) {
  if (!container || typeof draw !== 'function') return;

  // The width a caller reports is the width its draw really used. Re-measuring
  // here would record 0 for a chapter that is still hidden, and the draw's own
  // fallback width (say 780) would then look current and never be redrawn.
  const reported = typeof opts.width === 'number' && opts.width > 0 ? opts.width : null;

  const existing = watchers.get(container);
  // A re-render keeps the same chart container. Re-record the width this draw
  // was made at so a retired draw's stale width cannot suppress the next redraw.
  if (existing) {
    existing.draw = draw;
    existing.drawnAt = reported;
    return;
  }

  const state = {
    draw,
    drawnAt: reported,
    lastWidth: reported ?? container.getBoundingClientRect().width,
    frame: 0,
    lastRun: 0,
    observer: null,
  };
  watchers.set(container, state);

  if (typeof ResizeObserver === 'undefined') return;

  state.observer = new ResizeObserver(() => {
    const width = container.getBoundingClientRect().width;
    // Skip until the container is laid out (0 while its chapter is hidden) and
    // skip sub-pixel jitter, which would otherwise redraw on every scroll tick.
    if (width <= 0 || Math.abs(width - state.lastWidth) < 1) return;
    if (state.frame) return; // coalesce a burst of entries into one draw
    // Throttle, but never drop the last size of a burst: run immediately when
    // the interval allows, otherwise check again once it has passed so the
    // chart still settles on the final width of a drag.
    const wait = REDRAW_INTERVAL_MS - (performance.now() - state.lastRun);
    state.frame = requestAnimationFrame(() => {
      state.frame = 0;
      if (wait > 0) {
        setTimeout(() => run(), wait);
        return;
      }
      run();
    });
  });

  function run() {
    const settled = container.getBoundingClientRect().width;
    const reference = state.drawnAt ?? state.lastWidth;
    if (settled <= 0 || Math.abs(settled - reference) < 1) return;
    // Record before drawing: a draw that re-enters this module (hot reload, a
    // re-render triggered from inside the draw) must not see itself as stale
    // and schedule a second, redundant pass.
    state.lastWidth = settled;
    state.drawnAt = settled;
    state.lastRun = performance.now();
    try {
      state.draw();
    } catch (err) {
      console.error('[responsive-chart] redraw failed:', err);
    }
  }
  state.observer.observe(container);
}

/**
 * Stop watching a container. Panels that replace their whole body use this so a
 * detached element does not keep a closure alive.
 *
 * @param {HTMLElement} container
 * @returns {void}
 */
export function stopRedrawOnResize(container) {
  const state = container && watchers.get(container);
  if (!state) return;
  if (state.frame) cancelAnimationFrame(state.frame);
  if (state.observer) state.observer.disconnect();
  watchers.delete(container);
}
