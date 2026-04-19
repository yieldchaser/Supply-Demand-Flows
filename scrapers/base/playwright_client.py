"""Playwright-based async browser client for JS-rendered sites.

Why:
    Pipeline Electronic Bulletin Boards (EBBs) and some data portals render
    critical data via JavaScript.  A headless browser is the only reliable way
    to extract this data.

What:
    ``PlaywrightClient`` manages a headless Chromium instance with:
    * Common stealth patches (``navigator.webdriver`` spoof, viewport, locale).
    * Cookie persistence within one client lifetime.
    * ``fetch_html`` — load a page and return rendered HTML.
    * ``fetch_after_xhr`` — load a page and intercept matching XHR payloads.
    * ``screenshot`` — save a PNG for debugging.

Failure modes:
    * ``PlaywrightClientError`` if Playwright is not installed or the browser
      fails to launch.
    * Navigation timeout if the page never loads.
    * Selector timeout if ``wait_for_selector`` never matches.
"""

from __future__ import annotations

import logging
import re

from scrapers.base.errors import PlaywrightClientError

log = logging.getLogger(__name__)

_DEFAULT_USER_AGENT = "BlueTide/0.1 (+https://github.com/yieldchaser/Supply-Demand-Flows)"

# Stealth script injected into every page to mask headless fingerprints.
_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => false });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
"""


class PlaywrightClient:
    """Async Playwright browser for JS-rendered scraping.

    Why:
        See module docstring.

    What:
        Wraps ``playwright.async_api`` with stealth, cookie persistence,
        and XHR interception.

    Failure modes:
        ``PlaywrightClientError`` on browser or navigation failures.
    """

    def __init__(
        self,
        *,
        headless: bool = True,
        timeout_seconds: float = 60.0,
        user_agent: str = _DEFAULT_USER_AGENT,
        stealth: bool = True,
    ) -> None:
        self._headless = headless
        self._timeout_ms = int(timeout_seconds * 1000)
        self._user_agent = user_agent
        self._stealth = stealth

        # Set after start().
        self._playwright: object | None = None
        self._browser: object | None = None
        self._context: object | None = None

    # ------------------------------------------------------------------
    # Context-manager support
    # ------------------------------------------------------------------

    async def __aenter__(self) -> PlaywrightClient:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object | None,
    ) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Launch browser and create a persistent context.

        Why:
            Must be called (or use ``async with``) before any fetch.

        What:
            Launches Chromium, creates a browser context with stealth
            settings applied via ``add_init_script``.

        Failure modes:
            ``PlaywrightClientError`` if Playwright is not installed or
            the browser binary is missing.
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise PlaywrightClientError(
                "playwright is not installed. Run: pip install playwright && "
                "playwright install chromium"
            ) from exc

        try:
            self._playwright = await async_playwright().start()  # type: ignore[assignment]
            pw = self._playwright  # type: ignore[assignment]
            self._browser = await pw.chromium.launch(headless=self._headless)  # type: ignore[union-attr]
            self._context = await self._browser.new_context(  # type: ignore[union-attr]
                user_agent=self._user_agent,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
            )
            if self._stealth:
                await self._context.add_init_script(_STEALTH_SCRIPT)  # type: ignore[union-attr]
            log.info("PlaywrightClient started (headless=%s)", self._headless)
        except Exception as exc:
            raise PlaywrightClientError(f"Failed to launch browser: {exc}") from exc

    async def close(self) -> None:
        """Shut down browser and Playwright instance.

        Why:
            Prevents leaked browser processes.

        What:
            Closes context → browser → playwright in order.

        Failure modes:
            None significant — safe to call multiple times.
        """
        if self._context is not None:
            await self._context.close()  # type: ignore[union-attr]
            self._context = None
        if self._browser is not None:
            await self._browser.close()  # type: ignore[union-attr]
            self._browser = None
        if self._playwright is not None:
            await self._playwright.stop()  # type: ignore[union-attr]
            self._playwright = None
        log.info("PlaywrightClient closed")

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------

    async def new_page(self) -> object:
        """Create a fresh page within the current browser context.

        Why:
            Some scrapers need multiple tabs or fresh DOM state.

        What:
            Returns a ``playwright.async_api.Page`` object.

        Failure modes:
            ``PlaywrightClientError`` if the client has not been started.
        """
        if self._context is None:
            raise PlaywrightClientError("Client not started. Call start() first.")
        return await self._context.new_page()  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def refresh_session(self) -> None:
        """Clear cookies and reload session state.

        Why:
            Some EBB portals require periodic session refreshes.

        What:
            Clears all cookies in the current context.

        Failure modes:
            ``PlaywrightClientError`` if the client has not been started.
        """
        if self._context is None:
            raise PlaywrightClientError("Client not started. Call start() first.")
        await self._context.clear_cookies()  # type: ignore[union-attr]
        log.info("Session cookies cleared for PlaywrightClient")

    # ------------------------------------------------------------------
    # Fetch methods
    # ------------------------------------------------------------------

    async def fetch_html(
        self,
        url: str,
        wait_for_selector: str | None = None,
        wait_timeout_ms: int = 15_000,
    ) -> str:
        """Load *url* and return the fully-rendered HTML.

        Why:
            JS-rendered pages need a real browser to produce meaningful DOM.

        What:
            Navigates to *url*, optionally waits for a CSS selector, then
            returns ``page.content()``.

        Failure modes:
            ``PlaywrightClientError`` on navigation or selector timeout.
        """
        if self._context is None:
            raise PlaywrightClientError("Client not started. Call start() first.")

        page = await self._context.new_page()  # type: ignore[union-attr]
        try:
            await page.goto(url, timeout=self._timeout_ms, wait_until="domcontentloaded")  # type: ignore[union-attr]
            if wait_for_selector:
                await page.wait_for_selector(  # type: ignore[union-attr]
                    wait_for_selector,
                    timeout=wait_timeout_ms,
                )
            return await page.content()  # type: ignore[union-attr, no-any-return]
        except Exception as exc:
            raise PlaywrightClientError(f"fetch_html failed for {url}: {exc}") from exc
        finally:
            await page.close()  # type: ignore[union-attr]

    async def fetch_after_xhr(
        self,
        url: str,
        xhr_pattern: str,
        wait_timeout_ms: int = 20_000,
    ) -> list[dict]:  # type: ignore[type-arg]
        """Load *url* and capture XHR responses matching *xhr_pattern*.

        Why:
            Many pipeline EBBs load data via background XHR after page load.

        What:
            Registers a response listener that checks each response URL
            against the compiled regex *xhr_pattern*.  Captured JSON
            payloads are returned as a list.

        Failure modes:
            ``PlaywrightClientError`` if no matching XHR is captured within
            *wait_timeout_ms*.
        """
        if self._context is None:
            raise PlaywrightClientError("Client not started. Call start() first.")

        import asyncio

        pattern = re.compile(xhr_pattern)
        captured: list[dict] = []  # type: ignore[type-arg]
        done_event = asyncio.Event()

        page = await self._context.new_page()  # type: ignore[union-attr]

        async def _on_response(response: object) -> None:
            resp_url: str = response.url  # type: ignore[union-attr]
            if pattern.search(resp_url):
                try:
                    body = await response.json()  # type: ignore[union-attr]
                    captured.append(body)
                    done_event.set()
                except Exception:  # noqa: BLE001
                    log.warning("Failed to parse XHR response from %s", resp_url)

        page.on("response", _on_response)  # type: ignore[union-attr]

        try:
            await page.goto(url, timeout=self._timeout_ms, wait_until="domcontentloaded")  # type: ignore[union-attr]
            try:
                await asyncio.wait_for(
                    done_event.wait(),
                    timeout=wait_timeout_ms / 1000.0,
                )
            except TimeoutError as exc:
                raise PlaywrightClientError(
                    f"No XHR matching '{xhr_pattern}' captured within "
                    f"{wait_timeout_ms}ms for {url}"
                ) from exc
            return captured
        except PlaywrightClientError:
            raise
        except Exception as exc:
            raise PlaywrightClientError(f"fetch_after_xhr failed for {url}: {exc}") from exc
        finally:
            await page.close()  # type: ignore[union-attr]

    # ------------------------------------------------------------------
    # Debugging
    # ------------------------------------------------------------------

    async def screenshot(self, url: str, path: str) -> None:
        """Navigate to *url* and save a full-page screenshot.

        Why:
            Visual debugging when a page layout changes unexpectedly.

        What:
            Saves a PNG at *path*.

        Failure modes:
            ``PlaywrightClientError`` on navigation failure or write error.
        """
        if self._context is None:
            raise PlaywrightClientError("Client not started. Call start() first.")

        page = await self._context.new_page()  # type: ignore[union-attr]
        try:
            await page.goto(url, timeout=self._timeout_ms, wait_until="domcontentloaded")  # type: ignore[union-attr]
            await page.screenshot(path=path, full_page=True)  # type: ignore[union-attr]
            log.info("Screenshot saved → %s", path)
        except Exception as exc:
            raise PlaywrightClientError(f"screenshot failed for {url}: {exc}") from exc
        finally:
            await page.close()  # type: ignore[union-attr]
