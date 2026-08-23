"""GASNom ESG-Latitude ColdFusion client — HTML OAC fetch + bulk TSV fetch.

Why:
    gasnom.com fronts its ColdFusion ESG (Electronic Posting System) with
    Imperva/Incapsula in passthrough mode.  A plain session works once the
    ``frameindex.cfm`` landing page has been visited (CFID/CFTOKEN plus
    visid_incap/incap_ses cookies are minted), but datacenter IPs — GitHub
    Actions runners among them — intermittently receive either an HTTP 403
    or a 200 whose body is an Incapsula challenge page instead of data.

What:
    ``GasnomClient`` is a thin synchronous wrapper that
      1. primes the session cookies via ``frameindex.cfm``,
      2. GETs ``oauc.cfm?dt=MM/DD/YYYY&type=1`` for the single-cycle HTML view,
      3. POSTs the ``transposting.cfm?id=1`` form to ``OAC.cfm`` for the bulk
         tab-delimited TSV (all cycles per gas day, 90-day window max),
      4. on a WAF response (403 status or "Incapsula incident ID" body),
         retries ONCE through ``curl_cffi`` with ``impersonate="chrome124"``
         reusing the same cookie jar, then fails health if that also trips.

Failure modes:
    * ``GasnomWafError`` — both the plain and impersonated attempts hit the
      WAF; callers should record a health failure for the slug.
    * Parsers return empty lists for genuinely empty gas days (e.g. Port
      Arthur before postings began) — emptiness is data, not an error.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import httpx

from scrapers.base.errors import HttpClientError
from scrapers.gasnom.pipelines import GasnomPipeline

log = logging.getLogger(__name__)

GASNOM_BASE_URL = "https://www.gasnom.com"
_USER_AGENT = "BlueTide/0.1 (+https://github.com/yieldchaser/Supply-Demand-Flows)"
_TIMEOUT_SECONDS = 30.0

# ONLY this string indicates an actual challenge page.  Note that every
# legitimate gasnom.com page embeds a /_Incapsula_Resource <script> tag, so
# that substring must NOT be treated as a challenge signal.
_INCAPSULA_MARKERS: tuple[str, ...] = ("Incapsula incident ID",)


class GasnomWafError(HttpClientError):
    """Raised when GASNom serves a WAF challenge on every attempt."""


@dataclass(frozen=True)
class GasnomHeader:
    """Page header above one OAC HTML table (latest posted cycle only).

    Attributes:
        tsp_name: Reporting pipeline name.
        posting_time: Raw "Posting Date/Time" cell (e.g.
            "August 22, 2026 09:31 PM CT"); empty string when absent.
        gas_day_time: Raw "Eff Gas Day/Time" cell; empty string when absent.
        cycle_desc: Raw cycle description (e.g. "Intraday 3"); empty when the
            day carries no posting at all.
    """

    tsp_name: str
    posting_time: str
    gas_day_time: str
    cycle_desc: str


def _is_waf_response(status: int, body: str) -> bool:
    """Detect an Incapsula challenge regardless of HTTP status."""
    if status == 403:
        return True
    return any(marker in body for marker in _INCAPSULA_MARKERS)


def parse_header_date(cell: str) -> date | None:
    """Parse a header date like 'August 22, 2026 10:00 PM CT' into a date.

    Failure modes:
        Returns ``None`` for blank or unparseable cells rather than raising.
    """
    match = re.search(r"([A-Z][a-z]+ \d{1,2}, \d{4})", cell or "")
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%B %d, %Y").date()
    except ValueError:
        return None


def parse_html_oac(html: str) -> tuple[GasnomHeader, list[dict[str, str]]]:
    """Parse one oauc.cfm response into (header, data rows).

    Why:
        The template renders a fixed column order (0-based):
        Loc Name | Loc | Loc Zn | Loc Purp | Loc/QTI | Flow Ind | All Qty |
        Design Cap | Operating Cap | TSQ | OAC [, IT Indicator, Qty Reason].
        The last two columns only appear on some pipelines/days, so columns
        are mapped positionally after locating the header row via "TSQ".

    What:
        Extracts TSP name, posting timestamp, gas day and cycle description
        from the label/value header table, then every data row of the main
        table into dicts keyed by canonical column names.

    Failure modes:
        A gas day with no posting yields an empty row list with a header
        whose cells are all blank — callers treat that as legitimately empty.
    """
    header_cells = re.findall(
        r'<td class="hdrlabel"[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>', html, re.S
    )

    def _clean(fragment: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", fragment)).strip()

    hdr_map = {_clean(k).rstrip(":"): _clean(v) for k, v in header_cells}
    gas_header = GasnomHeader(
        tsp_name=hdr_map.get("TSP Name", ""),
        posting_time=hdr_map.get("Posting Date/Time", ""),
        gas_day_time=hdr_map.get("Eff Gas Day/Time", ""),
        cycle_desc=hdr_map.get("Cycle Indicator Description", ""),
    )

    rows: list[dict[str, str]] = []
    col_names: list[str] | None = None
    for m in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        cells = [_clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", m.group(1), re.S)]
        if col_names is None:
            # The data-table header row contains "TSQ" and at least 11 cells;
            # every other <tr> on the page (the hdr label table) has fewer.
            if "TSQ" not in " ".join(cells) or len(cells) < 11:
                continue
            col_names = [
                "Loc Name",
                "Loc",
                "Loc Zn",
                "Loc Purp",
                "Loc/QTI",
                "Flow Ind",
                "All Qty",
                "Design Capacity",
                "Operating Capacity",
                "TSQ",
                "OAC",
                "IT Indicator",
                "Qty Reason",
            ][: len(cells)]
            continue
        if len(cells) < len(col_names) or not cells[1]:
            # Spacer rows carry colspan cells; skip anything without a Loc.
            continue
        rows.append(dict(zip(col_names, cells[: len(col_names)], strict=False)))
    return gas_header, rows


def parse_bulk_tsv(tsv_text: str) -> list[dict[str, str]]:
    """Parse a bulk OAC.cfm TSV download into clean dictionaries.

    What:
        The bulk export's tab-delimited header is
        ``TSP Name, TSP, Location, Location_Name, Location_Zone, Loc_Purp,
        All_Qty_Avail, Loc/QTI, Flow_Ind, IT, Design_Capacity,
        Operational_Capacity, TSQ, OAC, Eff_Gas_Day/Time, CycleDesc,
        Posting_Date/Time, Measurement_Basis, Pressure_Base``.

    Failure modes:
        Blank lines and stray whitespace are tolerated; an Incapsula challenge
        served with HTTP 200 parses as zero rows — callers must check WAF
        markers BEFORE calling this function.
    """
    reader = csv.DictReader(io.StringIO(tsv_text), delimiter="\t")
    out: list[dict[str, str]] = []
    for raw in reader:
        clean = {
            (k.strip() if k else ""): (v.strip() if v else "")
            for k, v in raw.items()
            if k is not None
        }
        if not clean.get("Location"):
            continue
        out.append(clean)
    return out


def cycle_code_from_description(cycle_desc: str) -> str:
    """Map a cycle description ('Timely', 'Intraday 2', …) to a series code.

    Failure modes:
        Unknown descriptions fall back to a lowercase slug of the text so no
        row is silently dropped.
    """
    normalized = (cycle_desc or "").strip().lower()
    if "timely" in normalized:
        return "timely"
    if "evening" in normalized:
        return "evening"
    for number, code in ((1, "id1"), (2, "id2"), (3, "id3")):
        if f"intraday {number}" in normalized or normalized == code:
            return code
    return re.sub(r"[^a-z0-9]+", "_", normalized).strip("_") or "unknown"


class GasnomClient:
    """Session-aware client for one pipeline slug on gasnom.com.

    Why:
        Cookies acquired from ``frameindex.cfm`` must persist across OAC
        requests; the WAF retry must reuse them too (a fresh cookie jar
        re-trips Incapsula immediately).

    What:
        Synchronous ``httpx.Client`` plus a lazily-created ``curl_cffi``
        chrome124-impersonated session used as the single WAF fallback.

    Failure modes:
        ``GasnomWafError`` after the impersonated retry also trips; network
        errors during priming surface as ``GasnomWafError`` (status=None);
        network errors on data fetches fall through to the impersonated path
        before any exception escapes.
    """

    def __init__(self, pipeline: GasnomPipeline) -> None:
        self.pipeline = pipeline
        self._client = httpx.Client(
            base_url=GASNOM_BASE_URL,
            headers={"User-Agent": _USER_AGENT},
            timeout=_TIMEOUT_SECONDS,
            follow_redirects=True,
        )
        self._primed = False

    def close(self) -> None:
        """Release the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> GasnomClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Session priming
    # ------------------------------------------------------------------

    def prime_session(self) -> None:
        """GET frameindex.cfm once to acquire CFID/CFTOKEN/visid cookies.

        Failure modes:
            Network errors propagate wrapped as ``GasnomWafError``; a WAF
            challenge here is retried once through the impersonated path
            before raising.
        """
        referer_url = self._pipeline_url("frameindex.cfm")
        try:
            res = self._client.get(referer_url)
            if _is_waf_response(res.status_code, res.text):
                log.warning(
                    "%s: WAF challenge while priming — retrying via curl_cffi",
                    self.pipeline.slug,
                )
                self._impersonated_get(referer_url)
            self._primed = True
        except httpx.HTTPError as exc:
            raise GasnomWafError(
                url=GASNOM_BASE_URL + referer_url,
                status=None,
                attempts=1,
                elapsed_s=0.0,
                reason=f"{type(exc).__name__}: {exc}",
            ) from exc

    # ------------------------------------------------------------------
    # Public fetches
    # ------------------------------------------------------------------

    def fetch_oac_html(self, gas_day: date) -> tuple[GasnomHeader, list[dict[str, str]]]:
        """Fetch and parse the single-latest-cycle HTML view for *gas_day*.

        Failure modes:
            ``GasnomWafError`` when both plain and impersonated attempts trip
            the WAF.  An unposted gas day returns an empty row list.
        """
        url = (
            f"{self._pipeline_url('oauc.cfm')}"
            f"?dt={gas_day.strftime('%m/%d/%Y')}&type=1"
        )
        body = self._get_with_waf_retry(url, referer=self._pipeline_url("frameindex.cfm"))
        return parse_html_oac(body)

    def fetch_bulk_tsv(self, start: date, end: date) -> str:
        """POST the transposting form and return the raw bulk TSV text.

        Why:
            The HTML view exposes only the latest cycle; the bulk export
            includes ``CycleDesc`` + ``Posting_Date/Time`` for EVERY cycle in
            the window, which is what makes multi-cycle history recoverable.
            The window is capped at 90 days server-side.

        Failure modes:
            ``GasnomWafError`` when both attempts trip the WAF; the returned
            text is unvalidated beyond the WAF markers.
        """
        form = {
            "qry": "1",
            "frmEffectiveDt": start.strftime("%m/%d/%Y"),
            "FRMENDDT": end.strftime("%m/%d/%Y"),
            "B1": "Download",
        }
        url = self._pipeline_url("OAC.cfm")
        referer = self._pipeline_url("transposting.cfm?id=1")
        return self._post_with_waf_retry(url, form=form, referer=referer)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _pipeline_url(self, filename: str) -> str:
        return f"/ip/{self.pipeline.slug}/{filename}"

    def _ensure_primed(self) -> None:
        if not self._primed:
            self.prime_session()

    def _get_with_waf_retry(self, url: str, *, referer: str) -> str:
        self._ensure_primed()
        try:
            res = self._client.get(url, headers={"Referer": referer})
        except httpx.HTTPError as exc:
            log.warning(
                "%s: %s on GET %s — falling back to curl_cffi",
                self.pipeline.slug, type(exc).__name__, url,
            )
            return self._impersonated_get(url, referer=referer)
        if not _is_waf_response(res.status_code, res.text):
            return res.text
        log.warning(
            "%s: WAF challenge (HTTP %s) on GET %s — retrying via curl_cffi",
            self.pipeline.slug, res.status_code, url,
        )
        return self._impersonated_get(url, referer=referer)

    def _post_with_waf_retry(self, url: str, *, form: dict[str, str], referer: str) -> str:
        self._ensure_primed()
        try:
            res = self._client.post(url, data=form, headers={"Referer": referer})
        except httpx.HTTPError as exc:
            log.warning(
                "%s: %s on POST %s — falling back to curl_cffi",
                self.pipeline.slug, type(exc).__name__, url,
            )
            return self._impersonated_post(url, form=form, referer=referer)
        if not _is_waf_response(res.status_code, res.text):
            return res.text
        log.warning(
            "%s: WAF challenge (HTTP %s) on POST %s — retrying via curl_cffi",
            self.pipeline.slug, res.status_code, url,
        )
        return self._impersonated_post(url, form=form, referer=referer)

    def _impersonated_session(self) -> Any:
        """Create a ``curl_cffi`` session sharing this client's cookie jar.

        Failure modes:
            ``ImportError`` propagates when curl_cffi isn't installed — it is
            pinned in pyproject, so this only bites stripped environments.
        """
        from curl_cffi import requests as cffi_requests

        session = cffi_requests.Session(impersonate="chrome124")
        session.headers.update({"User-Agent": _USER_AGENT})
        for cookie in self._client.cookies.jar:
            if cookie.name and cookie.value:
                session.cookies.set(cookie.name, cookie.value, domain="www.gasnom.com")
        return session

    def _impersonated_get(self, url: str, referer: str | None = None) -> str:
        headers = {"Referer": referer} if referer else None
        with self._impersonated_session() as session:
            res: Any = session.get(
                GASNOM_BASE_URL + url, headers=headers, timeout=_TIMEOUT_SECONDS
            )
        status: int = int(res.status_code)
        text: str = str(res.text)
        if _is_waf_response(status, text):
            raise GasnomWafError(
                url=GASNOM_BASE_URL + url,
                status=status,
                attempts=2,
                elapsed_s=0.0,
                reason="Imperva/Incapsula challenge persisted through chrome124 impersonation",
            )
        return text

    def _impersonated_post(self, url: str, *, form: dict[str, str], referer: str) -> str:
        with self._impersonated_session() as session:
            res: Any = session.post(
                GASNOM_BASE_URL + url,
                data=form,
                headers={"Referer": referer},
                timeout=_TIMEOUT_SECONDS,
            )
        status: int = int(res.status_code)
        text: str = str(res.text)
        if _is_waf_response(status, text):
            raise GasnomWafError(
                url=GASNOM_BASE_URL + url,
                status=status,
                attempts=2,
                elapsed_s=0.0,
                reason="Imperva/Incapsula challenge persisted through chrome124 impersonation",
            )
        return text
