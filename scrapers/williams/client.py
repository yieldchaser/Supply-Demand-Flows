"""Williams/Transco 1Line client — legacy JSP OAC flow with WAF fallback.

Why:
    Transco's Operational Capacity page iframes the plain-JSP form
    ``ebbCode/OACQueryRequest.jsp?BUID=80&type=OAC`` (server-rendered; the
    new portal-app shell around it is cosmetic). Submitting that form stores
    session-scoped query state, after which ``ebbCode/OACreport.jsp`` renders
    a full HTML table — Loc | Loc Prop | Loc Purp Desc | Flow Ind | Loc Name |
    Design Capacity | Operating Capacity | TOTAL SCHEDULED QUANTITY |
    Operationally Available Capacity | IT Indicator — with header label cells
    carrying TSP:, Cycle Desc:, Effective Gas Day:, Posting Date:, Posting
    Time: and Meas Basis Desc:.

What:
    ``TranscoClient`` is a synchronous httpx.Client plus a lazily-created
    curl_cffi chrome124 fallback sharing the cookie jar (GASNom pattern):
      1. GETs the OAC form once to acquire the JSESSIONID cookie,
      2. POSTs the query to ``OACQueryRequest.jsp?BUID=80`` (form fields
         proven from the live-era Wayback captures of the form + oacquery.js),
      3. GETs ``OACreport.jsp?BUID=80`` and parses the TSQ table,
      4. on a gateway/WAF response (403, or the "Session has timed out"
         interstitial), retries ONCE through the impersonated path reusing
         the same cookies, then raises ``TranscoWafError``.

Failure modes:
    * ``TranscoWafError`` when both attempts are blocked — callers record a
      health failure. Azure Application Gateway serves bare 403 bodies to
      disallowed egress IPs; GitHub Actions runners currently pass.
    * Parsers return empty lists for unposted gas days — emptiness is data,
      not an error.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

import httpx

from scrapers.base.errors import HttpClientError
from scrapers.williams.config import BUID

log = logging.getLogger(__name__)

WILLIAMS_BASE_URL = "https://www.1line.williams.com"
_USER_AGENT = "BlueTide/0.1 (+https://github.com/yieldchaser/Supply-Demand-Flows)"
_TIMEOUT_SECONDS = 30.0

#: JSP route fragments (relative to WILLIAMS_BASE_URL).
_OAC_FORM_PATH = f"/ebbCode/OACQueryRequest.jsp?BUID={BUID}&type=OAC"
_OAC_REPORT_PATH = f"/ebbCode/OACreport.jsp?BUID={BUID}"

#: Cycle radio values from the live form (oacquery.js / Wayback captures).
CYCLE_VALUES: dict[str, str] = {
    "timely": "1",
    "evening": "2",
    "id1": "3",
    "id2": "4",
    "id3": "8",
}
_VALUE_TO_CODE = {v: k for k, v in CYCLE_VALUES.items()}

#: Strings that identify a block/challenge response. The gateway's bare 403
#: body carries no distinctive text, so the status code IS the signal; the
#: expired-session interstitial appears with HTTP 200 on stale report GETs.
_BLOCK_MARKERS: tuple[str, ...] = (
    "Session has timed out",
)


class TranscoWafError(HttpClientError):
    """Raised when 1Line blocks every attempt (gateway 403 / session loss)."""


@dataclass(frozen=True)
class TranscoHeader:
    """Header label/value cells above one OAC report table.

    Attributes:
        tsp_name: Reporting pipeline name.
        cycle_value: Raw cycle radio value parsed from Cycle Desc ("4").
        cycle_desc: Verbatim cycle description ("Intraday 2").
        gas_day: Effective gas day cell ("08/22/2026").
        posting_date: Posting Date cell.
        posting_time: Posting Time cell (joined into posted_at downstream).
        meas_basis: Meas Basis Desc cell ("Vol" or blank).
    """

    tsp_name: str
    cycle_value: str
    cycle_desc: str
    gas_day: str
    posting_date: str
    posting_time: str
    meas_basis: str


def _is_blocked(status: int, body: str) -> bool:
    """Detect gateway blocks or expired-session interstitials."""
    if status == 403 or status == 404 and "Page Not Found" in body[:2000]:
        return True
    return any(marker in body for marker in _BLOCK_MARKERS)


def cycle_code_from_value(cycle_value: str) -> str:
    """Map a raw cycle radio value ('1'|'2'|'3'|'4'|'8') to a series code.

    Failure modes:
        Unknown values fall back to 'unknown' so rows stay inspectable rather
        than silently dropped.
    """
    return _VALUE_TO_CODE.get(cycle_value.strip(), "unknown")


def cycle_desc_to_value(cycle_desc: str) -> str | None:
    """Map a verbatim Cycle Desc ('Intraday 2') back to its radio value.

    Failure modes:
        Returns None when the description matches no known NAESB cycle.
    """
    normalized = (cycle_desc or "").strip().lower()
    if normalized.startswith("timely"):
        return "1"
    if normalized.startswith("evening"):
        return "2"
    if normalized.startswith("intraday 1"):
        return "3"
    if normalized.startswith("intraday 2"):
        return "4"
    if normalized.startswith("intraday 3"):
        return "8"
    return None


def parse_oac_table(html: str) -> tuple[TranscoHeader, list[dict[str, str]]]:
    """Parse an OACreport.jsp response into (header, data rows).

    Why:
        The report is server-rendered HTML whose column order is fixed but
        whose label cells sit in their own table above the data grid.

    What:
        Extracts the header label/value pairs by scanning ``<th>`` pairs,
        then maps every subsequent ``<tr>`` of >=10 ``<td>`` cells onto the
        canonical positional keys.

    Failure modes:
        An unposted (gas day, cycle) returns an empty row list with a header
        whose cells are all blank — callers treat that as legitimately empty.
        Numeric cells keep their thousands separators; normalization happens
        at transform time (gasnom lesson: never lose the raw text early).
    """

    def _clean(fragment: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", fragment)).strip()

    # ---- header label/value pairs ---------------------------------------
    hdr_cells = re.findall(
        r"<th[^>]*>(.*?)</th>\s*(?:<td[^>]*>(.*?)</td>)?", html, re.S | re.I
    )
    hdr_map: dict[str, str] = {}
    pending_label: str | None = None
    for label_frag, value_frag in hdr_cells:
        label = _clean(label_frag).rstrip(":")
        value = _clean(value_frag) if value_frag else ""
        if value:
            hdr_map[label] = value
            pending_label = None
        elif label and not label.replace(" ", "").isdigit():
            # Label-only <th>: its value arrives as the NEXT <th>/<td> pair
            # in some renderings; remember it and fill from the next value.
            pending_label = label
        elif pending_label is not None:
            hdr_map[pending_label] = label
            pending_label = None
        else:
            # Column-header cells (Loc Name etc.) carry no pair partner.
            continue

    cycle_value = ""
    cycle_desc = hdr_map.get("Cycle Desc", "")
    mapped = cycle_desc_to_value(cycle_desc)
    if mapped is not None:
        cycle_value = mapped

    header = TranscoHeader(
        tsp_name=hdr_map.get("TSP", ""),
        cycle_value=cycle_value,
        cycle_desc=cycle_desc,
        gas_day=hdr_map.get("Effective Gas Day", ""),
        posting_date=hdr_map.get("Posting Date", ""),
        posting_time=hdr_map.get("Posting Time", ""),
        meas_basis=hdr_map.get("Meas Basis Desc", ""),
    )

    # ---- data rows --------------------------------------------------------
    col_names: tuple[str, ...] = (
        "loc",
        "loc_prop",
        "loc_purp",
        "flow_ind",
        "loc_name",
        "design_cap",
        "operating_cap",
        "tsq",
        "oac",
        "it_indicator",
    )
    rows: list[dict[str, str]] = []
    seen_data_header = False
    for m in re.finditer(r"<tr[^>]*>(.*?)</tr>", html, re.S | re.I):
        cells_th = re.findall(r"<th[^>]*>(.*?)</th>", m.group(1), re.S)
        if cells_th:
            joined = " ".join(_clean(c) for c in cells_th)
            # The data-grid header row repeats the column titles.
            if "Loc Name" in joined and "Scheduled" in joined:
                seen_data_header = True
                continue
            continue
        cells = [_clean(c) for c in re.findall(r"<td[^>]*>(.*?)</td>", m.group(1), re.S)]
        if len(cells) < len(col_names):
            continue
        if not cells[0].strip():
            continue
        if not seen_data_header and not cells[0].strip().isdigit():
            # Rows before the real grid (stray layout tables) have non-numeric
            # first cells; genuine loc ids are numeric strings.
            continue
        seen_data_header = True
        rows.append(dict(zip(col_names, cells[: len(col_names)], strict=False)))
    return header, rows


class TranscoClient:
    """Session-aware client for the 1Line legacy OAC JSP flow.

    Why:
        The form POST stores query state on the session; the report GET must
        reuse those exact cookies. A fresh jar re-trips the gateway/session
        check immediately.

    What:
        Synchronous ``httpx.Client`` plus a lazily-created curl_cffi
        chrome124-impersonated session used as the single WAF fallback.

    Failure modes:
        ``TranscoWafError`` after the impersonated retry also trips; network
        errors during priming surface as ``TranscoWafError``; network errors
        on data fetches fall through to the impersonated path first.
    """

    def __init__(self) -> None:
        self._client = httpx.Client(
            base_url=WILLIAMS_BASE_URL,
            headers={"User-Agent": _USER_AGENT},
            timeout=_TIMEOUT_SECONDS,
            follow_redirects=True,
        )
        self._primed = False

    def close(self) -> None:
        """Release the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> TranscoClient:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def prime_session(self) -> None:
        """GET the OAC form once to acquire the JSESSIONID cookie.

        Failure modes:
            Network errors propagate wrapped as ``TranscoWafError``; a blocked
            response here is retried once through the impersonated path before
            raising.
        """
        try:
            res = self._client.get(_OAC_FORM_PATH)
        except httpx.HTTPError as exc:
            raise TranscoWafError(
                url=_OAC_FORM_PATH,
                status=None,
                attempts=1,
                elapsed_s=0.0,
                reason=f"{type(exc).__name__} while priming session: {exc}",
            ) from exc
        if _is_blocked(res.status_code, res.text):
            log.warning(
                "transco: block (%s) while priming — retrying via curl_cffi",
                res.status_code,
            )
            body = self._impersonated_get(_OAC_FORM_PATH)
            if _is_blocked(200, body):  # pragma: no cover - defensive
                raise TranscoWafError(
                    url=_OAC_FORM_PATH,
                    status=200,
                    attempts=2,
                    elapsed_s=0.0,
                    reason="blocked body persisted through impersonation",
                )
        self._primed = True

    def fetch_oac(self, cycle: str, gas_day: date) -> tuple[TranscoHeader, list[dict[str, str]]]:
        """Fetch and parse the OAC report for (cycle, gas_day).

        What:
            Submits the form exactly as the browser would (submitflag=true so
            openReport() opens OACreport.jsp), then parses the report table.

        Failure modes:
            ``TranscoWafError`` when both attempts trip; an unposted gas day
            returns an empty row list.
        """
        self._ensure_primed()
        cycle_value = CYCLE_VALUES.get(cycle)
        if cycle_value is None:
            raise ValueError(f"Unknown cycle {cycle!r}; expected one of {sorted(CYCLE_VALUES)}")
        form = {
            "MapID": "0",
            "submitflag": "true",
            "tbGasFlowBeginDate": gas_day.strftime("%m/%d/%Y"),
            "tbGasFlowEndDate": gas_day.strftime("%m/%d/%Y"),
            "cycle": cycle_value,
            "locationIDs": "",
            "reportType": "OAC",
        }
        referer = WILLIAMS_BASE_URL + _OAC_FORM_PATH
        self._post_with_retry(_OAC_FORM_PATH.replace("?type=OAC", ""), form=form, referer=referer)
        body = self._get_with_retry(_OAC_REPORT_PATH, referer=referer)
        return parse_oac_table(body)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _ensure_primed(self) -> None:
        if not self._primed:
            self.prime_session()

    def _post_with_retry(
        self, url: str, *, form: dict[str, str], referer: str
    ) -> None:
        try:
            res = self._client.post(url, data=form, headers={"Referer": referer})
        except httpx.HTTPError as exc:
            log.warning(
                "transco: %s on POST %s — falling back to curl_cffi",
                type(exc).__name__, url,
            )
            self._impersonated_post(url, form=form, referer=referer)
            return
        if _is_blocked(res.status_code, res.text):
            log.warning(
                "transco: block (%s) on POST %s — retrying via curl_cffi",
                res.status_code, url,
            )
            body = self._impersonated_post(url, form=form, referer=referer)
            if _is_blocked(200, body):
                raise TranscoWafError(
                    url=url,
                    status=200,
                    attempts=2,
                    elapsed_s=0.0,
                    reason="blocked body persisted through impersonation (POST)",
                )

    def _get_with_retry(self, url: str, *, referer: str) -> str:
        try:
            res = self._client.get(url, headers={"Referer": referer})
        except httpx.HTTPError as exc:
            log.warning(
                "transco: %s on GET %s — falling back to curl_cffi",
                type(exc).__name__, url,
            )
            return self._impersonated_get(url, referer=referer)
        if not _is_blocked(res.status_code, res.text):
            return res.text
        log.warning(
            "transco: block (%s) on GET %s — retrying via curl_cffi",
            res.status_code, url,
        )
        body = self._impersonated_get(url, referer=referer)
        if _is_blocked(200, body):
            raise TranscoWafError(
                url=url,
                status=200,
                attempts=2,
                elapsed_s=0.0,
                reason="blocked body persisted through impersonation (GET)",
            )
        return body

    def _impersonated_session(self) -> Any:
        """Create a curl_cffi session sharing this client's cookie jar.

        What:
            Typed as ``Any`` because the curl_cffi stub surface varies across
            versions; the runtime contract (Session with get/post/cookies)
            is stable and matches how scrapers/gasnom/client.py uses it.

        Failure modes:
            ``ImportError`` propagates when curl_cffi isn't installed — it is
            pinned in pyproject, so this only bites stripped environments.
        """
        from curl_cffi import requests as cffi_requests

        session: Any = cffi_requests.Session(impersonate="chrome124")
        session.headers.update({"User-Agent": _USER_AGENT})
        for cookie in self._client.cookies.jar:
            if cookie.name and cookie.value:
                session.cookies.set(cookie.name, cookie.value, domain="www.1line.williams.com")
        return session

    def _impersonated_get(self, url: str, referer: str | None = None) -> str:
        headers = {"Referer": referer} if referer else None
        with self._impersonated_session() as session:
            res: Any = session.get(
                WILLIAMS_BASE_URL + url, headers=headers, timeout=_TIMEOUT_SECONDS
            )
        status: int = int(res.status_code)
        text: str = str(res.text)
        if _is_blocked(status, text):
            raise TranscoWafError(
                url=WILLIAMS_BASE_URL + url,
                status=status,
                attempts=2,
                elapsed_s=0.0,
                reason="Azure Application Gateway block persisted through chrome124 impersonation",
            )
        return text

    def _impersonated_post(
        self, url: str, *, form: dict[str, str], referer: str
    ) -> str:
        with self._impersonated_session() as session:
            res: Any = session.post(
                WILLIAMS_BASE_URL + url,
                data=form,
                headers={"Referer": referer},
                timeout=_TIMEOUT_SECONDS,
            )
        status: int = int(res.status_code)
        text: str = str(res.text)
        if _is_blocked(status, text):
            raise TranscoWafError(
                url=WILLIAMS_BASE_URL + url,
                status=status,
                attempts=2,
                elapsed_s=0.0,
                reason="Azure Application Gateway block persisted through chrome124 impersonation",
            )
        return text



