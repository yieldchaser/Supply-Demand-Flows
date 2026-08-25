"""KM scraper — pipeline2.kindermorgan.com OpAvail postback client with
per-cycle pinning (SOLVED 2026-08-25, live-verified on TGP).

Contract (live-verified from CI runners, 2026-08-25):
    1. GET /Capacity/OpAvailPoint.aspx?code={NGPL|TGP|KMLP}  (~170 KB shell)
       - harvest ALL hidden <input> fields (__VIEWSTATE, __VIEWSTATEGENERATOR,
         __EVENTVALIDATION, Infragistics clientState hiddens).
    2. POST the same URL echoing every field + the retrieve image-button
       coordinate pair:
           ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve.x=10
           ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve.y=10
    3. Response: ~330 KB server-rendered grid. Columns (positional):
       View | Loc | Loc Name | Loc Zn | Loc(Segment) | Design Capacity |
       Operating Capacity | Total Scheduled Quantity |
       Operationally Available Capacity | IT | Flow Ind | All Qty Avail

Tenant-fallback trap (MANDATORY identity assertion):
    Unknown ?code= values do NOT 404 — the site silently serves the TENNESSEE
    GAS PIPELINE page. Every response's <title> must name the requested
    pipeline before parsing; otherwise raise TenantFallbackError.

Cycle pinning (SOLVED — live-verified on TGP against all five cycles):
    The cycle dropdown is an Infragistics WebDropDown whose pending state
    rides in hidden input ``...ddlCycleDD_clientState`` as a pipe-delimited
    delta prefix followed by the control's initial JSON blob:

        |0|{CODE}&tilda;{selectedIndex}||<initial JSON blob>
        + a pending-change log naming property 23 -> CODE

    (captured verbatim from a real browser selection; see tests). The POST
    must additionally be an ASP.NET AJAX partial postback:
        ctl00$WebScriptManager1 = updContent|btnRetrieve marker
        __ASYNCPOST = true
        X-MicrosoftAjax: Delta=true header
    Each of TIMELY/EVNG/ITRD1/ITRD2 then returns its own grid; ITRD3 falls
    back to the freshest posted cycle for today's gas day until it publishes
    (late next morning CT), so the scraper treats that specific mismatch as
    retry-later via CyclePinError.

    VERIFICATION GUARD: the served page's ``lCycle`` span MUST contain the
    requested cycle's description ('CycleDesc:  INTRADAY 1'), else
    CyclePinError — same discipline as assert_response_identity.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from scrapers.base.health_writer import HealthWriter
from scrapers.base.identity import assert_response_identity
from scrapers.base.safe_writer import safe_write_json

log = logging.getLogger(__name__)

BASE_URL = "https://pipeline2.kindermorgan.com"
SOURCE_NAME = "kinder_morgan"
RAW_DIR = Path("data/raw/kinder_morgan")

#: Tenant code -> expected <title> fragment + canonical prefix.
TENANTS: dict[str, dict[str, str]] = {
    "NGPL": {"title": "Natural Gas Pipeline Company", "prefix": "ngpl"},
    "TGP": {"title": "Tennessee Gas Pipeline", "prefix": "tgp"},
    "KMLP": {"title": "Kinder Morgan Louisiana Pipeline", "prefix": "kmlp"},
}

#: Cycle code -> (dropdown selected index, expected CycleDesc text).
CYCLES: dict[str, tuple[int, str]] = {
    "TIMELY": (1, "TIMELY"),
    "EVNG": (2, "EVENING"),
    "ITRD1": (3, "INTRADAY 1"),
    "ITRD2": (4, "INTRADAY 2"),
    "ITRD3": (5, "INTRADAY 3"),
}

_BTN_X = (
    "ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve.x"
)
_BTN_Y = (
    "ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve.y"
)
_CS_CYCLE = "WebSplitter1_tmpl1_ContentPlaceHolder1_ddlCycleDD_clientState"
_PFX = "WebSplitter1_tmpl1_ContentPlaceHolder1_"
_AJAX_MARKER = (
    "ctl00$WebSplitter1$tmpl1$updContent|ctl00$WebSplitter1$tmpl1$"
    "ContentPlaceHolder1$HeaderBTN1$btnRetrieve"
)

_LCYCLE_RE = re.compile(
    r'id="WebSplitter1_tmpl1_ContentPlaceHolder1_lCycle"[^>]*>([^<]*)<'
)
_POST_DATE_RE = re.compile(r"Post\s*Date:\s*(\d{1,2}/\d{1,2}/\d{4})", re.I)
_POST_TIME_RE = re.compile(
    r"Post\s*Time:\s*(\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?)", re.I
)


def parse_posting_stamp(html: str) -> dict[str, str]:
    """Extract the served grid's cycle description + post date/time stamp.

    What:
        On BEST-AVAILABLE pages the ``lCycle`` span reads e.g.
        ``CycleDesc:  EVENING | Post Date: 08/24/2026 | Post Time: 6:45 PM``.
        On AJAX pinned pulls the span carries only ``CycleDesc:  TIMELY`` and
        the combined stamp appears in a second delta fragment — so after the
        span attempt, the full document is searched for
        ``CycleDesc ... | Post Date ... | Post Time``.

        The post date — not the wall clock — anchors which GAS DAY the grid
        belongs to (see :func:`derive_gas_day`).

    Failure modes:
        Returns empty strings when neither shape matches; callers treat a
        missing post date as unpinnable rather than guessing the gas day.
    """
    seg = ""
    m = _LCYCLE_RE.search(html)
    if m:
        seg = m.group(1)
    stamp = _stamp_from_segment(seg)
    if not stamp["post_date"]:
        # AJAX delta fallback: the combined fragment lives elsewhere in the
        # document — grab a window around the first 'Post Date:' occurrence
        # and parse it (desc may come back empty; the lCycle span already
        # supplied it).
        dm2 = _POST_DATE_RE.search(html)
        if dm2:
            window = html[max(0, dm2.start() - 300) : dm2.end() + 160]
            found = _stamp_from_segment(window)
            if found["post_date"]:
                if not found["cycle_desc"] and stamp["cycle_desc"]:
                    found["cycle_desc"] = stamp["cycle_desc"]
                return found
    return stamp


def _stamp_from_segment(seg: str) -> dict[str, str]:
    """Parse one 'CycleDesc ... | Post Date ... | Post Time ...' segment."""
    dm = _POST_DATE_RE.search(seg)
    tm = _POST_TIME_RE.search(seg)
    desc = ""
    head = seg.split("|", 1)[0]
    hm = re.match(r"\s*CycleDesc:\s*(.+)", head, re.I)
    if hm:
        desc = hm.group(1).strip().upper()
    return {
        "cycle_desc": desc,
        "post_date": dm.group(1) if dm else "",
        "post_time": tm.group(1).strip() if tm else "",
    }


def _parse_us_date(raw: str) -> date | None:
    """Parse an ``MM/DD/YYYY`` posting stamp; None when absent/malformed."""
    try:
        mm, dd, yyyy = (int(p) for p in raw.split("/"))
        return date(yyyy, mm, dd)
    except (ValueError, AttributeError):
        return None


def derive_gas_day(cycle_code: str, post_date: str, post_time: str = "") -> str:
    """Derive the GAS DAY a posted grid belongs to from its posting stamp.

    Why:
        NAESB timing (live-verified 2026-08-25 03:35 CT — the site served
        'CycleDesc: EVENING, Post Date: 08/24/2026' while the wall-clock
        date was already 08/25): each gas day G's cycles post across
        calendar days G-1 and G —

            TIMELY  ~11:00 CT G-1     -> gas day G (roll forward)
            EVNG    ~18:45 CT G-1     -> gas day G (roll forward)
            ITRD1   ~22:00 CT G-1     -> gas day G (roll forward)
            ITRD2   ~01:30 CT G       -> gas day G (same calendar day)
            ITRD3   ~09:00 CT G       -> gas day G (same calendar day)

        Stamping payloads with the wall-clock date silently mislabels every
        pull made before the posting calendar catches up.

    Failure modes:
        Returns '' when the post date is missing/unparseable — callers must
        refuse to emit rows rather than guess. ITRD1's prior-evening slot is
        the standard KM tariff schedule; payloads carry post_date/post_time/
        posted_cycle verbatim so any deviation stays auditable downstream.
    """
    d = _parse_us_date(post_date)
    if d is None:
        return ""
    if cycle_code in ("TIMELY", "EVNG", "ITRD1"):
        return (d + timedelta(days=1)).isoformat()
    return d.isoformat()

_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_INPUT_RE = re.compile(r"<input[^>]*>", re.I)


class CyclePinError(RuntimeError):
    """Raised when the served grid's cycle does not match the requested one.

    Why:
        The whole point of per-cycle scraping is trustworthy measured
        feedgas; silently accepting BEST AVAILABLE when ITRD3 was requested
        would reintroduce the exact ambiguity this module exists to remove.
        For not-yet-posted cycles (ITRD3 early in the gas day) callers should
        catch this and schedule a retry.
    """


def _clean(fragment: str) -> str:
    return re.sub(r"\s+", " ", _TAG_RE.sub("", fragment)).strip()


def _harvest_hidden_inputs(html: str) -> dict[str, str]:
    """Collect every hidden input name/value pair from the page."""
    fields: dict[str, str] = {}
    for tag in _INPUT_RE.findall(html):
        nm = re.search(r'name="([^"]+)"', tag)
        if not nm:
            continue
        vl = re.search(r'value="([^"]*)"', tag)
        fields[nm.group(1)] = vl.group(1) if vl else ""
    return fields


def build_cycle_client_state(cycle_code: str) -> str:
    """Build the Infragistics clientState delta string for one pinned cycle.

    What:
        Reproduces, byte-for-byte in structure, what a real browser selection
        serializes into ``ddlCycleDD_clientState``:

        ``|0|{CODE}&tilda;{idx}||`` + initial JSON blob + pending-change log
        naming property 23 (the dropdown's value slot) -> {CODE}.

    Failure modes:
        ``KeyError`` for codes outside CYCLES — callers validate first.
    """
    idx, _desc = CYCLES[cycle_code]
    blob_arr: list[Any] = [None] * 50
    blob_arr[7] = -1
    blob_arr[23] = "BEST%20AVAILABLE"
    blob_arr[37] = 0
    blob_arr[41] = -1
    initial = json.dumps(
        [[[blob_arr], [], None], [{}, [{}]], None], separators=(",", ":")
    )
    # Splice the pending-change log into element [1][0] of the initial blob.
    changes = f'[{{"0":[7,{idx}],"1":[23,"{cycle_code}"]}},[{{"0":[0,7,0],"1":[{idx},7,1]}}]]'
    needle = "],[{},[{}]],null]"
    replacement = f"],[{changes[1:-1]}],null]"
    spliced = initial.replace(needle, replacement)
    return f"|0|{cycle_code}&tilda;{idx}||" + spliced


def parse_cycle_label(html: str) -> str:
    """Extract the served grid's cycle label from the ``lCycle`` span.

    What:
        The span reads e.g. 'CycleDesc:  INTRADAY 1' after a successful
        pinned retrieve. Empty when the span is missing entirely.

    Failure modes:
        Returns '' when the page shape changes — callers must treat that as
        unpinnable rather than guess.
    """
    m = _LCYCLE_RE.search(html)
    return m.group(1).strip() if m else ""


def parse_opavail_grid(html: str) -> list[dict[str, str]]:
    """Parse the server-rendered OpAvail grid into row dicts.

    What:
        Keeps <tr> rows with >= 10 <td> cells whose second cell is a
        4-6 digit loc id; maps cells positionally onto the column contract.
    """
    rows: list[dict[str, str]] = []
    for tr in _TR_RE.findall(html):
        cells = [_clean(cc) for cc in _TD_RE.findall(tr)]
        if len(cells) < 10:
            continue
        loc = cells[1]
        if not re.fullmatch(r"\d{4,6}", loc):
            continue
        rows.append(
            {
                "loc": loc,
                "loc_name": cells[2],
                "loc_zone": cells[3],
                "loc_segment": cells[4],
                "design_capacity": cells[5],
                "operating_capacity": cells[6],
                "total_scheduled_quantity": cells[7],
                "operationally_available_capacity": cells[8],
                "it": cells[9],
                "flow_ind": cells[10] if len(cells) > 10 else "",
                "all_qty_avail": cells[11] if len(cells) > 11 else "",
            }
        )
    return rows


def scrape_tenant_cycle(
    code: str,
    cycle_code: str,
    gas_day: date | None = None,
) -> dict[str, Any]:
    """Scrape one KM tenant's OpAvail grid pinned to *cycle_code*.

    Why:
        BEST_AVAILABLE conflates whichever cycle posted last with real
        intraday evolution; per-cycle pulls give Corpus Christi a measured,
        comparable feedgas number.

    What:
        GET shell → harvest hidden inputs → inject the Infragistics
        clientState delta for *cycle_code* → AJAX partial postback echoing
        every field + btnRetrieve coordinates → assert tenant identity AND
        cycle identity → parse the grid.

    Parameters:
        code: tenant code (NGPL/TGP/KMLP).
        cycle_code: one of CYCLES keys (TIMELY/EVNG/ITRD1/ITRD2/ITRD3).
        gas_day: optional historical gas day via the dtePickerBegin
            clientState. Same-day operation omits it (server defaults to
            today); historical pulls are best-effort — the endpoint
            intermittently redirects dated queries during busy windows.

    Returns:
        Raw payload dict with ``cycle`` set to *cycle_code* and a verified
        ``cycle_label`` from the served page.

    Failure modes:
        ``TenantFallbackError`` on tenant mismatch; ``CyclePinError`` when
        the served cycle label does not match the request (includes the
        not-yet-posted case, e.g. ITRD3 before ~09:00 CT next gas day);
        ``HttpClientError`` on transport faults after retries.
    """
    meta = TENANTS[code]
    url = f"{BASE_URL}/Capacity/OpAvailPoint.aspx?code={code}"
    with httpx.Client(headers={"User-Agent": "Mozilla/5.0"}, timeout=90.0) as c:
        r1 = c.get(url)
        assert_response_identity(
            expected=meta["title"],
            response_text=r1.text,
            context=f"kinder_morgan/{code} GET",
        )
        fields = _harvest_hidden_inputs(r1.text)

        data = dict(fields)
        data[_CS_CYCLE] = build_cycle_client_state(cycle_code)
        data["ctl00$WebScriptManager1"] = _AJAX_MARKER
        data["__ASYNCPOST"] = "true"
        data["ctl00$hdnIsDownload"] = "false"
        data["ctl00$" + _PFX + "location"] = "rbDelivery"
        if gas_day is not None:
            date_stamp = f"01{gas_day.year}-{gas_day.month}-{gas_day.day}-0-0-0-0"
            data[_PFX + "dtePickerBegin_clientState"] = (
                f'|0|{date_stamp}|[[[[]],[],[]],[{{}},[]],"{date_stamp}"]'
            )
        data[_BTN_X] = "45"
        data[_BTN_Y] = "15"

        r2 = c.post(
            url,
            data=data,
            headers={
                "Referer": url,
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-MicrosoftAjax": "Delta=true",
            },
        )

    assert_response_identity(
        expected=meta["title"],
        response_text=r2.text,
        context=f"kinder_morgan/{code} POST cycle={cycle_code}",
    )

    stamp = parse_posting_stamp(r2.text)
    served_label = (
        f"CycleDesc:  {stamp['cycle_desc']}"
        if stamp["cycle_desc"]
        else parse_cycle_label(r2.text)
    )
    _, expected_desc = CYCLES[cycle_code]
    if not stamp["post_date"] or expected_desc not in served_label.upper():
        raise CyclePinError(
            f"Requested cycle {cycle_code} ({expected_desc}) but server "
            f"served {served_label!r} (stamp={stamp}) for {code} gas_day={gas_day or 'today'}"
        )
    gas_day_served = derive_gas_day(cycle_code, stamp["post_date"], stamp["post_time"])
    if gas_day is not None and gas_day_served and gas_day_served != gas_day.isoformat():
        raise CyclePinError(
            f"Dated pull unsupported: requested {gas_day.isoformat()} but the "
            f"served {cycle_code} grid belongs to gas day {gas_day_served} "
            f"(post date {stamp['post_date']}). Historical backfill must come "
            "from scheduled daily accumulation, not dated queries."
        )

    rows = parse_opavail_grid(r2.text)

    fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "fetched_at": fetched_at,
        "source": SOURCE_NAME,
        "tenant_code": code,
        "pipeline_prefix": meta["prefix"],
        "gas_day_requested": (gas_day or date.today()).isoformat(),
        # Authoritative period anchors — derived from the SERVER's own
        # posting stamp, never the wall clock (see derive_gas_day).
        "cycle": cycle_code,
        "posted_cycle": stamp["cycle_desc"],
        "post_date": stamp["post_date"],
        "post_time": stamp["post_time"],
        "gas_day_served": gas_day_served,
        "cycle_label": served_label,
        "row_count": len(rows),
        "data": rows,
    }


def scrape_tenant_best_available(code: str, gas_day: date | None = None) -> dict[str, Any]:
    """Legacy BEST-AVAILABLE pull retained for fallback/diffing purposes."""
    meta = TENANTS[code]
    url = f"{BASE_URL}/Capacity/OpAvailPoint.aspx?code={code}"
    with httpx.Client(headers={"User-Agent": "Mozilla/5.0"}, timeout=90.0) as c:
        r1 = c.get(url)
        assert_response_identity(
            expected=meta["title"],
            response_text=r1.text,
            context=f"kinder_morgan/{code} GET",
        )
        fields = _harvest_hidden_inputs(r1.text)
        data = dict(fields)
        data[_BTN_X] = "10"
        data[_BTN_Y] = "10"
        r2 = c.post(
            url,
            data=data,
            headers={
                "Referer": url,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

    assert_response_identity(
        expected=meta["title"],
        response_text=r2.text,
        context=f"kinder_morgan/{code} POST best-available",
    )
    rows = parse_opavail_grid(r2.text)

    fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "fetched_at": fetched_at,
        "source": SOURCE_NAME,
        "tenant_code": code,
        "pipeline_prefix": meta["prefix"],
        "gas_day_requested": (gas_day or date.today()).isoformat(),
        "cycle": "BEST_AVAILABLE",
        "row_count": len(rows),
        "data": rows,
    }


def run(
    tenants: tuple[str, ...] = ("NGPL", "TGP", "KMLP"),
    cycles: tuple[str, ...] = ("TIMELY", "EVNG", "ITRD1", "ITRD2", "ITRD3"),
    gas_day: date | None = None,
) -> dict[str, Any]:
    """Scrape all *tenants* x *cycles* and write one raw JSON per combination.

    What:
        Files are named ``{today}_{tenant}_{CYCLE}.json``. A same-named file
        skips the fetch (staleness gate). Per-combination failures are
        contained; the run records no-op health only when nothing at all was
        processed.

    Failure modes:
        CyclePinError (not-yet-posted cycle) and tenant faults are contained
        and counted; health reflects reality either way.
    """
    health = HealthWriter(source_name=SOURCE_NAME)
    raw_dir = Path(RAW_DIR)
    raw_dir.mkdir(parents=True, exist_ok=True)

    processed: list[str] = []
    failed: list[str] = []
    not_posted: list[str] = []
    today = date.today().isoformat()

    for code in tenants:
        for cycle in cycles:
            key = f"{code}_{cycle}"
            out_path = raw_dir / f"{today}_{code.lower()}_{cycle}.json"
            if out_path.exists():
                log.info("Skipping %s: already fetched today", key)
                processed.append(key)
                continue
            try:
                payload = scrape_tenant_cycle(code, cycle, gas_day=gas_day)
                safe_write_json(out_path, payload)
                log.info("Written %d rows to %s", payload["row_count"], out_path)
                processed.append(key)
            except CyclePinError as exc:
                # Expected before a cycle's posting time: the server clamps
                # to the freshest posted cycle and we refuse to mislabel it.
                log.info("Cycle %s not yet posted for %s: %s", cycle, code, exc)
                not_posted.append(key)
            except Exception as exc:
                log.error("KM scrape failed for %s: %s: %s", key, type(exc).__name__, exc)
                failed.append(key)

    if not processed:
        reason = "; ".join(failed) or "no tenant/cycle combinations attempted"
        health.record_no_op(reason=reason, metadata={"failed": failed, "not_posted": not_posted})
        return {
            "status": "skipped",
            "processed": processed,
            "failed": failed,
            "not_posted": not_posted,
        }

    health.record_success(metadata={
        "processed": len(processed),
        "failed": failed,
        "not_posted": not_posted,
    })
    return {"status": "ok", "processed": processed, "failed": failed, "not_posted": not_posted}


if __name__ == "__main__":
    result = run()
    print(result)
