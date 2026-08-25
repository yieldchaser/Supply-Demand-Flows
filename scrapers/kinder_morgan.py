"""KM scraper — pipeline2.kindermorgan.com OpAvail postback client.

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

Cycles:
    The page defaults to BEST AVAILABLE (freshest posted cycle). The cycle
    dropdown exposes TIMELY/EVNG/ITRD1-3 but posting ddlCycleDD_clientState
    overrides did not change results in testing — per-cycle pinning is
    UNSOLVED (Infragistics drop-down postback format). We ship BEST AVAILABLE
    and record the caveat in every raw payload.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, date, datetime
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

_BTN_X = (
    "ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve.x"
)
_BTN_Y = (
    "ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnRetrieve.y"
)

_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_INPUT_RE = re.compile(r"<input[^>]*>", re.I)


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


def scrape_tenant(code: str, gas_day: date | None = None) -> dict[str, Any]:
    """Scrape one KM tenant's BEST-AVAILABLE OpAvail grid.

    Failure modes:
        ``TenantFallbackError`` when the served <title> does not name the
        requested pipeline (the KM silent-fallback trap); ``HttpClientError``
        on transport faults after retries.
    """
    meta = TENANTS[code]
    url = f"{BASE_URL}/Capacity/OpAvailPoint.aspx?code={code}"
    with httpx.Client(headers={"User-Agent": "Mozilla/5.0"}, timeout=90.0) as c:
        r1 = c.get(url)
        # MANDATORY identity assertion on the GET (fallback trap fires here
        # too: unknown codes serve TGP's shell).
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

    # Identity assertion again on the POSTED grid page.
    assert_response_identity(
        expected=meta["title"],
        response_text=r2.text,
        context=f"kinder_morgan/{code} POST",
    )
    rows = parse_opavail_grid(r2.text)

    fetched_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "fetched_at": fetched_at,
        "source": SOURCE_NAME,
        "tenant_code": code,
        "pipeline_prefix": meta["prefix"],
        "gas_day_requested": (gas_day or date.today()).isoformat(),
        "cycle": "BEST_AVAILABLE",  # per-cycle pinning UNSOLVED (see module docstring)
        "row_count": len(rows),
        "data": rows,
    }


def run(
    tenants: tuple[str, ...] = ("NGPL", "TGP", "KMLP"),
    gas_day: date | None = None,
) -> dict[str, Any]:
    """Scrape all *tenants* and write one raw JSON per tenant.

    Failure modes:
        Per-tenant faults are contained (logged, counted); the run only
        fails outright when EVERY tenant fails. Health is recorded either
        way (record_no_op when zero tenants produced rows).
    """
    health = HealthWriter(source_name=SOURCE_NAME)
    raw_dir = Path(RAW_DIR)
    raw_dir.mkdir(parents=True, exist_ok=True)

    processed: list[str] = []
    failed: list[str] = []
    today = date.today().isoformat()

    for code in tenants:
        out_path = raw_dir / f"{today}_{code.lower()}_best.json"
        if out_path.exists():
            log.info("Skipping %s: already fetched today", code)
            processed.append(code)
            continue
        try:
            payload = scrape_tenant(code, gas_day=gas_day)
            safe_write_json(out_path, payload)
            log.info("Written %d rows to %s", payload["row_count"], out_path)
            processed.append(code)
        except Exception as exc:
            log.error("KM scrape failed for %s: %s: %s", code, type(exc).__name__, exc)
            failed.append(code)

    if not processed:
        reason = "; ".join(failed) or "no tenants attempted"
        health.record_no_op(reason=reason, metadata={"failed": failed})
        return {"status": "skipped", "processed": processed, "failed": failed}

    health.record_success(metadata={"processed": processed, "failed": failed})
    return {"status": "ok", "processed": processed, "failed": failed}


if __name__ == "__main__":
    result = run()
    print(result)
