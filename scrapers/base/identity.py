"""Response-identity assertions for multi-tenant scrapers.

Why:
    The KM pipeline2.kindermorgan.com trap (found during the Sabine Pass
    recon, 2026-08): for an UNKNOWN tenant code the server silently falls
    back to a different tenant's page — HTTP 200, valid HTML/JSON, correct
    shape, WRONG ENTITY. A scraper that trusts `status == 200` ships rows
    labeled Elba Express that are actually TGP data. This is silent
    corruption of the worst kind: the response is valid, just for the wrong
    entity.

What:
    ``assert_response_identity`` — one call before parsing. Give it the
    identity you REQUESTED (tsp code / slug / title fragment) and evidence
    from the RESPONSE (page <title>, a TSP-name field, a code field). It
    raises :class:`TenantFallbackError` when none of the response evidence
    matches, so the run fails loudly instead of shipping cross-tenant rows.

Failure modes:
    Raises on ANY identity mismatch. Never warns-and-continues: a fallback
    page means every subsequent row is mislabeled.
"""

from __future__ import annotations

import re
from typing import Any

from scrapers.base.errors import ScraperError


class TenantFallbackError(ScraperError):
    """Raised when a response fails the requested-entity identity check.

    Why:
        Multi-tenant portals (KM pipeline2, Quorum IPWS, Enbridge rtba)
        can silently serve a DIFFERENT tenant's page for unknown codes.
        Shipping those rows would mislabel an entire payload.

    Attributes:
        expected: The identity marker that was requested.
        evidence: Mapping of evidence-name -> truncated value from the
            actual response, for diagnosis.
    """

    def __init__(self, expected: str, evidence: dict[str, str], context: str) -> None:
        self.expected = expected
        self.evidence = evidence
        summary = "; ".join(f"{k}={v[:80]!r}" for k, v in evidence.items()) or (
            "no evidence collected"
        )
        super().__init__(
            f"tenant-fallback trap suspected for {context or 'request'}: expected "
            f"identity {expected!r} not found in response evidence [{summary}]. "
            "Refusing to parse — the portal may have served a different tenant."
        )


def _page_title(html: str) -> str | None:
    """Extract the first <title>...</title> from *html* (None if absent)."""
    m = re.search(r"<title[^>]*>(.*?)</title>", html or "", re.S | re.I)
    if not m:
        return None
    return m.group(1).strip()


def _as_evidence_strings(value: Any) -> list[str]:
    """Coerce arbitrary JSON-ish evidence into comparable strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, int | float | bool):
        return [str(value).lower()]
    if isinstance(value, dict):
        out: list[str] = []
        for v in value.values():
            out.extend(_as_evidence_strings(v))
        return out
    if isinstance(value, list | tuple):
        out = []
        for v in value:
            out.extend(_as_evidence_strings(v))
        return out
    return [str(value)]


def assert_response_identity(
    *,
    expected: str,
    response_text: str | None = None,
    response_json: Any = None,
    json_fields: list[str] | None = None,
    context: str = "",
) -> None:
    """Assert the response identifies the entity that was REQUESTED.

    Parameters
    ----------
    expected:
        The requested entity's identity marker: a tsp/slug/code string or a
        distinctive title fragment (e.g. ``"Elba Express"``, ``"TSPNO=2"``,
        ``"BUID=80"``). Comparison is case-insensitive substring.
    response_text:
        Raw text (HTML/XML) to scan — its <title> and full body are checked.
    response_json:
        Parsed JSON payload; if *json_fields* is given only those fields are
        consulted (dot-paths allowed), otherwise the whole structure.
    json_fields:
        Dot-path fields whose values count as identity evidence,
        e.g. ``["tsp.name", "header.tspCode"]``.
    context:
        Human-readable label for the error message (scraper + tenant).

    Raises
    ------
    TenantFallbackError
        When NO supplied evidence matches *expected*. This is deliberate:
        a fallback page means every subsequent row would be mislabeled.
    """
    evidence: dict[str, str] = {}
    if response_text is not None:
        title = _page_title(response_text)
        if title:
            evidence["page_title"] = title
        # first 20k chars are enough for identity and keep matching cheap
        evidence.setdefault("body_head", (response_text or "")[:20000])
    if response_json is not None:
        if json_fields:
            for field in json_fields:
                cur: Any = response_json
                ok = True
                for part in field.split("."):
                    if isinstance(cur, dict) and part in cur:
                        cur = cur[part]
                    else:
                        ok = False
                        break
                if ok:
                    vals = _as_evidence_strings(cur)
                    if vals:
                        evidence[field] = " ".join(vals)
        else:
            vals = _as_evidence_strings(response_json)
            if vals:
                evidence["json"] = " ".join(vals)[:20000]

    needle = expected.strip().lower()
    if not needle:
        raise ValueError("assert_response_identity: 'expected' must be non-empty")

    matched = [name for name, hay in evidence.items() if needle in hay.lower()]
    if matched:
        return

    raise TenantFallbackError(expected=expected, evidence=evidence, context=context or "request")
