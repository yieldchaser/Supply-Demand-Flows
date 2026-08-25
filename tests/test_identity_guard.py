"""Tests for scrapers.base.identity — the tenant-fallback trap guard."""

from __future__ import annotations

import pytest

from scrapers.base.identity import (
    TenantFallbackError,
    _page_title,
    assert_response_identity,
)

HTML_GULF_SOUTH = """<html><head><title>Gulf South Op Availability</title></head>
<body><table><tr><td>24329</td></tr></table></body></html>"""

HTML_TGP = """<html><head><title>Tennessee Gas Pipeline OpAvail</title></head><body></body></html>"""


class TestPageTitle:
    def test_extracts_title(self) -> None:
        assert _page_title(HTML_GULF_SOUTH) == "Gulf South Op Availability"

    def test_none_when_missing(self) -> None:
        assert _page_title("<html><body>x</body></html>") is None

    def test_multiline_title(self) -> None:
        assert _page_title("<title>Foo\n  Bar </title>") == "Foo\n  Bar"


class TestAssertResponseIdentity:
    def test_passes_on_matching_title(self) -> None:
        assert_response_identity(
            expected="Gulf South", response_text=HTML_GULF_SOUTH, context="gulf_south"
        )

    def test_case_insensitive(self) -> None:
        assert_response_identity(
            expected="gulf south", response_text=HTML_GULF_SOUTH
        )

    def test_raises_on_wrong_entity(self) -> None:
        # THE TRAP: asked for Elba Express, got the TGP page (HTTP 200).
        with pytest.raises(TenantFallbackError, match="tenant-fallback"):
            assert_response_identity(
                expected="Elba Express",
                response_text=HTML_TGP,
                context="km_pipeline2/elba",
            )

    def test_json_field_dotpath(self) -> None:
        payload = {"header": {"tspCode": "2"}, "rows": []}
        assert_response_identity(
            expected="2",
            response_json=payload,
            json_fields=["header.tspCode"],
            context="quorum",
        )

    def test_json_field_mismatch_raises(self) -> None:
        payload = {"header": {"tspCode": "10"}}
        with pytest.raises(TenantFallbackError):
            assert_response_identity(
                expected="2",
                response_json=payload,
                json_fields=["header.tspCode"],
                context="quorum",
            )

    def test_whole_payload_fallback(self) -> None:
        payload = {"name": "Transcontinental Gas Pipe Line"}
        assert_response_identity(expected="transcontinental", response_json=payload)

    def test_missing_field_counts_as_no_evidence(self) -> None:
        payload = {"unrelated": True}
        with pytest.raises(TenantFallbackError):
            assert_response_identity(
                expected="2", response_json=payload, json_fields=["header.tspCode"]
            )

    def test_numeric_code_stringified(self) -> None:
        payload = {"tspNo": 2}
        assert_response_identity(expected="2", response_json=payload)

    def test_empty_expected_is_valueerror(self) -> None:
        with pytest.raises(ValueError):
            assert_response_identity(expected="  ", response_text=HTML_TGP)

    def test_error_message_includes_context_and_evidence(self) -> None:
        with pytest.raises(TenantFallbackError) as excinfo:
            assert_response_identity(
                expected="Elba Express",
                response_text=HTML_TGP,
                context="km_pipeline2/elba",
            )
        msg = str(excinfo.value)
        assert "km_pipeline2/elba" in msg
        assert "Tennessee" in msg  # shows what we ACTUALLY got
