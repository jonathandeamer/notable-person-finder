"""Offline contract tests for ArticleFetcher and Trafilatura ArticleExtractor.

HTTP is served by a real ``HttpTransport`` over ``httpx.MockTransport`` so URL
safety, redirects, ``ResponseLimit.ARTICLE``, and provider key wiring are
exercised. Extraction uses fixture HTML only — never the network (K3).
"""

from __future__ import annotations

import socket
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Literal

import httpx
import pytest

from notable_person_finder.config.models import TransportConfig
from notable_person_finder.providers import articles
from notable_person_finder.providers.articles import (
    ARTICLE_PROVIDER,
    EXTRACTOR_VERSION,
    OPERATION_FETCH_ARTICLE,
    ArticleAccessDenied,
    ArticleAccessKind,
    ArticleFetcher,
    ArticleFetchSuccess,
    ArticleTextBlock,
    ExtractedArticle,
    ExtractionQuality,
    HttpxArticleFetcher,
    TrafilaturaArticleExtractor,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import (
    HttpResponse,
    HttpTransport,
    ResponseLimit,
    build_transport,
)
from notable_person_finder.runs.clock import FakeClock

FIXTURES = Path(__file__).parent / "fixtures"

ARTICLE_URL = "https://news.example.com/stories/ada-lovelace"
RESOLVER = StaticHostResolver(
    {
        "news.example.com": ("93.184.216.34",),
        "cdn.example.com": ("93.184.216.34",),
    }
)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def streaming_body(payload: bytes) -> list[bytes]:
    if not payload:
        return [payload]
    return [payload[start : start + 64] for start in range(0, len(payload), 64)]


def transport_for(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    config: TransportConfig | None = None,
) -> HttpTransport:
    return build_transport(
        config or TransportConfig(),
        version="0.1.0",
        resolver=RESOLVER,
        clock=FakeClock(),
        http_transport=httpx.MockTransport(handler),
    )


def fetcher_for(
    payload: bytes,
    *,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
    seen: list[httpx.Request] | None = None,
    transport_config: TransportConfig | None = None,
    url: str = ARTICLE_URL,
) -> HttpxArticleFetcher:
    del url  # reserved for call sites; response does not depend on path

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(
            status,
            headers=dict(headers or {"content-type": "text/html; charset=utf-8"}),
            content=streaming_body(payload),
        )

    return HttpxArticleFetcher(
        transport_for(handler, config=transport_config),
        clock=FakeClock(),
    )


class RecordingTransport(HttpTransport):
    """Stand-in at the ``request`` seam; does not construct a real client."""

    def __init__(
        self,
        *,
        response: HttpResponse | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.calls: list[dict[str, object]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        provider: str,
        operation: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str | int] | None = None,
        limit: ResponseLimit = ResponseLimit.API,
        profile: Literal["ordinary", "llm"] = "ordinary",
    ) -> HttpResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "provider": provider,
                "operation": operation,
                "headers": dict(headers) if headers is not None else None,
                "params": dict(params) if params is not None else None,
                "limit": limit,
                "profile": profile,
            }
        )
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response


def http_response(
    payload: bytes,
    *,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
    requested_url: str = ARTICLE_URL,
    final_url: str | None = None,
    redirect_chain: tuple[str, ...] = (),
) -> HttpResponse:
    final = final_url if final_url is not None else requested_url
    return HttpResponse(
        requested_url=requested_url,
        final_url=final,
        redirect_chain=redirect_chain,
        destination_host="news.example.com",
        status_code=status,
        headers=dict(headers or {"content-type": "text/html; charset=utf-8"}),
        content=payload,
        encoded_bytes=len(payload),
        decoded_bytes=len(payload),
    )


# --- Protocol / constants ----------------------------------------------------


def test_httpx_fetcher_satisfies_protocol() -> None:
    fetcher: ArticleFetcher = fetcher_for(fixture_bytes("article_full.html"))
    assert callable(fetcher.fetch_article)


def test_extractor_version_is_one() -> None:
    assert EXTRACTOR_VERSION == 1


# --- fetch_article happy path ------------------------------------------------


def test_fetch_article_returns_success_with_html_for_current_attempt() -> None:
    payload = fixture_bytes("article_full.html")
    seen: list[httpx.Request] = []
    fetcher = fetcher_for(payload, seen=seen)

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleFetchSuccess)
    assert result.requested_url == ARTICLE_URL
    assert result.final_url == ARTICLE_URL
    assert result.redirect_chain == ()
    assert result.status_code == 200
    assert result.content_type is not None
    assert "text/html" in result.content_type
    assert result.html == payload
    assert result.response_bytes == len(payload)
    assert len(seen) == 1
    assert seen[0].method == "GET"
    assert str(seen[0].url).startswith("https://news.example.com/")


def test_fetch_article_exactly_one_get_with_article_limit_and_provider_key() -> None:
    payload = fixture_bytes("article_full.html")
    transport = RecordingTransport(response=http_response(payload))
    fetcher = HttpxArticleFetcher(transport, clock=FakeClock())

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleFetchSuccess)
    assert len(transport.calls) == 1
    call = transport.calls[0]
    assert call["method"] == "GET"
    assert call["url"] == ARTICLE_URL
    assert call["provider"] == ARTICLE_PROVIDER == "article_http"
    assert call["operation"] == OPERATION_FETCH_ARTICLE == "fetch_article"
    assert call["limit"] == ResponseLimit.ARTICLE
    assert call["params"] is None


def test_fetch_article_records_redirect_chain_on_success() -> None:
    payload = fixture_bytes("article_full.html")
    transport = RecordingTransport(
        response=http_response(
            payload,
            final_url="https://cdn.example.com/stories/ada-lovelace",
            redirect_chain=("https://news.example.com/stories/ada-lovelace",),
        )
    )
    fetcher = HttpxArticleFetcher(transport, clock=FakeClock())

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleFetchSuccess)
    assert result.final_url == "https://cdn.example.com/stories/ada-lovelace"
    assert result.redirect_chain == (ARTICLE_URL,)


# --- Typed access states -----------------------------------------------------


def test_http_404_is_not_found_typed_access_not_exception() -> None:
    body = b"<html><body>not found SECRET_404_BODY</body></html>"
    fetcher = fetcher_for(body, status=404)

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleAccessDenied)
    assert result.kind == ArticleAccessKind.NOT_FOUND
    assert result.status_code == 404
    assert result.requested_url == ARTICLE_URL
    assert "SECRET_404_BODY" not in (result.detail or "")
    assert "SECRET_404_BODY" not in str(result)


def test_http_410_is_not_found_typed_access_not_exception() -> None:
    """410 Gone is not_found; removing 410 from the status set must fail this test."""
    body = b"<html><body>gone SECRET_410_BODY</body></html>"
    fetcher = fetcher_for(body, status=410)

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleAccessDenied)
    assert result.kind == ArticleAccessKind.NOT_FOUND
    assert result.status_code == 410
    assert result.requested_url == ARTICLE_URL
    assert "SECRET_410_BODY" not in (result.detail or "")
    assert "SECRET_410_BODY" not in str(result)


def test_http_401_is_authentication_required_typed_access() -> None:
    body = b'{"error":"login required LEAK_AUTH"}'
    fetcher = fetcher_for(
        body,
        status=401,
        headers={"content-type": "application/json", "www-authenticate": "Bearer"},
    )

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleAccessDenied)
    assert result.kind == ArticleAccessKind.AUTHENTICATION_REQUIRED
    assert result.status_code == 401
    assert "LEAK_AUTH" not in (result.detail or "")
    assert "LEAK_AUTH" not in str(result)


def test_http_403_is_access_denied_typed_access() -> None:
    body = b"<html>paywall LEAK_403</html>"
    fetcher = fetcher_for(body, status=403)

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleAccessDenied)
    assert result.kind == ArticleAccessKind.ACCESS_DENIED
    assert result.status_code == 403
    assert "LEAK_403" not in (result.detail or "")


def test_unsupported_content_type_is_typed_access_without_body_in_detail() -> None:
    pdf_magic = b"%PDF-1.4 LEAK_PDF_BODY binary-content"
    fetcher = fetcher_for(
        pdf_magic,
        headers={"content-type": "application/pdf"},
    )

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleAccessDenied)
    assert result.kind == ArticleAccessKind.UNSUPPORTED_CONTENT
    assert result.status_code == 200
    assert "LEAK_PDF" not in (result.detail or "")
    assert "LEAK_PDF" not in str(result)
    assert "%PDF" not in (result.detail or "")


def test_application_xhtml_xml_content_type_is_accepted() -> None:
    """Design step 4: application/xhtml+xml is HTML-ish (not only text/html)."""
    payload = fixture_bytes("article_full.html")
    fetcher = fetcher_for(
        payload,
        headers={"content-type": "application/xhtml+xml; charset=utf-8"},
    )

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleFetchSuccess)
    assert result.html == payload
    assert result.content_type is not None
    assert "application/xhtml+xml" in result.content_type


def test_missing_content_type_accepts_body_that_looks_like_html() -> None:
    """Missing Content-Type still succeeds when the body looks like HTML.

    Use RecordingTransport so the adapter sees a true absent content-type;
    httpx.Response may invent ``text/html`` when the body looks like HTML.
    """
    payload = fixture_bytes("article_full.html")
    # Truthy headers map without content-type (``{}`` is falsy and would
    # re-trigger http_response's text/html default).
    transport = RecordingTransport(
        response=http_response(payload, headers={"cache-control": "no-store"})
    )
    fetcher = HttpxArticleFetcher(transport, clock=FakeClock())

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleFetchSuccess)
    assert result.html == payload
    assert result.content_type is None


def test_text_plain_content_type_accepts_body_that_looks_like_html() -> None:
    """Tolerant text/*: text/plain with HTML-shaped body is accepted."""
    payload = fixture_bytes("article_full.html")
    fetcher = fetcher_for(
        payload,
        headers={"content-type": "text/plain; charset=utf-8"},
    )

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleFetchSuccess)
    assert result.html == payload
    assert result.content_type is not None
    assert result.content_type.startswith("text/plain")


def test_response_too_large_is_typed_access_not_provider_failure() -> None:
    transport = RecordingTransport(
        error=ProviderFailure(
            FailureCategory.RESPONSE_TOO_LARGE,
            provider=ARTICLE_PROVIDER,
            operation=OPERATION_FETCH_ARTICLE,
            detail="decoded body exceeded 10 bytes",
        )
    )
    fetcher = HttpxArticleFetcher(transport, clock=FakeClock())

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleAccessDenied)
    assert result.kind == ArticleAccessKind.RESPONSE_TOO_LARGE
    assert result.requested_url == ARTICLE_URL
    assert len(transport.calls) == 1
    # Compact detail only — never a body sample.
    assert result.detail is not None
    assert "<html" not in result.detail


def test_response_too_large_via_transport_byte_limit() -> None:
    # Positive control: real transport path for ARTICLE limit.
    huge = b"<html>" + (b"x" * 200) + b"</html>"
    fetcher = fetcher_for(
        huge,
        transport_config=TransportConfig(max_article_response_bytes=50),
    )

    result = fetcher.fetch_article(ARTICLE_URL)

    assert isinstance(result, ArticleAccessDenied)
    assert result.kind == ArticleAccessKind.RESPONSE_TOO_LARGE


def test_unsafe_request_url_is_provider_failure_configuration() -> None:
    fetcher = fetcher_for(b"unused")

    with pytest.raises(ProviderFailure) as caught:
        fetcher.fetch_article("http://127.0.0.1/local-only")

    assert caught.value.category == FailureCategory.CONFIGURATION
    assert caught.value.provider == ARTICLE_PROVIDER
    assert caught.value.operation == OPERATION_FETCH_ARTICLE
    assert "unsafe" in (caught.value.detail or "").lower()


def test_unsafe_redirect_destination_is_provider_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "news.example.com":
            return httpx.Response(
                302,
                headers={"location": "http://127.0.0.1/private"},
                content=b"",
            )
        return httpx.Response(200, content=b"should-not-reach")

    fetcher = HttpxArticleFetcher(
        transport_for(handler),
        clock=FakeClock(),
    )

    with pytest.raises(ProviderFailure) as caught:
        fetcher.fetch_article(ARTICLE_URL)

    assert caught.value.category == FailureCategory.CONFIGURATION
    assert "unsafe" in (caught.value.detail or "").lower()


def test_transient_server_error_remains_provider_failure() -> None:
    fetcher = fetcher_for(b"boom LEAK_500", status=500)

    with pytest.raises(ProviderFailure) as caught:
        fetcher.fetch_article(ARTICLE_URL)

    assert caught.value.category == FailureCategory.TRANSIENT_SERVER_ERROR
    assert caught.value.status_code == 500
    assert "LEAK_500" not in str(caught.value)
    assert "LEAK_500" not in (caught.value.detail or "")


def test_provider_failure_from_transport_is_not_rewrapped() -> None:
    original = ProviderFailure(
        FailureCategory.TIMEOUT,
        provider=ARTICLE_PROVIDER,
        operation=OPERATION_FETCH_ARTICLE,
        detail="ReadTimeout",
    )
    transport = RecordingTransport(error=original)
    fetcher = HttpxArticleFetcher(transport, clock=FakeClock())

    with pytest.raises(ProviderFailure) as caught:
        fetcher.fetch_article(ARTICLE_URL)

    assert caught.value is original


def test_unexpected_exception_becomes_internal_without_body_in_detail() -> None:
    class BodyBearingFault(Exception):
        def __str__(self) -> str:
            return "remote said: <html>SECRET_RESPONSE_BODY</html>"

    transport = RecordingTransport(error=BodyBearingFault())
    fetcher = HttpxArticleFetcher(transport, clock=FakeClock())

    with pytest.raises(ProviderFailure) as caught:
        fetcher.fetch_article(ARTICLE_URL)

    assert caught.value.category == FailureCategory.INTERNAL
    assert caught.value.detail == "BodyBearingFault"
    assert "SECRET_RESPONSE_BODY" not in str(caught.value)
    assert "<html" not in (caught.value.detail or "")


# --- Extractor ---------------------------------------------------------------


def test_extract_full_fixture_returns_blocks_metadata_and_full_quality() -> None:
    extractor = TrafilaturaArticleExtractor()
    html = fixture_bytes("article_full.html")

    extracted = extractor.extract_article(html)

    assert isinstance(extracted, ExtractedArticle)
    assert not hasattr(extracted, "html")
    assert "html" not in extracted.__dataclass_fields__
    assert extracted.title is not None
    assert "Ada Lovelace" in extracted.title
    assert extracted.dek is not None
    assert "Analytical Engine" in extracted.dek or "profile" in extracted.dek.lower()
    assert extracted.byline is not None
    assert "Jane" in extracted.byline
    assert extracted.published_at is not None
    assert extracted.published_at.startswith("2024-06-15")
    assert "biography" in extracted.editorial_labels or "computing" in (
        extracted.editorial_labels
    )
    assert len(extracted.blocks) >= 2
    assert all(isinstance(block, ArticleTextBlock) for block in extracted.blocks)
    assert extracted.blocks[0].id == "b1"
    assert extracted.blocks[1].id == "b2"
    assert all(block.text.strip() for block in extracted.blocks)
    body = " ".join(block.text for block in extracted.blocks)
    assert "Analytical Engine" in body or "computer programmer" in body
    assert "Cookie policy" not in body
    assert extracted.quality == ExtractionQuality.FULL


def test_extract_accepts_str_html() -> None:
    extractor = TrafilaturaArticleExtractor()
    html = fixture_bytes("article_full.html").decode("utf-8")

    extracted = extractor.extract_article(html)

    assert extracted.quality == ExtractionQuality.FULL
    assert extracted.blocks
    assert not hasattr(extracted, "html")


def test_extract_empty_fixture_is_empty_quality() -> None:
    extractor = TrafilaturaArticleExtractor()
    extracted = extractor.extract_article(fixture_bytes("article_empty.html"))

    assert extracted.quality == ExtractionQuality.EMPTY
    assert extracted.blocks == ()
    assert "empty_body" in extracted.warnings
    assert not hasattr(extracted, "html")


def test_extract_partial_fixture_is_partial_quality() -> None:
    extractor = TrafilaturaArticleExtractor()
    extracted = extractor.extract_article(fixture_bytes("article_partial.html"))

    assert extracted.quality == ExtractionQuality.PARTIAL
    assert len(extracted.blocks) >= 1
    assert sum(len(block.text) for block in extracted.blocks) < 250
    assert not hasattr(extracted, "html")


def test_extract_is_network_free_even_if_url_like_strings_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Extractor must not open sockets; pure in-process Trafilatura only.

    Asserting only on the returned object proves nothing: it looks identical
    whether or not the embedded link was fetched. Sockets are therefore made
    unusable for the duration of the call, so any attempt fails loudly.
    """

    def forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("the extractor opened a socket")

    monkeypatch.setattr(socket, "socket", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)

    extractor = TrafilaturaArticleExtractor()
    # Relative asset URLs and external links must not trigger downloads.
    html = (
        b"<!DOCTYPE html><html><head><title>Linked</title></head><body>"
        b"<article><h1>Linked</h1>"
        b'<p>See <a href="https://definitely-not-fetched.invalid/page">here</a>.</p>'
        b'<img src="/assets/hero.jpg">'
        b"<p>Second paragraph keeps this from collapsing to empty under "
        b"generic extraction while remaining short enough for partial quality.</p>"
        b"</article></body></html>"
    )
    extracted = extractor.extract_article(html)
    assert isinstance(extracted, ExtractedArticle)
    assert not hasattr(extracted, "html")


def test_socket_guard_in_the_network_free_test_actually_bites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Positive control for the guard above: the patch really blocks sockets."""

    def forbidden(*args: object, **kwargs: object) -> object:
        raise AssertionError("opened a socket")

    monkeypatch.setattr(socket, "socket", forbidden)
    with pytest.raises(AssertionError, match="opened a socket"):
        socket.socket()


_MARKUP = "<html><body><p>secret markup</p></body></html>"


def test_extract_failure_detail_path_never_embeds_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If extraction internals fail, surface stays typed without HTML leak.

    The happy-path warnings are hard-coded constants, so asserting `"<html"
    not in warning` over them can never fail. This drives the real failure
    branch with an exception whose message *is* the document, which is the
    only way markup could reach a warning.
    """
    raised: list[str] = []

    def exploding(document: str, **kwargs: object) -> object:
        raised.append(document)
        raise RuntimeError(document)

    monkeypatch.setattr(articles.trafilatura, "bare_extraction", exploding)

    extractor = TrafilaturaArticleExtractor()
    extracted = extractor.extract_article(_MARKUP.encode("utf-8"))

    # The failure branch was genuinely taken, with markup in the exception.
    assert raised == [_MARKUP]
    assert extracted.warnings == ("extract_failed", "empty_body")
    assert extracted.quality is ExtractionQuality.EMPTY
    assert isinstance(extracted, ExtractedArticle)
    assert not hasattr(extracted, "html")
    for warning in extracted.warnings:
        assert "<html" not in warning
        assert "<body" not in warning
        assert "secret markup" not in warning
    assert "secret markup" not in repr(extracted)


def test_extract_failure_branch_would_show_a_leak_if_one_existed() -> None:
    """Positive control: the assertions above can fail.

    Markup fed through the same warning-shaped tuple trips every check, so a
    green run of the test above is evidence about the extractor, not about
    unreachable strings.
    """
    leaked = (f"extract_failed: {_MARKUP}",)
    assert any("<html" in warning for warning in leaked)
    assert any("secret markup" in warning for warning in leaked)
