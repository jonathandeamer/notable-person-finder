from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import TracebackType
from typing import Any, Literal
from urllib.parse import urlsplit
import zlib

import httpx

from notable_person_finder.config.models import TransportConfig
from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
    parse_retry_after,
)
from notable_person_finder.providers.safety import HostResolver, UnsafeUrl, assert_safe_url
from notable_person_finder.runs.clock import Clock

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})

_STATUS_CATEGORIES: tuple[tuple[range, FailureCategory], ...] = (
    (range(500, 600), FailureCategory.TRANSIENT_SERVER_ERROR),
    (range(400, 500), FailureCategory.CONFIGURATION),
)

_EXACT_STATUS_CATEGORIES: Mapping[int, FailureCategory] = {
    401: FailureCategory.AUTHENTICATION,
    402: FailureCategory.ACCESS_DENIED,
    403: FailureCategory.ACCESS_DENIED,
    404: FailureCategory.ACCESS_DENIED,
    408: FailureCategory.TIMEOUT,
    410: FailureCategory.ACCESS_DENIED,
    429: FailureCategory.RATE_LIMIT,
    451: FailureCategory.ACCESS_DENIED,
    502: FailureCategory.PROVIDER_UNAVAILABLE,
    503: FailureCategory.PROVIDER_UNAVAILABLE,
    504: FailureCategory.TIMEOUT,
}

_TRANSPORT_ERROR_CATEGORIES: tuple[tuple[type[Exception], FailureCategory], ...] = (
    (httpx.TimeoutException, FailureCategory.TIMEOUT),
    (httpx.RemoteProtocolError, FailureCategory.MALFORMED_RESPONSE),
    (httpx.DecodingError, FailureCategory.MALFORMED_RESPONSE),
    (httpx.TransportError, FailureCategory.NETWORK),
)

# Caller-supplied headers exist on this transport almost exclusively to
# authenticate to the *originally requested* origin (Brave's
# X-Subscription-Token, OpenRouter's bearer token, ...). httpx's own
# follow_redirects logic strips Authorization on a cross-origin hop; our
# manual redirect loop reproduces that guarantee for every caller header, not
# just the standard ones (`authorization`, `proxy-authorization`, `cookie`
# are treated as credentials at minimum, but no caller header of any name is
# forwarded to a different origin), since a provider that can be induced to
# return a redirect must not hand its credentials to whatever host the
# Location header names.


def _origin(url: str) -> tuple[str, str, int]:
    """Scheme + host + port, with the scheme's default port made explicit."""
    parsed = urlsplit(url)
    scheme = parsed.scheme.lower()
    host = (parsed.hostname or "").lower()
    port = parsed.port if parsed.port is not None else (443 if scheme == "https" else 80)
    return (scheme, host, port)


class ResponseLimit(StrEnum):
    API = "api"
    ARTICLE = "article"


@dataclass(frozen=True, slots=True)
class HttpResponse:
    requested_url: str
    final_url: str
    redirect_chain: tuple[str, ...]
    destination_host: str
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    encoded_bytes: int
    decoded_bytes: int


def _decoder(content_encoding: str) -> Any | None:
    encoding = content_encoding.strip().lower()
    if encoding in {"", "identity"}:
        return None
    if encoding == "gzip":
        return zlib.decompressobj(16 + zlib.MAX_WBITS)
    if encoding == "deflate":
        return zlib.decompressobj()
    raise ValueError("unsupported content encoding")


def _bounded_body(response: httpx.Response, *, byte_limit: int) -> tuple[bytes, int]:
    """Read encoded bytes and incrementally cap decoded output."""
    try:
        decoder = _decoder(response.headers.get("content-encoding", ""))
    except ValueError as error:
        raise httpx.DecodingError(str(error), request=response.request) from error

    encoded_bytes = 0
    decoded = bytearray()
    for chunk in response.iter_raw():
        encoded_bytes += len(chunk)
        if encoded_bytes > byte_limit:
            raise OverflowError("encoded")
        if decoder is None:
            piece = chunk
        else:
            piece = decoder.decompress(chunk, byte_limit - len(decoded) + 1)
        decoded.extend(piece)
        if len(decoded) > byte_limit:
            raise OverflowError("decoded")

    if decoder is not None:
        decoded.extend(decoder.flush(byte_limit - len(decoded) + 1))
        if len(decoded) > byte_limit:
            raise OverflowError("decoded")
    return bytes(decoded), encoded_bytes


class HttpTransport:
    """Run-scoped synchronous HTTP boundary shared by every ordinary adapter."""

    def __init__(
        self,
        client: httpx.Client,
        *,
        config: TransportConfig,
        user_agent: str,
        resolver: HostResolver,
        clock: Clock,
    ) -> None:
        self._client = client
        self._config = config
        self._user_agent = user_agent
        self._resolver = resolver
        self._clock = clock

    def __enter__(self) -> HttpTransport:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def automatic_retries_enabled(self) -> bool:
        """Only the central retry coordinator starts a repeat request."""
        return False

    def _timeout_for(self, *, profile: Literal["ordinary", "llm"]) -> httpx.Timeout:
        read = (
            self._config.llm_read_timeout_seconds
            if profile == "llm"
            else self._config.read_timeout_seconds
        )
        return httpx.Timeout(
            read, connect=self._config.connect_timeout_seconds, pool=read
        )

    def _byte_limit(self, limit: ResponseLimit) -> int:
        if limit is ResponseLimit.ARTICLE:
            return self._config.max_article_response_bytes
        return self._config.max_api_response_bytes

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
        requested_url = url
        current_url = url
        redirect_chain: list[str] = []
        byte_limit = self._byte_limit(limit)
        original_origin = _origin(url)

        for _ in range(self._config.max_redirects + 1):
            try:
                host = assert_safe_url(current_url, resolver=self._resolver)
            except UnsafeUrl as error:
                raise ProviderFailure(
                    FailureCategory.CONFIGURATION,
                    provider=provider,
                    operation=operation,
                    detail="unsafe request destination",
                ) from error
            hop_headers = headers
            if redirect_chain and headers and _origin(current_url) != original_origin:
                # A cross-origin redirect target never receives the caller's
                # headers: they exist on this transport to authenticate to
                # the originally requested origin, so none of them is safe
                # to hand to a different host, not only the named
                # credential headers.
                hop_headers = None
            response = self._send(
                method,
                current_url,
                provider=provider,
                operation=operation,
                headers=hop_headers,
                params=params if not redirect_chain else None,
                profile=profile,
                byte_limit=byte_limit,
                host=host,
                requested_url=requested_url,
                redirect_chain=tuple(redirect_chain),
            )
            if response is None:
                continue
            if isinstance(response, str):
                redirect_chain.append(current_url)
                current_url = response
                continue
            return response

        raise ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider=provider,
            operation=operation,
            detail=f"exceeded {self._config.max_redirects} redirects",
        )

    def _send(
        self,
        method: str,
        url: str,
        *,
        provider: str,
        operation: str,
        headers: Mapping[str, str] | None,
        params: Mapping[str, str | int] | None,
        profile: Literal["ordinary", "llm"],
        byte_limit: int,
        host: str,
        requested_url: str,
        redirect_chain: tuple[str, ...],
    ) -> HttpResponse | str | None:
        merged = {
            "user-agent": self._user_agent,
            # Only encodings handled by the bounded incremental decoder.
            "accept-encoding": "gzip, deflate",
        }
        if headers:
            for name, value in headers.items():
                lowered = name.lower()
                if lowered in {"user-agent", "accept-encoding"}:
                    continue
                merged[lowered] = value

        try:
            request = self._client.build_request(
                method,
                url,
                headers=merged,
                params=params,
                timeout=self._timeout_for(profile=profile),
            )
        except UnicodeEncodeError as error:
            raise ProviderFailure(
                FailureCategory.CONFIGURATION,
                provider=provider,
                operation=operation,
                detail="request contains non-ASCII characters",
            ) from error
        try:
            response = self._client.send(request, stream=True)
        except Exception as error:  # translated below; no httpx type escapes
            raise self._translate(error, provider=provider, operation=operation) from error

        try:
            if response.status_code in _REDIRECT_STATUSES:
                location = response.headers.get("location")
                if not location:
                    raise ProviderFailure(
                        FailureCategory.MALFORMED_RESPONSE,
                        provider=provider,
                        operation=operation,
                        detail=f"HTTP {response.status_code} without a Location header",
                    )
                return str(httpx.URL(url).join(location))

            if response.status_code >= 400:
                raise self._status_failure(response, provider=provider, operation=operation)

            try:
                content, encoded_bytes = _bounded_body(response, byte_limit=byte_limit)
            except OverflowError as error:
                raise ProviderFailure(
                    FailureCategory.RESPONSE_TOO_LARGE,
                    provider=provider,
                    operation=operation,
                    detail=f"{error.args[0]} body exceeded {byte_limit} bytes",
                ) from error

            return HttpResponse(
                requested_url=requested_url,
                final_url=url,
                redirect_chain=redirect_chain,
                destination_host=host,
                status_code=response.status_code,
                headers={
                    name.lower(): value
                    for name, value in response.headers.items()
                    if name.lower() not in {"authorization", "set-cookie", "cookie"}
                },
                content=content,
                encoded_bytes=encoded_bytes,
                decoded_bytes=len(content),
            )
        except ProviderFailure:
            raise
        except Exception as error:
            raise self._translate(error, provider=provider, operation=operation) from error
        finally:
            response.close()

    def _status_failure(
        self, response: httpx.Response, *, provider: str, operation: str
    ) -> ProviderFailure:
        status = response.status_code
        category = _EXACT_STATUS_CATEGORIES.get(status)
        if category is None:
            category = next(
                (mapped for span, mapped in _STATUS_CATEGORIES if status in span),
                FailureCategory.INTERNAL,
            )
        return ProviderFailure(
            category,
            provider=provider,
            operation=operation,
            status_code=status,
            retry_after_ms=parse_retry_after(
                response.headers.get("retry-after"), now=self._clock.now()
            ),
        )

    def _translate(
        self, error: Exception, *, provider: str, operation: str
    ) -> ProviderFailure:
        for error_type, category in _TRANSPORT_ERROR_CATEGORIES:
            if isinstance(error, error_type):
                return ProviderFailure(
                    category,
                    provider=provider,
                    operation=operation,
                    detail=type(error).__name__,
                )
        return ProviderFailure(
            FailureCategory.INTERNAL,
            provider=provider,
            operation=operation,
            detail=type(error).__name__,
        )


def build_transport(
    config: TransportConfig,
    *,
    version: str,
    resolver: HostResolver,
    clock: Clock,
    http_transport: httpx.BaseTransport | None = None,
) -> HttpTransport:
    client = httpx.Client(
        follow_redirects=False,  # each hop is validated by assert_safe_url first
        transport=http_transport,
        timeout=httpx.Timeout(
            config.read_timeout_seconds, connect=config.connect_timeout_seconds
        ),
    )
    return HttpTransport(
        client,
        config=config,
        user_agent=config.resolved_user_agent(version),
        resolver=resolver,
        clock=clock,
    )
