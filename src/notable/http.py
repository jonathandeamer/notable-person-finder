"""One httpx client: fixed headers, bounded retry, per-host pacing, caching."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from notable.cache import Cache, cache_key
from notable.config import TransportConfig
from notable.errors import ProviderFailure

logger = logging.getLogger(__name__)

# Fixed response-affecting headers. Authorization is excluded because no
# secret may reach disk; the actual User-Agent is folded into the per-client
# transport profile below.
_FIXED_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en",
    "Accept-Encoding": "gzip, deflate",
}


def transport_profile(user_agent: str) -> str:
    return json.dumps(
        {**_FIXED_HEADERS, "User-Agent": user_agent},
        sort_keys=True,
        separators=(",", ":"),
    )


def _noop() -> None:
    """Commit for a response that is already stored, or must never be."""


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    text: str
    content_type: str | None = None
    # A cache hit cost nothing. `llm.py` relies on this to keep replayed calls
    # out of the run's spend: counting them would report money that was never
    # spent and could trip the budget cap during a free crash replay.
    from_cache: bool = False
    # Stores this response. A no-op unless the call was made with
    # `defer_cache=True` and has not been committed yet. The caller invokes it
    # once the response has passed every downstream check.
    commit: Callable[[], None] = _noop

    def json(self) -> Any:
        return json.loads(self.text)


@dataclass(frozen=True, slots=True)
class RetryStats:
    """What the transport actually did, for the run report.

    Recovered failures raise nothing and log nothing. Counting is the only way
    the live gate can see them.
    """

    attempts: int = 0
    retries: int = 0
    rate_limited: int = 0


class Transport:
    def __init__(
        self,
        config: TransportConfig,
        cache: Cache,
        *,
        client: httpx.Client,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._cache = cache
        self._client = client
        self._sleep = sleep
        self._last_call: dict[str, float] = {}
        self._user_agent = f"notable/0.1 (+{config.contact_url})"
        self._transport_profile = transport_profile(self._user_agent)
        # The client is a transport seam for tests, not a source of headers or
        # cookies. Clear both so client-level defaults cannot vary a response
        # without appearing in the cache profile.
        self._client.headers.clear()
        self._client.cookies.clear()
        self.stats = RetryStats()
        self.cache_hits = 0
        self.cache_misses = 0

    def request(
        self,
        *,
        provider: str,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        ttl_seconds: int | None,
        timeout: float | None = None,
        max_bytes: int | None = None,
        extra_key: dict[str, object] | None = None,
        auth_token: str | None = None,
        auth_header: str = "Authorization",
        bypass_cache: bool = False,
        defer_cache: bool = False,
    ) -> Response:
        key = cache_key(
            provider=provider,
            method=method,
            url=url,
            body={"params": params or {}, "json": json_body},
            transport_profile=self._transport_profile,
            extra=extra_key,
        )
        # `bypass_cache` skips the *read* only; the fresh response is still
        # stored. `ttl_seconds=None` cannot express this -- it means "never
        # expires", so using it for --fresh-feeds does the opposite.
        if not bypass_cache:
            cached = self._cache.get(key, ttl_seconds=ttl_seconds)
            if cached is not None:
                self.cache_hits += 1
                return Response(
                    status=int(cached["status"]),
                    text=str(cached["text"]),
                    content_type=cached.get("content_type"),
                    from_cache=True,
                )
        self.cache_misses += 1

        headers = dict(_FIXED_HEADERS)
        headers["User-Agent"] = self._user_agent
        if auth_token:
            headers[auth_header] = auth_token

        response = self._send(
            method=method,
            url=url,
            params=params,
            json_body=json_body,
            headers=headers,
            timeout=timeout or self._config.read_timeout_seconds,
            max_bytes=max_bytes,
        )

        stored = False

        def commit() -> None:
            # Idempotent: callers may commit on a path that also runs on a
            # cache hit, and a double commit must not rewrite the entry.
            nonlocal stored
            if stored:
                return
            stored = True
            self._cache.put(
                key,
                {
                    "status": response.status,
                    "text": response.text,
                    "content_type": response.content_type,
                },
            )

        # Only *validated* successes are cached. A transport-level 200 is not
        # yet a success: JSON parsing and domain validation are downstream, and
        # a malformed response stored here becomes a permanent cache hit that
        # is re-rejected on every retry until the item is abandoned -- and is
        # then recorded into the fixture later phases are graded against.
        if not defer_cache:
            commit()
            return response
        return Response(
            status=response.status, text=response.text, from_cache=False, commit=commit
        )

    def _send(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json_body: dict[str, Any] | None,
        headers: dict[str, str],
        timeout: float,
        max_bytes: int | None = None,
    ) -> Response:
        backoff = self._config.initial_backoff_seconds
        last: Exception | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            self._pace(url)
            self._count(attempts=1, retries=1 if attempt > 1 else 0)
            self._client.cookies.clear()
            try:
                request = httpx.Request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                )
                request.extensions["timeout"] = httpx.Timeout(
                    timeout, connect=self._config.connect_timeout_seconds
                ).as_dict()
                # send() transmits this fully-formed request as-is. Using
                # client.request() would merge client-level headers/cookies
                # back into the request and undermine the cache profile.
                # stream=True only when a size bound is set: reading the
                # body incrementally is how the bound is enforced without
                # trusting a Content-Length header that many servers omit
                # or lie about.
                raw = self._client.send(
                    request, follow_redirects=True, stream=max_bytes is not None
                )
            except httpx.HTTPError as error:
                last = ProviderFailure(
                    f"{type(error).__name__}: {error}", permanent=False
                )
            else:
                try:
                    try:
                        text = self._read_body(raw, max_bytes, url)
                    except httpx.HTTPError as error:
                        # Mid-stream errors (timeouts, connection drops,
                        # decoding failures) on the streamed path must become
                        # retryable ProviderFailures too, matching the
                        # non-streamed path where the whole body was read
                        # inside the client.send() try above.
                        last = ProviderFailure(
                            f"{type(error).__name__}: {error}", permanent=False
                        )
                    else:
                        if raw.status_code < 400:
                            self._client.cookies.clear()
                            return Response(
                                status=raw.status_code,
                                text=text,
                                content_type=raw.headers.get("content-type"),
                            )
                        if raw.status_code < 500 and raw.status_code != 429:
                            raise ProviderFailure(
                                f"HTTP {raw.status_code} from {url}", permanent=True
                            )
                        if raw.status_code == 429:
                            self._count(rate_limited=1)
                        last = ProviderFailure(
                            f"HTTP {raw.status_code} from {url}", permanent=False
                        )
                finally:
                    if max_bytes is not None:
                        raw.close()
            finally:
                # Do not carry Set-Cookie state into a later request, including
                # after failures and redirects handled by httpx.
                self._client.cookies.clear()

            if attempt < self._config.max_attempts:
                logger.info(
                    "retrying %s after %s (attempt %d/%d)",
                    url,
                    last,
                    attempt,
                    self._config.max_attempts,
                )
                self._sleep(backoff)
                backoff *= 2
        raise last or ProviderFailure(f"no response from {url}", permanent=False)

    @staticmethod
    def _read_body(raw: httpx.Response, max_bytes: int | None, url: str) -> str:
        """Read the body, enforcing `max_bytes` by streaming rather than by
        trusting `Content-Length` -- many servers omit it or lie."""
        if max_bytes is None:
            return raw.text
        total = 0
        chunks: list[bytes] = []
        for chunk in raw.iter_bytes():
            total += len(chunk)
            if total > max_bytes:
                raise ProviderFailure(
                    f"response exceeded {max_bytes} bytes from {url}", permanent=True
                )
            chunks.append(chunk)
        return b"".join(chunks).decode(raw.encoding or "utf-8", errors="replace")

    def _count(
        self, *, attempts: int = 0, retries: int = 0, rate_limited: int = 0
    ) -> None:
        self.stats = RetryStats(
            attempts=self.stats.attempts + attempts,
            retries=self.stats.retries + retries,
            rate_limited=self.stats.rate_limited + rate_limited,
        )

    def _pace(self, url: str) -> None:
        interval = self._config.per_host_min_interval_ms / 1000
        if interval <= 0:
            return
        host = urlsplit(url).netloc
        previous = self._last_call.get(host)
        now = time.monotonic()
        if previous is not None:
            wait = interval - (now - previous)
            if wait > 0:
                self._sleep(wait)
        self._last_call[host] = time.monotonic()
