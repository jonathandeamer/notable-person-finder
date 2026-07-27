"""The single application-owned URL identity policy.

`canonicalize_article_url` is the only function in the codebase permitted to
normalize an article URL, and `publisher_key` is the only function permitted
to derive a publisher identity from one. Milestone 5's search occurrences and
redirect destinations both funnel through this exact function so that the
same article, seen from a feed link and from a search result, converges on
one identity.

Every rule here is deliberately conservative. Over-normalizing two distinct
articles onto the same canonical URL merges them, and a merge cannot be
undone once downstream state (assessments, a digest entry, a human decision)
has accumulated on top of it. Under-normalizing merely leaves a duplicate,
which is untidy but recoverable. When a rule is ambiguous, this module always
resolves it toward stripping less, not more.
"""

from __future__ import annotations

import re
from enum import StrEnum
from urllib.parse import urlsplit

# The utm_* family is matched by prefix (case-sensitive: only a literal
# lowercase "utm_" prefix counts). Every other tracking parameter here is
# matched by exact, case-sensitive string equality -- including the "cmp"
# and "CMP" entries below. Both are listed because the brief did not intend
# for one casing to imply the other: a case-insensitive reading would make
# listing both entries redundant, so the deliberate reading is that matching
# is exact and "Cmp" (or "cMp", etc.) is not on the list and survives.
_TRACKING_PREFIX = "utm_"
_TRACKING_PARAMETERS = frozenset(
    {
        "fbclid",
        "gclid",
        "mc_cid",
        "mc_eid",
        "igshid",
        "ref",
        "ref_src",
        "s",
        "cmp",
        "CMP",
    }
)

_DEFAULT_PORTS = {"http": 80, "https": 443}

# Percent-encoding is normalized in two ways only: hex digits are
# uppercased, and any percent-encoded octet that spells out an RFC 3986
# "unreserved" character (letters, digits, "-", ".", "_", "~") is decoded to
# that literal character. Nothing else about percent-encoding is touched --
# in particular, an encoded reserved character such as %2F is never decoded,
# because doing so could change how the path or query is later parsed.
_PERCENT_ENCODED = re.compile(r"%[0-9A-Fa-f]{2}")
_UNRESERVED = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)

# The registrable-domain approximation. A real public-suffix list is out of
# the dependency budget, so this hard-codes the common multi-part suffixes
# this application is expected to encounter. It is deliberately approximate:
# any second-level domain registered directly under a suffix not listed here
# (or any suffix with more than three labels) will be mis-split. Milestone
# 5's source policy is what actually depends on precise publisher identity;
# if it turns up a publisher this table gets wrong, that policy -- not this
# module -- should decide whether to grow the table or add a real
# public-suffix dependency.
_TWO_LABEL_SUFFIXES = frozenset(
    {"co.uk", "org.uk", "ac.uk", "com.au", "co.jp", "co.nz", "com.br"}
)


class UnusableUrlReason(StrEnum):
    """Why a URL cannot serve as an article identity."""

    UNSUPPORTED_SCHEME = "unsupported_scheme"
    MISSING_HOST = "missing_host"
    EMBEDDED_CREDENTIALS = "embedded_credentials"


class UnusableArticleUrl(ValueError):
    """A URL that cannot be canonicalized into an article identity."""

    def __init__(self, reason: UnusableUrlReason, *, url: str) -> None:
        self.reason = reason
        self.url = url
        super().__init__(f"{url}: {reason}")


def _normalize_percent_encoding(text: str) -> str:
    def replace(match: re.Match[str]) -> str:
        hex_digits = match.group(0)[1:]
        # Each %XX decodes one octet (0-255), not a Unicode code point. That
        # is fine here: the unreserved set is pure ASCII, so any octet at or
        # above 0x80 is never a candidate for decoding and only needs its
        # hex digits uppercased.
        octet = int(hex_digits, 16)
        char = chr(octet)
        if char in _UNRESERVED:
            return char
        return "%" + hex_digits.upper()

    return _PERCENT_ENCODED.sub(replace, text)


def _is_stripped_parameter(key: str) -> bool:
    return key in _TRACKING_PARAMETERS or key.startswith(_TRACKING_PREFIX)


def _filter_query(query: str) -> str:
    if not query:
        return ""
    kept: list[str] = []
    for part in query.split("&"):
        if not part:
            continue
        key, separator, value = part.partition("=")
        # A parameter can appear with no "=" at all (e.g. "?ref" rather than
        # "?ref=x"). It still counts as that parameter for stripping
        # purposes, and if it survives it is rendered back the same way --
        # never coerced into "key=".
        normalized_key = _normalize_percent_encoding(key)
        if _is_stripped_parameter(normalized_key):
            continue
        if separator:
            normalized_value = _normalize_percent_encoding(value)
            kept.append(f"{normalized_key}={normalized_value}")
        else:
            kept.append(normalized_key)
    return "&".join(kept)


def canonicalize_article_url(url: str) -> str:
    """Normalize `url` into this application's one article-identity string.

    Raises `UnusableArticleUrl` for anything that cannot be an article
    identity at all: a non-HTTP scheme, a missing host, or embedded
    credentials. Everything else is normalized conservatively -- see the
    module docstring for why under-normalizing is preferred to
    over-normalizing.
    """
    parsed = urlsplit(url)

    if parsed.scheme not in {"http", "https"}:
        raise UnusableArticleUrl(UnusableUrlReason.UNSUPPORTED_SCHEME, url=url)
    if not parsed.hostname:
        raise UnusableArticleUrl(UnusableUrlReason.MISSING_HOST, url=url)
    if parsed.username is not None or parsed.password is not None:
        raise UnusableArticleUrl(UnusableUrlReason.EMBEDDED_CREDENTIALS, url=url)

    # `.hostname` already lower-cases; the trailing dot is a separate, rare
    # DNS-root artifact that `urlsplit` leaves alone, so it is stripped here.
    scheme = parsed.scheme
    host = parsed.hostname.rstrip(".")
    port = parsed.port
    if port is not None and port != _DEFAULT_PORTS[scheme]:
        netloc = f"{host}:{port}"
    else:
        netloc = host

    path = _normalize_percent_encoding(parsed.path)
    query = _filter_query(parsed.query)
    # The fragment is dropped entirely rather than normalized: it never
    # identifies a distinct server-side resource, only a client-side scroll
    # position, so keeping it would only create spurious duplicates.

    result = f"{scheme}://{netloc}{path}"
    if query:
        result += f"?{query}"
    return result


def publisher_key(canonical_url: str) -> str:
    """Derive the registrable-domain publisher identity for `canonical_url`.

    Assumes `canonical_url` already passed through `canonicalize_article_url`
    and does not re-validate it. The `www.` prefix is stripped here, for the
    publisher key only -- it is never stripped from the canonical URL itself,
    since a publisher choosing to serve `www.` and bare-domain content as
    distinct articles is a decision this module should not overturn.
    """
    host = urlsplit(canonical_url).hostname or ""
    if host.startswith("www."):
        host = host[len("www.") :]

    labels = host.split(".")
    if len(labels) >= 3 and ".".join(labels[-2:]) in _TWO_LABEL_SUFFIXES:
        return ".".join(labels[-3:])
    if len(labels) >= 2:
        return ".".join(labels[-2:])
    return host
