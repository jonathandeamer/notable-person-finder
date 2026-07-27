from __future__ import annotations

import pytest

from notable_person_finder.providers.safety import (
    StaticHostResolver,
    UnsafeUrl,
    assert_safe_url,
)

PUBLIC = StaticHostResolver({"example.com": ("93.184.216.34",)})


def test_public_https_url_is_accepted() -> None:
    assert (
        assert_safe_url("https://example.com/feed.xml", resolver=PUBLIC)
        == "example.com"
    )


def test_public_http_url_is_accepted() -> None:
    assert (
        assert_safe_url("http://example.com/feed.xml", resolver=PUBLIC) == "example.com"
    )


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/x",
        "file:///etc/passwd",
        "gopher://example.com",
        "//example.com/x",
        "https:///nohost",
    ],
)
def test_non_http_schemes_are_rejected(url: str) -> None:
    with pytest.raises(UnsafeUrl):
        assert_safe_url(url, resolver=PUBLIC)


def test_embedded_credentials_are_rejected() -> None:
    with pytest.raises(UnsafeUrl, match="credentials"):
        assert_safe_url("https://user:pass@example.com/x", resolver=PUBLIC)


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",  # loopback
        "10.0.0.5",  # private
        "192.168.1.10",  # private
        "169.254.169.254",  # link-local metadata endpoint
        "224.0.0.1",  # multicast
        "0.0.0.0",  # reserved
        "::1",  # IPv6 loopback
        "fd00::1",  # IPv6 unique local
    ],
)
def test_non_public_resolved_addresses_are_rejected(address: str) -> None:
    resolver = StaticHostResolver({"rebind.example": (address,)})
    with pytest.raises(UnsafeUrl, match="not publicly routable"):
        assert_safe_url("https://rebind.example/x", resolver=resolver)


def test_a_single_private_answer_rejects_the_whole_host() -> None:
    # A host that resolves to both a public and a private address must be
    # refused: choosing the public answer is not something HTTPX guarantees.
    resolver = StaticHostResolver({"mixed.example": ("93.184.216.34", "127.0.0.1")})
    with pytest.raises(UnsafeUrl):
        assert_safe_url("https://mixed.example/x", resolver=resolver)


def test_literal_private_address_is_rejected_without_resolution() -> None:
    resolver = StaticHostResolver({})
    with pytest.raises(UnsafeUrl):
        assert_safe_url("https://127.0.0.1/x", resolver=resolver)


def test_unresolvable_host_is_rejected() -> None:
    with pytest.raises(UnsafeUrl, match="could not be resolved"):
        assert_safe_url("https://missing.example/x", resolver=StaticHostResolver({}))


def test_ordinary_public_ports_are_permitted() -> None:
    assert (
        assert_safe_url("https://example.com:8443/x", resolver=PUBLIC) == "example.com"
    )
