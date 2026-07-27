from __future__ import annotations

import ipaddress
import socket
from collections.abc import Mapping
from typing import Protocol
from urllib.parse import urlsplit


class UnsafeUrl(ValueError):
    """A URL or redirect destination the application refuses to request."""


class HostResolver(Protocol):
    def resolve(self, host: str) -> tuple[str, ...]: ...


class SystemHostResolver:
    def resolve(self, host: str) -> tuple[str, ...]:
        try:
            answers = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
        except OSError:
            return ()
        return tuple(dict.fromkeys(answer[4][0] for answer in answers))


class StaticHostResolver:
    """Deterministic resolver for tests; the default suite never uses DNS."""

    def __init__(self, mapping: Mapping[str, tuple[str, ...]]) -> None:
        self._mapping = dict(mapping)

    def resolve(self, host: str) -> tuple[str, ...]:
        return self._mapping.get(host.rstrip(".").lower(), ())


def _is_public_address(address: str) -> bool:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return False
    # `is_global` alone is not sufficient: Python's ipaddress module treats
    # multicast addresses (e.g. 224.0.0.0/4) as globally routable, but they
    # are not a valid unicast request destination for this application.
    return parsed.is_global and not parsed.is_multicast


def assert_safe_url(url: str, *, resolver: HostResolver) -> str:
    """Validate a request or redirect destination, returning its hostname.

    Applied before the initial request and again before every redirect, so a
    server cannot redirect the application onto a loopback, private, or
    link-local address.
    """
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeUrl(f"{url}: only http and https URLs are requested")
    if not parsed.hostname:
        raise UnsafeUrl(f"{url}: URL has no host")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeUrl(f"{url}: URL must not contain embedded credentials")

    host = parsed.hostname.rstrip(".").lower()
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None

    if literal is not None:
        if not _is_public_address(host):
            raise UnsafeUrl(f"{url}: address is not publicly routable")
        return host

    addresses = resolver.resolve(host)
    if not addresses:
        raise UnsafeUrl(f"{url}: host could not be resolved")
    for address in addresses:
        if not _is_public_address(address):
            raise UnsafeUrl(f"{url}: host resolves to an address that is not publicly routable")
    return host
