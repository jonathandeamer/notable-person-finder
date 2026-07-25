from __future__ import annotations

import socket

import pytest


class BlockedNetwork(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def deny_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default suite is offline; an accidental live request must fail loudly."""

    def blocked(*args: object, **kwargs: object) -> None:
        raise BlockedNetwork(
            "the default test suite is offline; inject a fake transport instead"
        )

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)
