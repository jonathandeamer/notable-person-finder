"""CLI integration tests for the ingestion milestone.

Every test is offline: `build_transport` is monkey-patched to return a real
`HttpTransport` whose network layer is an `httpx.MockTransport`. The resolver is
replaced with `StaticHostResolver` so no DNS lookup ever happens.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.models import TransportConfig
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import HttpTransport, build_transport
from notable_person_finder.runs.clock import SystemClock
from tests.run_engine.helpers import ENVIRONMENT

FIXTURES = Path(__file__).parent / "fixtures"
RESOLVER = StaticHostResolver({"example.com": ("93.184.216.34",)})

OPERATIONAL_SECTIONS = """\
[transport]
contact_url = "https://example.com/contact"

[retry]
max_attempts = 3

[concurrency]
http_workers = 4

[budget]
openrouter_usd_per_run = "2.50"
"""


def streaming_body(payload: bytes) -> list[bytes]:
    """Hand `httpx.Response` a genuine stream rather than eager bytes.

    `httpx.Response(status, content=<bytes>)` eagerly reads and closes its own
    stream during construction, which makes the transport's `iter_raw()` raise
    `StreamConsumed`. A list of chunks is treated as a real stream instead.
    """
    if not payload:
        return [payload]
    return [payload[start : start + 64] for start in range(0, len(payload), 64)]


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def write_ingestion_graph(
    root: Path,
    *,
    feeds: str,
    operational: str = OPERATIONAL_SECTIONS,
) -> Path:
    """Write a complete configuration tree with enabled feed(s)."""
    config_file = root / "notable.toml"
    config_file.write_text(
        """\
schema_version = 1
timezone = "Europe/Paris"
feeds_file = "feeds.toml"
domain_profile_file = "profiles/art.toml"

[paths]
root = "portable"

[secrets]
openrouter_api_key = "TEST_OPENROUTER"
brave_api_key = "TEST_BRAVE"
"""
        + operational,
        encoding="utf-8",
    )
    (root / "profiles").mkdir()
    (root / "feeds.toml").write_text(feeds, encoding="utf-8")
    (root / "profiles" / "art.toml").write_text(
        """\
schema_version = 1
key = "visual-arts-en"
label = "English visual arts"
language = "en"
[attention_examples]
significant_recognition = ["major art prize"]
""",
        encoding="utf-8",
    )
    return config_file


def _build_transport_patch(
    handler: Callable[[httpx.Request], httpx.Response],
) -> Callable[..., HttpTransport]:
    """Factory for a `build_transport` replacement that never touches the network."""

    def _patch(
        config: TransportConfig,
        *,
        version: str,
        resolver: object,
        clock: SystemClock,
        http_transport: httpx.BaseTransport | None = None,
    ) -> HttpTransport:
        return build_transport(
            config,
            version=version,
            resolver=RESOLVER,
            clock=clock,
            http_transport=httpx.MockTransport(handler),
        )

    return _patch


def _single_feed_handler(
    payload: bytes,
    *,
    status: int = 200,
    seen: list[httpx.Request] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, content=streaming_body(payload))

    return handler


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    return write_ingestion_graph(
        tmp_path,
        feeds="""\
schema_version = 1
[[feeds]]
key = "art-news"
label = "Art News"
url = "https://example.com/feed.xml"
""",
    )


@pytest.fixture
def monkeypatch_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_OPENROUTER", ENVIRONMENT["TEST_OPENROUTER"])
    monkeypatch.setenv("TEST_BRAVE", ENVIRONMENT["TEST_BRAVE"])


def test_run_ingests_feed_and_renders_counts(
    config_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    payload = fixture_bytes("rss20.xml")
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(_single_feed_handler(payload)),
    )

    exit_code = cli_main.command_run(config_file, verbose=False)

    assert exit_code == cli_main.EXIT_OK
    digests = config_file.parent / "portable" / "data" / "digests"
    latest = (digests / "latest.md").read_text(encoding="utf-8")
    assert "### Ingestion" in latest
    assert "Feeds fetched: 1" in latest
    assert "Feeds not modified: 0" in latest
    assert "Feeds failed: 0" in latest
    assert "Source items created: 2" in latest
    assert "Articles created: 2" in latest


def test_second_run_reports_zero_new_source_items(
    config_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    payload = fixture_bytes("rss20.xml")

    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                headers={"ETag": '"fixture-etag"'},
                content=streaming_body(payload),
            )
        return httpx.Response(304, content=streaming_body(b""))

    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(handler),
    )

    assert cli_main.command_run(config_file, verbose=False) == cli_main.EXIT_OK
    assert cli_main.command_run(config_file, verbose=False) == cli_main.EXIT_OK

    digests = config_file.parent / "portable" / "data" / "digests"
    dated = [path for path in digests.iterdir() if path.name != "latest.md"]
    assert len(dated) == 2
    latest = (digests / "latest.md").read_text(encoding="utf-8")
    assert "Feeds fetched: 0" in latest
    assert "Feeds not modified: 1" in latest
    assert "Source items created: 0" in latest
    assert "Articles created: 0" in latest


def test_failing_feed_still_reports_successful_ones(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    config_file = write_ingestion_graph(
        tmp_path,
        feeds="""\
schema_version = 1
[[feeds]]
key = "good-feed"
label = "Good Feed"
url = "https://example.com/good.xml"
[[feeds]]
key = "bad-feed"
label = "Bad Feed"
url = "https://example.com/bad.xml"
[[feeds]]
key = "also-bad-feed"
label = "Also Bad Feed"
url = "https://example.com/also-bad.xml"
""",
    )
    good_payload = fixture_bytes("rss20.xml")

    def handler(request: httpx.Request) -> httpx.Response:
        if "/good.xml" in str(request.url):
            return httpx.Response(200, content=streaming_body(good_payload))
        return httpx.Response(401, content=streaming_body(b"authentication required"))

    monkeypatch.setattr(cli_main, "build_transport", _build_transport_patch(handler))

    exit_code = cli_main.command_run(config_file, verbose=False)

    assert exit_code == cli_main.EXIT_PARTIAL
    latest = (
        config_file.parent / "portable" / "data" / "digests" / "latest.md"
    ).read_text(encoding="utf-8")
    assert "Feeds fetched: 1" in latest
    assert "Feeds failed: 2" in latest


def test_status_shows_source_items_and_latest_fetch(
    config_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = fixture_bytes("rss20.xml")
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(_single_feed_handler(payload)),
    )

    assert cli_main.command_run(config_file, verbose=False) == cli_main.EXIT_OK
    capsys.readouterr()

    exit_code = cli_main.command_status(config_file)
    assert exit_code == cli_main.EXIT_OK
    output = capsys.readouterr().out
    assert "source items: 2" in output
    assert "articles: 2" in output
    assert "latest successful fetch:" in output
    assert re.search(r"art-news: \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", output)


def test_transport_is_closed_when_run_raises(
    config_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    payload = fixture_bytes("rss20.xml")
    built_transport: HttpTransport | None = None

    def capturing_patch(
        config: TransportConfig,
        *,
        version: str,
        resolver: object,
        clock: SystemClock,
        http_transport: httpx.BaseTransport | None = None,
    ) -> HttpTransport:
        nonlocal built_transport
        transport = build_transport(
            config,
            version=version,
            resolver=RESOLVER,
            clock=clock,
            http_transport=httpx.MockTransport(_single_feed_handler(payload)),
        )
        built_transport = transport
        transport.closed = False  # type: ignore[attr-defined]
        original_close = transport.close

        def _close() -> None:
            transport.closed = True  # type: ignore[attr-defined]
            original_close()

        transport.close = _close  # type: ignore[method-assign]
        return transport

    monkeypatch.setattr(cli_main, "build_transport", capturing_patch)

    def _boom(self: object, handlers: object, *, seed: object = None) -> None:
        raise RuntimeError("synthetic failure for transport-close test")

    monkeypatch.setattr(cli_main.RunEngine, "execute", _boom)

    with pytest.raises(RuntimeError, match="synthetic failure"):
        cli_main.command_run(config_file, verbose=False)

    assert built_transport is not None
    assert built_transport.closed is True  # type: ignore[attr-defined]
