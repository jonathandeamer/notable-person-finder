"""Executable ownership tests for the ingestion CLI, engine, and persistence seams.

Each test names the source-level break it is intended to catch.  The command
tests use a genuine ``HttpTransport`` with an ``httpx.MockTransport`` below it:
the only fake boundary is the network, while migration, scheduling, retry,
handler persistence, and digest rendering remain real.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.models import FeedConfig, FeedsConfig, TransportConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.ingestion import service
from notable_person_finder.ingestion.models import (
    PublishedIssue,
    UrlIssue,
)
from notable_person_finder.ingestion.service import (
    FETCH_FEED_TASK_TYPE,
    build_fetch_handler,
    persist_fetch,
    seed_feeds,
)
from notable_person_finder.providers.feeds import (
    FeedEntry,
    FeedValidators,
    Modified,
    NotModified,
)
from notable_person_finder.providers.pacing import PacingGate
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import HttpTransport, build_transport
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import SystemClock
from notable_person_finder.runs.models import WorkState
from tests.ingestion.helpers import immediate, insert_run, moment
from tests.run_engine.helpers import ENVIRONMENT

FIXTURES = Path(__file__).parent / "fixtures"
RESOLVER = StaticHostResolver({"example.com": ("93.184.216.34",)})
_CONFIG = """\
schema_version = 1
timezone = "Europe/Paris"
feeds_file = "feeds.toml"
domain_profile_file = "profiles/art.toml"

[paths]
root = "portable"

[secrets]
openrouter_api_key = "TEST_OPENROUTER"
brave_api_key = "TEST_BRAVE"

[transport]
contact_url = "https://example.com/contact"
{transport_extra}

[retry]
max_attempts = 3
initial_backoff_seconds = 0.001
max_backoff_seconds = 0.001
backoff_multiplier = 1.0
jitter_ratio = 0.0

[concurrency]
http_workers = 1

[budget]
openrouter_usd_per_run = "2.50"
"""


def _streaming_body(payload: bytes) -> list[bytes]:
    return [payload[offset : offset + 64] for offset in range(0, len(payload), 64)]


def _write_config(
    root: Path, *, transport_extra: str = "", enabled: bool = True
) -> Path:
    config = root / "notable.toml"
    config.write_text(_CONFIG.format(transport_extra=transport_extra), encoding="utf-8")
    (root / "profiles").mkdir()
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
    (root / "feeds.toml").write_text(
        f"""\
schema_version = 1
[[feeds]]
key = "art-news"
label = "Art News"
url = "https://example.com/feed.xml"
enabled = {str(enabled).lower()}
""",
        encoding="utf-8",
    )
    return config


def _patch_transport(
    handler: Callable[[httpx.Request], httpx.Response],
) -> Callable[..., HttpTransport]:
    def patched(
        config: TransportConfig,
        *,
        version: str,
        resolver: object,
        clock: SystemClock,
        http_transport: httpx.BaseTransport | None = None,
        pacing_gate: PacingGate | None = None,
    ) -> HttpTransport:
        return build_transport(
            config,
            version=version,
            resolver=RESOLVER,
            clock=clock,
            http_transport=httpx.MockTransport(handler),
            pacing_gate=pacing_gate,
        )

    return patched


def _feed_config(*, enabled: bool = True) -> FeedsConfig:
    return FeedsConfig(
        schema_version=1,
        feeds=(
            FeedConfig(
                key="alpha",
                label="Alpha",
                url="https://alpha.example.com/feed.xml",
                enabled=enabled,
            ),
        ),
    )


def _modified(*, entries: tuple[FeedEntry, ...]) -> Modified:
    return Modified(
        requested_url="https://alpha.example.com/feed.xml",
        final_url="https://alpha.example.com/feed.xml",
        redirect_chain=(),
        status_code=200,
        validators=FeedValidators(etag=None, last_modified=None),
        feed_type="rss20",
        source_title=None,
        source_link=None,
        source_updated_raw=None,
        entries=entries,
        warnings=(),
        response_bytes=100,
    )


@pytest.fixture
def command_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_OPENROUTER", ENVIRONMENT["TEST_OPENROUTER"])
    monkeypatch.setenv("TEST_BRAVE", ENVIRONMENT["TEST_BRAVE"])


def test_command_run_seed_hook_creates_and_fetches_feed_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command_environment: None
) -> None:
    """Kills removing ``seed=seed`` from the command's engine execution."""
    config = _write_config(tmp_path)
    payload = (FIXTURES / "rss20.xml").read_bytes()
    requests: list[httpx.Request] = []

    def response(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=_streaming_body(payload))

    monkeypatch.setattr(cli_main, "build_transport", _patch_transport(response))

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    assert [str(request.url) for request in requests] == [
        "https://example.com/feed.xml"
    ]
    database = connect_database(tmp_path / "portable" / "data" / "notable.sqlite3")
    try:
        assert database.execute("SELECT COUNT(*) FROM work_item").fetchone()[0] == 1
        assert database.execute("SELECT COUNT(*) FROM feed_fetch").fetchone()[0] == 1
    finally:
        database.close()


def test_command_run_registered_fetch_handler_persists_ingestion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command_environment: None
) -> None:
    """Kills registering an empty handler mapping in ``command_run``."""
    config = _write_config(tmp_path)
    payload = (FIXTURES / "rss20.xml").read_bytes()
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _patch_transport(
            lambda request: httpx.Response(200, content=_streaming_body(payload))
        ),
    )

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    database = connect_database(tmp_path / "portable" / "data" / "notable.sqlite3")
    try:
        assert database.execute("SELECT COUNT(*) FROM feed_fetch").fetchone()[0] == 1
        assert database.execute("SELECT COUNT(*) FROM source_item").fetchone()[0] == 2
    finally:
        database.close()


def test_command_run_uses_configured_response_size_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command_environment: None
) -> None:
    """Kills building the feed client with a transport that ignores its config."""
    config = _write_config(tmp_path, transport_extra="max_api_response_bytes = 64")
    payload = (FIXTURES / "rss20.xml").read_bytes()
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _patch_transport(
            lambda request: httpx.Response(200, content=_streaming_body(payload))
        ),
    )

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_PARTIAL
    database = connect_database(tmp_path / "portable" / "data" / "notable.sqlite3")
    try:
        fetch = database.execute(
            "SELECT outcome, failure_category FROM feed_fetch"
        ).fetchone()
        assert tuple(fetch) == ("failed", "response_too_large")
        assert database.execute("SELECT COUNT(*) FROM source_item").fetchone()[0] == 0
    finally:
        database.close()


def test_retry_adjudicated_fetch_failure_persists_feed_fetch_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, command_environment: None
) -> None:
    """Kills skipping ``persist_failure``'s ``feed_fetch`` write on failure."""
    config = _write_config(tmp_path)
    calls = 0

    def unauthorized(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(401, content=_streaming_body(b"unauthorized"))

    monkeypatch.setattr(cli_main, "build_transport", _patch_transport(unauthorized))

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_FAILED
    database = connect_database(tmp_path / "portable" / "data" / "notable.sqlite3")
    try:
        # Positive control: an actual external call was recorded as a failed
        # attempt, so the fetch row below cannot pass merely because no work ran.
        assert calls == 1
        assert database.execute("SELECT outcome FROM attempt").fetchone()[0] == "failed"
        assert tuple(
            database.execute(
                "SELECT outcome, failure_category FROM feed_fetch"
            ).fetchone()
        ) == ("failed", "authentication")
    finally:
        database.close()


def test_persist_fetch_keeps_an_entry_with_a_typed_issue_after_resolution_fault(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kills swallowing a per-entry resolution exception by dropping its issue."""
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=_feed_config(), run_id=run_id, now=moment())
    feed_id = int(connection.execute("SELECT id FROM feed_identity").fetchone()[0])
    entry = FeedEntry(
        entry_id="broken-url",
        url="https://alpha.example.com/article",
        title="Still retained",
        summary=None,
        content=None,
        author=None,
        published_raw=None,
        updated_raw=None,
    )

    def resolution_fault(*args: object, **kwargs: object) -> object:
        raise RuntimeError("canonicalizer fault")

    monkeypatch.setattr(service, "_resolve_article", resolution_fault)
    with immediate(connection):
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=_modified(entries=(entry,)),
            now=moment(),
        )

    assert counts.source_items_created == 1
    assert counts.source_items_existing == 0
    assert counts.articles_created == 0
    assert counts.entry_issues == {
        f"url:{UrlIssue.UNUSABLE}": 1,
        f"published:{PublishedIssue.MISSING}": 1,
    }
    assert len(counts.created_source_item_ids) == 1
    row = connection.execute(
        "SELECT title_text, canonical_article_id, url_issue FROM source_item"
    ).fetchone()
    assert tuple(row) == ("Still retained", None, "unusable")


class _NotModifiedClient:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_feed(self, feed: FeedConfig, validators: FeedValidators) -> NotModified:
        self.calls += 1
        return NotModified(
            requested_url=feed.url,
            final_url=feed.url,
            redirect_chain=(),
            status_code=304,
            validators=validators,
        )


def test_fetch_handler_executes_on_worker_without_sqlite_access(
    connection: sqlite3.Connection,
) -> None:
    """Kills capturing and using the SQLite connection in the worker closure."""
    run_id = insert_run(connection)
    feeds = _feed_config()
    seed_feeds(connection, feeds=feeds, run_id=run_id, now=moment())
    item = repository.claim_batch(
        connection,
        run_id=run_id,
        now=moment(1),
        task_types=[FETCH_FEED_TASK_TYPE],
        limit=1,
    )[0]
    client = _NotModifiedClient()
    handler = build_fetch_handler(connection, client=client, feeds=feeds)
    assert handler.prepare is not None
    preparation = handler.prepare(item)  # positive control: SQLite is valid here.

    with ThreadPoolExecutor(max_workers=1) as workers:
        outcome = workers.submit(handler.execute, item, 1, preparation.payload).result()

    assert outcome.state is WorkState.SUCCEEDED
    assert outcome.reason == "not modified"
    assert client.calls == 1


def test_seeding_disabled_feed_supersedes_stored_identity_work(
    connection: sqlite3.Connection,
) -> None:
    """Kills omitting retirement of disabled feeds from ``seed_feeds``."""
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=_feed_config(), run_id=run_id, now=moment())
    seed_feeds(
        connection,
        feeds=_feed_config(enabled=False),
        run_id=run_id,
        now=moment(60),
    )

    row = connection.execute("SELECT state, reason FROM work_item").fetchone()
    assert tuple(row) == ("superseded", "feed disabled")
