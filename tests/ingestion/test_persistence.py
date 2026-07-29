"""Tests for `persist_fetch`, `persist_failed_fetch`, and entry normalization.

These tests exercise the persistence layer directly, without a run engine,
because what is under test is the translation from a fetch result to durable
rows -- not the engine's scheduling or retry logic.
"""

from __future__ import annotations

import os
import sqlite3
import time

from notable_person_finder.ingestion.models import FetchPersistResult
from notable_person_finder.ingestion.repository import (
    upsert_feed_identity,
)
from notable_person_finder.ingestion.service import (
    NotInspected,
    persist_failed_fetch,
    persist_fetch,
)
from notable_person_finder.ingestion.urls import canonicalize_article_url
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.feeds import FeedEntry, FeedValidators, Modified
from tests.ingestion.helpers import immediate, insert_run, moment


def _feed_identity(
    connection: sqlite3.Connection,
    *,
    key: str = "alpha",
    url: str = "https://alpha.example.com/feed.xml",
) -> int:
    return upsert_feed_identity(
        connection,
        key=key,
        label=f"{key} label",
        url=url,
        now=moment(),
    )


def _entry(
    *,
    entry_id: str = "entry-1",
    url: str | None = "https://alpha.example.com/article",
    title: str = "Title",
    summary: str | None = "Summary",
    author: str | None = None,
    published_raw: str | None = "Fri, 24 Jul 2026 12:00:00 GMT",
) -> FeedEntry:
    return FeedEntry(
        entry_id=entry_id,
        url=url,
        title=title,
        summary=summary,
        content=None,
        author=author,
        published_raw=published_raw,
        updated_raw=None,
    )


def _modified(
    *,
    requested_url: str = "https://alpha.example.com/feed.xml",
    final_url: str = "https://alpha.example.com/feed.xml",
    entries: tuple[FeedEntry, ...],
    response_bytes: int = 1024,
    warnings: tuple[str, ...] = (),
) -> Modified:
    return Modified(
        requested_url=requested_url,
        final_url=final_url,
        redirect_chain=(),
        status_code=200,
        validators=FeedValidators(
            etag='"e1"', last_modified="Wed, 22 Jul 2026 06:00:00 GMT"
        ),
        feed_type="rss20",
        source_title="Alpha",
        source_link="https://alpha.example.com/",
        source_updated_raw=None,
        entries=entries,
        warnings=warnings,
        response_bytes=response_bytes,
    )


def _run_and_identity(connection: sqlite3.Connection) -> tuple[int, int]:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    return run_id, feed_id


def test_modified_fetch_creates_source_items_and_a_fetch_row(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    result = _modified(
        entries=(
            _entry(entry_id="a", url="https://alpha.example.com/1"),
            _entry(entry_id="b", url="https://alpha.example.com/2"),
            _entry(entry_id="c", url="https://alpha.example.com/3"),
        )
    )

    with immediate(connection):
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )

    assert counts == FetchPersistResult(
        source_items_created=3,
        source_items_existing=0,
        articles_created=3,
        entry_issues={},
    )
    fetch_row = connection.execute(
        "SELECT outcome, entry_count FROM feed_fetch WHERE feed_identity_id = ?",
        (feed_id,),
    ).fetchone()
    assert fetch_row["outcome"] == "modified"
    assert fetch_row["entry_count"] == 3
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM source_item").fetchone()["n"] == 3
    )


def test_rerunning_identical_fetch_creates_no_new_source_items(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    result = _modified(entries=(_entry(url="https://alpha.example.com/article"),))

    with immediate(connection):
        first = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )
    with immediate(connection):
        second = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(60),
        )

    assert first.source_items_created == 1
    assert second.source_items_created == 0
    assert second.source_items_existing == 1
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM feed_fetch").fetchone()["n"] == 2
    )
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM source_item").fetchone()["n"] == 1
    )


def test_not_modified_fetch_creates_a_fetch_row_and_no_source_items(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    from notable_person_finder.providers.feeds import NotModified

    result = NotModified(
        requested_url="https://alpha.example.com/feed.xml",
        final_url="https://alpha.example.com/feed.xml",
        redirect_chain=(),
        status_code=304,
        validators=FeedValidators(etag='"e1"', last_modified=None),
    )

    with immediate(connection):
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )

    assert counts == FetchPersistResult(0, 0, 0, {})
    fetch_row = connection.execute(
        "SELECT outcome, entry_count FROM feed_fetch"
    ).fetchone()
    assert fetch_row["outcome"] == "not_modified"
    assert fetch_row["entry_count"] == 0
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM source_item").fetchone()["n"] == 0
    )


def test_failed_fetch_creates_a_fetch_row_with_failure_category(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    failure = ProviderFailure(
        FailureCategory.NETWORK,
        provider="feeds",
        operation="fetch_feed",
        status_code=None,
        detail="connection reset",
    )

    with immediate(connection):
        counts = persist_failed_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            requested_url="https://alpha.example.com/feed.xml",
            failure=failure,
            now=moment(),
        )

    assert counts == FetchPersistResult(0, 0, 0, {})
    fetch_row = connection.execute(
        "SELECT outcome, failure_category, http_status FROM feed_fetch"
    ).fetchone()
    assert fetch_row["outcome"] == "failed"
    assert fetch_row["failure_category"] == "network"
    assert fetch_row["http_status"] is None


def test_entry_with_no_url_becomes_a_source_item_with_url_issue(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    result = _modified(entries=(_entry(entry_id="no-url", url=None),))

    with immediate(connection):
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )

    assert counts.source_items_created == 1
    assert counts.entry_issues == {"url:missing": 1}
    row = connection.execute(
        "SELECT canonical_article_id, url_issue FROM source_item"
    ).fetchone()
    assert row["canonical_article_id"] is None
    assert row["url_issue"] == "missing"


def test_entry_with_unparseable_date_becomes_a_source_item_with_published_issue(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    result = _modified(
        entries=(_entry(entry_id="bad-date", published_raw="not a date"),)
    )

    with immediate(connection):
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )

    assert counts.source_items_created == 1
    assert counts.entry_issues == {"published:unparseable": 1}
    row = connection.execute(
        "SELECT published_raw, published_at, published_issue FROM source_item"
    ).fetchone()
    assert row["published_raw"] == "not a date"
    assert row["published_at"] is None
    assert row["published_issue"] == "unparseable"


def test_entry_issues_namespace_url_and_published_keys(
    connection: sqlite3.Connection,
) -> None:
    """UrlIssue.MISSING and PublishedIssue.MISSING must not share a counter key."""
    run_id, feed_id = _run_and_identity(connection)
    result = _modified(
        entries=(_entry(entry_id="both-missing", url=None, published_raw=None),)
    )

    with immediate(connection):
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )

    assert counts.entry_issues == {"url:missing": 1, "published:missing": 1}


def test_naive_rfc_publication_date_is_interpreted_as_utc_in_a_non_utc_process(
    connection: sqlite3.Connection,
) -> None:
    """A missing RFC offset has the same explicit UTC policy as naive ISO.

    ``datetime.astimezone`` otherwise borrows the host process timezone for a
    naive value. The environment change is process-global, so restore both the
    variable and the C runtime timezone even if persistence raises.
    """
    old_tz = os.environ.get("TZ")
    os.environ["TZ"] = "PST8PDT"
    time.tzset()
    try:
        run_id, feed_id = _run_and_identity(connection)
        result = _modified(
            entries=(
                _entry(
                    entry_id="naive-rfc-date",
                    published_raw="Fri, 24 Jul 2026 12:00:00",
                ),
            )
        )

        with immediate(connection):
            persist_fetch(
                connection,
                feed_identity_id=feed_id,
                run_id=run_id,
                result=result,
                now=moment(),
            )

        row = connection.execute(
            "SELECT published_at, published_issue FROM source_item"
        ).fetchone()
        assert row["published_at"] == "2026-07-24T12:00:00.000000Z"
        assert row["published_issue"] is None
    finally:
        if old_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old_tz
        time.tzset()


def test_html_in_title_is_stripped_in_text_and_preserved_in_raw(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    result = _modified(
        entries=(_entry(entry_id="html", title="<b>Bold</b> &amp; text"),)
    )

    with immediate(connection):
        persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )

    row = connection.execute("SELECT title_raw, title_text FROM source_item").fetchone()
    assert row["title_raw"] == "<b>Bold</b> &amp; text"
    assert row["title_text"] == "Bold & text"


def test_two_feeds_serving_the_same_url_yield_one_source_item(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    alpha_id = _feed_identity(connection, key="alpha")
    beta_id = _feed_identity(
        connection, key="beta", url="https://beta.example.com/feed.xml"
    )
    shared_url = "https://example.com/shared"
    alpha_result = _modified(
        requested_url="https://alpha.example.com/feed.xml",
        entries=(_entry(entry_id="a1", url=shared_url),),
    )
    beta_result = _modified(
        requested_url="https://beta.example.com/feed.xml",
        entries=(_entry(entry_id="b1", url=shared_url),),
    )

    with immediate(connection):
        alpha_counts = persist_fetch(
            connection,
            feed_identity_id=alpha_id,
            run_id=run_id,
            result=alpha_result,
            now=moment(),
        )
    with immediate(connection):
        beta_counts = persist_fetch(
            connection,
            feed_identity_id=beta_id,
            run_id=run_id,
            result=beta_result,
            now=moment(60),
        )

    assert alpha_counts.source_items_created == 1
    assert beta_counts.source_items_created == 0
    assert beta_counts.source_items_existing == 1
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM source_item").fetchone()["n"] == 1
    )
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM canonical_article").fetchone()[
            "n"
        ]
        == 1
    )


def test_tracking_parameter_url_and_bare_url_converge_on_one_article(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    alpha_id = _feed_identity(connection, key="alpha")
    beta_id = _feed_identity(
        connection, key="beta", url="https://beta.example.com/feed.xml"
    )
    bare_url = "https://example.com/article"
    tracking_url = "https://example.com/article?utm_source=feed"

    with immediate(connection):
        persist_fetch(
            connection,
            feed_identity_id=alpha_id,
            run_id=run_id,
            result=_modified(
                requested_url="https://alpha.example.com/feed.xml",
                entries=(_entry(entry_id="a1", url=tracking_url),),
            ),
            now=moment(),
        )
    with immediate(connection):
        persist_fetch(
            connection,
            feed_identity_id=beta_id,
            run_id=run_id,
            result=_modified(
                requested_url="https://beta.example.com/feed.xml",
                entries=(_entry(entry_id="b1", url=bare_url),),
            ),
            now=moment(60),
        )

    canonical = canonicalize_article_url(bare_url)
    article_id = int(
        connection.execute(
            "SELECT id FROM canonical_article WHERE canonical_url = ?", (canonical,)
        ).fetchone()["id"]
    )
    aliases = [
        row["url"]
        for row in connection.execute(
            """
            SELECT url FROM article_url_alias
             WHERE canonical_article_id = ? ORDER BY url
            """,
            (article_id,),
        )
    ]
    assert aliases == [bare_url, tracking_url]


def test_not_inspected_fetch_records_a_failed_fetch_row(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    result = NotInspected(
        requested_url="https://alpha.example.com/feed.xml",
        category=FailureCategory.RESPONSE_TOO_LARGE,
        status_code=None,
        detail="body exceeded 5242880 bytes",
    )

    with immediate(connection):
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )

    assert counts == FetchPersistResult(0, 0, 0, {})
    fetch_row = connection.execute(
        "SELECT outcome, failure_category FROM feed_fetch"
    ).fetchone()
    assert fetch_row["outcome"] == "failed"
    assert fetch_row["failure_category"] == "response_too_large"


def test_persist_fetch_counts_are_returned_correctly(
    connection: sqlite3.Connection,
) -> None:
    run_id, feed_id = _run_and_identity(connection)
    result = _modified(
        entries=(
            _entry(entry_id="good", url="https://alpha.example.com/good"),
            _entry(entry_id="no-url", url=None),
            _entry(entry_id="bad-date", published_raw="garbage"),
        )
    )

    with immediate(connection):
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            result=result,
            now=moment(),
        )

    assert counts.source_items_created == 3
    assert counts.articles_created == 2  # the no-url entry has no article
    assert counts.entry_issues == {"url:missing": 1, "published:unparseable": 1}
