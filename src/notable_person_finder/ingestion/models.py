from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum


class PublishedIssue(StrEnum):
    """Why a feed entry's publication date could not be turned into UTC."""

    MISSING = "missing"
    UNPARSEABLE = "unparseable"
    IMPLAUSIBLE = "implausible"


class UrlIssue(StrEnum):
    """Why a feed entry's URL could not become a canonical article identity."""

    MISSING = "missing"
    NOT_HTTP = "not_http"
    UNSAFE = "unsafe"
    UNUSABLE = "unusable"


@dataclass(frozen=True, slots=True)
class FeedIdentity:
    """One stored feed identity: which feed it is, and where it last lived.

    `key` is the configured stable key and never the URL. `id` is the durable
    subject work items point at; retirement of outstanding feed work is by
    that subject, so a URL move that left multiple active fingerprints still
    retires cleanly when the feed is disabled or deleted. `current_url` is the
    URL the feed was last *seeded* with.
    """

    id: int
    key: str
    current_label: str
    current_url: str


@dataclass(frozen=True, slots=True)
class SourceItemCounts:
    """A summary of `source_item` rows: the whole corpus, plus this run's delta.

    `total` and `articles_total` are unscoped whole-table counts -- every
    source item ever discovered, and how many of them carry a resolved
    article. They are deliberately not filtered by run or by feed: the digest
    reports corpus size, which is a standing figure rather than something one
    run owns.

    `created_in_run` is the delta, scoped to `discovered_by_run_id`, matching
    the attribution `insert_source_item` stamps at insert time. Reading the
    pair together answers "how big is the corpus, and how much did this run
    add", which is what a per-run digest line needs.
    """

    total: int
    created_in_run: int
    articles_total: int


@dataclass(frozen=True, slots=True)
class FetchPersistResult:
    """The result of persisting one feed fetch and the entries it produced."""

    source_items_created: int
    source_items_existing: int
    articles_created: int
    entry_issues: Mapping[str, int]
