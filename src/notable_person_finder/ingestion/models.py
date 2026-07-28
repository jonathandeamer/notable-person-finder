from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FeedIdentity:
    """One stored feed identity: which feed it is, and where it last lived.

    `key` is the configured stable key and never the URL. `current_url` is the
    URL the feed was last *seeded* with, which is what makes this row usable
    for retiring outstanding work: seeding refreshes the identity only for
    feeds that are still enabled, so for a feed that has since been switched
    off or deleted from configuration this URL is still the one its
    outstanding work item was fingerprinted from.
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
