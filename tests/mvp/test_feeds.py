import httpx
import pytest

from notable.config import Feed
from notable.feeds import canonical_url, fetch_new

FEED_XML = """<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>Painter wins prize</title><description>A summary.</description>
<link>https://a.test/one?utm_source=rss&amp;utm_medium=feed</link>
<pubDate>Tue, 01 Jul 2025 10:00:00 GMT</pubDate></item>
<item><title>Second piece</title><description>More.</description>
<link>https://a.test/two#section</link></item>
</channel></rss>"""


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://a.test/one?utm_source=rss", "https://a.test/one"),
        ("https://a.test/one#frag", "https://a.test/one"),
        ("https://a.test/one?b=2&a=1", "https://a.test/one?a=1&b=2"),
        ("https://A.TEST/one", "https://a.test/one"),
        ("javascript:alert(1)", None),
        ("ftp://a.test/x", None),
        ("", None),
    ],
)
def test_canonical_url(raw, expected):
    assert canonical_url(raw) == expected


def _ok(_request):
    return httpx.Response(200, text=FEED_XML)


def test_parses_items_and_canonicalizes_links(make_config, make_transport, store):
    items = list(fetch_new(make_config(), make_transport(_ok), store))
    assert [item.url for item in items] == ["https://a.test/one", "https://a.test/two"]
    assert items[0].title == "Painter wins prize"
    assert items[0].summary == "A summary."
    assert items[0].publisher_label == "Feed A"


def test_settled_items_are_skipped(make_config, make_transport, store):
    store.commit(["https://a.test/one"], [], [])
    urls = [i.url for i in fetch_new(make_config(), make_transport(_ok), store)]
    assert urls == ["https://a.test/two"]


def test_an_abandoned_item_is_skipped(make_config, make_transport, store):
    for _ in range(3):
        store.commit([], ["https://a.test/one"], [])
    urls = [i.url for i in fetch_new(make_config(), make_transport(_ok), store)]
    assert urls == ["https://a.test/two"], "an item at the attempt cap is abandoned"


def test_a_failing_feed_does_not_stop_the_others(make_config, make_transport, store):
    config = make_config(
        feeds=(
            Feed(key="bad", label="Bad", url="https://bad.test/rss"),
            Feed(key="a", label="Feed A", url="https://a.test/rss"),
        )
    )

    def handler(request):
        if "bad.test" in str(request.url):
            return httpx.Response(500, text="down")
        return httpx.Response(200, text=FEED_XML)

    urls = [i.url for i in fetch_new(config, make_transport(handler), store)]
    assert urls == ["https://a.test/one", "https://a.test/two"]


def test_duplicate_urls_across_feeds_yield_once(make_config, make_transport, store):
    config = make_config(
        feeds=(
            Feed(key="a", label="A", url="https://a.test/rss"),
            Feed(key="b", label="B", url="https://b.test/rss"),
        )
    )
    urls = [i.url for i in fetch_new(config, make_transport(_ok), store)]
    assert urls == ["https://a.test/one", "https://a.test/two"]


def test_fresh_bypasses_the_feed_cache(make_config, make_transport, store):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text=FEED_XML)

    transport = make_transport(handler)
    config = make_config()
    list(fetch_new(config, transport, store))
    list(fetch_new(config, transport, store))  # cached
    list(fetch_new(config, transport, store, fresh=True))
    assert len(calls) == 2, "only --fresh-feeds refetches within the TTL"
    list(fetch_new(config, transport, store))
    assert len(calls) == 2, "the fresh fetch replaced the cached entry"


def test_an_unparseable_feed_is_not_cached(make_config, make_transport, store):
    # Cached on arrival, a malformed 200 is a twelve-hour "success" that
    # yields nothing and reports no error -- the quietest possible failure.
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(
            200, text="<rss><channel><item" if len(calls) < 2 else FEED_XML
        )

    transport = make_transport(handler)
    config = make_config()
    assert list(fetch_new(config, transport, store)) == []
    urls = [i.url for i in fetch_new(config, transport, store)]
    assert urls == ["https://a.test/one", "https://a.test/two"]
    assert len(calls) == 2, "the bad response must not have been cached"


def test_a_valid_feed_with_minor_xml_defects_is_still_used(
    make_config, make_transport, store
):
    # feedparser sets bozo for defects real feeds routinely carry. Rejecting
    # on bozo alone would discard working publishers.
    bozo_but_usable = FEED_XML.replace('<?xml version="1.0"?>', "")
    items = list(
        fetch_new(
            make_config(),
            make_transport(lambda r: httpx.Response(200, text=bozo_but_usable)),
            store,
        )
    )
    assert [i.url for i in items] == ["https://a.test/one", "https://a.test/two"]
