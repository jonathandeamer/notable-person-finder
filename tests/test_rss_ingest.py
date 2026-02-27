import json
import tempfile
import unittest
from pathlib import Path

from ingest.rss_ingest import (
    FeedFetchResult,
    canonical_key,
    normalize_url,
    parse_datetime_to_rfc3339,
    parse_feed_bytes,
    run_ingest,
)


RSS_FIXTURE = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example Feed</title>
    <item>
      <title>Alpha Person Dies at 88</title>
      <link>https://example.com/news/story?utm_source=rss&amp;id=10#frag</link>
      <guid>alpha-1</guid>
      <pubDate>Thu, 18 Jan 2024 09:30:00 GMT</pubDate>
      <description>Obituary text</description>
    </item>
  </channel>
</rss>
"""


ATOM_FIXTURE = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Feed</title>
  <entry>
    <title>Beta Person Remembered</title>
    <id>beta-1</id>
    <link rel="alternate" href="https://example.com/beta?fbclid=abc" />
    <updated>2024-01-18T11:22:00Z</updated>
    <summary>Summary</summary>
  </entry>
</feed>
"""


class TestRssIngest(unittest.TestCase):
    def test_normalize_url_tracking_removed_and_sorted(self) -> None:
        raw = "HTTPS://Example.com:443/a//b/?utm_source=x&b=2&a=1&fbclid=abc#top"
        normalized = normalize_url(raw)
        self.assertEqual(normalized, "https://example.com/a/b?a=1&b=2")

    def test_parse_datetime(self) -> None:
        value, err = parse_datetime_to_rfc3339("Thu, 18 Jan 2024 09:30:00 GMT")
        self.assertFalse(err)
        self.assertEqual(value, "2024-01-18T09:30:00Z")

        value, err = parse_datetime_to_rfc3339("2024-01-18T11:22:00Z")
        self.assertFalse(err)
        self.assertEqual(value, "2024-01-18T11:22:00Z")

        value, err = parse_datetime_to_rfc3339("not-a-date")
        self.assertTrue(err)
        self.assertIsNone(value)

    def test_canonical_key_is_deterministic(self) -> None:
        url = "https://example.com/a"
        self.assertEqual(canonical_key(url), canonical_key(url))

    def test_parse_feed_bytes_rss_and_atom(self) -> None:
        title, entries = parse_feed_bytes(RSS_FIXTURE)
        self.assertEqual(title, "Example Feed")
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].guid, "alpha-1")

        title, entries = parse_feed_bytes(ATOM_FIXTURE)
        self.assertEqual(title, "Atom Feed")
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0].guid, "beta-1")

    def test_run_ingest_idempotent_and_cross_feed_dedupe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            feeds_path = root / "config" / "feeds.md"
            feeds_path.parent.mkdir(parents=True, exist_ok=True)
            feeds_path.write_text(
                "\n".join(
                    [
                        "## Feeds",
                        "- http://feed-one.test/rss.xml",
                        "- http://feed-two.test/rss.xml",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            state_dir = root / "state"

            content_by_feed = {
                "http://feed-one.test/rss.xml": RSS_FIXTURE,
                "http://feed-two.test/rss.xml": RSS_FIXTURE,
            }

            def fake_fetcher(
                source_feed_url_original: str,
                prior_state: dict[str, str],
                timeout_seconds: float,
                retries: int,
                user_agent: str,
            ) -> FeedFetchResult:
                del prior_state, timeout_seconds, retries, user_agent
                return FeedFetchResult(
                    source_feed_url_original=source_feed_url_original,
                    source_feed_url_resolved=source_feed_url_original.replace(
                        "http://", "https://"
                    ),
                    fetched_at_utc="2024-01-18T12:00:00Z",
                    http_status=200,
                    content=content_by_feed[source_feed_url_original],
                    etag="etag-1",
                    last_modified="Thu, 18 Jan 2024 12:00:00 GMT",
                    not_modified=False,
                    error=None,
                )

            exit_code = run_ingest(
                feeds_path=feeds_path,
                state_dir=state_dir,
                fetcher=fake_fetcher,
            )
            self.assertEqual(exit_code, 0)

            events_path = state_dir / "events.jsonl"
            events = [
                json.loads(line)
                for line in events_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(events), 1)
            self.assertEqual(
                events[0]["entry_url_canonical"],
                "https://example.com/news/story?id=10",
            )

            # Running again should append no new events.
            exit_code = run_ingest(
                feeds_path=feeds_path,
                state_dir=state_dir,
                fetcher=fake_fetcher,
            )
            self.assertEqual(exit_code, 0)
            events = [
                json.loads(line)
                for line in events_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(events), 1)


if __name__ == "__main__":
    unittest.main()
