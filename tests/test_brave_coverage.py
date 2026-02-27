import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ROOT = Path(__file__).resolve().parents[1]


class TestBraveCoverage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.bc = load_module(
            "det_brave_coverage", ROOT / "scripts" / "det_brave_coverage.py"
        )

    # ------------------------------------------------------------------
    # Unit tests for pure functions
    # ------------------------------------------------------------------

    def test_build_queries_name_only(self) -> None:
        queries = self.bc.build_queries("Rose Freedman", None)
        self.assertEqual(queries, ['"Rose Freedman"'])

    def test_build_queries_obit_adds_second(self) -> None:
        queries = self.bc.build_queries("Rose Freedman", "Rose Freedman obituary")
        self.assertEqual(len(queries), 2)
        self.assertEqual(queries[0], '"Rose Freedman"')
        self.assertIn("obituary", queries[1])

    def test_build_queries_obit_case_insensitive(self) -> None:
        queries = self.bc.build_queries("Jane Doe", "Jane Doe Obituary")
        self.assertEqual(len(queries), 2)

    def test_build_result(self) -> None:
        raw = {
            "title": "Rose Freedman, activist",
            "url": "https://www.bbc.co.uk/news/article-123",
            "description": "A profile of Rose Freedman.",
            "age": "2 days ago",
            "page_age": "2026-02-19",
        }
        result = self.bc.build_result(1, raw)
        self.assertEqual(result["rank"], 1)
        self.assertEqual(result["title"], "Rose Freedman, activist")
        self.assertEqual(result["url"], "https://www.bbc.co.uk/news/article-123")
        self.assertEqual(result["description"], "A profile of Rose Freedman.")
        self.assertEqual(result["age"], "2 days ago")
        self.assertEqual(result["page_age"], "2026-02-19")
        self.assertEqual(result["source_domain"], "bbc.co.uk")

    # ------------------------------------------------------------------
    # Integration tests using mocked _fetch_json
    # ------------------------------------------------------------------

    def _make_input(self, tmp: Path, rows: list[dict]) -> Path:
        input_path = tmp / "input.jsonl"
        with input_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        return input_path

    def _brave_response(self, titles: list[str]) -> dict:
        return {
            "results": [
                {
                    "title": t,
                    "url": f"https://example.com/{i}",
                    "description": f"Desc {i}",
                }
                for i, t in enumerate(titles, start=1)
            ]
        }

    def test_run_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            rows = [
                {
                    "event_id": "aaa",
                    "subject_name": "Rose Freedman",
                    "gate3_status": "MISSING",
                    "source_context": {
                        "entry_title": "Rose Freedman dies",
                        "summary": None,
                        "source": None,
                        "publication_date": None,
                    },
                },
                {
                    "event_id": "bbb",
                    "subject_name": "John Doe",
                    "gate3_status": "MISSING",
                    "source_context": {
                        "entry_title": "John Doe tribute",
                        "summary": None,
                        "source": None,
                        "publication_date": None,
                    },
                },
            ]
            input_path = self._make_input(tmp, rows)
            output_path = tmp / "out.jsonl"
            cache_dir = tmp / "cache"

            fetch_calls = []

            def fake_fetch(url, headers, max_retries, throttle_ms, log_file):
                fetch_calls.append(url)
                return self._brave_response(["Title A", "Title B"])

            orig = self.bc._fetch_json
            try:
                self.bc._fetch_json = fake_fetch
                rc = self.bc.run(
                    input_path=input_path,
                    output_path=output_path,
                    statuses=["MISSING", "UNCERTAIN"],
                    count=10,
                    pages=1,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    api_key="test-key",
                    overwrite=False,
                    progress_every=0,
                    log_file=None,
                )
            finally:
                self.bc._fetch_json = orig

            self.assertEqual(rc, 0)
            output_rows = [
                json.loads(l) for l in output_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(output_rows), 2)
            self.assertEqual(output_rows[0]["event_id"], "aaa")
            self.assertEqual(output_rows[0]["subject_name"], "Rose Freedman")
            self.assertEqual(output_rows[0]["brave_result_count"], 2)
            self.assertEqual(output_rows[0]["brave_results"][0]["title"], "Title A")

    def test_run_skips_non_missing(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            rows = [
                {
                    "event_id": "aaa",
                    "subject_name": "Has Page Person",
                    "gate3_status": "HAS_PAGE",
                    "source_context": {"entry_title": None, "summary": None, "source": None, "publication_date": None},
                },
                {
                    "event_id": "bbb",
                    "subject_name": "Missing Person",
                    "gate3_status": "MISSING",
                    "source_context": {"entry_title": None, "summary": None, "source": None, "publication_date": None},
                },
            ]
            input_path = self._make_input(tmp, rows)
            output_path = tmp / "out.jsonl"
            cache_dir = tmp / "cache"

            def fake_fetch(url, headers, max_retries, throttle_ms, log_file):
                return self._brave_response(["News item"])

            orig = self.bc._fetch_json
            try:
                self.bc._fetch_json = fake_fetch
                rc = self.bc.run(
                    input_path=input_path,
                    output_path=output_path,
                    statuses=["MISSING"],
                    count=10,
                    pages=1,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    api_key="test-key",
                    overwrite=False,
                    progress_every=0,
                    log_file=None,
                )
            finally:
                self.bc._fetch_json = orig

            self.assertEqual(rc, 0)
            output_rows = [
                json.loads(l) for l in output_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(output_rows), 1)
            self.assertEqual(output_rows[0]["subject_name"], "Missing Person")

    def test_run_deduplicates_urls(self) -> None:
        """Same URL returned by two queries appears only once in brave_results."""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            rows = [
                {
                    "event_id": "aaa",
                    "subject_name": "Rose Freedman",
                    "gate3_status": "MISSING",
                    "source_context": {
                        "entry_title": "Rose Freedman obituary",
                        "summary": None,
                        "source": None,
                        "publication_date": None,
                    },
                },
            ]
            input_path = self._make_input(tmp, rows)
            output_path = tmp / "out.jsonl"
            cache_dir = tmp / "cache"

            call_count = [0]

            def fake_fetch(url, headers, max_retries, throttle_ms, log_file):
                call_count[0] += 1
                # Both queries return the same URL
                return {
                    "results": [
                        {"title": "Duplicate story", "url": "https://example.com/dup"},
                        {"title": "Unique story", "url": f"https://example.com/unique-{call_count[0]}"},
                    ]
                }

            orig = self.bc._fetch_json
            try:
                self.bc._fetch_json = fake_fetch
                rc = self.bc.run(
                    input_path=input_path,
                    output_path=output_path,
                    statuses=["MISSING", "UNCERTAIN"],
                    count=10,
                    pages=1,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    api_key="test-key",
                    overwrite=False,
                    progress_every=0,
                    log_file=None,
                )
            finally:
                self.bc._fetch_json = orig

            self.assertEqual(rc, 0)
            output_rows = [
                json.loads(l) for l in output_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(output_rows), 1)
            urls = [r["url"] for r in output_rows[0]["brave_results"]]
            # Duplicate URL should appear only once
            self.assertEqual(urls.count("https://example.com/dup"), 1)
            # Obit query triggered second call
            self.assertEqual(call_count[0], 2)

    def test_run_collision_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = self._make_input(tmp, [])
            output_path = tmp / "out.jsonl"
            output_path.write_text("existing", encoding="utf-8")

            with self.assertRaises(FileExistsError):
                self.bc.run(
                    input_path=input_path,
                    output_path=output_path,
                    statuses=["MISSING"],
                    count=10,
                    pages=1,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=tmp / "cache",
                    cache_ttl_days=None,
                    api_key="test-key",
                    overwrite=False,
                    progress_every=0,
                    log_file=None,
                )

    def test_cache_hit_avoids_fetch(self) -> None:
        """If a response is cached, _fetch_json should not be called."""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            cache_dir = tmp / "cache"
            cache_dir.mkdir()

            # Pre-populate cache
            query = '"Rose Freedman"'
            count = 10
            key = self.bc._cache_key(query, count)
            cached_data = {
                "results": [
                    {"title": "Cached story", "url": "https://example.com/cached"}
                ]
            }
            self.bc._cache_set(cache_dir, key, cached_data)

            rows = [
                {
                    "event_id": "aaa",
                    "subject_name": "Rose Freedman",
                    "gate3_status": "MISSING",
                    "source_context": {
                        "entry_title": "Rose Freedman news",
                        "summary": None,
                        "source": None,
                        "publication_date": None,
                    },
                }
            ]
            input_path = self._make_input(tmp, rows)
            output_path = tmp / "out.jsonl"

            fetch_calls = []

            def fake_fetch(url, headers, max_retries, throttle_ms, log_file):
                fetch_calls.append(url)
                return {"results": []}

            orig = self.bc._fetch_json
            try:
                self.bc._fetch_json = fake_fetch
                rc = self.bc.run(
                    input_path=input_path,
                    output_path=output_path,
                    statuses=["MISSING"],
                    count=count,
                    pages=1,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    api_key="test-key",
                    overwrite=False,
                    progress_every=0,
                    log_file=None,
                )
            finally:
                self.bc._fetch_json = orig

            self.assertEqual(rc, 0)
            # fetch should not have been called
            self.assertEqual(fetch_calls, [])
            output_rows = [
                json.loads(l) for l in output_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(output_rows[0]["brave_results"][0]["title"], "Cached story")


if __name__ == "__main__":
    unittest.main()
