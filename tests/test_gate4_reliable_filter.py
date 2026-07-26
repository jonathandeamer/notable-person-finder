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


class TestReliableFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.rf = load_module(
            "det_gate4_reliable_filter",
            ROOT / "scripts" / "det_gate4_reliable_filter.py",
        )

    # ------------------------------------------------------------------
    # Unit tests for is_reliable_source()
    # ------------------------------------------------------------------

    def test_reliable_exact_domain(self) -> None:
        self.assertTrue(
            self.rf.is_reliable_source("https://www.nytimes.com/article", "nytimes.com")
        )

    def test_reliable_subdomain(self) -> None:
        # uk.news.yahoo.com should match news.yahoo.com
        self.assertTrue(
            self.rf.is_reliable_source(
                "https://uk.news.yahoo.com/story", "uk.news.yahoo.com"
            )
        )

    def test_reliable_path_prefix(self) -> None:
        # atlasobscura.com/articles is a path-prefixed entry
        self.assertTrue(
            self.rf.is_reliable_source(
                "https://www.atlasobscura.com/articles/some-story",
                "atlasobscura.com",
            )
        )

    def test_reliable_path_prefix_no_www(self) -> None:
        self.assertTrue(
            self.rf.is_reliable_source(
                "https://atlasobscura.com/articles/some-story",
                "atlasobscura.com",
            )
        )

    def test_reliable_path_prefix_wrong_path(self) -> None:
        # atlasobscura.com/places is NOT in the list (only /articles is)
        self.assertFalse(
            self.rf.is_reliable_source(
                "https://www.atlasobscura.com/places/some-place",
                "atlasobscura.com",
            )
        )

    def test_reliable_full_url_prefix(self) -> None:
        self.assertTrue(
            self.rf.is_reliable_source(
                "https://open.spotify.com/show/6D4W8XJFVJ15tvqAnbmwJH/episode/123",
                "open.spotify.com",
            )
        )

    def test_unreliable_domain(self) -> None:
        self.assertFalse(
            self.rf.is_reliable_source(
                "https://randomnewsblog.com/story", "randomnewsblog.com"
            )
        )

    def test_unreliable_looks_similar(self) -> None:
        # "fakenytimes.com" should not match "nytimes.com"
        self.assertFalse(
            self.rf.is_reliable_source(
                "https://fakenytimes.com/story", "fakenytimes.com"
            )
        )

    # ------------------------------------------------------------------
    # Integration tests
    # ------------------------------------------------------------------

    def _make_input(self, tmp: Path, rows: list[dict]) -> Path:
        p = tmp / "input.jsonl"
        with p.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        return p

    def _make_row(self, results: list[dict], event_id: str = "aaa") -> dict:
        return {
            "event_id": event_id,
            "subject_name": "Test Person",
            "gate3_status": "MISSING",
            "source_context": {"entry_title": None, "summary": None, "source": None, "publication_date": None},
            "brave_queries": ['"Test Person"'],
            "brave_results": results,
            "brave_result_count": len(results),
            "errors": [],
            "fetched_at_utc": "2026-02-21T20:00:00Z",
        }

    def test_run_filters_to_reliable(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            results = [
                {"rank": 1, "title": "NYT story", "url": "https://www.nytimes.com/story", "description": None, "age": None, "page_age": None, "source_domain": "nytimes.com"},
                {"rank": 2, "title": "Random blog", "url": "https://randomnewsblog.com/story", "description": None, "age": None, "page_age": None, "source_domain": "randomnewsblog.com"},
                {"rank": 3, "title": "BBC story", "url": "https://www.bbc.co.uk/news/123", "description": None, "age": None, "page_age": None, "source_domain": "bbc.co.uk"},
            ]
            input_path = self._make_input(tmp, [self._make_row(results)])
            output_path = tmp / "out.jsonl"

            rc = self.rf.run(
                input_path=input_path,
                output_path=output_path,
                overwrite=False,
                progress_every=0,
            )
            self.assertEqual(rc, 0)
            rows = [json.loads(l) for l in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["brave_result_count"], 2)
            self.assertEqual(rows[0]["brave_result_count_unfiltered"], 3)
            domains = [r["source_domain"] for r in rows[0]["brave_results"]]
            self.assertIn("nytimes.com", domains)
            self.assertIn("bbc.co.uk", domains)
            self.assertNotIn("randomnewsblog.com", domains)

    def test_run_reranks_after_filter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            results = [
                {"rank": 1, "title": "Unreliable", "url": "https://blog.example.com/1", "description": None, "age": None, "page_age": None, "source_domain": "blog.example.com"},
                {"rank": 2, "title": "Reuters", "url": "https://www.reuters.com/story", "description": None, "age": None, "page_age": None, "source_domain": "reuters.com"},
            ]
            input_path = self._make_input(tmp, [self._make_row(results)])
            output_path = tmp / "out.jsonl"

            self.rf.run(input_path=input_path, output_path=output_path, overwrite=False, progress_every=0)
            rows = [json.loads(l) for l in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(rows[0]["brave_results"][0]["rank"], 1)
            self.assertEqual(rows[0]["brave_results"][0]["source_domain"], "reuters.com")

    def test_run_all_filtered_out(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            results = [
                {"rank": 1, "title": "Spam", "url": "https://spam.example.com/1", "description": None, "age": None, "page_age": None, "source_domain": "spam.example.com"},
            ]
            input_path = self._make_input(tmp, [self._make_row(results)])
            output_path = tmp / "out.jsonl"

            self.rf.run(input_path=input_path, output_path=output_path, overwrite=False, progress_every=0)
            rows = [json.loads(l) for l in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(rows[0]["brave_result_count"], 0)
            self.assertEqual(rows[0]["brave_result_count_unfiltered"], 1)
            self.assertEqual(rows[0]["brave_results"], [])

    def test_run_collision_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = self._make_input(tmp, [])
            output_path = tmp / "out.jsonl"
            output_path.write_text("existing", encoding="utf-8")

            with self.assertRaises(FileExistsError):
                self.rf.run(
                    input_path=input_path,
                    output_path=output_path,
                    overwrite=False,
                    progress_every=0,
                )

    def test_run_preserves_other_fields(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            row = self._make_row([])
            row["brave_queries"] = ['"Test Person"', '"Test Person" obituary']
            row["fetched_at_utc"] = "2026-02-21T12:00:00Z"
            input_path = self._make_input(tmp, [row])
            output_path = tmp / "out.jsonl"

            self.rf.run(input_path=input_path, output_path=output_path, overwrite=False, progress_every=0)
            rows = [json.loads(l) for l in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(rows[0]["brave_queries"], ['"Test Person"', '"Test Person" obituary'])
            self.assertEqual(rows[0]["fetched_at_utc"], "2026-02-21T12:00:00Z")
            self.assertEqual(rows[0]["subject_name"], "Test Person")


if __name__ == "__main__":
    unittest.main()
