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


class TestMwCandidates(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.mw = load_module("det_mw_candidates", root / "scripts" / "det_mw_candidates.py")

    def test_query_variants(self) -> None:
        variants = self.mw.query_variants("Dr Ada Lovelace")
        self.assertIn("Ada Lovelace", variants)

        variants = self.mw.query_variants("Smith, John")
        self.assertIn("John Smith", variants)

    def test_build_candidate(self) -> None:
        item = {"title": "Ada Lovelace", "snippet": "Test"}
        page = {
            "title": "Ada Lovelace",
            "pageid": 123,
            "fullurl": "https://en.wikipedia.org/wiki/Ada_Lovelace",
            "pageprops": {"disambiguation": None},
            "description": "English mathematician",
            "extract": "Ada Lovelace was...",
        }
        c = self.mw.build_candidate(1, item, page, None)
        self.assertEqual(c["rank"], 1)
        self.assertEqual(c["title"], "Ada Lovelace")
        self.assertEqual(c["pageid"], 123)
        self.assertEqual(c["is_disambig"], False)

    def test_biography_score(self) -> None:
        score_positive = self.mw.biography_score(
            ["Category:Living people", "Category:1950 births"]
        )
        self.assertGreaterEqual(score_positive, 3)

        score_negative = self.mw.biography_score(
            ["Category:Lists of foo", "Category:Municipalities in Bar"]
        )
        self.assertLess(score_negative, 0)

    def test_search_cap_limits_results(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "input.jsonl"
            output_path = tmp / "out.jsonl"
            cache_dir = tmp / "cache"

            input_path.write_text(
                json.dumps(
                    {
                        "event_id": "1",
                        "subject_name_full": "Ada Lovelace",
                        "gate1_decision": "WEAK_PASS",
                        "entry_title": "Ada Lovelace obituary",
                        "summary": "Test summary",
                        "source": "Example",
                        "publication_date": "2026-02-01",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            def fake_search(*args, **kwargs):
                # Real mw_search caps to search_max_results; the cap is the 9th
                # positional argument (index 8) in the call signature.
                cap = args[8] if len(args) > 8 else kwargs.get("search_max_results", 10)
                items = [{"title": f"Title {i}", "snippet": "x"} for i in range(20)]
                return {"query": {"search": items[:cap]}}

            calls = {"count": 0}

            def fake_page(*_args, **_kwargs):
                calls["count"] += 1
                return {
                    "query": {
                        "pages": {
                            "123": {
                                "title": "Title",
                                "pageid": 123,
                                "fullurl": "https://en.wikipedia.org/wiki/Title",
                                "pageprops": {},
                                "description": "Example",
                                "extract": "Example",
                            }
                        }
                    }
                }

            orig_search = self.mw.mw_search
            orig_page = self.mw.mw_page_details
            try:
                self.mw.mw_search = fake_search
                self.mw.mw_page_details = fake_page
                rc = self.mw.run(
                    input_path=input_path,
                    output_path=output_path,
                    srlimit=10,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    overwrite=False,
                    user_agent="ua",
                    progress_every=0,
                    log_file=None,
                    search_max_results=3,
                )
                self.assertEqual(rc, 0)
            finally:
                self.mw.mw_search = orig_search
                self.mw.mw_page_details = orig_page

            rows = [json.loads(x) for x in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(len(rows[0]["mw_search"]["results"]), 3)
            self.assertEqual(calls["count"], 3)

    def test_run_writes_output(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "input.jsonl"
            output_path = tmp / "out.jsonl"
            cache_dir = tmp / "cache"

            input_path.write_text(
                json.dumps(
                    {
                        "event_id": "1",
                        "subject_name_full": "Ada Lovelace",
                        "gate1_decision": "WEAK_PASS",
                        "entry_title": "Ada Lovelace obituary",
                        "summary": "Test summary",
                        "source": "Example",
                        "publication_date": "2026-02-01",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            def fake_search(*_args, **_kwargs):
                return {
                    "query": {
                        "search": [
                            {"title": "Ada Lovelace", "snippet": "Mathematician"}
                        ]
                    }
                }

            def fake_page(*_args, **_kwargs):
                return {
                    "query": {
                        "pages": {
                            "123": {
                                "title": "Ada Lovelace",
                                "pageid": 123,
                                "fullurl": "https://en.wikipedia.org/wiki/Ada_Lovelace",
                                "pageprops": {},
                                "description": "English mathematician",
                                "extract": "Ada Lovelace was...",
                            }
                        }
                    }
                }

            # monkeypatch
            orig_search = self.mw.mw_search
            orig_page = self.mw.mw_page_details
            try:
                self.mw.mw_search = fake_search
                self.mw.mw_page_details = fake_page
                rc = self.mw.run(
                    input_path=input_path,
                    output_path=output_path,
                    srlimit=5,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    overwrite=False,
                    user_agent="ua",
                    progress_every=0,
                    log_file=None,
                    search_max_results=5,
                )
                self.assertEqual(rc, 0)
            finally:
                self.mw.mw_search = orig_search
                self.mw.mw_page_details = orig_page

            rows = [json.loads(x) for x in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["event_id"], "1")
            self.assertEqual(rows[0]["mw_search"]["results"][0]["title"], "Ada Lovelace")

    def test_subject_from_parsed_output(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "input.jsonl"
            output_path = tmp / "out.jsonl"
            cache_dir = tmp / "cache"

            input_path.write_text(
                json.dumps(
                    {
                        "event_id": "1",
                        "parsed_output": {
                            "subject_name_full": "Ada Lovelace",
                            "subject_name_as_written": "Ada Lovelace",
                            "gate1_decision": "STRONG_PASS",
                        },
                        "entry_title": "Ada Lovelace obituary",
                        "summary": "Test summary",
                        "source": "Example",
                        "publication_date": "2026-02-01",
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            def fake_search(*_args, **_kwargs):
                return {"query": {"search": [{"title": "Ada Lovelace", "snippet": "Mathematician"}]}}

            def fake_page(*_args, **_kwargs):
                return {
                    "query": {
                        "pages": {
                            "123": {
                                "title": "Ada Lovelace",
                                "pageid": 123,
                                "fullurl": "https://en.wikipedia.org/wiki/Ada_Lovelace",
                                "pageprops": {},
                                "description": "English mathematician",
                                "extract": "Ada Lovelace was...",
                            }
                        }
                    }
                }

            orig_search = self.mw.mw_search
            orig_page = self.mw.mw_page_details
            try:
                self.mw.mw_search = fake_search
                self.mw.mw_page_details = fake_page
                rc = self.mw.run(
                    input_path=input_path,
                    output_path=output_path,
                    srlimit=5,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    overwrite=False,
                    user_agent="ua",
                    progress_every=0,
                    log_file=None,
                    search_max_results=5,
                )
                self.assertEqual(rc, 0)
            finally:
                self.mw.mw_search = orig_search
                self.mw.mw_page_details = orig_page

            rows = [json.loads(x) for x in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["subject_name"], "Ada Lovelace")

    def test_dedupe_first_record_wins_even_if_later_duplicate_is_valid(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "input.jsonl"
            output_path = tmp / "out.jsonl"
            cache_dir = tmp / "cache"

            rows = [
                {"event_id": "1", "parsed_output": None},
                {
                    "event_id": "1",
                    "parsed_output": {
                        "subject_name_full": "Ada Lovelace",
                        "gate1_decision": "WEAK_PASS",
                    },
                    "entry_title": "Ada Lovelace obituary",
                    "summary": "Test summary",
                    "source": "Example",
                    "publication_date": "2026-02-01",
                },
            ]
            with input_path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")

            def fake_search(*_args, **_kwargs):
                return {"query": {"search": [{"title": "Ada Lovelace", "snippet": "Mathematician"}]}}

            def fake_page(*_args, **_kwargs):
                return {
                    "query": {
                        "pages": {
                            "123": {
                                "title": "Ada Lovelace",
                                "pageid": 123,
                                "fullurl": "https://en.wikipedia.org/wiki/Ada_Lovelace",
                                "pageprops": {},
                                "description": "English mathematician",
                                "extract": "Ada Lovelace was...",
                            }
                        }
                    }
                }

            orig_search = self.mw.mw_search
            orig_page = self.mw.mw_page_details
            try:
                self.mw.mw_search = fake_search
                self.mw.mw_page_details = fake_page
                rc = self.mw.run(
                    input_path=input_path,
                    output_path=output_path,
                    srlimit=5,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    overwrite=False,
                    user_agent="ua",
                    progress_every=0,
                    log_file=None,
                    search_max_results=5,
                )
                self.assertEqual(rc, 0)
            finally:
                self.mw.mw_search = orig_search
                self.mw.mw_page_details = orig_page

            rows = [json.loads(x) for x in output_path.read_text(encoding="utf-8").splitlines()]
            # Current semantics: first record for an event_id wins, so the later duplicate
            # (even if valid) is skipped.
            self.assertEqual(len(rows), 0)


    # --- nickname expansion tests ---

    def test_nickname_to_formal(self) -> None:
        result = self.mw.expand_nickname_variants("Nick White")
        self.assertEqual(result, ["Nicholas White"])

    def test_formal_to_nicknames(self) -> None:
        result = self.mw.expand_nickname_variants("Nicholas White")
        self.assertIn("Nick White", result)
        self.assertIn("Nicky White", result)

    def test_no_match(self) -> None:
        # "Agatha" is not in NICKNAME_MAP and no nickname maps to it
        result = self.mw.expand_nickname_variants("Agatha Smith")
        self.assertEqual(result, [])

    def test_single_token_nickname(self) -> None:
        result = self.mw.expand_nickname_variants("Nick")
        self.assertIn("Nicholas", result)

    def test_single_token_formal(self) -> None:
        result = self.mw.expand_nickname_variants("Nicholas")
        self.assertIn("Nick", result)
        self.assertIn("Nicky", result)

    def test_query_variants_includes_formal(self) -> None:
        variants = self.mw.query_variants("Nick White")
        self.assertIn("Nicholas White", variants)

    def test_query_variants_no_duplicate(self) -> None:
        for name in ("Nick White", "Nicholas White", "Dr Bill Smith", "Bob Jones"):
            variants = self.mw.query_variants(name)
            self.assertEqual(len(variants), len(set(variants)), f"duplicate in {name}: {variants}")

    def test_source_context_from_gate_input(self) -> None:
        """source_context fields fall back to gate_input when not top-level (Gate 1 output format)."""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "input.jsonl"
            output_path = tmp / "out.jsonl"
            cache_dir = tmp / "cache"

            # Gate 1-style row: summary/source/publication_date live inside gate_input,
            # not at the top level.
            input_path.write_text(
                json.dumps(
                    {
                        "event_id": "abc",
                        "entry_title": "Fred Smith obituary",
                        "subject_name_full": "Fred Smith",
                        "gate1_decision": "WEAK_PASS",
                        "gate_input": {
                            "title": "Fred Smith obituary",
                            "summary": "Bass player who provided subtle but potent rhythm.",
                            "source": "Obituaries | The Guardian",
                            "publication_date": "2026-02-19T17:23:48Z",
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )

            def fake_search(*_args, **_kwargs):
                return {"query": {"search": [{"title": "Fred Smith", "snippet": "Musician"}]}}

            def fake_page(*_args, **_kwargs):
                return {
                    "query": {
                        "pages": {
                            "999": {
                                "title": "Fred Smith",
                                "pageid": 999,
                                "fullurl": "https://en.wikipedia.org/wiki/Fred_Smith",
                                "pageprops": {},
                                "description": "Musician",
                                "extract": "Fred Smith was a musician.",
                            }
                        }
                    }
                }

            orig_search = self.mw.mw_search
            orig_page = self.mw.mw_page_details
            try:
                self.mw.mw_search = fake_search
                self.mw.mw_page_details = fake_page
                rc = self.mw.run(
                    input_path=input_path,
                    output_path=output_path,
                    srlimit=5,
                    throttle_ms=0,
                    max_retries=0,
                    cache_dir=cache_dir,
                    cache_ttl_days=None,
                    overwrite=False,
                    user_agent="ua",
                    progress_every=0,
                    log_file=None,
                    search_max_results=5,
                )
                self.assertEqual(rc, 0)
            finally:
                self.mw.mw_search = orig_search
                self.mw.mw_page_details = orig_page

            rows = [json.loads(x) for x in output_path.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(rows), 1)
            ctx = rows[0]["source_context"]
            self.assertEqual(ctx["summary"], "Bass player who provided subtle but potent rhythm.")
            self.assertEqual(ctx["source"], "Obituaries | The Guardian")
            self.assertEqual(ctx["publication_date"], "2026-02-19T17:23:48Z")


if __name__ == "__main__":
    unittest.main()
