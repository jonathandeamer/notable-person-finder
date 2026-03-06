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


class TestPrefilterGate0(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.prefilter = load_module(
            "det_gate0_prefilter", root / "scripts" / "det_gate0_prefilter.py"
        )
        cls.gate1_trial = load_module(
            "llm_gate1_runner", root / "scripts" / "llm_gate1_runner.py"
        )

    def test_classify_full_name_pass(self) -> None:
        event = {"entry_title": "Ed Crane dies at 86", "summary": "Policy leader remembered"}
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PREFILTER_PASS_TO_LLM")
        self.assertEqual(out["prefilter_reason_codes"], ["NAME_FULL_MATCH"])

    def test_classify_initial_surname_pass(self) -> None:
        event = {"entry_title": "J. Smith obituary", "summary": ""}
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PREFILTER_PASS_TO_LLM")
        self.assertEqual(out["prefilter_reason_codes"], ["NAME_INITIAL_SURNAME_MATCH"])

    def test_classify_obit_guardrail_pass(self) -> None:
        event = {"entry_title": "Poet remembered", "summary": "Obituary for celebrated artist"}
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PREFILTER_PASS_TO_LLM")
        self.assertEqual(out["prefilter_reason_codes"], ["OBIT_CUE_WITH_CAPITALIZED_TOKEN"])

    def test_classify_obit_without_guardrail_skip(self) -> None:
        event = {"entry_title": "man died", "summary": "tributes paid by locals"}
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PREFILTER_SKIP_NO_NAME")
        self.assertEqual(
            out["prefilter_reason_codes"],
            ["NO_NAME_SIGNAL_OBIT_CUE_WITHOUT_GUARDRAIL"],
        )

    def test_classify_default_skip(self) -> None:
        event = {"entry_title": "rain expected this weekend", "summary": "travel update"}
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PREFILTER_SKIP_NO_NAME")
        self.assertEqual(out["prefilter_reason_codes"], ["NO_NAME_SIGNAL_DEFAULT_SKIP"])

    def test_run_prefilter_integration(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            events = tmp / "events.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            known_pages = tmp / "known.json"

            rows = [
                {"event_id": "1", "entry_title": "Ed Crane dies at 86", "summary": ""},
                {"event_id": "2", "entry_title": "man died", "summary": "tributes paid"},
                {"event_id": "3", "entry_title": "weather forecast", "summary": ""},
            ]
            with events.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")
                f.write("{invalid-json\n")

            rc = self.prefilter.run_prefilter(
                events,
                pass_out,
                skip_out,
                overwrite=False,
                known_pages_path=known_pages,
            )
            self.assertEqual(rc, 0)

            pass_rows = [json.loads(x) for x in pass_out.read_text(encoding="utf-8").splitlines()]
            skip_rows = [json.loads(x) for x in skip_out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(pass_rows), 1)
            self.assertEqual(len(skip_rows), 2)
            self.assertEqual(pass_rows[0]["event_id"], "1")
            self.assertEqual(skip_rows[0]["event_id"], "2")
            self.assertIn("prefilter_decision", pass_rows[0])
            self.assertIn("prefilter_signals", pass_rows[0])

    def test_run_prefilter_collision_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            events = tmp / "events.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            events.write_text(json.dumps({"event_id": "1", "entry_title": "a", "summary": "b"}) + "\n", encoding="utf-8")
            pass_out.write_text("", encoding="utf-8")
            skip_out.write_text("", encoding="utf-8")

            with self.assertRaises(FileExistsError):
                self.prefilter.run_prefilter(
                    events, pass_out, skip_out, overwrite=False, known_pages_path=None
                )

    def test_known_pages_skip(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            events = tmp / "events.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            known_pages = tmp / "known.json"

            known_pages.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "updated_at_utc": "2026-02-20T12:00:00Z",
                        "entries": {
                            "ed crane": {"normalized_name": "ed crane", "pageid": 1}
                        },
                    }
                ),
                encoding="utf-8",
            )
            events.write_text(
                json.dumps({"event_id": "1", "entry_title": "Ed Crane dies at 86", "summary": ""})
                + "\n",
                encoding="utf-8",
            )

            rc = self.prefilter.run_prefilter(
                events, pass_out, skip_out, overwrite=False, known_pages_path=known_pages
            )
            self.assertEqual(rc, 0)

            pass_rows = [json.loads(x) for x in pass_out.read_text(encoding="utf-8").splitlines()]
            skip_rows = [json.loads(x) for x in skip_out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(pass_rows), 0)
            self.assertEqual(len(skip_rows), 1)
            self.assertEqual(
                skip_rows[0]["prefilter_decision"], "PREFILTER_SKIP_HAS_WIKI_PAGE"
            )
            self.assertIn("KNOWN_WIKI_PAGE", skip_rows[0]["prefilter_reason_codes"])

    def test_letters_header_skipped(self) -> None:
        event = {
            "entry_title": "Letters: John Lucas obituary",
            "summary": "Readers respond to the recent obituary",
        }
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "LETTERS_HEADER_SKIP")
        self.assertIn("LETTERS_HEADER_SKIP", out["prefilter_reason_codes"])

    def test_letter_singular_header_skipped(self) -> None:
        event = {
            "entry_title": "Letter: Mark Fisher obituary",
            "summary": "A reader writes about the philosopher",
        }
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "LETTERS_HEADER_SKIP")
        self.assertIn("LETTERS_HEADER_SKIP", out["prefilter_reason_codes"])

    def test_letters_header_skip_in_integration(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            events = tmp / "events.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            rows = [
                {"event_id": "1", "entry_title": "Letters: John Lucas obituary", "summary": ""},
                {"event_id": "2", "entry_title": "Letter: Mark Fisher obituary", "summary": ""},
                {"event_id": "3", "entry_title": "Ed Crane dies at 86", "summary": ""},
            ]
            with events.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")
            rc = self.prefilter.run_prefilter(
                events, pass_out, skip_out, overwrite=False, known_pages_path=None
            )
            self.assertEqual(rc, 0)
            pass_rows = [json.loads(x) for x in pass_out.read_text(encoding="utf-8").splitlines()]
            skip_rows = [json.loads(x) for x in skip_out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(pass_rows), 1)
            self.assertEqual(pass_rows[0]["event_id"], "3")
            letters_skips = [r for r in skip_rows if r.get("prefilter_decision") == "LETTERS_HEADER_SKIP"]
            self.assertEqual(len(letters_skips), 2)
            self.assertEqual({r["event_id"] for r in letters_skips}, {"1", "2"})

    def test_personal_obit_husband_skipped(self) -> None:
        event = {
            "entry_title": "Aidan Chidarikire obituary",
            "summary": "<p>My husband, Aidan Chidarikire, who has died aged 92...</p>",
        }
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PERSONAL_TRIBUTE_OBIT_SKIP")
        self.assertIn("PERSONAL_TRIBUTE_OBIT_SKIP", out["prefilter_reason_codes"])

    def test_letters_pipe_suffix_skipped(self) -> None:
        event = {
            "entry_title": "Art and loss in the modern age | Letters",
            "summary": "Readers respond",
        }
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "LETTERS_HEADER_SKIP")
        self.assertIn("LETTERS_HEADER_SKIP", out["prefilter_reason_codes"])

    def test_letter_pipe_suffix_singular_skipped(self) -> None:
        event = {
            "entry_title": "On memory and grief | Letter",
            "summary": "A reader writes",
        }
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "LETTERS_HEADER_SKIP")
        self.assertIn("LETTERS_HEADER_SKIP", out["prefilter_reason_codes"])

    def test_in_pictures_structural_skip(self) -> None:
        event = {
            "entry_title": "Art Basel 2026 – in pictures",
            "summary": "A gallery roundup",
        }
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PREFILTER_SKIP_STRUCTURAL_PATTERN")
        self.assertIn("STRUCTURAL_TOPIC_SKIP", out["prefilter_reason_codes"])

    def test_trivia_structural_skip(self) -> None:
        event = {
            "entry_title": "Weekend Trivia: Famous Artists",
            "summary": "Test your knowledge",
        }
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PREFILTER_SKIP_STRUCTURAL_PATTERN")
        self.assertIn("STRUCTURAL_TOPIC_SKIP", out["prefilter_reason_codes"])

    def test_personal_obit_father_skipped(self) -> None:
        event = {
            "entry_title": "Desmond McConnell obituary",
            "summary": "My late father, Desmond McConnell, was a mineralogist.",
        }
        out = self.prefilter.classify_event(event)
        self.assertEqual(out["prefilter_decision"], "PERSONAL_TRIBUTE_OBIT_SKIP")
        self.assertIn("PERSONAL_TRIBUTE_OBIT_SKIP", out["prefilter_reason_codes"])

    def test_gate1_trial_resolve_events_path(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            explicit = tmp / "explicit.jsonl"
            explicit.write_text("", encoding="utf-8")
            pre = tmp / "prefilter_pass.jsonl"
            raw = tmp / "events.jsonl"
            raw.write_text("", encoding="utf-8")

            got = self.gate1_trial.resolve_events_path(explicit, pre, raw)
            self.assertEqual(got, explicit)

            pre.write_text("", encoding="utf-8")
            got = self.gate1_trial.resolve_events_path(None, pre, raw)
            self.assertEqual(got, pre)

            pre.unlink()
            got = self.gate1_trial.resolve_events_path(None, pre, raw)
            self.assertEqual(got, raw)

    def test_parse_feed_priorities_basic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            feeds = tmp / "feeds.md"
            feeds.write_text(
                "# Feed sources\n"
                "- https://rss.nytimes.com/services/xml/rss/nyt/Obituaries.xml 1\n"
                "- https://www.theguardian.com/tone/obituaries/rss 2\n"
                "- https://www.theatlantic.com/feed/all/\n",
                encoding="utf-8",
            )
            priorities = self.prefilter.parse_feed_priorities(feeds)
            self.assertEqual(priorities.get("https://rss.nytimes.com/services/xml/rss/nyt/Obituaries.xml"), 1)
            self.assertEqual(priorities.get("https://www.theguardian.com/tone/obituaries/rss"), 2)
            self.assertNotIn("https://www.theatlantic.com/feed/all/", priorities)

    def test_parse_feed_priorities_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            feeds = tmp / "nonexistent.md"
            priorities = self.prefilter.parse_feed_priorities(feeds)
            self.assertEqual(priorities, {})

    def test_run_prefilter_feed_priority_assigned(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            events = tmp / "events.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            feeds = tmp / "feeds.md"

            feeds.write_text(
                "# Feeds\n"
                "- https://example.com/feed1 5\n",
                encoding="utf-8",
            )
            events.write_text(
                json.dumps({
                    "event_id": "1",
                    "entry_title": "John Smith dies at 90",
                    "summary": "",
                    "source_feed_url_original": "https://example.com/feed1",
                }) + "\n",
                encoding="utf-8",
            )

            rc = self.prefilter.run_prefilter(
                events, pass_out, skip_out, overwrite=False, known_pages_path=None, feeds_path=feeds
            )
            self.assertEqual(rc, 0)

            pass_rows = [json.loads(x) for x in pass_out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(pass_rows), 1)
            self.assertEqual(pass_rows[0].get("feed_priority"), 5)

    def test_run_prefilter_feed_priority_none_for_unknown_feed(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            events = tmp / "events.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            feeds = tmp / "feeds.md"

            feeds.write_text(
                "# Feeds\n"
                "- https://example.com/feed1 5\n",
                encoding="utf-8",
            )
            events.write_text(
                json.dumps({
                    "event_id": "1",
                    "entry_title": "John Smith dies at 90",
                    "summary": "",
                    "source_feed_url_original": "https://example.com/unknown",
                }) + "\n",
                encoding="utf-8",
            )

            rc = self.prefilter.run_prefilter(
                events, pass_out, skip_out, overwrite=False, known_pages_path=None, feeds_path=feeds
            )
            self.assertEqual(rc, 0)

            pass_rows = [json.loads(x) for x in pass_out.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(len(pass_rows), 1)
            self.assertIsNone(pass_rows[0].get("feed_priority"))


if __name__ == "__main__":
    unittest.main()
