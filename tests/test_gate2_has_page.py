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


class TestGate2HasPage(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.mod = load_module("det_gate2_has_page", root / "scripts" / "det_gate2_has_page.py")

    def test_levenshtein_normalization(self) -> None:
        norm = self.mod.normalize_name
        a = norm("John Smith (artist)")
        b = norm("John Smith")
        self.assertEqual(self.mod.levenshtein(a, b), 0)

        a = norm("Jesse Jackson")
        b = norm("Jessie Jackson")
        self.assertLessEqual(self.mod.levenshtein(a, b), 5)
        self.assertLessEqual(self.mod.levenshtein(a, b), 2)

    def test_exact_match_goes_to_pass(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            known = tmp / "known.json"

            row = {
                "event_id": "1",
                "subject_name": "Jesse Jackson",
                "mw_search": {
                    "results": [
                        {
                            "title": "Jesse Jackson",
                            "pageid": 10,
                            "fullurl": "https://en.wikipedia.org/wiki/Jesse_Jackson",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        }
                    ]
                },
            }
            input_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

            rc = self.mod.run_gate2(
                input_path=input_path,
                pass_output=pass_out,
                skip_output=skip_out,
                known_pages_path=known,
                gate2_run_id=None,
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            pass_text = pass_out.read_text(encoding="utf-8")
            self.assertIn("Jesse Jackson", pass_text)
            self.assertEqual(skip_out.read_text(encoding="utf-8").strip(), "")
            record = json.loads(pass_text.strip())
            self.assertEqual(record["det_gate2_signal"], "EXACT_MATCH")
            self.assertEqual(record["det_gate2_best_match_type"], "title")

    def test_pass_when_similar_bio_present(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            known = tmp / "known.json"

            row = {
                "event_id": "1",
                "subject_name": "Jesse Jackson",
                "mw_search": {
                    "results": [
                        {
                            "title": "Jesse Jackson",
                            "pageid": 10,
                            "fullurl": "https://en.wikipedia.org/wiki/Jesse_Jackson",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        },
                        {
                            "title": "Jessie Jackson",
                            "pageid": 11,
                            "fullurl": "https://en.wikipedia.org/wiki/Jessie_Jackson",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        },
                    ]
                },
            }
            input_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

            rc = self.mod.run_gate2(
                input_path=input_path,
                pass_output=pass_out,
                skip_output=skip_out,
                known_pages_path=known,
                gate2_run_id=None,
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            pass_text = pass_out.read_text(encoding="utf-8")
            self.assertIn("Jesse Jackson", pass_text)
            self.assertEqual(skip_out.read_text(encoding="utf-8").strip(), "")
            record = json.loads(pass_text.strip())
            self.assertEqual(record["det_gate2_signal"], "EXACT_MATCH_AMBIGUOUS")
            self.assertTrue(record["det_gate2_has_similar_bio"])

    def test_pass_when_not_biography(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            known = tmp / "known.json"

            row = {
                "event_id": "1",
                "subject_name": "Jesse Jackson",
                "mw_search": {
                    "results": [
                        {
                            "title": "Jesse Jackson",
                            "pageid": 10,
                            "fullurl": "https://en.wikipedia.org/wiki/Jesse_Jackson",
                            "biography_prioritized": False,
                            "biography_score": -3,
                        }
                    ]
                },
            }
            input_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

            rc = self.mod.run_gate2(
                input_path=input_path,
                pass_output=pass_out,
                skip_output=skip_out,
                known_pages_path=known,
                gate2_run_id=None,
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            pass_text = pass_out.read_text(encoding="utf-8")
            self.assertIn("Jesse Jackson", pass_text)
            self.assertEqual(skip_out.read_text(encoding="utf-8").strip(), "")
            record = json.loads(pass_text.strip())
            self.assertEqual(record["det_gate2_signal"], "NO_BIO_CANDIDATES")
            self.assertEqual(record["det_gate2_bio_candidates_count"], 0)

    def test_pass_when_no_exact_match(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            known = tmp / "known.json"

            row = {
                "event_id": "1",
                "subject_name": "Hannah Windross",
                "mw_search": {
                    "results": [
                        {
                            "title": "Ayia Napa",
                            "pageid": 10,
                            "fullurl": "https://en.wikipedia.org/wiki/Ayia_Napa",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        }
                    ]
                },
            }
            input_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

            rc = self.mod.run_gate2(
                input_path=input_path,
                pass_output=pass_out,
                skip_output=skip_out,
                known_pages_path=known,
                gate2_run_id=None,
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            pass_text = pass_out.read_text(encoding="utf-8")
            self.assertIn("Hannah Windross", pass_text)
            self.assertEqual(skip_out.read_text(encoding="utf-8").strip(), "")
            record = json.loads(pass_text.strip())
            self.assertEqual(record["det_gate2_signal"], "NO_EXACT_MATCH")

    def test_redirect_match_goes_to_pass(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            known = tmp / "known.json"

            row = {
                "event_id": "1",
                "subject_name": "Jesse Jackson",
                "mw_search": {
                    "results": [
                        {
                            "title": "Rev. Jesse Jackson",
                            "redirected_from": "Jesse Jackson",
                            "pageid": 10,
                            "fullurl": "https://en.wikipedia.org/wiki/Jesse_Jackson",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        }
                    ]
                },
            }
            input_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

            rc = self.mod.run_gate2(
                input_path=input_path,
                pass_output=pass_out,
                skip_output=skip_out,
                known_pages_path=known,
                gate2_run_id=None,
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            pass_text = pass_out.read_text(encoding="utf-8")
            self.assertIn("Jesse Jackson", pass_text)
            self.assertEqual(skip_out.read_text(encoding="utf-8").strip(), "")
            record = json.loads(pass_text.strip())
            self.assertEqual(record["det_gate2_signal"], "EXACT_MATCH")
            self.assertEqual(record["det_gate2_best_match_type"], "redirect")

    def test_exact_match_known_pages_not_updated(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"
            known = tmp / "known.json"

            row = {
                "event_id": "1",
                "subject_name": "Jesse Jackson",
                "mw_search": {
                    "results": [
                        {
                            "title": "Jesse Jackson",
                            "pageid": 10,
                            "fullurl": "https://en.wikipedia.org/wiki/Jesse_Jackson",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        }
                    ]
                },
            }
            input_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

            rc = self.mod.run_gate2(
                input_path=input_path,
                pass_output=pass_out,
                skip_output=skip_out,
                known_pages_path=known,
                gate2_run_id="run-1",
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            self.assertFalse(known.exists(), "known_pages file must not be written by Gate 2")


if __name__ == "__main__":
    unittest.main()
