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

    # ------------------------------------------------------------------
    # Levenshtein boundary conditions (SIMILARITY_DISTANCE = 2)
    # ------------------------------------------------------------------

    def test_levenshtein_distance_exactly_2(self) -> None:
        """Distance 2 is within threshold → triggers EXACT_MATCH_AMBIGUOUS."""
        # "alice" vs "alicia": insert 'i', sub 'e'→'a' = 2 edits; last name identical
        self.assertEqual(self.mod.levenshtein("alice johnson", "alicia johnson"), 2)

    def test_levenshtein_distance_exactly_3(self) -> None:
        """Distance 3 is outside threshold → should NOT trigger ambiguity."""
        # "alice" vs "alex": i→e, c→x, del e = 3 edits; last name identical
        self.assertEqual(self.mod.levenshtein("alice johnson", "alex johnson"), 3)

    def test_distance_2_triggers_ambiguous(self) -> None:
        """Levenshtein distance exactly 2 between exact match and similar bio → EXACT_MATCH_AMBIGUOUS."""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"

            row = {
                "event_id": "1",
                "subject_name": "Alice Johnson",
                "mw_search": {
                    "results": [
                        {
                            "title": "Alice Johnson",
                            "pageid": 10,
                            "fullurl": "https://en.wikipedia.org/wiki/Alice_Johnson",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        },
                        {
                            # Distance from "alice johnson" = 2 — within threshold
                            "title": "Alicia Johnson",
                            "pageid": 11,
                            "fullurl": "https://en.wikipedia.org/wiki/Alicia_Johnson",
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
                known_pages_path=tmp / "known.json",
                gate2_run_id=None,
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            record = json.loads(pass_out.read_text(encoding="utf-8").strip())
            self.assertEqual(record["det_gate2_signal"], "EXACT_MATCH_AMBIGUOUS")
            self.assertTrue(record["det_gate2_has_similar_bio"])

    def test_distance_3_does_not_trigger_ambiguous(self) -> None:
        """Levenshtein distance exactly 3 between exact match and other bio → EXACT_MATCH (not ambiguous)."""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"

            row = {
                "event_id": "1",
                "subject_name": "Alice Johnson",
                "mw_search": {
                    "results": [
                        {
                            "title": "Alice Johnson",
                            "pageid": 10,
                            "fullurl": "https://en.wikipedia.org/wiki/Alice_Johnson",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        },
                        {
                            # Distance from "alice johnson" = 3 — outside threshold
                            "title": "Alex Johnson",
                            "pageid": 11,
                            "fullurl": "https://en.wikipedia.org/wiki/Alex_Johnson",
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
                known_pages_path=tmp / "known.json",
                gate2_run_id=None,
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            record = json.loads(pass_out.read_text(encoding="utf-8").strip())
            self.assertEqual(record["det_gate2_signal"], "EXACT_MATCH")
            self.assertFalse(record["det_gate2_has_similar_bio"])

    # ------------------------------------------------------------------
    # pick_best_match: biography_score tiebreaker
    # ------------------------------------------------------------------

    def test_pick_best_match_prefers_higher_score(self) -> None:
        """When two exact matches exist, the one with the higher biography_score wins."""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "in.jsonl"
            pass_out = tmp / "pass.jsonl"
            skip_out = tmp / "skip.jsonl"

            row = {
                "event_id": "1",
                "subject_name": "Sam Rivers",
                "mw_search": {
                    "results": [
                        {
                            "title": "Sam Rivers",
                            "pageid": 20,
                            "fullurl": "https://en.wikipedia.org/wiki/Sam_Rivers_(musician)",
                            "biography_prioritized": True,
                            "biography_score": 6,
                        },
                        {
                            "title": "Sam Rivers",
                            "pageid": 21,
                            "fullurl": "https://en.wikipedia.org/wiki/Sam_Rivers_(athlete)",
                            "biography_prioritized": True,
                            "biography_score": 3,
                        },
                    ]
                },
            }
            input_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

            rc = self.mod.run_gate2(
                input_path=input_path,
                pass_output=pass_out,
                skip_output=skip_out,
                known_pages_path=tmp / "known.json",
                gate2_run_id=None,
                overwrite=True,
                log_file=None,
            )
            self.assertEqual(rc, 0)
            record = json.loads(pass_out.read_text(encoding="utf-8").strip())
            self.assertEqual(record["det_gate2_best_match_score"], 6)
            self.assertEqual(record["det_gate2_best_match_pageid"], 20)

    def test_pick_best_match_score_threshold_boundary(self) -> None:
        """biography_score=3 (BIO_SCORE_THRESHOLD) beats score=2 for best match selection."""
        # Note: biography_prioritized is set upstream by det_mw_candidates based on score >= 3.
        # Here we test that pick_best_match correctly ranks score=3 above score=2.
        candidates = [
            {"title": "Sam Rivers", "pageid": 1, "biography_score": 2, "biography_prioritized": True},
            {"title": "Sam Rivers", "pageid": 2, "biography_score": 3, "biography_prioritized": True},
        ]
        best = self.mod.pick_best_match(candidates)
        self.assertEqual(best["pageid"], 2)
        self.assertEqual(best["biography_score"], 3)

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
