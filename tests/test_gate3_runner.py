import importlib.util
import json
import unittest
from pathlib import Path


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestGate3Runner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path(__file__).resolve().parents[1]
        cls.mod = load_module("llm_gate3_runner", root / "scripts" / "llm_gate3_runner.py")

    # --- select_candidates ---

    def test_select_candidates_bio_positive_only(self) -> None:
        results = [
            {"title": "A", "biography_score": 3},
            {"title": "B", "biography_score": 6},
            {"title": "C", "biography_score": 1},
        ]
        selected = self.mod.select_candidates(results)
        titles = [c["title"] for c in selected]
        self.assertIn("A", titles)
        self.assertIn("B", titles)
        self.assertIn("C", titles)
        self.assertEqual(len(selected), 3)

    def test_select_candidates_fallback(self) -> None:
        # Only 1 bio-positive → should add top non-bio as fallback
        results = [
            {"title": "BioOnly", "biography_score": 3},
            {"title": "NonBio1", "biography_score": 0},
            {"title": "NonBio2", "biography_score": -1},
        ]
        selected = self.mod.select_candidates(results)
        titles = [c["title"] for c in selected]
        self.assertIn("BioOnly", titles)
        self.assertIn("NonBio1", titles)
        self.assertEqual(len(selected), 2)

    def test_select_candidates_none_positive(self) -> None:
        # No bio-positive → returns top 1 non-bio only
        results = [
            {"title": "X", "biography_score": 0},
            {"title": "Y", "biography_score": -2},
        ]
        selected = self.mod.select_candidates(results)
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["title"], "X")

    def test_select_candidates_empty(self) -> None:
        selected = self.mod.select_candidates([])
        self.assertEqual(selected, [])

    def test_select_candidates_two_bio_positive(self) -> None:
        # Exactly 2 bio-positive → return them, no non-bio appended
        results = [
            {"title": "Bio1", "biography_score": 3},
            {"title": "Bio2", "biography_score": 6},
            {"title": "NonBio", "biography_score": -1},
        ]
        selected = self.mod.select_candidates(results)
        titles = [c["title"] for c in selected]
        self.assertIn("Bio1", titles)
        self.assertIn("Bio2", titles)
        self.assertNotIn("NonBio", titles)
        self.assertEqual(len(selected), 2)

    # --- extract_prose_snippet ---

    def test_extract_prose_snippet_strips_templates(self) -> None:
        text = "{{Infobox person|name=Jane}} Jane Smith was a novelist born in 1950."
        result = self.mod.extract_prose_snippet(text)
        self.assertNotIn("{{Infobox", result)
        self.assertIn("Jane Smith", result)

    def test_extract_prose_snippet_first_para_longer(self) -> None:
        # First paragraph > 700 chars → return first paragraph (not truncated to 700)
        long_para = "A" * 800
        text = long_para + "\n\nSecond paragraph here."
        result = self.mod.extract_prose_snippet(text)
        self.assertEqual(result, long_para)
        self.assertGreater(len(result), 700)

    def test_extract_prose_snippet_700_chars_longer(self) -> None:
        # First paragraph < 700 chars → return text[:700]
        short_para = "Short first paragraph."
        filler = "B" * 800
        text = short_para + "\n\n" + filler
        result = self.mod.extract_prose_snippet(text)
        self.assertEqual(len(result), 700)
        self.assertTrue(result.startswith("Short first paragraph"))

    def test_extract_prose_snippet_empty(self) -> None:
        self.assertEqual(self.mod.extract_prose_snippet(""), "")
        self.assertEqual(self.mod.extract_prose_snippet(None), "")

    def test_extract_prose_snippet_normalises_whitespace(self) -> None:
        text = "Jane   Smith   was   a   writer."
        result = self.mod.extract_prose_snippet(text)
        self.assertNotIn("   ", result)

    # --- gate3_output_schema ---

    def test_gate3_output_schema_valid_has_page(self) -> None:
        import jsonschema

        schema = self.mod.gate3_output_schema()
        valid = {
            "status": "HAS_PAGE",
            "matched_title": "Jane Smith (author)",
            "confidence": 0.9,
            "evidence": ["Article mentions novelist career, Wikipedia confirms same"],
        }
        # Should not raise
        jsonschema.validate(valid, schema)

    def test_gate3_output_schema_valid_missing(self) -> None:
        import jsonschema

        schema = self.mod.gate3_output_schema()
        valid = {
            "status": "MISSING",
            "matched_title": None,
            "confidence": 0.1,
            "evidence": ["No candidate matches subject career details"],
        }
        jsonschema.validate(valid, schema)

    def test_gate3_output_schema_valid_uncertain(self) -> None:
        import jsonschema

        schema = self.mod.gate3_output_schema()
        valid = {
            "status": "UNCERTAIN",
            "matched_title": None,
            "confidence": 0.5,
            "evidence": ["Name matches but extract too sparse to confirm"],
        }
        jsonschema.validate(valid, schema)

    def test_gate3_output_schema_missing_field(self) -> None:
        import jsonschema

        schema = self.mod.gate3_output_schema()
        invalid = {
            "status": "HAS_PAGE",
            # missing matched_title, confidence, evidence
        }
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(invalid, schema)

    def test_gate3_output_schema_invalid_status(self) -> None:
        import jsonschema

        schema = self.mod.gate3_output_schema()
        invalid = {
            "status": "UNKNOWN_STATUS",
            "matched_title": None,
            "confidence": 0.5,
            "evidence": ["something"],
        }
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(invalid, schema)

    def test_gate3_output_schema_too_many_evidence(self) -> None:
        import jsonschema

        schema = self.mod.gate3_output_schema()
        invalid = {
            "status": "MISSING",
            "matched_title": None,
            "confidence": 0.2,
            "evidence": ["a", "b", "c", "d"],  # maxItems=3
        }
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(invalid, schema)

    # --- safe_json_parse ---

    def test_safe_json_parse_valid(self) -> None:
        text = '{"status": "HAS_PAGE", "matched_title": "Test", "confidence": 0.9, "evidence": ["ok"]}'
        ok, parsed, err = self.mod.safe_json_parse(text)
        self.assertTrue(ok)
        self.assertIsNotNone(parsed)
        self.assertIsNone(err)
        self.assertEqual(parsed["status"], "HAS_PAGE")

    def test_safe_json_parse_valid_with_preamble(self) -> None:
        # JSON wrapped in prose (model sometimes adds text before JSON)
        text = 'Here is my answer:\n{"status": "MISSING", "matched_title": null, "confidence": 0.1, "evidence": ["no match"]}'
        ok, parsed, err = self.mod.safe_json_parse(text)
        self.assertTrue(ok)
        self.assertEqual(parsed["status"], "MISSING")

    def test_safe_json_parse_invalid(self) -> None:
        text = "This is not JSON at all"
        ok, parsed, err = self.mod.safe_json_parse(text)
        self.assertFalse(ok)
        self.assertIsNone(parsed)
        self.assertIsNotNone(err)

    def test_safe_json_parse_empty(self) -> None:
        ok, parsed, err = self.mod.safe_json_parse("")
        self.assertFalse(ok)
        self.assertIsNone(parsed)
        self.assertEqual(err, "empty_output")

    # --- format_gate3_prompt ---

    def test_format_gate3_prompt_includes_subject(self) -> None:
        prompt_body = "You are Gate 3."
        source_context = {
            "entry_title": "Jane Smith dies aged 80",
            "summary": "Novelist Jane Smith has died.",
            "source": "The Times",
            "publication_date": "2026-01-01",
        }
        candidates = [
            {
                "title": "Jane Smith (author)",
                "description": "British novelist",
                "extract": "Jane Smith was a British novelist born in 1945.",
            }
        ]
        result = self.mod.format_gate3_prompt(
            prompt_body=prompt_body,
            subject="Jane Smith",
            source_context=source_context,
            candidates=candidates,
        )
        self.assertIn("Subject: Jane Smith", result)
        self.assertIn("Jane Smith dies aged 80", result)
        self.assertIn("Jane Smith (author)", result)
        self.assertIn("British novelist", result)

    def test_format_gate3_prompt_numbered_candidates(self) -> None:
        prompt_body = "Gate 3 prompt."
        source_context = {"entry_title": "T", "summary": "S", "source": "P", "publication_date": "2026-01-01"}
        candidates = [
            {"title": "First", "description": "Desc1", "extract": "Extract1"},
            {"title": "Second", "description": "Desc2", "extract": "Extract2"},
        ]
        result = self.mod.format_gate3_prompt(
            prompt_body=prompt_body,
            subject="Test Person",
            source_context=source_context,
            candidates=candidates,
        )
        self.assertIn("1. First", result)
        self.assertIn("2. Second", result)


if __name__ == "__main__":
    unittest.main()
