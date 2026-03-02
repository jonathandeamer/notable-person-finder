import argparse
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


def _make_result(rank: int, domain: str = "reuters.com", title: str = "Subject does things") -> dict:
    return {
        "rank": rank,
        "title": title,
        "description": f"An article about the subject (rank {rank}).",
        "url": f"https://{domain}/story-{rank}",
        "source_domain": domain,
    }


def _make_input_record(
    subject_name: str = "Test Person",
    brave_results: list | None = None,
    entry_title: str = "Test Person does something",
    source: str | None = None,
    event_id: str = "event-abc",
) -> dict:
    return {
        "event_id": event_id,
        "subject_name": subject_name,
        "gate3_status": "MISSING",
        "source_context": {
            "entry_title": entry_title,
            "summary": "A brief summary.",
            "source": source,
            "publication_date": "2026-02-21T00:00:00Z",
        },
        "brave_results": brave_results or [],
        "brave_result_count": len(brave_results or []),
    }


def _write_input(tmp: Path, records: list[dict]) -> Path:
    p = tmp / "input.jsonl"
    with p.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return p


def _read_output(path: Path) -> list[dict]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _make_args(
    input_path: Path,
    output_path: Path,
    prompt_path: Path,
    fresh_output: bool = False,
    min_reliable_results: int = 2,
    max_attempts: int = 1,
    delay_seconds: float = 0.0,
    max_output_chars: int = 5000,
    model: str = "test-model",
    retry_parse_failures: bool = False,
    brave_input: Path | None = None,
    unlisted_prompt: Path | None = None,
) -> argparse.Namespace:
    return argparse.Namespace(
        input=input_path,
        output=output_path,
        prompt=prompt_path,
        model=model,
        backend="claude-cli",
        cwd=Path("/tmp"),
        codex_cwd=Path("/tmp"),
        min_reliable_results=min_reliable_results,
        max_output_chars=max_output_chars,
        delay_seconds=delay_seconds,
        max_attempts=max_attempts,
        fresh_output=fresh_output,
        retry_parse_failures=retry_parse_failures,
        brave_input=brave_input,
        unlisted_prompt=unlisted_prompt,
    )


class TestGate4bRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_module(
            "llm_gate4b_runner", ROOT / "scripts" / "llm_gate4b_runner.py"
        )
        # Use the real prompt file so format_gate4b_prompt has a body to work with
        cls.prompt_path = ROOT / "prompts" / "gate4b.md"

    # ------------------------------------------------------------------
    # test 1: subject with only 1 reliable result → SKIPPED, no LLM call
    # ------------------------------------------------------------------
    def test_skipped_below_threshold(self) -> None:
        llm_calls = {"count": 0}

        def fake_llm(*_args, **_kwargs):
            llm_calls["count"] += 1
            return '{"results": []}', {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                records = [_make_input_record(brave_results=[_make_result(1)])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(input_path, output_path, self.prompt_path, min_reliable_results=2)
                rc = self.mod.run(args)
                self.assertEqual(rc, 0)
                rows = _read_output(output_path)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["gate4b_status"], "SKIPPED")
                self.assertEqual(rows[0]["confirmed_count"], 0)
                self.assertEqual(llm_calls["count"], 0)
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertEqual(rows[0]["second_pass_confirmed_count"], 0)
                self.assertIsNone(rows[0]["second_pass_llm_error"])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
                self.assertEqual(rows[0]["second_pass_raw_output"], "")
                self.assertIsNone(rows[0]["second_pass_parsed_output"])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # test 2: LLM returns about_subject=true for both → LIKELY_NOTABLE
    # ------------------------------------------------------------------
    def test_notable_two_confirmed(self) -> None:
        llm_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.9, "reasoning": "Article is about the subject."},
                {"rank": 2, "about_subject": True, "confidence": 0.85, "reasoning": "Confirmed primary subject."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            return llm_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(input_path, output_path, self.prompt_path)
                rc = self.mod.run(args)
                self.assertEqual(rc, 0)
                rows = _read_output(output_path)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["gate4b_status"], "LIKELY_NOTABLE")
                self.assertEqual(rows[0]["confirmed_count"], 2)
                self.assertTrue(rows[0]["json_parse_ok"])
                self.assertIsNone(rows[0]["llm_error"])
                # second_pass_* sentinel fields should be present
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertEqual(rows[0]["second_pass_confirmed_count"], 0)
                self.assertIsNone(rows[0]["second_pass_llm_error"])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
                self.assertEqual(rows[0]["second_pass_raw_output"], "")
                self.assertIsNone(rows[0]["second_pass_parsed_output"])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # test 3: LLM returns about_subject=false for all → NOT_NOTABLE
    # ------------------------------------------------------------------
    def test_not_notable_none_confirmed(self) -> None:
        llm_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": False, "confidence": 0.8, "reasoning": "About a different person."},
                {"rank": 2, "about_subject": False, "confidence": 0.75, "reasoning": "Passing mention only."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            return llm_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(input_path, output_path, self.prompt_path)
                rc = self.mod.run(args)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["gate4b_status"], "NOT_NOTABLE")
                self.assertEqual(rows[0]["confirmed_count"], 0)
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # test 4: 1 of 2 confirmed → UNCERTAIN
    # ------------------------------------------------------------------
    def test_uncertain_one_confirmed(self) -> None:
        llm_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.8, "reasoning": "Clearly about the subject."},
                {"rank": 2, "about_subject": False, "confidence": 0.7, "reasoning": "Different person with same name."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            return llm_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(input_path, output_path, self.prompt_path)
                rc = self.mod.run(args)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["gate4b_status"], "UNCERTAIN")
                self.assertEqual(rows[0]["confirmed_count"], 1)
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # test 5: result matching entry_title is excluded, drops below threshold → SKIPPED
    # ------------------------------------------------------------------
    def test_original_source_excluded_from_count(self) -> None:
        llm_calls = {"count": 0}

        def fake_llm(*_args, **_kwargs):
            llm_calls["count"] += 1
            return '{"results": []}', {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                entry_title = "Test Person wins award at major event"
                # One result whose title matches the entry_title (original source)
                # One independent result
                results = [
                    _make_result(1, domain="bbc.com", title=entry_title),
                    _make_result(2, domain="reuters.com", title="Test Person does something else"),
                ]
                records = [_make_input_record(
                    brave_results=results,
                    entry_title=entry_title,
                )]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                # With 1 result excluded, only 1 remains → below threshold of 2
                args = _make_args(input_path, output_path, self.prompt_path, min_reliable_results=2)
                self.mod.run(args)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["gate4b_status"], "SKIPPED")
                self.assertEqual(rows[0]["results_excluded_count"], 1)
                self.assertEqual(llm_calls["count"], 0)
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # test 6: CLI failure → llm_error set, gate4b_status UNCERTAIN
    # ------------------------------------------------------------------
    def test_llm_error_recorded(self) -> None:
        def fake_llm(*_args, **_kwargs):
            raise RuntimeError("claude cli failed (code 1): connection refused")

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(input_path, output_path, self.prompt_path, max_attempts=1)
                self.mod.run(args)
                rows = _read_output(output_path)
                self.assertEqual(len(rows), 1)
                self.assertIsNotNone(rows[0]["llm_error"])
                self.assertIn("connection refused", rows[0]["llm_error"])
                # LLM error → we don't know notability; must be UNCERTAIN not NOT_NOTABLE
                self.assertEqual(rows[0]["gate4b_status"], "UNCERTAIN")
                self.assertEqual(rows[0]["confirmed_count"], 0)
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # test 7: non-JSON output → json_parse_ok=False, gate4b_status UNCERTAIN
    # ------------------------------------------------------------------
    def test_json_parse_failure_recorded(self) -> None:
        def fake_llm(*_args, **_kwargs):
            return "Sorry, I cannot answer that.", {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(input_path, output_path, self.prompt_path)
                self.mod.run(args)
                rows = _read_output(output_path)
                self.assertEqual(len(rows), 1)
                self.assertFalse(rows[0]["json_parse_ok"])
                self.assertIsNotNone(rows[0]["json_parse_error"])
                # Parse failure → we don't know notability; must be UNCERTAIN not NOT_NOTABLE
                self.assertEqual(rows[0]["gate4b_status"], "UNCERTAIN")
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
        finally:
            self.mod.call_claude_cli = orig

    def test_distinct_domains_required_for_likely_notable(self) -> None:
        llm_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.9, "reasoning": "About subject."},
                {"rank": 2, "about_subject": True, "confidence": 0.88, "reasoning": "About subject too."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            return llm_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                # Two URLs, same domain => one distinct domain.
                records = [_make_input_record(brave_results=[_make_result(1, "bbc.com"), _make_result(2, "bbc.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(input_path, output_path, self.prompt_path)
                rc = self.mod.run(args)
                self.assertEqual(rc, 0)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["gate4b_status"], "UNCERTAIN")
                self.assertEqual(rows[0]["confirmed_count"], 1)
                self.assertEqual(rows[0]["first_pass_domains"], ["bbc.com"])
        finally:
            self.mod.call_claude_cli = orig

    def test_second_pass_promotes_when_combined_domains_reach_two(self) -> None:
        call_count = {"n": 0}
        first_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.9, "reasoning": "About subject."},
                {"rank": 2, "about_subject": False, "confidence": 0.6, "reasoning": "Different subject."},
            ]
        })
        second_pass_response = json.dumps({
            "results": [
                {"rank": 2, "about_subject": True, "is_reliable_source": True, "confidence": 0.9, "reasoning": "About subject and reliable."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return first_pass_response, {"backend": "claude-cli"}
            return second_pass_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                brave_results = [_make_result(1, "reuters.com"), _make_result(2, "apnews.com")]
                brave_record = _make_input_record(brave_results=brave_results, event_id="event-abc")
                brave_path = tmp / "brave_coverage.jsonl"
                brave_path.write_text(json.dumps(brave_record) + "\n", encoding="utf-8")
                unlisted_prompt_path = tmp / "gate4b_unlisted.md"
                unlisted_prompt_path.write_text("Unlisted prompt body.", encoding="utf-8")

                records = [_make_input_record(brave_results=[_make_result(1, "reuters.com"), _make_result(2, "bbc.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(
                    input_path, output_path, self.prompt_path,
                    brave_input=brave_path, unlisted_prompt=unlisted_prompt_path,
                )
                self.mod.run(args)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["confirmed_count"], 1)
                self.assertEqual(rows[0]["second_pass_confirmed_count"], 1)
                self.assertEqual(rows[0]["gate4b_status"], "POSSIBLY_NOTABLE")
                self.assertEqual(set(rows[0]["all_reliable_brave_domains"]), {"reuters.com", "apnews.com"})
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # test 8: --fresh-output flag clears existing file
    # ------------------------------------------------------------------
    def test_fresh_output_overwrites(self) -> None:
        def fake_llm(*_args, **_kwargs):
            return '{"results": []}', {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                output_path = tmp / "out.jsonl"
                # Pre-populate with stale content (2 fake rows)
                old_row = json.dumps({"gate4b_status": "STALE", "subject_name": "Old"})
                output_path.write_text(old_row + "\n" + old_row + "\n", encoding="utf-8")

                # Run with 1 subject below threshold → writes 1 SKIPPED row
                records = [_make_input_record(brave_results=[_make_result(1)])]
                input_path = _write_input(tmp, records)
                args = _make_args(input_path, output_path, self.prompt_path, fresh_output=True)
                self.mod.run(args)

                rows = _read_output(output_path)
                # Old content should be gone; only 1 new row present
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["gate4b_status"], "SKIPPED")
                self.assertNotEqual(rows[0].get("subject_name"), "Old")
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # Unit tests: _is_original_source
    # ------------------------------------------------------------------

    def test_is_original_source_title_match(self) -> None:
        result = _make_result(1, title="Test Person wins award at major championship event in Rome")
        ctx = {
            "entry_title": "Test Person wins award at major championship event in Rome",
            "source": None,
        }
        self.assertTrue(self.mod._is_original_source(result, ctx))

    def test_is_original_source_domain_token_match(self) -> None:
        result = _make_result(1, domain="bbc.com")
        ctx = {"entry_title": "Some different title entirely", "source": "BBC News"}
        self.assertTrue(self.mod._is_original_source(result, ctx))

    def test_is_original_source_no_match(self) -> None:
        result = _make_result(1, domain="reuters.com", title="Independent story")
        ctx = {"entry_title": "Original article title", "source": "BBC News"}
        self.assertFalse(self.mod._is_original_source(result, ctx))

    def test_is_original_source_null_source(self) -> None:
        # Null source → only title comparison applies
        result = _make_result(1, domain="bbc.com", title="Independent story")
        ctx = {"entry_title": "Original article title", "source": None}
        self.assertFalse(self.mod._is_original_source(result, ctx))

    # ------------------------------------------------------------------
    # Unit tests: gate4b_output_schema
    # ------------------------------------------------------------------

    def test_schema_valid(self) -> None:
        import jsonschema

        schema = self.mod.gate4b_output_schema()
        valid = {
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.9, "reasoning": "Clearly about subject."},
                {"rank": 2, "about_subject": False, "confidence": 0.3, "reasoning": "Different person."},
            ]
        }
        jsonschema.validate(valid, schema)

    def test_schema_missing_results_key(self) -> None:
        import jsonschema

        schema = self.mod.gate4b_output_schema()
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate({}, schema)

    def test_schema_item_missing_required_field(self) -> None:
        import jsonschema

        schema = self.mod.gate4b_output_schema()
        invalid = {
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.9}  # missing "reasoning"
            ]
        }
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(invalid, schema)

    # ------------------------------------------------------------------
    # Unit tests: format_gate4b_prompt
    # ------------------------------------------------------------------

    def test_format_gate4b_prompt_includes_subject(self) -> None:
        prompt_body = "You are Gate 4b."
        ctx = {
            "entry_title": "Jane Smith wins award",
            "summary": "She won.",
            "source": "BBC News",
            "publication_date": "2026-01-01",
        }
        results = [_make_result(1, domain="reuters.com", title="Jane Smith wins award")]
        out = self.mod.format_gate4b_prompt(
            prompt_body=prompt_body,
            subject="Jane Smith",
            source_context=ctx,
            results=results,
        )
        self.assertIn("Subject: Jane Smith", out)
        self.assertIn("Jane Smith wins award", out)
        self.assertIn("reuters.com", out)

    def test_format_gate4b_prompt_numbered_by_rank(self) -> None:
        prompt_body = "Gate 4b."
        ctx = {"entry_title": "T", "summary": "S", "source": "P", "publication_date": "2026-01-01"}
        results = [_make_result(1), _make_result(3, "apnews.com")]
        out = self.mod.format_gate4b_prompt(
            prompt_body=prompt_body,
            subject="Test",
            source_context=ctx,
            results=results,
        )
        self.assertIn("1.", out)
        self.assertIn("3.", out)

    # ------------------------------------------------------------------
    # New test 1: second pass not run when brave_input=None
    # ------------------------------------------------------------------
    def test_second_pass_not_run_when_no_brave_input(self) -> None:
        llm_calls = {"count": 0}
        llm_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.9, "reasoning": "About subject."},
                {"rank": 2, "about_subject": True, "confidence": 0.85, "reasoning": "Confirmed."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            llm_calls["count"] += 1
            return llm_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(input_path, output_path, self.prompt_path, brave_input=None)
                rc = self.mod.run(args)
                self.assertEqual(rc, 0)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["gate4b_status"], "LIKELY_NOTABLE")
                self.assertEqual(llm_calls["count"], 1)
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertEqual(rows[0]["second_pass_confirmed_count"], 0)
                self.assertIsNone(rows[0]["second_pass_llm_error"])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
                self.assertEqual(rows[0]["second_pass_raw_output"], "")
                self.assertIsNone(rows[0]["second_pass_parsed_output"])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # New test 2: second pass skipped when first pass is LIKELY_NOTABLE
    # ------------------------------------------------------------------
    def test_second_pass_skipped_for_likely_notable(self) -> None:
        llm_calls = {"count": 0}
        llm_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.9, "reasoning": "About subject."},
                {"rank": 2, "about_subject": True, "confidence": 0.85, "reasoning": "Confirmed."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            llm_calls["count"] += 1
            return llm_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                brave_results = [_make_result(1), _make_result(2, "apnews.com"), _make_result(3, "bbc.com")]
                brave_record = _make_input_record(brave_results=brave_results, event_id="event-abc")
                brave_path = tmp / "brave_coverage.jsonl"
                brave_path.write_text(json.dumps(brave_record) + "\n", encoding="utf-8")
                unlisted_prompt_path = tmp / "gate4b_unlisted.md"
                unlisted_prompt_path.write_text("Unlisted prompt body.", encoding="utf-8")

                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(
                    input_path, output_path, self.prompt_path,
                    brave_input=brave_path, unlisted_prompt=unlisted_prompt_path,
                )
                rc = self.mod.run(args)
                self.assertEqual(rc, 0)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["gate4b_status"], "LIKELY_NOTABLE")
                # LLM called once (first pass only)
                self.assertEqual(llm_calls["count"], 1)
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # New test 3: second pass produces POSSIBLY_NOTABLE
    # ------------------------------------------------------------------
    def test_second_pass_produces_possibly_notable(self) -> None:
        call_count = {"n": 0}
        first_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": False, "confidence": 0.8, "reasoning": "Different person."},
                {"rank": 2, "about_subject": False, "confidence": 0.7, "reasoning": "Passing mention."},
            ]
        })
        second_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "is_reliable_source": True, "confidence": 0.9, "reasoning": "On subject, reliable."},
                {"rank": 2, "about_subject": True, "is_reliable_source": True, "confidence": 0.85, "reasoning": "On subject, reliable."},
                {"rank": 3, "about_subject": True, "is_reliable_source": True, "confidence": 0.8, "reasoning": "On subject, reliable."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return first_pass_response, {"backend": "claude-cli"}
            return second_pass_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                brave_results = [_make_result(1), _make_result(2, "apnews.com"), _make_result(3, "bbc.com")]
                brave_record = _make_input_record(brave_results=brave_results, event_id="event-abc")
                brave_path = tmp / "brave_coverage.jsonl"
                brave_path.write_text(json.dumps(brave_record) + "\n", encoding="utf-8")
                unlisted_prompt_path = tmp / "gate4b_unlisted.md"
                unlisted_prompt_path.write_text("Unlisted prompt body.", encoding="utf-8")

                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(
                    input_path, output_path, self.prompt_path,
                    brave_input=brave_path, unlisted_prompt=unlisted_prompt_path,
                )
                rc = self.mod.run(args)
                self.assertEqual(rc, 0)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["gate4b_status"], "POSSIBLY_NOTABLE")
                self.assertEqual(rows[0]["second_pass_confirmed_count"], 3)
                self.assertTrue(rows[0]["second_pass_json_parse_ok"])
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # New test 4: second pass requires both about_subject AND is_reliable_source
    # ------------------------------------------------------------------
    def test_second_pass_requires_both_flags_true(self) -> None:
        call_count = {"n": 0}
        first_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": False, "confidence": 0.8, "reasoning": "Different person."},
                {"rank": 2, "about_subject": False, "confidence": 0.7, "reasoning": "Passing mention."},
            ]
        })
        second_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "is_reliable_source": True, "confidence": 0.9, "reasoning": "On subject, reliable."},
                {"rank": 2, "about_subject": True, "is_reliable_source": False, "confidence": 0.5, "reasoning": "On subject but PR wire."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return first_pass_response, {"backend": "claude-cli"}
            return second_pass_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                brave_results = [_make_result(1), _make_result(2, "prnewswire.com")]
                brave_record = _make_input_record(brave_results=brave_results, event_id="event-abc")
                brave_path = tmp / "brave_coverage.jsonl"
                brave_path.write_text(json.dumps(brave_record) + "\n", encoding="utf-8")
                unlisted_prompt_path = tmp / "gate4b_unlisted.md"
                unlisted_prompt_path.write_text("Unlisted prompt body.", encoding="utf-8")

                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(
                    input_path, output_path, self.prompt_path,
                    brave_input=brave_path, unlisted_prompt=unlisted_prompt_path,
                )
                self.mod.run(args)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["second_pass_confirmed_count"], 1)
                self.assertNotEqual(rows[0]["gate4b_status"], "POSSIBLY_NOTABLE")
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # New test 5: second pass excludes original source from candidates
    # ------------------------------------------------------------------
    def test_second_pass_excludes_original_source(self) -> None:
        call_count = {"n": 0}
        entry_title = "Test Person wins award at major event"
        first_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": False, "confidence": 0.6, "reasoning": "Not subject."},
                {"rank": 2, "about_subject": False, "confidence": 0.6, "reasoning": "Not subject."},
            ]
        })
        second_pass_response = json.dumps({
            "results": [
                {"rank": 2, "about_subject": True, "is_reliable_source": True, "confidence": 0.8, "reasoning": "About subject."},
                {"rank": 3, "about_subject": False, "is_reliable_source": True, "confidence": 0.5, "reasoning": "Not about subject."},
            ]
        })
        sent_candidates = {"results": None}

        original_format = self.mod.format_gate4b_unlisted_prompt

        def fake_format_unlisted(prompt_body, subject, source_context, results):
            sent_candidates["results"] = results
            return original_format(prompt_body, subject, source_context, results)

        def fake_llm(*_args, **_kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return first_pass_response, {"backend": "claude-cli"}
            return second_pass_response, {"backend": "claude-cli"}

        orig_llm = self.mod.call_claude_cli
        orig_fmt = self.mod.format_gate4b_unlisted_prompt
        try:
            self.mod.call_claude_cli = fake_llm
            self.mod.format_gate4b_unlisted_prompt = fake_format_unlisted
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                # 3 results in brave: one is original source (title match), 2 are independent
                brave_results = [
                    _make_result(1, domain="bbc.com", title=entry_title),
                    _make_result(2, domain="apnews.com"),
                    _make_result(3, domain="reuters.com"),
                ]
                brave_record = _make_input_record(brave_results=brave_results, entry_title=entry_title, event_id="event-abc")
                brave_path = tmp / "brave_coverage.jsonl"
                brave_path.write_text(json.dumps(brave_record) + "\n", encoding="utf-8")
                unlisted_prompt_path = tmp / "gate4b_unlisted.md"
                unlisted_prompt_path.write_text("Unlisted prompt body.", encoding="utf-8")

                records = [_make_input_record(
                    brave_results=[_make_result(1, domain="apnews.com"), _make_result(2, domain="reuters.com")],
                    entry_title=entry_title,
                )]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(
                    input_path, output_path, self.prompt_path,
                    brave_input=brave_path, unlisted_prompt=unlisted_prompt_path,
                )
                self.mod.run(args)
                # Original source (bbc.com with title match) excluded → only 2 candidates sent
                self.assertIsNotNone(sent_candidates["results"])
                self.assertEqual(len(sent_candidates["results"]), 2)
                rows = _read_output(output_path)
                # second pass LLM confirmed only 1 (rank 2 about_subject=True)
                self.assertEqual(rows[0]["second_pass_confirmed_count"], 1)
        finally:
            self.mod.call_claude_cli = orig_llm
            self.mod.format_gate4b_unlisted_prompt = orig_fmt

    # ------------------------------------------------------------------
    # New test 6: second pass LLM error recorded, status not upgraded
    # ------------------------------------------------------------------
    def test_second_pass_llm_error_recorded(self) -> None:
        call_count = {"n": 0}
        first_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": True, "confidence": 0.8, "reasoning": "About subject."},
                {"rank": 2, "about_subject": False, "confidence": 0.6, "reasoning": "Not subject."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return first_pass_response, {"backend": "claude-cli"}
            raise RuntimeError("second pass connection refused")

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                brave_results = [_make_result(1), _make_result(2, "apnews.com"), _make_result(3, "bbc.com")]
                brave_record = _make_input_record(brave_results=brave_results, event_id="event-abc")
                brave_path = tmp / "brave_coverage.jsonl"
                brave_path.write_text(json.dumps(brave_record) + "\n", encoding="utf-8")
                unlisted_prompt_path = tmp / "gate4b_unlisted.md"
                unlisted_prompt_path.write_text("Unlisted prompt body.", encoding="utf-8")

                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(
                    input_path, output_path, self.prompt_path,
                    brave_input=brave_path, unlisted_prompt=unlisted_prompt_path,
                )
                self.mod.run(args)
                rows = _read_output(output_path)
                self.assertIsNotNone(rows[0]["second_pass_llm_error"])
                self.assertFalse(rows[0]["second_pass_json_parse_ok"])
                self.assertEqual(rows[0]["gate4b_status"], "UNCERTAIN")
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # New test 7: second pass invalid JSON recorded, status not upgraded
    # ------------------------------------------------------------------
    def test_second_pass_invalid_json(self) -> None:
        call_count = {"n": 0}
        first_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": False, "confidence": 0.6, "reasoning": "Not subject."},
                {"rank": 2, "about_subject": False, "confidence": 0.6, "reasoning": "Not subject."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return first_pass_response, {"backend": "claude-cli"}
            return "Sorry, I cannot answer that.", {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                brave_results = [_make_result(1), _make_result(2, "apnews.com"), _make_result(3, "bbc.com")]
                brave_record = _make_input_record(brave_results=brave_results, event_id="event-abc")
                brave_path = tmp / "brave_coverage.jsonl"
                brave_path.write_text(json.dumps(brave_record) + "\n", encoding="utf-8")
                unlisted_prompt_path = tmp / "gate4b_unlisted.md"
                unlisted_prompt_path.write_text("Unlisted prompt body.", encoding="utf-8")

                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")])]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(
                    input_path, output_path, self.prompt_path,
                    brave_input=brave_path, unlisted_prompt=unlisted_prompt_path,
                )
                self.mod.run(args)
                rows = _read_output(output_path)
                self.assertFalse(rows[0]["second_pass_json_parse_ok"])
                self.assertIsNone(rows[0]["second_pass_llm_error"])
                self.assertEqual(rows[0]["gate4b_status"], "NOT_NOTABLE")
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # New test 8: brave_input provided but no matching event_id
    # ------------------------------------------------------------------
    def test_second_pass_no_brave_record_for_event(self) -> None:
        call_count = {"n": 0}
        first_pass_response = json.dumps({
            "results": [
                {"rank": 1, "about_subject": False, "confidence": 0.6, "reasoning": "Not subject."},
                {"rank": 2, "about_subject": False, "confidence": 0.6, "reasoning": "Not subject."},
            ]
        })

        def fake_llm(*_args, **_kwargs):
            call_count["n"] += 1
            return first_pass_response, {"backend": "claude-cli"}

        orig = self.mod.call_claude_cli
        try:
            self.mod.call_claude_cli = fake_llm
            with tempfile.TemporaryDirectory() as td:
                tmp = Path(td)
                # brave record has a DIFFERENT event_id
                brave_record = _make_input_record(brave_results=[_make_result(1)], event_id="different-event")
                brave_path = tmp / "brave_coverage.jsonl"
                brave_path.write_text(json.dumps(brave_record) + "\n", encoding="utf-8")
                unlisted_prompt_path = tmp / "gate4b_unlisted.md"
                unlisted_prompt_path.write_text("Unlisted prompt body.", encoding="utf-8")

                records = [_make_input_record(brave_results=[_make_result(1), _make_result(2, "apnews.com")], event_id="event-abc")]
                input_path = _write_input(tmp, records)
                output_path = tmp / "out.jsonl"
                args = _make_args(
                    input_path, output_path, self.prompt_path,
                    brave_input=brave_path, unlisted_prompt=unlisted_prompt_path,
                )
                self.mod.run(args)
                rows = _read_output(output_path)
                self.assertEqual(rows[0]["second_pass_results_sent"], [])
                self.assertIsNone(rows[0]["second_pass_json_parse_ok"])
                # Status is NOT_NOTABLE from first pass (not upgraded)
                self.assertEqual(rows[0]["gate4b_status"], "NOT_NOTABLE")
                # LLM called only once (first pass only)
                self.assertEqual(call_count["n"], 1)
        finally:
            self.mod.call_claude_cli = orig

    # ------------------------------------------------------------------
    # Unit tests: gate4b_unlisted_output_schema
    # ------------------------------------------------------------------

    def test_unlisted_schema_valid(self) -> None:
        import jsonschema

        schema = self.mod.gate4b_unlisted_output_schema()
        valid = {
            "results": [
                {
                    "rank": 1,
                    "about_subject": True,
                    "is_reliable_source": True,
                    "confidence": 0.85,
                    "reasoning": "Article is clearly about the subject.",
                },
            ]
        }
        jsonschema.validate(valid, schema)

    def test_unlisted_schema_missing_field(self) -> None:
        import jsonschema

        schema = self.mod.gate4b_unlisted_output_schema()
        invalid = {
            "results": [
                {
                    "rank": 1,
                    "about_subject": True,
                    # missing is_reliable_source
                    "confidence": 0.85,
                    "reasoning": "Some reasoning.",
                }
            ]
        }
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(invalid, schema)

    def test_format_unlisted_prompt_header(self) -> None:
        prompt_body = "You are the unlisted gate."
        ctx = {
            "entry_title": "Jane Smith wins award",
            "summary": "She won.",
            "source": "BBC News",
            "publication_date": "2026-01-01",
        }
        results = [_make_result(1, domain="somesite.com", title="Jane Smith wins award")]
        out = self.mod.format_gate4b_unlisted_prompt(
            prompt_body=prompt_body,
            subject="Jane Smith",
            source_context=ctx,
            results=results,
        )
        self.assertIn("All news results to evaluate:", out)
        self.assertIn("Subject: Jane Smith", out)
        self.assertIn("somesite.com", out)


if __name__ == "__main__":
    unittest.main()
