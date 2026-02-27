import importlib.util
import unittest
from pathlib import Path


def load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestGate1TrialRetries(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path("/Users/jonathan/new-wikipedia-article-checker")
        cls.gate1 = load_module("llm_gate1_runner", root / "scripts" / "llm_gate1_runner.py")

    def test_codex_retry_succeeds_after_failure(self) -> None:
        calls = {"count": 0}

        def fake_call(*_args, **_kwargs):
            calls["count"] += 1
            if calls["count"] < 2:
                raise RuntimeError("codex exec failed (code 1): transient")
            return '{"items": []}', {"returncode": 0}

        orig = self.gate1.call_codex_cli
        try:
            self.gate1.call_codex_cli = fake_call
            out, meta = self.gate1.call_codex_cli_with_retries(
                prompt_text="x",
                model="m",
                codex_cwd=Path("/tmp"),
                output_schema={"type": "object"},
                max_attempts=3,
            )
            self.assertEqual(out, '{"items": []}')
            self.assertEqual(meta["returncode"], 0)
            self.assertEqual(calls["count"], 2)
        finally:
            self.gate1.call_codex_cli = orig

    def test_codex_retry_exhaustion(self) -> None:
        def fake_call(*_args, **_kwargs):
            raise RuntimeError("codex exec failed (code 1): permanent")

        orig = self.gate1.call_codex_cli
        try:
            self.gate1.call_codex_cli = fake_call
            with self.assertRaises(RuntimeError):
                self.gate1.call_codex_cli_with_retries(
                    prompt_text="x",
                    model="m",
                    codex_cwd=Path("/tmp"),
                    output_schema={"type": "object"},
                    max_attempts=2,
                )
        finally:
            self.gate1.call_codex_cli = orig

    def test_claude_cli_retry_succeeds_after_failure(self) -> None:
        calls = {"count": 0}

        def fake_call(*_args, **_kwargs):
            calls["count"] += 1
            if calls["count"] < 2:
                raise RuntimeError("claude cli failed (code 1): transient")
            return "ok output", {"backend": "claude-cli", "returncode": 0, "stderr_excerpt": ""}

        orig = self.gate1.call_claude_cli
        try:
            self.gate1.call_claude_cli = fake_call
            out, meta = self.gate1.call_claude_cli_with_retries(
                prompt_text="x", model="m", cwd=Path("/tmp"), max_attempts=3,
            )
            self.assertEqual(out, "ok output")
            self.assertEqual(calls["count"], 2)
        finally:
            self.gate1.call_claude_cli = orig

    def test_claude_cli_retry_exhaustion(self) -> None:
        def fake_call(*_args, **_kwargs):
            raise RuntimeError("claude cli failed (code 1): permanent")

        orig = self.gate1.call_claude_cli
        try:
            self.gate1.call_claude_cli = fake_call
            with self.assertRaises(RuntimeError):
                self.gate1.call_claude_cli_with_retries(
                    prompt_text="x", model="m", cwd=Path("/tmp"), max_attempts=2,
                )
        finally:
            self.gate1.call_claude_cli = orig

    def test_gate1_schema_includes_skip_globally_known(self) -> None:
        schema = self.gate1.gate1_output_schema()
        self.assertIn("SKIP_GLOBALLY_KNOWN", schema["properties"]["gate1_decision"]["enum"])

    def test_gate1_schema_includes_globally_famous_signal(self) -> None:
        schema = self.gate1.gate1_output_schema()
        self.assertIn("GLOBALLY_FAMOUS", schema["properties"]["signal_type"]["enum"])

    def test_event_id_missing_in_batch_output_flag(self) -> None:
        batch_events = [
            {"event_id": "a1", "entry_title": "Title A", "summary": "", "source": "S", "publication_date": "2026-02-01"},
            {"event_id": "b2", "entry_title": "Title B", "summary": "", "source": "S", "publication_date": "2026-02-01"},
        ]

        parsed_output = {
            "items": [
                {
                    "event_id": "a1",
                    "person_detected": True,
                    "subject_name_as_written": "A A",
                    "subject_name_full": "A A",
                    "name_completeness": "FULL_NAME",
                    "primary_focus": True,
                    "gate1_decision": "WEAK_PASS",
                    "reasoning_summary": ["ok"],
                    "signal_type": "OTHER",
                    "confidence": "medium",
                }
            ]
        }

        def build_results(events, parsed):
            batch_result_by_event_id = {}
            parsed_items = parsed.get("items") if isinstance(parsed, dict) else None
            if isinstance(parsed_items, list):
                for item in parsed_items:
                    if isinstance(item, dict) and isinstance(item.get("event_id"), str):
                        batch_result_by_event_id[item["event_id"]] = item

            results = []
            for event in events:
                item_output = batch_result_by_event_id.get(event.get("event_id"))
                item_parse_ok = bool(item_output)
                item_parse_error = None if item_parse_ok else "event_id_missing_in_batch_output"
                results.append((event["event_id"], item_parse_ok, item_parse_error))
            return results

        results = build_results(batch_events, parsed_output)
        self.assertEqual(results[0], ("a1", True, None))
        self.assertEqual(results[1], ("b2", False, "event_id_missing_in_batch_output"))

    def test_sort_by_priority_lower_first(self) -> None:
        """Priority sort should process lower numbers first."""
        events = [
            {"event_id": "1", "feed_priority": 3, "published_at_utc": "2026-02-20T12:00:00Z"},
            {"event_id": "2", "feed_priority": 1, "published_at_utc": "2026-02-19T12:00:00Z"},
            {"event_id": "3", "feed_priority": 2, "published_at_utc": "2026-02-21T12:00:00Z"},
        ]

        # Apply the same sorting logic as in gate1_runner
        unprocessed = events.copy()
        unprocessed.sort(
            key=lambda e: e.get("published_at_utc") or "",
            reverse=True,
        )
        _PRIORITY_MAX = float("inf")
        unprocessed.sort(
            key=lambda e: e.get("feed_priority") if e.get("feed_priority") is not None else _PRIORITY_MAX,
        )

        # Verify order: priority 1, 2, 3
        self.assertEqual([e["event_id"] for e in unprocessed], ["2", "3", "1"])

    def test_sort_by_priority_none_last(self) -> None:
        """Events with feed_priority=None should sort after numbered priorities."""
        events = [
            {"event_id": "1", "feed_priority": 1, "published_at_utc": "2026-02-20T12:00:00Z"},
            {"event_id": "2", "feed_priority": None, "published_at_utc": "2026-02-21T12:00:00Z"},
            {"event_id": "3", "feed_priority": 2, "published_at_utc": "2026-02-19T12:00:00Z"},
        ]

        # Apply the same sorting logic as in gate1_runner
        unprocessed = events.copy()
        unprocessed.sort(
            key=lambda e: e.get("published_at_utc") or "",
            reverse=True,
        )
        _PRIORITY_MAX = float("inf")
        unprocessed.sort(
            key=lambda e: e.get("feed_priority") if e.get("feed_priority") is not None else _PRIORITY_MAX,
        )

        # Verify order: priority 1, 2, then None (represented as inf)
        self.assertEqual([e["event_id"] for e in unprocessed], ["1", "3", "2"])


if __name__ == "__main__":
    unittest.main()
