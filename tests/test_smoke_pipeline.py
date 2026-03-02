#!/usr/bin/env python3
"""End-to-end smoke test for run_pipeline.py orchestrator.

Uses a temporary state directory and a mock 'claude' CLI
(tests/mock_bin/claude) to exercise the full pipeline without making real
API calls.  MW and Brave responses are served from pre-populated cache files.

Run with:
    python3 -m unittest tests.test_smoke_pipeline -v
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MOCK_BIN = PROJECT_ROOT / "tests" / "mock_bin"


# ---------------------------------------------------------------------------
# Cache-key helpers (must mirror the real scripts exactly)
# ---------------------------------------------------------------------------

def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _mw_search_key(query: str, srlimit: int = 10) -> str:
    return _sha256(f"search:{query}:{srlimit}")


def _mw_page_key(title: str) -> str:
    return _sha256(f"page:{title}")


def _brave_key(query: str, count: int = 20, offset: int = 0) -> str:
    return _sha256(f"brave_news:{query}:{count}:{offset}")


# ---------------------------------------------------------------------------
# Fixture data
# ---------------------------------------------------------------------------

FIXTURE_EVENTS = [
    {
        "event_id": "evt-thatcher",
        "entry_title": "Margaret Thatcher receives honorary degree",
        "summary": "Former prime minister Margaret Thatcher receives honorary degree.",
        "source_feed_title": "BBC News",
        "published_at_utc": "2025-01-01T10:00:00Z",
        "feed_priority": 1,
    },
    {
        "event_id": "evt-worthington",
        "entry_title": "James Worthington named professor of economics",
        "summary": "James Worthington has been named professor of economics at Oxford.",
        "source_feed_title": "Guardian",
        "published_at_utc": "2025-01-01T11:00:00Z",
        "feed_priority": 1,
    },
    {
        "event_id": "evt-osei",
        "entry_title": "Clara Osei-Mensah wins prestigious science award",
        "summary": "Clara Osei-Mensah, a Ghanaian-British scientist, wins the annual prize.",
        "source_feed_title": "Science Daily",
        "published_at_utc": "2025-01-01T12:00:00Z",
        "feed_priority": 1,
    },
    {
        "event_id": "evt-fielding",
        "entry_title": "Bob Fielding loses council seat in by-election",
        "summary": "Bob Fielding, a local councillor, loses his seat in the by-election.",
        "source_feed_title": "Local News",
        "published_at_utc": "2025-01-01T13:00:00Z",
        "feed_priority": 2,
    },
    {
        "event_id": "evt-product",
        "entry_title": "BuyNow Pro Software launches version 2",
        "summary": "BuyNow Pro Software has released v2 of its CRM platform.",
        "source_feed_title": "Tech News",
        "published_at_utc": "2025-01-01T14:00:00Z",
        "feed_priority": 2,
    },
]


# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------

def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return records


def _write_cache(cache_dir: Path, key: str, payload: dict) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / f"{key}.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# MW cache setup
# ---------------------------------------------------------------------------

def _setup_mw_cache(cache_dir: Path) -> None:
    """Pre-populate MW cache so det_mw_candidates.py never makes network calls."""

    def search(query: str, results: list[dict]) -> None:
        _write_cache(
            cache_dir,
            _mw_search_key(query),
            {"query": {"search": results}, "continue": None},
        )

    def page(title: str, page_obj: dict) -> None:
        _write_cache(
            cache_dir,
            _mw_page_key(title),
            {"query": {"pages": {str(page_obj["pageid"]): page_obj}}},
        )

    # James Worthington: first variant returns a match, others are empty.
    # det_mw_candidates expands "James" → ["Jim", "Jimmy"] via FORMAL_TO_NICKNAMES.
    search("James Worthington", [
        {"title": "James Worthington", "pageid": 9001, "snippet": "British academic"},
    ])
    search("Jim Worthington", [])
    search("Jimmy Worthington", [])

    page("James Worthington", {
        "pageid": 9001,
        "ns": 0,
        "title": "James Worthington",
        "fullurl": "https://en.wikipedia.org/wiki/James_Worthington",
        "description": "British academic and professor",
        "extract": "James Worthington is a British academic and professor of economics.",
        "categories": [
            {"ns": 14, "title": "Category:1970 births"},
            {"ns": 14, "title": "Category:Living people"},
            {"ns": 14, "title": "Category:Oxford academics"},
        ],
        "pageprops": {},
    })

    # Clara Osei-Mensah: no Wikipedia match.
    search("Clara Osei-Mensah", [])

    # Bob Fielding: no Wikipedia match.
    # det_mw_candidates expands "Bob" → "Robert" via NICKNAME_MAP.
    search("Bob Fielding", [])
    search("Robert Fielding", [])


# ---------------------------------------------------------------------------
# Brave cache setup
# ---------------------------------------------------------------------------

def _setup_brave_cache(cache_dir: Path) -> None:
    """Pre-populate Brave cache so det_brave_coverage.py never calls the API."""

    def brave(query: str, offset: int, results: list[dict]) -> None:
        _write_cache(cache_dir, _brave_key(query, offset=offset), {"results": results})

    # Clara Osei-Mensah: 2 results from reliable domains + 1 unreliable.
    # build_queries wraps the subject name in quotes: '"Clara Osei-Mensah"'
    clara_q = '"Clara Osei-Mensah"'
    brave(clara_q, 0, [
        {
            "title": "Clara Osei-Mensah wins science prize",
            "url": "https://www.bbc.com/news/science-clara-osei-mensah",
            "description": "Osei-Mensah awarded the annual prize for her research.",
            "age": "1 hour ago",
            "page_age": None,
        },
        {
            "title": "Ghanaian-British scientist honoured at ceremony",
            "url": "https://www.theguardian.com/science/2025/jan/01/clara-osei-mensah",
            "description": "Guardian report on the award ceremony in London.",
            "age": "2 hours ago",
            "page_age": None,
        },
        {
            "title": "Science awards 2025 annual ceremony",
            "url": "https://example-science-news.example.com/awards-2025",
            "description": "Annual science awards ceremony held in London.",
            "age": "1 day ago",
            "page_age": None,
        },
    ])
    brave(clara_q, 1, [])  # Second page: empty

    # Bob Fielding: all results from unreliable domains → gate4_filter keeps none.
    bob_q = '"Bob Fielding"'
    brave(bob_q, 0, [
        {
            "title": "Council by-election results",
            "url": "https://localcouncil.example.com/results",
            "description": "By-election results for district council.",
            "age": "1 day ago",
            "page_age": None,
        },
        {
            "title": "Fielding announces retirement from council",
            "url": "https://townsnews.example.com/bob-fielding-retires",
            "description": "Local councillor retires after losing by-election.",
            "age": "2 days ago",
            "page_age": None,
        },
        {
            "title": "New council member takes seat",
            "url": "https://districtgazette.example.com/council-update",
            "description": "District council seat filled after by-election.",
            "age": "3 days ago",
            "page_age": None,
        },
    ])
    brave(bob_q, 1, [])  # Second page: empty


# ---------------------------------------------------------------------------
# Base test class
# ---------------------------------------------------------------------------

class SmokeTestBase(unittest.TestCase):
    """Sets up an isolated temp state directory and tears it down after each test."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp(prefix="smoke_test_")
        self.state = Path(self._tmpdir) / "state"
        self.output = Path(self._tmpdir) / "output"
        self.state.mkdir(parents=True)
        self.output.mkdir(parents=True)

        # Fixture input
        _write_jsonl(self.state / "prefilter_pass.jsonl", FIXTURE_EVENTS)
        (self.state / "wiki_known_pages.json").write_text(
            json.dumps({"version": 1, "entries": {}}), encoding="utf-8"
        )

        # Pre-populate caches
        _setup_mw_cache(self.state / "mw_cache")
        _setup_brave_cache(self.state / "brave_cache")

    def tearDown(self) -> None:
        shutil.rmtree(self._tmpdir, ignore_errors=True)

    def _run_pipeline(
        self,
        scenario: str = "happy_path",
        extra_args: list[str] | None = None,
    ) -> subprocess.CompletedProcess:
        """Run run_pipeline.py with smoke-test settings."""
        env = {
            **os.environ,
            "PATH": f"{MOCK_BIN}:{os.environ.get('PATH', '')}",
            "SMOKE_MOCK_SCENARIO": scenario,
            "BRAVE_API_KEY": "smoke_test_dummy_key",
        }
        env.pop("CLAUDECODE", None)

        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "run_pipeline.py"),
            "--from-gate", "gate1",
            "--backend-gate1", "claude-cli",
            "--backend-gate3", "claude-cli",
            "--backend-gate4b", "claude-cli",
            "--model-gate1", "smoke-mock",
            "--model-gate3", "smoke-mock",
            "--model-gate4b", "smoke-mock",
            "--state-dir", str(self.state),
            "--output-dir", str(self.output),
            "--gate1-budget", "10",
        ]
        if extra_args:
            cmd.extend(extra_args)

        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            env=env,
            timeout=120,
        )


# ---------------------------------------------------------------------------
# Happy-path smoke test
# ---------------------------------------------------------------------------

class TestSmokePipelineHappyPath(SmokeTestBase):
    """Full pipeline run with the happy_path mock scenario."""

    @classmethod
    def setUpClass(cls) -> None:
        # We run once and share results across all test methods for speed.
        # Each test method reads from cls.state / cls.output / cls.proc.
        cls._shared_tmpdir = tempfile.mkdtemp(prefix="smoke_happy_")
        cls.state = Path(cls._shared_tmpdir) / "state"
        cls.output = Path(cls._shared_tmpdir) / "output"
        cls.state.mkdir(parents=True)
        cls.output.mkdir(parents=True)

        _write_jsonl(cls.state / "prefilter_pass.jsonl", FIXTURE_EVENTS)
        (cls.state / "wiki_known_pages.json").write_text(
            json.dumps({"version": 1, "entries": {}}), encoding="utf-8"
        )
        _setup_mw_cache(cls.state / "mw_cache")
        _setup_brave_cache(cls.state / "brave_cache")

        env = {
            **os.environ,
            "PATH": f"{MOCK_BIN}:{os.environ.get('PATH', '')}",
            "SMOKE_MOCK_SCENARIO": "happy_path",
            "BRAVE_API_KEY": "smoke_test_dummy_key",
        }
        env.pop("CLAUDECODE", None)

        cls.proc = subprocess.run(
            [
                sys.executable,
                str(PROJECT_ROOT / "run_pipeline.py"),
                "--from-gate", "gate1",
                "--backend-gate1", "claude-cli",
                "--backend-gate3", "claude-cli",
                "--backend-gate4b", "claude-cli",
                "--model-gate1", "smoke-mock",
                "--model-gate3", "smoke-mock",
                "--model-gate4b", "smoke-mock",
                "--state-dir", str(cls.state),
                "--output-dir", str(cls.output),
                "--gate1-budget", "10",
            ],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            env=env,
            timeout=120,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls._shared_tmpdir, ignore_errors=True)

    # Override SmokeTestBase.setUp/tearDown so they don't create a second tmpdir
    def setUp(self) -> None:
        pass

    def tearDown(self) -> None:
        pass

    def _dump_on_failure(self) -> str:
        """Return debug output for assertion failure messages."""
        return (
            f"\nSTDOUT:\n{self.proc.stdout[-3000:]}"
            f"\nSTDERR:\n{self.proc.stderr[-2000:]}"
        )

    def test_pipeline_exits_zero(self) -> None:
        self.assertEqual(
            self.proc.returncode, 0,
            msg=f"Pipeline exited {self.proc.returncode}" + self._dump_on_failure(),
        )

    def test_gate1_results_written(self) -> None:
        records = _read_jsonl(self.state / "gate1_llm_results.jsonl")
        self.assertEqual(len(records), 5, msg=self._dump_on_failure())

    def test_gate1_thatcher_skip_globally_known(self) -> None:
        records = _read_jsonl(self.state / "gate1_llm_results.jsonl")
        thatcher = next(
            (r for r in records if r.get("event_id") == "evt-thatcher"), None
        )
        self.assertIsNotNone(thatcher, msg="No record for evt-thatcher")
        self.assertEqual(thatcher.get("gate1_decision"), "SKIP_GLOBALLY_KNOWN")

    def test_gate1_worthington_strong_pass(self) -> None:
        records = _read_jsonl(self.state / "gate1_llm_results.jsonl")
        rec = next((r for r in records if r.get("event_id") == "evt-worthington"), None)
        self.assertIsNotNone(rec)
        self.assertEqual(rec.get("gate1_decision"), "STRONG_PASS")

    def test_gate1_index_writes_thatcher(self) -> None:
        known = json.loads(
            (self.state / "wiki_known_pages.json").read_text(encoding="utf-8")
        )
        # Normalized "margaret thatcher" should be present
        self.assertIn("margaret thatcher", known.get("entries", {}))

    def test_mw_candidates_written(self) -> None:
        records = _read_jsonl(self.state / "wiki_candidates.jsonl")
        # Only STRONG_PASS + WEAK_PASS events go through: Worthington, Osei, Fielding
        self.assertEqual(len(records), 3, msg=self._dump_on_failure())

    def test_mw_worthington_has_candidates(self) -> None:
        records = _read_jsonl(self.state / "wiki_candidates.jsonl")
        worthington = next(
            (r for r in records if r.get("subject_name") == "James Worthington"), None
        )
        self.assertIsNotNone(worthington)
        candidates = (worthington.get("mw_search") or {}).get("results") or []
        self.assertGreater(len(candidates), 0, msg="James Worthington should have MW candidates")

    def test_gate2_passes_all_candidates(self) -> None:
        # gate2 always passes everything — wiki_candidates_pass == wiki_candidates
        pass_records = _read_jsonl(self.state / "wiki_candidates_pass.jsonl")
        self.assertEqual(len(pass_records), 3, msg=self._dump_on_failure())

    def test_gate3_results_written(self) -> None:
        records = _read_jsonl(self.state / "gate3_llm_results.jsonl")
        self.assertEqual(len(records), 3, msg=self._dump_on_failure())

    def test_gate3_worthington_has_page(self) -> None:
        records = _read_jsonl(self.state / "gate3_llm_results.jsonl")
        rec = next(
            (r for r in records if r.get("subject_name") == "James Worthington"), None
        )
        self.assertIsNotNone(rec)
        self.assertEqual(rec.get("gate3_status"), "HAS_PAGE")

    def test_gate3_osei_missing(self) -> None:
        records = _read_jsonl(self.state / "gate3_llm_results.jsonl")
        rec = next(
            (r for r in records if "Osei" in (r.get("subject_name") or "")), None
        )
        self.assertIsNotNone(rec)
        self.assertEqual(rec.get("gate3_status"), "MISSING")

    def test_gate3_index_writes_worthington(self) -> None:
        known = json.loads(
            (self.state / "wiki_known_pages.json").read_text(encoding="utf-8")
        )
        self.assertIn("james worthington", known.get("entries", {}))

    def test_brave_coverage_written(self) -> None:
        records = _read_jsonl(self.state / "brave_coverage.jsonl")
        # Only MISSING/UNCERTAIN go to brave: Clara and Bob
        self.assertEqual(len(records), 2, msg=self._dump_on_failure())

    def test_gate4_filter_written(self) -> None:
        records = _read_jsonl(self.state / "gate4_reliable_coverage.jsonl")
        self.assertEqual(len(records), 2, msg=self._dump_on_failure())

    def test_gate4_filter_clara_has_reliable_results(self) -> None:
        records = _read_jsonl(self.state / "gate4_reliable_coverage.jsonl")
        clara = next(
            (r for r in records if "Osei" in (r.get("subject_name") or "")), None
        )
        self.assertIsNotNone(clara)
        self.assertGreaterEqual(
            clara.get("brave_result_count", 0), 2,
            msg="Clara should have ≥2 reliable-domain results",
        )

    def test_gate4b_results_written(self) -> None:
        records = _read_jsonl(self.state / "gate4b_llm_results.jsonl")
        self.assertGreater(len(records), 0, msg=self._dump_on_failure())

    def test_gate4b_osei_likely_notable(self) -> None:
        records = _read_jsonl(self.state / "gate4b_llm_results.jsonl")
        clara = next(
            (r for r in records if "Osei" in (r.get("subject_name") or "")), None
        )
        self.assertIsNotNone(clara, msg="No gate4b record for Clara Osei-Mensah")
        self.assertEqual(clara.get("gate4b_status"), "LIKELY_NOTABLE")

    def test_gate4b_bob_skipped(self) -> None:
        records = _read_jsonl(self.state / "gate4b_llm_results.jsonl")
        bob = next(
            (r for r in records if "Fielding" in (r.get("subject_name") or "")), None
        )
        self.assertIsNotNone(bob)
        self.assertEqual(bob.get("gate4b_status"), "SKIPPED")

    def test_output_summary_written(self) -> None:
        runs_dir = self.output / "runs"
        summaries = list(runs_dir.glob("*_summary.json"))
        self.assertEqual(len(summaries), 1, msg=f"Expected one summary file in {runs_dir}")
        data = json.loads(summaries[0].read_text(encoding="utf-8"))
        likely = [e["subject_name"] for e in data.get("likely_notable", [])]
        self.assertTrue(
            any("Osei" in name for name in likely),
            msg=f"Clara Osei-Mensah not in likely_notable: {likely}",
        )

    def test_manifest_written(self) -> None:
        runs_dir = self.state / "runs"
        manifests = list(runs_dir.glob("*.json"))
        self.assertGreater(len(manifests), 0, msg="No manifest files written")
        data = json.loads(manifests[0].read_text(encoding="utf-8"))
        self.assertIn("stages", data)
        self.assertIn("run_id", data)
        self.assertEqual(data.get("finished_at") is not None, True)


# ---------------------------------------------------------------------------
# Gate1 malformed: retry mechanism
# ---------------------------------------------------------------------------

class TestSmokePipelineGate1Malformed(SmokeTestBase):
    """gate1_malformed scenario: gate1 returns garbage JSON; retry is triggered."""

    def test_retry_records_present(self) -> None:
        proc = self._run_pipeline(scenario="gate1_malformed")

        gate1_records = _read_jsonl(self.state / "gate1_llm_results.jsonl")
        # Initial pass: 5 records (all json_parse_ok=False)
        # Retry pass (--retry-parse-failures): up to 5 more records
        self.assertGreaterEqual(
            len(gate1_records), 5,
            msg=f"Expected ≥5 gate1 records; got {len(gate1_records)}\n"
                f"STDOUT: {proc.stdout[-2000:]}\nSTDERR: {proc.stderr[-1000:]}",
        )

        malformed = [r for r in gate1_records if not r.get("json_parse_ok")]
        self.assertGreater(
            len(malformed), 0,
            msg="Expected at least one record with json_parse_ok=False",
        )

    def test_manifest_written_on_failure(self) -> None:
        self._run_pipeline(scenario="gate1_malformed")
        runs_dir = self.state / "runs"
        manifests = list(runs_dir.glob("*.json"))
        self.assertGreater(len(manifests), 0, msg="Manifest not written after failure")


# ---------------------------------------------------------------------------
# Gate3 all-HAS_PAGE: only affects subjects with MW candidates (James);
# Clara + Bob always short-circuit to MISSING since they have no candidates.
# ---------------------------------------------------------------------------

class TestSmokePipelineGate3AllHasPage(SmokeTestBase):
    """gate3_all_has_page: mock returns HAS_PAGE for any subject with candidates."""

    def test_pipeline_succeeds(self) -> None:
        proc = self._run_pipeline(scenario="gate3_all_has_page")
        self.assertEqual(
            proc.returncode, 0,
            msg=(
                f"Pipeline should succeed; got RC={proc.returncode}\n"
                f"STDOUT: {proc.stdout[-2000:]}\nSTDERR: {proc.stderr[-1000:]}"
            ),
        )

    def test_james_still_has_page(self) -> None:
        """James gets HAS_PAGE (from LLM mock). Clara/Bob short-circuit to MISSING."""
        self._run_pipeline(scenario="gate3_all_has_page")
        records = _read_jsonl(self.state / "gate3_llm_results.jsonl")
        james = next(
            (r for r in records if r.get("subject_name") == "James Worthington"), None
        )
        self.assertIsNotNone(james)
        self.assertEqual(james.get("gate3_status"), "HAS_PAGE")

    def test_clara_still_missing(self) -> None:
        """Clara has no MW candidates → gate3 short-circuits to MISSING regardless of scenario."""
        self._run_pipeline(scenario="gate3_all_has_page")
        records = _read_jsonl(self.state / "gate3_llm_results.jsonl")
        clara = next(
            (r for r in records if "Osei" in (r.get("subject_name") or "")), None
        )
        self.assertIsNotNone(clara)
        self.assertEqual(clara.get("gate3_status"), "MISSING")


# ---------------------------------------------------------------------------
# Dry-run: pipeline prints commands but writes nothing
# ---------------------------------------------------------------------------

class TestSmokePipelineDryRun(SmokeTestBase):
    """--dry-run flag should exit 0 without writing any state files."""

    def test_dry_run_exits_zero(self) -> None:
        proc = self._run_pipeline(extra_args=["--dry-run"])
        self.assertEqual(
            proc.returncode, 0,
            msg=f"Dry run should exit 0\nSTDOUT: {proc.stdout[-1000:]}\nSTDERR: {proc.stderr[-500:]}",
        )

    def test_dry_run_prints_commands(self) -> None:
        proc = self._run_pipeline(extra_args=["--dry-run"])
        self.assertIn("[DRY-RUN]", proc.stdout)

    def test_dry_run_writes_no_gate1_results(self) -> None:
        self._run_pipeline(extra_args=["--dry-run"])
        gate1 = self.state / "gate1_llm_results.jsonl"
        self.assertFalse(
            gate1.exists(),
            msg="Dry run should not create gate1_llm_results.jsonl",
        )


if __name__ == "__main__":
    unittest.main()
