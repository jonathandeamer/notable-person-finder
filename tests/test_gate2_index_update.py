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


class TestGate2IndexUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        root = Path("/Users/jonathan/new-wikipedia-article-checker")
        cls.mod = load_module(
            "det_gate2_index_update", root / "scripts" / "det_gate2_index_update.py"
        )

    def test_updates_index_on_has_page(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td)
            input_path = tmp / "gate2.jsonl"
            output_path = tmp / "known.json"

            rows = [
                {
                    "event_id": "1",
                    "gate2_status": "HAS_PAGE",
                    "subject_name_full": "Ada Lovelace",
                    "matched_page": {
                        "pageid": 123,
                        "title": "Ada Lovelace",
                        "fullurl": "https://en.wikipedia.org/wiki/Ada_Lovelace",
                    },
                },
                {"event_id": "2", "gate2_status": "MISSING"},
            ]
            with input_path.open("w", encoding="utf-8") as f:
                for row in rows:
                    f.write(json.dumps(row) + "\n")

            rc = self.mod.run_update(
                input_path=input_path,
                known_pages_path=output_path,
                gate2_run_id="run-1",
                overwrite=False,
            )
            self.assertEqual(rc, 0)

            payload = json.loads(output_path.read_text(encoding="utf-8"))
            entries = payload.get("entries")
            self.assertIn("ada lovelace", entries)
            self.assertEqual(entries["ada lovelace"]["pageid"], 123)


if __name__ == "__main__":
    unittest.main()
